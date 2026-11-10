"""公众号采集的排队入口。

新增公众号、手动刷新、定时采集统一走 :class:`core.queue.TaskQueue`（单 worker 串行执行），
用来解决两个问题：

1. 同时添加多个公众号时，每个公众号的首次历史文章拉取都能排到队，不会因为
   并发抢微信接口而被吞掉；
2. 入队只传 ``mp_id`` 等可序列化参数，采集器在 worker 线程内创建。这样既不用把
   ORM 对象跨线程传递，也不会在请求线程里提前建立（排队久了会过期的）采集会话。

模块另外维护每个公众号最近一次采集的状态快照，供前端轮询展示。
"""

import threading
from datetime import datetime
from typing import Any, Dict, List, Optional

from core.config import cfg
from core.print import print_error, print_info, print_success, print_warning

STATUS_QUEUED = "queued"
STATUS_RUNNING = "running"
STATUS_SUCCESS = "success"
STATUS_EMPTY = "empty"
STATUS_FAILED = "failed"

# 单次采集最多翻多少页（新增公众号的历史文章拉取深度）
DEFAULT_MAX_PAGE = 5
# 同步刷新接口的默认等待时间（秒）。前端 axios 超时是 100s，这里留出余量：
# 超过该时间就先把「已排队」的状态返回给前端，采集仍在队列里继续执行。
DEFAULT_WAIT_TIMEOUT = 90
# 采集任务失败重试次数
DEFAULT_MAX_RETRIES = 2
# 状态快照最多保留多少个公众号
_MAX_RECORDS = 500

FEATURED_MP_ID = "MP_WXS_FEATURED_ARTICLES"


class MpSyncError(RuntimeError):
    """采集前置条件不满足等确定性错误，重试没有意义。"""

    def __init__(self, message: str, retriable: bool = False):
        super().__init__(message)
        self.retriable = retriable


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


class _SyncRegistry:
    """公众号采集状态快照 + 等待者的线程安全登记表。"""

    def __init__(self):
        self._lock = threading.Lock()
        self._records: Dict[str, Dict[str, Any]] = {}
        self._waiters: Dict[str, threading.Event] = {}

    def waiter(self, mp_id: str) -> threading.Event:
        """获取（或创建）某个公众号的完成事件。"""
        with self._lock:
            event = self._waiters.get(mp_id)
            if event is None:
                event = threading.Event()
                self._waiters[mp_id] = event
            return event

    def release_waiter(self, mp_id: str, event: threading.Event) -> None:
        with self._lock:
            if self._waiters.get(mp_id) is event:
                self._waiters.pop(mp_id, None)

    def mark(self, mp_id: str, **fields: Any) -> Dict[str, Any]:
        """更新并返回某个公众号的状态快照。"""
        with self._lock:
            record = self._records.get(mp_id)
            if record is None:
                record = {
                    "mp_id": mp_id,
                    "mp_name": "",
                    "status": STATUS_QUEUED,
                    "reason": "",
                    "article_count": 0,
                    "error": "",
                    "attempts": 0,
                    "queued_at": "",
                    "started_at": "",
                    "finished_at": "",
                }
                self._records[mp_id] = record
                self._trim_locked()
            record.update(fields)
            record["updated_at"] = _now()
            return dict(record)

    def _trim_locked(self) -> None:
        if len(self._records) <= _MAX_RECORDS:
            return
        # 只清理已完成的最旧记录，保留仍在排队/执行中的
        finished = [
            (record.get("updated_at") or "", mp_id)
            for mp_id, record in self._records.items()
            if record.get("status") not in (STATUS_QUEUED, STATUS_RUNNING)
        ]
        finished.sort()
        for _, mp_id in finished[: len(self._records) - _MAX_RECORDS]:
            self._records.pop(mp_id, None)

    def get(self, mp_id: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            record = self._records.get(mp_id)
            return dict(record) if record else None

    def all(self) -> List[Dict[str, Any]]:
        with self._lock:
            records = [dict(record) for record in self._records.values()]
        records.sort(key=lambda item: item.get("updated_at") or "", reverse=True)
        return records

    def is_active(self, mp_id: str) -> bool:
        record = self.get(mp_id)
        return bool(record and record.get("status") in (STATUS_QUEUED, STATUS_RUNNING))

    def finish(self, mp_id: str, **fields: Any) -> Dict[str, Any]:
        """写入终态并唤醒等待者。"""
        record = self.mark(mp_id, **fields)
        with self._lock:
            event = self._waiters.get(mp_id)
        if event is not None:
            event.set()
        return record


_registry = _SyncRegistry()


def reset_registry() -> None:
    """重建状态表（测试用）。"""
    global _registry
    _registry = _SyncRegistry()


def load_feed(mp_id: str) -> Optional[Dict[str, Any]]:
    """在调用者线程内读取公众号的基本信息，返回纯字典，避免 ORM 对象跨线程。"""
    from core.db import DB
    from core.models.feed import Feed

    session = DB.get_session()
    try:
        feed = session.query(Feed).filter(Feed.id == mp_id).first()
        if feed is None:
            return None
        return {
            "id": feed.id,
            "mp_name": feed.mp_name or feed.id,
            "faker_id": feed.faker_id,
        }
    finally:
        session.close()


def _is_weread_mp(feed: Dict[str, Any]) -> bool:
    feed_id = str(feed.get("id") or "")
    return feed_id.startswith("MP_WXS_") and feed_id != FEATURED_MP_ID


def build_collector(feed: Dict[str, Any]):
    """按公众号类型创建采集器（必须在 worker 线程内调用）。"""
    if _is_weread_mp(feed):
        # 和 jobs.mps.do_job 保持一致：微信读书公众号固定走 weread_mp 采集器，
        # 没有 Cookie 时直接判失败，避免无谓的重试和浏览器启动。
        from core.wx.model.weread_mp import MpsWereadMP

        collector = MpsWereadMP()
        collector._load_weread_auth()
        if not getattr(collector, "_weread_cookies", None):
            raise MpSyncError("微信读书 Cookie 未配置或已失效，请先完成微信读书授权")
        return collector

    from core.wx import WxGather

    return WxGather().Model()



def _resolve_max_page(max_page: Optional[int]) -> int:
    if max_page is None:
        max_page = cfg.get("max_page", DEFAULT_MAX_PAGE)
    try:
        page_limit = int(max_page)
    except (TypeError, ValueError):
        page_limit = DEFAULT_MAX_PAGE
    return max(page_limit, 1)


def _article_callback():
    """文章入库回调（延迟导入，避免 core 层反向依赖 jobs 包）。"""
    from jobs.article import UpdateArticle

    return UpdateArticle


def sync_mp(
    mp_id: str,
    reason: str = "manual",
    start_page: int = 0,
    max_page: Optional[int] = None,
) -> Dict[str, Any]:
    """队列 worker 入口：采集单个公众号的文章。

    失败会抛出异常交给队列重试；前置条件不满足（如 Cookie 缺失）属于确定性失败，
    只记录状态、不重试。
    """
    current = _registry.get(mp_id) or {}
    _registry.mark(
        mp_id,
        status=STATUS_RUNNING,
        reason=reason,
        error="",
        started_at=_now(),
        finished_at="",
        attempts=int(current.get("attempts") or 0) + 1,
    )

    try:
        feed = load_feed(mp_id)
        if feed is None:
            raise MpSyncError(f"公众号不存在或已删除: {mp_id}")
        _registry.mark(mp_id, mp_name=feed["mp_name"])

        collector = build_collector(feed)

        collector.get_Articles(
            feed["faker_id"],
            Mps_id=feed["id"],
            Mps_title=feed["mp_name"],
            CallBack=_article_callback(),
            start_page=max(int(start_page or 0), 0),
            MaxPage=_resolve_max_page(max_page),
        )

        count = collector.all_count() if hasattr(collector, "all_count") else 0
        status = STATUS_SUCCESS if count > 0 else STATUS_EMPTY
        record = _registry.finish(
            mp_id,
            status=status,
            article_count=int(count or 0),
            finished_at=_now(),
        )
        if status == STATUS_SUCCESS:
            print_success(f"[{feed['mp_name']}] 采集完成，本次入库 {count} 篇")
        else:
            print_warning(f"[{feed['mp_name']}] 采集完成，但没有新增文章（可能已是最新）")
        return record
    except MpSyncError as error:
        record = _registry.finish(
            mp_id,
            status=STATUS_FAILED,
            error=str(error),
            finished_at=_now(),
        )
        print_error(f"采集失败 [{mp_id}]: {error}")
        if error.retriable:
            raise
        return record
    except Exception as error:
        _registry.finish(
            mp_id,
            status=STATUS_FAILED,
            error=str(error),
            finished_at=_now(),
        )
        print_error(f"采集异常 [{mp_id}]: {error}")
        raise


def enqueue_sync(
    mp_id: str,
    reason: str = "manual",
    start_page: int = 0,
    max_page: Optional[int] = None,
    mp_name: Optional[str] = None,
    max_retries: int = DEFAULT_MAX_RETRIES,
) -> bool:
    """把公众号采集任务放进主队列。同一公众号在排队/执行中时不会重复入队。"""
    from core.queue import TaskQueue

    if not mp_name:
        feed = load_feed(mp_id)
        mp_name = feed["mp_name"] if feed else mp_id

    return TaskQueue.add_task(
        sync_mp,
        mp_id,
        reason,
        int(start_page or 0),
        max_page,
        task_name=mp_name,
        task_key=f"mp:{mp_id}",
        max_retries=max_retries,
    )


def _queue_has_key(mp_id: str) -> bool:
    """该公众号是否已被队列里的任务（如定时采集）占用。"""
    from core.queue import TaskQueue

    return TaskQueue.has_task(f"mp:{mp_id}")


def request_sync(
    mp_id: str,
    reason: str = "manual",
    start_page: int = 0,
    max_page: Optional[int] = None,
    wait_timeout: float = DEFAULT_WAIT_TIMEOUT,
) -> Dict[str, Any]:
    """请求采集一个公众号。

    ``wait_timeout`` > 0 时阻塞等待本次采集结束（兼容既有的同步刷新接口），
    否则立即返回排队状态。多个请求同时到达时仍然是串行执行。
    """
    feed = load_feed(mp_id)
    if feed is None:
        return {
            "mp_id": mp_id,
            "mp_name": "",
            "queued": False,
            "status": STATUS_FAILED,
            "article_count": 0,
            "error": "公众号不存在",
            "timed_out": False,
        }

    # 先登记「排队中」再入队：worker 可能在入队瞬间就开始执行，
    # 如果先入队后登记，快速完成的采集会被覆盖回 queued 状态。
    was_active = _registry.is_active(mp_id)
    event = _registry.waiter(mp_id)
    if not was_active:
        _registry.mark(
            mp_id,
            mp_name=feed["mp_name"],
            status=STATUS_QUEUED,
            reason=reason,
            error="",
            article_count=0,
            queued_at=_now(),
            started_at="",
            finished_at="",
        )
    queued = enqueue_sync(
        mp_id,
        reason=reason,
        start_page=start_page,
        max_page=max_page,
        mp_name=feed["mp_name"],
    )
    # 同一公众号正被定时任务等其它入口采集：复用那条通道等待，不重复入队
    owned_elsewhere = (not queued) and (not was_active) and _queue_has_key(mp_id)
    if not queued and not was_active and not owned_elsewhere:
        # 队列拒绝入队（如队列已满），不要留下假的「排队中」状态
        _registry.finish(
            mp_id,
            status=STATUS_FAILED,
            error="加入采集队列失败",
            finished_at=_now(),
        )

    waited = False
    try:
        if wait_timeout and (queued or was_active or owned_elsewhere):
            waited = True
            event.wait(wait_timeout)
        record = _registry.get(mp_id) or {}
    finally:
        _registry.release_waiter(mp_id, event)


    timed_out = bool(
        waited and record.get("status") in (STATUS_QUEUED, STATUS_RUNNING)
    )
    result = {
        "mp_id": mp_id,
        "mp_name": feed["mp_name"],
        "queued": queued,
        "timed_out": timed_out,
    }
    result.update(record)
    result["queued"] = queued
    result["timed_out"] = timed_out
    return result


def request_sync_all(
    reason: str = "manual-batch",
    max_page: Optional[int] = None,
    wait_timeout: float = 0,
) -> Dict[str, Any]:
    """把全部启用的公众号批量加入采集队列（不阻塞）。

    配合队列去重，重复触发不会堆积任务，适合「全部刷新」这类入口。
    """
    from core.db import DB

    session = DB.get_session()
    try:
        feeds = [
            {"id": feed.id, "mp_name": feed.mp_name or feed.id}
            for feed in DB.get_all_mps()
        ]
    finally:
        session.close()

    queued: List[str] = []
    skipped: List[str] = []
    for feed in feeds:
        mp_id = feed["id"]
        was_active = _registry.is_active(mp_id)
        if not was_active:
            _registry.mark(
                mp_id,
                mp_name=feed["mp_name"],
                status=STATUS_QUEUED,
                reason=reason,
                error="",
                article_count=0,
                queued_at=_now(),
                started_at="",
                finished_at="",
            )
        if enqueue_sync(
            mp_id,
            reason=reason,
            max_page=max_page,
            mp_name=feed["mp_name"],
        ):
            queued.append(mp_id)
        else:
            if not was_active and not _queue_has_key(mp_id):
                _registry.finish(
                    mp_id,
                    status=STATUS_FAILED,
                    error="加入采集队列失败",
                    finished_at=_now(),
                )
            skipped.append(mp_id)

    print_info(f"批量采集入队完成：新增 {len(queued)} 个，跳过 {len(skipped)} 个（已在队列中）")
    return {"queued": queued, "skipped": skipped, "total": len(feeds)}


def get_sync_status(mp_id: Optional[str] = None) -> Dict[str, Any]:
    """读取采集状态快照。"""
    if mp_id:
        record = _registry.get(mp_id)
        return {"mp_id": mp_id, "record": record}
    return {"records": _registry.all()}


def note_job_running(mp_id: str, mp_name: str = "", reason: str = "scheduler") -> None:
    """定时任务等其它入口开始采集时登记状态，避免状态表里留下过期记录。"""
    _registry.mark(
        mp_id,
        mp_name=mp_name,
        status=STATUS_RUNNING,
        reason=reason,
        error="",
        started_at=_now(),
        finished_at="",
    )


def note_job_finished(mp_id: str, article_count: int = 0, error: str = "") -> None:
    """其它入口采集结束时写入终态，并唤醒可能在等待该公众号的请求。"""
    if error:
        _registry.finish(
            mp_id, status=STATUS_FAILED, error=error, finished_at=_now()
        )
        return
    _registry.finish(
        mp_id,
        status=STATUS_SUCCESS if article_count > 0 else STATUS_EMPTY,
        article_count=int(article_count or 0),
        error="",
        finished_at=_now(),
    )
