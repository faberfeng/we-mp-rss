"""公众号采集排队机制的回归测试。

覆盖三件事：
1. 队列按稳定 key（公众号 id）去重，而不是按展示名，同名公众号不会互相顶掉；
2. 采集请求只把 mp_id 入队，采集器在 worker 线程内创建；
3. 采集状态（排队/失败/成功/篇数）能被记录下来并唤醒等待方。
"""

import threading
import time
import unittest
from unittest.mock import patch

from core import mp_sync
from core.queue import TaskQueueManager
from core.queue.queue import TaskRecord


class FakeCollector:
    """记录调用参数的假采集器。"""

    def __init__(self, count=2, error=None):
        self.articles = [{"id": f"article-{index}"} for index in range(count)]
        self.error = error
        self.calls = []

    def get_Articles(self, faker_id=None, **kwargs):
        self.calls.append({"faker_id": faker_id, **kwargs})
        if self.error is not None:
            raise self.error

    def all_count(self):
        return len(self.articles)


def fake_feed(mp_id):
    return {
        "id": mp_id,
        "mp_name": f"公众号-{mp_id}",
        "faker_id": f"fakeid-{mp_id}",
    }


class TaskQueueDedupeTest(unittest.TestCase):
    """TaskQueueManager 的入队去重语义。"""

    def setUp(self):
        self.queue = TaskQueueManager(tag="测试队列")

    @staticmethod
    def _noop():
        return None

    def test_same_task_key_is_enqueued_once(self):
        self.assertTrue(
            self.queue.add_task(self._noop, task_name="账号A", task_key="mp:1")
        )
        self.assertFalse(
            self.queue.add_task(self._noop, task_name="账号A", task_key="mp:1")
        )
        self.assertEqual(len(self.queue._pending_items), 1)

    def test_same_display_name_with_different_keys_both_enqueued(self):
        # 两个公众号重名时，旧逻辑（按展示名去重）会把其中一个静默丢掉
        self.assertTrue(
            self.queue.add_task(self._noop, task_name="重名", task_key="mp:1")
        )
        self.assertTrue(
            self.queue.add_task(self._noop, task_name="重名", task_key="mp:2")
        )
        self.assertEqual(len(self.queue._pending_items), 2)

    def test_running_task_is_not_requeued(self):
        self.queue._current_task = TaskRecord(task_name="账号A", start_time="now")
        self.queue._current_task_key = "mp:1"
        self.assertFalse(
            self.queue.add_task(self._noop, task_name="账号A", task_key="mp:1")
        )

    def test_pending_item_keeps_task_key(self):
        self.queue.add_task(self._noop, task_name="账号A", task_key="mp:1")
        self.assertEqual(self.queue._pending_items[0].dedupe_key(), "mp:1")
        self.assertTrue(self.queue.has_task("mp:1"))
        self.assertFalse(self.queue.has_task("mp:2"))


class TaskQueueWorkerTest(unittest.TestCase):
    """worker 串行消费：带 task_key 的任务能正常执行并释放队列。"""

    def test_worker_runs_tasks_in_serial_order(self):
        queue = TaskQueueManager(tag="测试队列")
        executed = []

        def worker():
            queue.run_tasks(timeout=0.2)

        thread = threading.Thread(target=worker, daemon=True)
        thread.start()
        try:
            queue.add_task(executed.append, "a", task_name="账号A", task_key="mp:1")
            queue.add_task(executed.append, "b", task_name="账号B", task_key="mp:2")
            deadline = time.time() + 5
            while len(executed) < 2 and time.time() < deadline:
                time.sleep(0.05)
        finally:
            queue.stop()
            thread.join(timeout=5)

        self.assertEqual(executed, ["a", "b"])
        self.assertEqual(queue._pending_items, [])
        self.assertIsNone(queue._current_task_key)


class MpSyncQueueTest(unittest.TestCase):
    """core.mp_sync 的入队与状态记录。"""

    def setUp(self):
        mp_sync.reset_registry()
        self.collector = FakeCollector(count=3)
        self.enqueued = []
        self.pending_keys = set()

        self.patchers = [
            patch.object(mp_sync, "load_feed", side_effect=fake_feed),
            patch.object(mp_sync, "_article_callback", return_value=lambda art: True),
            patch.object(mp_sync, "build_collector", return_value=self.collector),
            patch.object(mp_sync, "enqueue_sync", side_effect=self.fake_enqueue),
        ]
        for patcher in self.patchers:
            patcher.start()

    def tearDown(self):
        for patcher in reversed(self.patchers):
            patcher.stop()

    def fake_enqueue(self, mp_id, **kwargs):
        key = f"mp:{mp_id}"
        if key in self.pending_keys:
            return False
        self.pending_keys.add(key)
        self.enqueued.append((mp_id, kwargs))
        return True

    def test_sync_mp_collects_and_records_article_count(self):
        record = mp_sync.sync_mp("MP_WXS_1", reason="add")

        self.assertEqual(record["status"], mp_sync.STATUS_SUCCESS)
        self.assertEqual(record["article_count"], 3)
        self.assertEqual(len(self.collector.calls), 1)
        call = self.collector.calls[0]
        self.assertEqual(call["faker_id"], "fakeid-MP_WXS_1")
        self.assertEqual(call["Mps_id"], "MP_WXS_1")
        self.assertEqual(call["Mps_title"], "公众号-MP_WXS_1")

    def test_sync_mp_marks_empty_when_nothing_new(self):
        self.collector.articles = []

        record = mp_sync.sync_mp("MP_WXS_1")

        self.assertEqual(record["status"], mp_sync.STATUS_EMPTY)
        self.assertEqual(record["article_count"], 0)

    def test_deterministic_failure_is_recorded_without_retry(self):
        with patch.object(
            mp_sync,
            "build_collector",
            side_effect=mp_sync.MpSyncError("微信读书 Cookie 未配置"),
        ):
            record = mp_sync.sync_mp("MP_WXS_1")

        self.assertEqual(record["status"], mp_sync.STATUS_FAILED)
        self.assertIn("Cookie", record["error"])

    def test_unexpected_failure_propagates_for_queue_retry(self):
        self.collector.error = RuntimeError("接口超时")

        with self.assertRaises(RuntimeError):
            mp_sync.sync_mp("MP_WXS_1")

        self.assertEqual(mp_sync.get_sync_status("MP_WXS_1")["record"]["status"], mp_sync.STATUS_FAILED)

    def test_request_sync_enqueues_by_mp_id(self):
        result = mp_sync.request_sync("MP_WXS_1", reason="add", wait_timeout=0)

        self.assertTrue(result["queued"])
        self.assertEqual(result["status"], mp_sync.STATUS_QUEUED)
        self.assertEqual(self.enqueued[0][0], "MP_WXS_1")
        self.assertEqual(self.enqueued[0][1]["reason"], "add")

    def test_duplicate_request_does_not_enqueue_twice(self):
        mp_sync.request_sync("MP_WXS_1", wait_timeout=0)
        second = mp_sync.request_sync("MP_WXS_1", wait_timeout=0)

        self.assertFalse(second["queued"])
        self.assertEqual(len(self.enqueued), 1)

    def test_request_sync_waits_until_worker_finishes(self):
        def run_in_thread(mp_id, **_kwargs):
            threading.Thread(
                target=mp_sync.sync_mp, args=(mp_id,), daemon=True
            ).start()
            return True

        with patch.object(mp_sync, "enqueue_sync", side_effect=run_in_thread):
            result = mp_sync.request_sync("MP_WXS_1", wait_timeout=5)

        self.assertEqual(result["status"], mp_sync.STATUS_SUCCESS)
        self.assertEqual(result["article_count"], 3)
        self.assertFalse(result["timed_out"])

    def test_request_sync_reports_missing_feed(self):
        with patch.object(mp_sync, "load_feed", return_value=None):
            result = mp_sync.request_sync("MP_WXS_404", wait_timeout=0)

        self.assertFalse(result["queued"])
        self.assertEqual(result["status"], mp_sync.STATUS_FAILED)
        self.assertIn("不存在", result["error"])

    def test_duplicate_mp_collectors_do_not_block_each_other(self):
        # 连续添加多个公众号：每个都要能排到队
        for index in range(3):
            mp_sync.request_sync(f"MP_WXS_{index}", wait_timeout=0)

        self.assertEqual(len(self.enqueued), 3)
        self.assertEqual(len({mp_id for mp_id, _ in self.enqueued}), 3)

    def test_request_sync_reuses_task_owned_by_scheduler(self):
        # 定时任务已占用该公众号：不重复入队，也不要把状态误标成失败
        with patch.object(mp_sync, "enqueue_sync", return_value=False), patch.object(
            mp_sync, "_queue_has_key", return_value=True
        ):
            result = mp_sync.request_sync("MP_WXS_1", wait_timeout=0)

        self.assertFalse(result["queued"])
        self.assertEqual(result["status"], mp_sync.STATUS_QUEUED)

    def test_scheduler_finish_wakes_manual_waiter(self):
        finished = {}

        def wait_for_scheduler(mp_id, **_kwargs):
            finished["result"] = mp_sync.request_sync(mp_id, wait_timeout=5)
            return False

        def run_wait():
            with patch.object(mp_sync, "enqueue_sync", return_value=False), patch.object(
                mp_sync, "_queue_has_key", return_value=True
            ):
                wait_for_scheduler("MP_WXS_1")

        thread = threading.Thread(target=run_wait, daemon=True)
        thread.start()
        time.sleep(0.1)
        mp_sync.note_job_finished("MP_WXS_1", article_count=2)
        thread.join(timeout=5)

        self.assertFalse(thread.is_alive())
        self.assertEqual(finished["result"]["status"], mp_sync.STATUS_SUCCESS)
        self.assertEqual(finished["result"]["article_count"], 2)


class MpSyncStatusTest(unittest.TestCase):
    def setUp(self):
        mp_sync.reset_registry()

    def test_status_snapshot_lists_records(self):
        with patch.object(mp_sync, "load_feed", side_effect=fake_feed), patch.object(
            mp_sync, "build_collector", return_value=FakeCollector(count=1)
        ), patch.object(mp_sync, "_article_callback", return_value=lambda art: True):
            mp_sync.sync_mp("MP_WXS_9")

        records = mp_sync.get_sync_status()["records"]
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["mp_id"], "MP_WXS_9")
        self.assertEqual(records[0]["status"], mp_sync.STATUS_SUCCESS)


if __name__ == "__main__":
    unittest.main()
