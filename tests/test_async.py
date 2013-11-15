
import unittest
import mock
import logging
import testutil
from coal import Task, TaskQueue, TaskPriority, Promise
from coal.async import AsyncTask, ThreadTask


class TestAsync(unittest.TestCase):

    def test_async_task(self):
        calls = {
            "start_working": 0,
            "wait_for_result": 0,
        }
        start_working_params = []

        class MockAsyncTask(AsyncTask):

            def start_working(self, callback):
                start_working_params.append(callback)
                calls["start_working"] += 1

            def wait_for_result(self):
                calls["wait_for_result"] += 1

        task = MockAsyncTask()

        # Fake being in a queue so we can resolve
        task.queue = mock.MagicMock()

        self.assertEqual(
            calls,
            {
                "start_working": 1,
                "wait_for_result": 0,
            }
        )

        self.assertEqual(
            len(start_working_params),
            1,
        )

        result_callback = mock.MagicMock()
        task.promise.then(result_callback)

        start_working_params[0](9)

        self.assertEqual(
            result_callback.call_count,
            0,
        )

        MockAsyncTask.work([task])

        result_callback.assert_called_with(9)

        self.assertEqual(
            calls,
            {
                "start_working": 1,
                "wait_for_result": 1,
            }
        )

        task.queue._record_result.assert_called_with(
            task,
            9,
        )

    def test_thread_task(self):

        class DummyThreadTask(ThreadTask):
            def thread_work(self):
                return 23

        task = DummyThreadTask()

        # Fake being in a queue so we can resolve
        task.queue = mock.MagicMock()

        result_callback = mock.MagicMock()
        task.promise.then(result_callback)

        DummyThreadTask.work([task])

        result_callback.assert_called_with(23)
        task.queue._record_result.assert_called_with(
            task,
            23,
        )
