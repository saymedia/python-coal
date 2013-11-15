
import unittest
import mock
import logging
import testutil
from coal import Task, TaskQueue, TaskPriority, Promise


class TestTaskQueue(unittest.TestCase):
    assert_work_log = testutil.assert_work_log

    def test_add_task(self):
        class TaskType1(testutil.MockTask):
            pass
        class TaskType2(testutil.MockTask):
            pass

        queue = TaskQueue()
        for task_type in (TaskType1, TaskType2):
            for priority in (TaskPriority.CACHE, TaskPriority.CLEANUP):
                for batch_key in ('a', 'b'):
                    for coal_key in ('Q', 'Q', 'R'):
                        mock_task = task_type(
                            priority,
                            batch_key,
                            coal_key,
                            )
                        queue.add_task(mock_task)

        self.assertEqual(
            set(queue.subqueues.keys()),
            set([1, 2, 3, 4]),
        )
        self.assertEqual(
            len(queue.subqueues[1]),
            4,
        )
        self.assertEqual(
            len(queue.subqueues[2]),
            0,
        )
        self.assertEqual(
            len(queue.subqueues[3]),
            0,
        )
        self.assertEqual(
            len(queue.subqueues[4]),
            4,
        )

        self.assertEqual(
            set(queue.subqueues[1].keys()),
            set([
                (TaskType1, 'a'),
                (TaskType1, 'b'),
                (TaskType2, 'a'),
                (TaskType2, 'b'),
            ]),
        )
        self.assertEqual(
            set(queue.subqueues[4].keys()),
            set([
                (TaskType1, 'a'),
                (TaskType1, 'b'),
                (TaskType2, 'a'),
                (TaskType2, 'b'),
            ]),
        )

        for compound_key in ((TaskType1, 'a'), (TaskType2, 'a')):
            self.assertEqual(
                set(queue.subqueues[1][compound_key].keys()),
                set([
                    'Q', 'R'
                ]),
            )

    def test_work(self):
        class TaskType1(testutil.MockTask):
            work = mock.MagicMock()

        class TaskType2(testutil.MockTask):
            work = mock.MagicMock()

        cache_task = TaskType1(
            TaskPriority.CACHE,
            'a',
            'b',
        )
        cleanup_task = TaskType2(
            TaskPriority.CLEANUP,
            'a',
            'b',
        )
        queue = TaskQueue()
        queue.add_task(cache_task)
        queue.add_task(cleanup_task)
        log_list = []
        attempted = queue.work(log_list=log_list)
        self.assertEqual(
            attempted,
            2,
        )
        TaskType1.work.assert_called_with([cache_task])
        TaskType2.work.assert_called_with([cleanup_task])

        self.assert_work_log(log_list, [
            ('CACHE', [
                ('TaskType1', 'a', 1)
            ]),
            ('CLEANUP', [
                ('TaskType2', 'a', 1)
            ]),
        ])

    def test_work_once(self):
        class TaskType1(testutil.MockTask):
            work = mock.MagicMock()

        class TaskType2(testutil.MockTask):
            work = mock.MagicMock()

        cache_task = TaskType1(
            TaskPriority.CACHE,
            'a',
            'b',
        )
        cleanup_task = TaskType2(
            TaskPriority.CLEANUP,
            'a',
            'b',
        )
        queue = TaskQueue()
        queue.add_task(cache_task)
        queue.add_task(cleanup_task)
        log_list = []
        attempted = queue.work_once(log_list=log_list)
        TaskType1.work.assert_called_with([cache_task])
        self.assertEqual(
            TaskType2.work.call_count,
            0,
        )
        self.assertEqual(
            attempted,
            1,
        )
        self.assert_work_log(log_list, [
            ('CACHE', [
                ('TaskType1', 'a', 1)
            ]),
        ])

        attempted = queue.work_once(log_list=log_list)
        self.assertEqual(
            attempted,
            1,
        )
        TaskType2.work.assert_called_with([cleanup_task])

        self.assert_work_log(log_list, [
            ('CACHE', [
                ('TaskType1', 'a', 1)
            ]),
            ('CLEANUP', [
                ('TaskType2', 'a', 1)
            ]),
        ])
