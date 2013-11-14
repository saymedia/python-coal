
import unittest
import mock
import logging
from coal import Task, TaskQueue, TaskPriority, Promise


class TestTaskQueue(unittest.TestCase):

    def assert_work_log(self, got_log, expected):
        got = []
        for entry in got_log:
            raw_entry = (entry.priority_name, [])
            got.append(raw_entry)
            for batch in entry.task_batches:
                raw_entry[1].append(
                    (
                        batch.task_type.__name__,
                        batch.batch_key,
                        batch.count,
                    )
                )
        self.assertEqual(
            got,
            expected,
        )

    def test_caching_use_case(self):
        fake_cache = {}
        CACHE_MISS = {}  # sentinel for a missed cache lookup

        class TryCache(Task):
            priority = TaskPriority.CACHE

            def __init__(self, key):
                self.key = key
                super(TryCache, self).__init__()

            def __repr__(self):
                return "<TryCache %s>" % self.key

            @classmethod
            def work(cls, tasks):
                logging.debug("in TryCache.work with %r" % tasks)
                for task in tasks:
                    task.resolve(
                        fake_cache.get(task.key, CACHE_MISS)
                    )

        class LoadData(Task):
            def __init__(self, data):
                self.data = data
                super(LoadData, self).__init__()

            def __repr__(self):
                return "<LoadData %r>" % self.data

            @classmethod
            def work(cls, tasks):
                logging.debug("in LoadData.work with %r" % tasks)
                for task in tasks:
                    task.resolve(task.data)

        class CachePopulate(Task):
            priority = TaskPriority.CLEANUP

            def __init__(self, key, value):
                self.key = key
                self.value = value
                super(CachePopulate, self).__init__()

            @classmethod
            def work(cls, tasks):
                logging.debug("in CachePopulate.work with %r" % tasks)
                for task in tasks:
                    fake_cache[task.key] = task.value

        cache_key = "baz"
        data_value = 5

        try_cache_task = TryCache(cache_key)
        def handle_cache_result(value):
            logging.debug("Cache result is %r", value)
            if value is CACHE_MISS:
                logging.debug("Cache miss, so will load data")
                load_data_task = LoadData(data_value)
                def handle_load_result(value):
                    logging.debug("Data result is %r", value)
                    load_data_task.followup(
                        CachePopulate(cache_key, value)
                    )
                load_data_task.then(handle_load_result)
                try_cache_task.followup(load_data_task)
                return load_data_task.promise
            else:
                logging.debug("Cache hit, so returning immediately")
                return value

        overall_promise = try_cache_task.then(handle_cache_result)

        self.assertEqual(
            type(overall_promise),
            Promise,
        )
        self.assertEqual(
            type(overall_promise.task),
            TryCache,
        )
        self.assertEqual(
            overall_promise.task,
            try_cache_task,
        )
        # The overall_promise is our derived promise that handles the
        # cache result, not the inner promise on the try_cache_task.
        self.assertNotEqual(
            overall_promise,
            try_cache_task.promise,
        )

        callback = mock.MagicMock()
        overall_promise.then(callback)

        # Now let's actually do this thing.
        task_queue = TaskQueue()
        task_queue.add_task(overall_promise.task)
        log_list = []
        task_queue.work(log_list=log_list)

        callback.assert_called_with(5)

        self.assert_work_log(log_list, [
            ('CACHE', [
                ('TryCache', (), 1),
            ]),
            ('SYNC_LOOKUP', [
                ('LoadData', (), 1),
            ]),
            ('CLEANUP', [
                ('CachePopulate', (), 1),
            ]),
        ])
