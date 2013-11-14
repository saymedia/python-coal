
import unittest
import mock
import logging
import testutil
from coal import Task, TaskQueue, TaskPriority, Promise
from coal.caching import cache_lookup_promise, CACHE_MISS


class TestCaching(unittest.TestCase):

    assert_work_log = testutil.assert_work_log

    def test_cache_lookup_promise(self):
        fake_cache = {}

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

        overall_promise = cache_lookup_promise(
            TryCache(cache_key),
            LoadData(data_value),
            cache_update_task_builder=lambda x: CachePopulate(cache_key, x)
        )

        self.assertEqual(
            type(overall_promise),
            Promise,
        )
        self.assertEqual(
            type(overall_promise.task),
            TryCache,
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
