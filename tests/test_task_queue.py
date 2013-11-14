
import unittest
import mock
from coal import Task, TaskQueue, TaskPriority, Promise


class TestTaskQueue(unittest.TestCase):

    def test_caching_use_case(self):
        fake_cache = {}
        CACHE_MISS = {}  # sentinel for a missed cache lookup

        class TryCache(Task):
            priority = TaskPriority.CACHE

            def __init__(self, key):
                self.key = key
                super(TryCache, self).__init__()

            @classmethod
            def work(self, tasks):
                for task in tasks:
                    task.resolve(
                        fake_cache.get(task.key, CACHE_MISS)
                    )

        class LoadData(Task):
            def __init__(self, data):
                self.data = data
                super(LoadData, self).__init__()

            @classmethod
            def work(self, tasks):
                for task in tasks:
                    task.resolve(self.data)

        class CachePopulate(Task):
            priority = TaskPriority.CLEANUP

            def __init__(self, key, value):
                self.key = key
                self.value = value
                super(CachePopulate, self).__init__()

            @classmethod
            def work(self, tasks):
                for task in tasks:
                    fake_cache[task.key] = task.value

        cache_key = "baz"
        data_value = 5

        try_cache_task = TryCache(cache_key)
        def handle_cache_result(value):
            if value is CACHE_MISS:
                load_data_task = LoadData(data_value)
                def handle_load_result(value):
                    load_data_task.followup(
                        CachePopulate(cache_key, value)
                    )
                load_data_task.then(handle_load_result)
                return load_data_task.promise
            else:
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

        # Now let's actually do this thing.
        task_queue = TaskQueue()
        task_queue.add_task(overall_promise.task)
        log_list = []
        task_queue.work(log_list=log_list)

        raise Exception(repr(log_list))
