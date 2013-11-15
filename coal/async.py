"""
:py:mod:`coal.async` builds on the basic functionality of
:py:mod:`coal` to provide a foundation for modelling tasks that
run asynchronously and concurrently with other processing, as opposed to those
that are run in synchronous batches.

This sort of task is characterized by the work beginning in the background
shortly after the task is instantiated, so in some cases the work may be
done by the time the task makes it to the front of a job queue. The blocking
"work" phase of the task is then simply to wait for the task to complete
if it hasn't already.

The :py:attr:`coal.TaskPriority.ASYNC_LOOKUP` task priority will cause tasks
to run only after all `CACHE` and `SYNC_LOOKUP` tasks have completed, thus
giving the background job the longest possible time to complete before we
start to block on it.
"""

from coal import Task, TaskPriority

import threading


class AsyncTask(Task):
    priority = TaskPriority.ASYNC_LOOKUP

    def __init__(self):
        super(AsyncTask, self).__init__()

        def callback(value):
            self.background_result = value

        self.start_working(callback)

    def start_working(self, callback):
        raise Exception('start_working is not implemented for %r' % (
            self
        ))

    def wait_for_result(self):
        raise Exception('wait_for_completion is not implemented for %r' % (
            self
        ))

    @classmethod
    def work(cls, tasks):
        # The "work" phase is just to wait for all of the tasks to
        # complete. Since async jobs happen in their own phase we assume
        # that it doesn't really matter what order we block on them in,
        # since we're always gonna wait for the longest one to complete before
        # we work on anything else.
        for task in tasks:
            task.wait_for_result()
            try:
                task.resolve(task.background_result)
            except AttributeError, ex:
                raise Exception('Async task %r did not complete' % task)


class ThreadTask(AsyncTask):

    def start_working(self, callback):
        def impl():
            result = self.thread_work()
            callback(result)
        self.thread = threading.Thread(
            target=impl
        )
        self.thread.start()

    def wait_for_result(self):
        self.thread.join()

    def thread_work(self):
        raise Exception('thread_work is not implemented for %r' % self)
