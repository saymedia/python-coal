
from datetime import datetime
import collections
import numbers


__all__ = [
    "Promise",
    "Defer",
    "when",
    "Task",
    "TaskQueue",
    "flatten_promises",
]


class Promise(object):
    task = None


def force_promise(value):
    if isinstance(value, Promise):
        return value
    else:
        promise = Promise()

        def then(callback):
            return force_promise(callback(value))

        promise.then = then
        return promise


def when(value, callback):
    force_promise(value).then(callback)


class ProxyPromise(Promise):
    def __init__(self, value):
        self.value = value

    def then(callback):
        callback(self.value)


class Defer(object):

    NOT_YET_RESOLVED = {}

    def __init__(self):
        self.pending = []
        self.value = self.NOT_YET_RESOLVED
        self.promise = Promise()

        # Construct "then" on the fly in here so that it can
        # access "self" via closure.
        def then(callback):
            result = Defer()

            wrapper_callback = lambda value: result.resolve(callback(value))

            if self.pending is not None:
                self.pending.append(wrapper_callback)
            else:
                self.value.then(wrapper_callback)

            # propagate out any assigned task so that we correctly indicate
            # what needs to get done before the new promise will be
            # resolved fully.
            result.promise.task = self.promise.task
            return result.promise

        self.promise.then = then

    def merge(self, other_defer):
        if self.pending is None and other_defer.pending is None:
            # both are already resolved, so we can't merge
            raise Exception("Can't merge already-resolved Defers")

        if self.pending is None:
            other_defer.resolve(self.value)
        elif other_defer.pending is None:
            self.resolve(other_defer.value)
        else:
            self.pending.extend(other_defer.pending)
            # empty the other defer to make sure we can't accidentally
            # call those callbacks twice, if the caller (incorrectly)
            # resolves other_defer later.
            other_defer.pending = []

    def resolve(self, value):
        if self.value is not self.NOT_YET_RESOLVED:
            raise DuplicateResolutionError(
                'Defer is already resolved'
            )
        if self.pending is not None:
            value = force_promise(value)
            self.value = value
            for callback in self.pending:
                value.then(callback)
            self.pending = None


class TaskPriority(object):
    CACHE = 1
    SYNC_LOOKUP = 2
    ASYNC_LOOKUP = 3
    CLEANUP = 4

    @staticmethod
    def all_values():
        return ['CACHE', 'SYNC_LOOKUP', 'ASYNC_LOOKUP', 'CLEANUP']


class Task(object):
    priority = TaskPriority.SYNC_LOOKUP

    def __init__(self):
        self.defer = Defer()
        self.promise = self.defer.promise
        self.promise.task = self
        # this will be assigned once the task is queued
        self.queue = None

    def resolve(self, value):
        if self.queue is not None:
            self.defer.resolve(value)
            self.queue._record_result(self, value)
        else:
            raise Exception(
                "Can't resolve a task that isn't in a task queue"
            )

    def then(self, callback):
        return self.promise.then(callback)

    def assign_queue(self, queue):
        if self.queue is not None:
            raise Exception('%r is already queued' % self)

        self.queue = queue

    def followup(self, task):
        return self.queue.add_task(task)

    def merge(self, other_task):
        self.defer.merge(other_task.defer)

    @property
    def batch_key(self):
        return ()

    @property
    def coalesce_key(self):
        # default is no coalescing at all, so each distinct task is
        # handled separately.
        return id(self)

    @classmethod
    def work(cls, tasks):
        raise NotImplemented('work not implemented for %r' % cls)

    def __del__(self):
        # Explicitly unassign these during __del__ to break some
        # reference cycles we created, in the hope that this stuff
        # can then get collected faster.
        self.queue = None
        self.promise = None
        self.defer = None


class WorkLogEntry(object):
    class WorkLogTaskBatch(object):
        def __repr__(self):
            return "<coal.WorkLogTaskBatch %i %s>" % (
                len(self.tasks),
                self.task_type.__name__,
            )

    def __init__(self, priority_name):
        self.priority_name = priority_name
        self.task_batches = []

    def log_task_batch(
        self,
        task_type,
        batch_key,
        tasks,
        start_time,
        end_time,
    ):
        batch = WorkLogEntry.WorkLogTaskBatch()
        batch.task_type = task_type
        batch.batch_key = batch_key
        batch.tasks = tasks
        batch.count = len(tasks)
        batch.start_time = start_time
        batch.end_time = end_time
        batch.time_spent = end_time - start_time
        self.task_batches.append(batch)

    @property
    def start_time(self):
        return self.task_batches[0].start_time

    @property
    def end_time(self):
        return self.task_batches[-1].end_time

    @property
    def time_spent(self):
        return self.end_time - self.start_time

    def __repr__(self):
        return "<coal.WorkLogEntry %s [%s]>" % (
            self.priority_name,
            ",".join([
                "%i %s" % (
                    len(x.tasks),
                    x.task_type.__name__,
                )
                for x in self.task_batches
            ])
        )


class TaskQueue(object):

    def __init__(self):
        self.subqueues = {}
        self.results = {}
        for x in TaskPriority.all_values():
            self.subqueues[getattr(TaskPriority, x)] = {}

    def add_task(self, task):
        priority = task.priority
        batch_key = task.batch_key
        coalesce_key = task.coalesce_key
        task_type = type(task)

        compound_key = (task_type, batch_key)
        if compound_key not in self.subqueues[priority]:
            self.subqueues[priority][compound_key] = {}

        if coalesce_key in self.subqueues[priority][compound_key]:
            # we already have a matching task, so merge them.
            self.subqueues[priority][compound_key][coalesce_key].merge(task)
        else:
            self.subqueues[priority][compound_key][coalesce_key] = task
            task.assign_queue(self)

        return task

    def add_tasks(self, tasks):
        for task in tasks:
            self.add_task(task)

    def _record_result(self, task, value):
        priority = task.priority
        batch_key = task.batch_key
        coalesce_key = task.coalesce_key
        task_type = type(task)
        result_key = (task_type, batch_key, coalesce_key)

        self.results[result_key] = value

    def work_once(self, log_list=None):
        subqueue = None
        for priority_name in TaskPriority.all_values():
            priority_id = getattr(TaskPriority, priority_name)
            if len(self.subqueues[priority_id]) > 0:
                subqueue = self.subqueues[priority_id]
                break

        if subqueue is None:
            # No tasks to run, so we're done!
            return 0

        # Reset this subqueue so that if any new items are queued while
        # we're working they won't mutate our existing queue.
        self.subqueues[priority_id] = {}

        log_entry = None
        if log_list is not None:
            log_entry = WorkLogEntry(priority_name)
            log_list.append(log_entry)

        attempted = 0

        for compound_key, tasks in subqueue.iteritems():

            task_type = compound_key[0]
            batch_key = compound_key[1]

            # First see if any of the tasks already have results from
            # previous phases.
            pending_tasks = []
            for coalesce_key, task in tasks.iteritems():
                result_key = (task_type, batch_key, coalesce_key)
                if result_key in self.results:
                    # we already know the result, so just resolve
                    # immediately.
                    task.resolve(self.results[result_key])
                else:
                    pending_tasks.append(task)

            task_count = len(pending_tasks)
            attempted = attempted + task_count
            start_time = datetime.now()
            task_type.work(pending_tasks)
            end_time = datetime.now()

            if log_entry is not None:
                log_entry.log_task_batch(
                    task_type,
                    batch_key,
                    pending_tasks,
                    start_time,
                    end_time,
                )

        return attempted

    def work(self, cycle_limit=15, log_list=None):
        cycles = 0
        total_attempted = 0
        while True:
            attempted = self.work_once(log_list=log_list)
            if attempted == 0:
                return total_attempted
            total_attempted = total_attempted + attempted
            cycles = cycles + 1
            if cycles > cycle_limit:
                raise TooManyCyclesError(
                    "Work queue did not deplete after %i cycles" % (
                        self.cycle_limit
                    )
                )


def flatten_promises(data, log_list=None):

    promises = []

    def flatten_obj(obj):
        if isinstance(obj, numbers.Number) or isinstance(obj, basestring):
            # numbers and strings can never contain promises, so
            # nothing to do here.
            return
        elif callable(obj):
            # skip callable stuff assuming it's stuff like methods.
            # This assumption means we won't resolve promises inside
            # callable objects, which is a reasonable compromise.
            return
        # the string check has to be before this one because strings
        # are sequences and thus containers.
        elif isinstance(obj, collections.Container):
            if isinstance(obj, collections.Sequence):
                member_generator = (
                    (i, value) for i, value in enumerate(obj)
                )
            elif isinstance(obj, collections.Mapping):
                member_generator = (
                    (k, obj[k]) for k in obj.keys()
                )
            else:
                raise TypeError(
                    "Don't know how to find promises in %s" % (
                        type(obj).__name__
                    )
                )
            for k, v in member_generator:
                flatten_key(obj, k, v)
        else:
            public_names = (
                name for name in dir(obj) if not name.startswith("_")
            )
            for attr_name in public_names:
                try:
                    v = getattr(obj, attr_name)
                except AttributeError:
                    # Ignore attributes that we can't read.
                    continue
                flatten_attr(obj, attr_name, v)

    def flatten_key(coll, k, v):
        if isinstance(v, Promise):
            promises.append(v)

            def afterwards(nextV):
                flatten_key(coll, k, nextV)

            v.then(afterwards)
        else:
            coll[k] = v
            flatten_obj(v)

    def flatten_attr(obj, name, v):
        if isinstance(v, Promise):
            promises.append(v)

            def afterwards(nextV):
                flatten_attr(obj, name, nextV)

            v.then(afterwards)
        else:
            if v is not getattr(obj, name):
                try:
                    setattr(obj, name, v)
                except AttributeError:
                    # ignore attributes that we can't write.
                    pass
            flatten_obj(v)

    flatten_obj(data)

    queue = TaskQueue()

    while len(promises) > 0:
        tasks = (
            promise.task for promise in promises
            if getattr(promise, "task", None) is not None
        )
        queue.add_tasks(tasks)
        promises = []
        # The resolution of promises may cause more promises to be queued.
        queue.work(log_list=log_list)


class DuplicateResolutionError(Exception):
    pass


class AlreadyQueuedError(Exception):
    pass


class TooManyCyclesError(Exception):
    pass
