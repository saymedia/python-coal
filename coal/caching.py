"""
:py:mod:`coal.caching` builds on the basic functionality of
:py:mod:`coal` to provide a design pattern for using caching and
some utilities to help with that design pattern.

The core :py:class:`coal.Task` class accepts a priority argument of
:py:attr:`coal.TaskPriority.CACHE` which is a hint to the task queue that
a particular task is trying a cache lookup and so it should be prioritized
first to get as much data from cache as possible before attempting more
expensive lookups.

A suggested design pattern is for cache-lookup tasks to resolve with
the special value :py:data:`CACHE_MISS` as a reliable signal of a cache
miss that is not ambiguous from an explicitly cached `None`. Tasks that
follow this pattern can then be used with :py:func:`cache_lookup_promise`
to easily assemble a task flow for looking up an item that may be cached.

The most obvious use-case is to have a task that represents getting
a key from `Memcached <http://memcached.org/>` (or similar) using its
`get_multi` command, allowing as much as possible to be retrieved in a single
cache round-trip and then the few misses to be handled via a more expensive
lookup, eventually writing the results back to memcached using `set_multi`.
"""


# create a singleton object that we can use to signal a cache miss
# while allowing None to be a valid cache value.
class CacheMiss(object):
    pass

CACHE_MISS = CacheMiss()
del CacheMiss  # Don't need the class anymore, just the instance.


def cache_lookup_promise(
    cache_lookup_task,
    real_lookup_task,
    cache_update_task_builder=None
):
    """
    A helper function to construct a typical task graph to handle
    the lookup of an item that can be cached.

    Will construct task flow that will first try a cache lookup, and then
    either return the result from the cache (in the case of a cache hit) or
    delegate to a "real" lookup (in the case of a cache miss).

    Can also optionally include a final task to write the result from the
    "real" lookup back to the cache, so the value will be cached for next time.
    """
    def handle_cache_result(value):
        if value is CACHE_MISS:
            # need to do the real lookup, then
            def handle_load_result(value):
                if cache_update_task_builder is not None:
                    real_lookup_task.followup(
                        cache_update_task_builder(value)
                    )

            real_lookup_task.then(handle_load_result)
            cache_lookup_task.followup(real_lookup_task)
            return real_lookup_task.promise
        else:
            # we can just return the value we got from the cache
            return value

    return cache_lookup_task.then(handle_cache_result)
