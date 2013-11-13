
__all__ = [
    "Promise",
    "Defer",
    "when",
]


class Promise(object):
    pass


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
            return result.promise

        self.promise.then = then

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


class DuplicateResolutionError(Exception):
    pass
