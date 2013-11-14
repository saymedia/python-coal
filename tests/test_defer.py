
import unittest
import mock
from coal import Defer, Promise, when, DuplicateResolutionError


class TestDefer(unittest.TestCase):

    def test_simple(self):
        defer = Defer()
        promise = defer.promise
        callback_1 = mock.MagicMock()
        callback_2 = mock.MagicMock()
        promise.then(callback_1)
        promise.then(callback_2)

        defer.resolve(5)

        callback_1.assert_called_with(5)
        callback_2.assert_called_with(5)

    def test_chained(self):

        defer = Defer()
        promise_1 = defer.promise
        promise_2 = promise_1.then(lambda x: x + 1)
        callback = mock.MagicMock()

        promise_2.then(callback)

        defer.resolve(7)

        callback.assert_called_with(8)

    def test_late_attach(self):
        defer = Defer()
        promise = defer.promise

        defer.resolve(16)

        callback = mock.MagicMock()
        promise.then(callback)

        callback.assert_called_with(16)

    def test_late_attach_chained(self):
        defer = Defer()
        promise_1 = defer.promise

        defer.resolve(24)

        promise_2 = promise_1.then(lambda x: x + 2)

        callback = mock.MagicMock()
        promise_2.then(callback)

        callback.assert_called_with(26)

    def test_double_resolve(self):
        defer = Defer()

        defer.resolve(2)
        self.assertRaises(
            DuplicateResolutionError,
            lambda: defer.resolve(3),
        )

    def test_propagate_task(self):
        defer = Defer()
        defer.promise.task = "baz"
        promise_2 = defer.promise.then(lambda value: value)
        self.assertEqual(
            promise_2.task,
            "baz",
        )
