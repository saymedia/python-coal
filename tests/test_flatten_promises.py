
import unittest
import mock
import logging
import testutil
from coal import Task, TaskPriority, flatten_promises


class DummyTask(Task):

    def __init__(self, result):
        self.future_result = result
        super(DummyTask, self).__init__()

    @classmethod
    def work(cls, tasks):
        for task in tasks:
            task.resolve(task.future_result)


class TestFlattenPromises(unittest.TestCase):

    def test_list(self):
        arr = [
            DummyTask(2).promise,
            DummyTask(5).promise,
            DummyTask(9).then(lambda x: x + 1),
            DummyTask(15).then(lambda x: [
                DummyTask(x + 1).promise,
            ]),
        ]

        flatten_promises(arr)

        self.assertEqual(
            arr,
            [2, 5, 10, [16]],
        )

    def test_dict(self):
        d = {
            "a": DummyTask(2).promise,
            "b": DummyTask(5).promise,
            "c": DummyTask(9).then(lambda x: x + 1),
            "d": DummyTask(15).then(lambda x: {
                "e": DummyTask(x + 1).promise,
            }),
        }

        flatten_promises(d)

        self.assertEqual(
            d,
            {
                "a": 2,
                "b": 5,
                "c": 10,
                "d": {
                    "e": 16
                }
            }
        )

    def test_random_obj(self):
        class Foo(object):
            def __init__(self, a, b, c):
                self.a = a
                self.b = b
                self.c = c

        obj = Foo(
            DummyTask(2).promise,
            DummyTask(9).then(lambda x: x + 1),
            DummyTask(15).then(lambda x: Foo(
                DummyTask(x + 1).promise,
                DummyTask(x + 2).promise,
                DummyTask(x + 3).promise,
            ))
        )

        flatten_promises(obj)

        self.assertEqual(
            obj.a,
            2,
        )
        self.assertEqual(
            obj.b,
            10,
        )
        self.assertEqual(
            type(obj.c),
            Foo,
        )
        self.assertEqual(
            obj.c.a,
            16,
        )
        self.assertEqual(
            obj.c.b,
            17,
        )
        self.assertEqual(
            obj.c.c,
            18,
        )

    def test_mixture(self):
        class Foo(object):
            def __init__(self, a):
                self.a = a

        ret = Foo(
            DummyTask(16).then(lambda x: [
                x,
                str(x + 1),
                {
                    "q": DummyTask(x + 2).then(lambda x: [x]),
                },
                x > 1
            ])
        )

        flatten_promises(ret)

        self.assertEqual(
            ret.a,
            [
                16,
                "17",
                {
                    "q": [18],
                },
                True
            ]
        )

    def test_passthrough(self):

        num = [1]
        flatten_promises(num)
        self.assertEqual(num[0], 1)

        string = ["ha"]
        flatten_promises(string)
        self.assertEqual(string[0], "ha")

        func = lambda: "hey"
        func_arr = [func]
        flatten_promises(func_arr)
        self.assertEqual(func_arr[0], func)
