
import unittest
import mock
import logging
import testutil
from coal import Task, TaskQueue, TaskPriority, Promise


class TestTaskQueue(unittest.TestCase):
    assert_work_log = testutil.assert_work_log
