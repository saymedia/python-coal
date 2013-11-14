
# assign this into a TestCase subclass to make it usable from that case
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
