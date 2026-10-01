# Run from the repo root: PYTHONPATH=. python3 test_buffer.py
# Tests the aggregation logic without a database (writes are faked).
import logging, threading, time
import psycopg
from ops_stats import OpsClient

logging.basicConfig(level=logging.CRITICAL)
K = ("Platform42", "CHANNEL", "WhatsApp")


class Fake(OpsClient):
    """OpsClient whose database writes are recorded instead of executed."""

    def __init__(self, *a, fail=None, **k):
        self.writes, self.fail = [], list(fail or [])
        super().__init__(*a, **k)

    def _write_stats(self, c, t, n, e, err, ms, *, log_failure=True):
        if self.fail:
            raise self.fail.pop(0)
        self.writes.append((c, t, n, e, err, ms))

    def _drop_connection(self):
        pass


# 1. Calls only aggregate; nothing is written before the interval.
c = Fake(flush_interval_s=0.5)
for i in range(10):
    c.update_stats(*K, 1, i % 2, 12)
assert c.writes == []
time.sleep(0.7)
assert c.writes == [(*K, 10, 5, 120.0)], c.writes
print("aggregate + interval flush ok")

# 2. Counters reset after a flush: next interval starts from zero.
for i in range(3):
    c.update_stats(*K, 1, 0, 10)
time.sleep(0.6)
assert c.writes[-1] == (*K, 3, 0, 30.0), c.writes
time.sleep(0.6)                      # quiet interval: nothing written, nothing kept
assert len(c.writes) == 2 and c._buffer == {}
c.close()
print("reset + quiet component ok")

# 3. Separate totals per component.
c = Fake(flush_interval_s=60)
c.update_stats("A", "CHANNEL", "x", 1, 0, 1)
c.update_stats("B", "CHANNEL", "x", 1, 1, 2)
c.update_stats("A", "CHANNEL", "x", 1, 0, 3)
c.close()
assert sorted(c.writes) == [("A", "CHANNEL", "x", 2, 0, 4.0), ("B", "CHANNEL", "x", 1, 1, 2.0)]
print("per component + flush on close ok")

# 4. Database down: kept and retried, merged with new transactions.
c = Fake(flush_interval_s=60, fail=[psycopg.OperationalError("down")])
c.update_stats(*K, 1, 0, 5)
c.flush()                            # fails, keeps the totals
assert c.writes == [] and c._buffer[K].total_events == 1
c.update_stats(*K, 1, 0, 5)
c.flush()
assert c.writes == [(*K, 2, 0, 10.0)], c.writes
c.close()
print("retain on db down ok")

# 5. Rejected batch: dropped and logged, never raised.
c = Fake(flush_interval_s=60, fail=[psycopg.errors.RaiseException("Unknown component")])
c.update_stats(*K, 1, 0, 5)
c.flush()
assert c.writes == [] and c._buffer == {}
c.close()
print("drop on reject ok")

# 6. Many threads: no lost or double-counted events.
c = Fake(flush_interval_s=0.05)
def worker():
    for _ in range(2000):
        c.update_stats(*K, 1, 0, 1)
threads = [threading.Thread(target=worker) for _ in range(8)]
[t.start() for t in threads]; [t.join() for t in threads]
c.close()
assert sum(w[3] for w in c.writes) == 16000 and sum(w[5] for w in c.writes) == 16000.0
print(f"threads ok: 16000 events in {len(c.writes)} writes")

# 7. Immediate mode (one-shot functions) and validation.
c = Fake(flush_interval_s=None)
c.update_stats(*K, 1, 0, 1)
assert len(c.writes) == 1 and c._flusher is None
c.close()
c = Fake(flush_interval_s=60); c.close()
try:
    c.update_stats(*K, 1, 0, 1); raise SystemExit("closed client must raise")
except RuntimeError:
    pass
try:
    OpsClient(flush_interval_s=0); raise SystemExit("must raise")
except ValueError:
    pass
print("immediate mode + validation ok")
