# coding: utf-8
"""#186 against real DEALER/ROUTER sockets, not fakes.

test_zmq_client_concurrency.py drives FakeSocket / FakeZmq. That pins the shape
of the fix -- one socket per thread, no lock across the round trip -- but it
cannot show that the fix *works*, because there is no ROUTER in it: the fake
hands each caller back whatever it was given, so per-thread IDENTITY would look
correct even if every thread shared one identity and the ROUTER sent every reply
to whichever socket connected last. Mocks encode the premise the code was
written from; this file checks the premise.

So: a genuine ROUTER on loopback, the real client driven from four threads, and
two questions a fake cannot answer --

  1. Do the calls actually overlap? Four 200ms round trips must finish in about
     200ms, not 800ms.
  2. Does each thread get back *its own* reply? Distinct identities, and the
     request_id that came back is the one that thread sent.

Both copies of the transport are covered. `bigqmt_no_redis/zmq_transport.py` is
a hand-maintained standalone branch -- it silently drifted for five weeks and
missed #177 and #186 entirely until 0.3.23 regenerated it, and it is the copy
the single-file builder inlines, so the pure-zmq deployments that most need the
concurrency are exactly the ones running it.

No QMT, no network beyond loopback, no orders.
"""
import importlib.util
import json
import os
import sys
import threading
import time
import unittest


ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(ROOT, "src"))

from bigqmt_signal_trader.redis_rpc import decode_rpc_request_payload

try:
    import zmq
except ImportError:  # pragma: no cover - pyzmq is optional for the suite
    zmq = None

# 200ms is long enough that four serialised calls (800ms) cannot be mistaken for
# four concurrent ones, and short enough not to weigh on the suite.
DELAY = 0.20
THREADS = 4
# Concurrent lands near DELAY, serialised near THREADS * DELAY. Half way between
# is nowhere near either, so a loaded machine does not flip the verdict.
CONCURRENT_CEILING = THREADS * DELAY * 0.6


class _Router(object):
    """A real ROUTER that holds each request DELAY seconds before replying.

    Single-threaded and non-blocking on purpose: it accepts every request as it
    arrives and never makes one caller wait for another, so any serialisation
    the test measures is the client's.
    """

    def __init__(self):
        self.ctx = zmq.Context.instance()
        self.sock = self.ctx.socket(zmq.ROUTER)
        self.sock.bind("tcp://127.0.0.1:0")
        self.address = self.sock.getsockopt(zmq.LAST_ENDPOINT).decode("ascii")
        self.max_in_flight = 0
        self._pending = []  # (due_time, identity, reply_bytes)
        self._running = True
        self._thread = threading.Thread(target=self._loop)
        self._thread.daemon = True
        self._thread.start()

    def _loop(self):
        poller = zmq.Poller()
        poller.register(self.sock, zmq.POLLIN)
        while self._running:
            if self.sock in dict(poller.poll(timeout=10)):
                frames = self.sock.recv_multipart()
                identity, payload = frames[0], frames[-1]
                request = json.loads(
                    decode_rpc_request_payload(payload.decode("utf-8")))
                reply = json.dumps({
                    "request_id": request.get("request_id"),
                    "echo_thread": request.get("probe_thread"),
                    "served_identity": identity.hex(),
                }).encode("utf-8")
                self._pending.append((time.time() + DELAY, identity, reply))
                self.max_in_flight = max(self.max_in_flight, len(self._pending))
            now = time.time()
            due = [item for item in self._pending if item[0] <= now]
            self._pending = [item for item in self._pending if item[0] > now]
            for _due, identity, reply in due:
                self.sock.send_multipart([identity, reply])

    def close(self):
        self._running = False
        self._thread.join(3.0)
        self.sock.close(linger=0)


def _load_no_redis_transport():
    """Load the standalone no-redis copy without importing it as a package."""
    path = os.path.join(ROOT, "bigqmt_no_redis", "zmq_transport.py")
    spec = importlib.util.spec_from_file_location("nr_zmq_transport_test", path)
    module = importlib.util.module_from_spec(spec)
    sys.modules["nr_zmq_transport_test"] = module
    try:
        spec.loader.exec_module(module)
    except Exception:
        sys.modules.pop("nr_zmq_transport_test", None)
        raise
    return module


@unittest.skipIf(zmq is None, "pyzmq not installed")
class RealSocketConcurrencyTest(unittest.TestCase):
    """Four threads over one real ROUTER: overlap, and replies land correctly."""

    def _drive(self, transport_cls):
        router = _Router()
        transport = transport_cls(connect_address=router.address,
                                  account_id="test186")
        results = {}
        errors = {}
        try:
            # Warm one socket so the connect handshake is not counted as
            # serialisation.
            transport.send_request({"method": "ping", "probe_thread": -1},
                                   timeout_seconds=15)

            def worker(index):
                try:
                    request = {"method": "ping", "probe_thread": index,
                               "request_id": "probe-%d" % index}
                    response = transport.send_request(dict(request),
                                                      timeout_seconds=15)
                    results[index] = response
                except Exception as exc:
                    errors[index] = "%s: %s" % (type(exc).__name__, exc)

            threads = [threading.Thread(target=worker, args=(i,))
                       for i in range(THREADS)]
            started = time.time()
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()
            elapsed = time.time() - started
        finally:
            transport.stop()
            router.close()
        return elapsed, results, errors, router.max_in_flight

    def _assert_concurrent_and_correctly_routed(self, transport_cls, label):
        elapsed, results, errors, in_flight = self._drive(transport_cls)

        self.assertEqual(errors, {}, "%s: calls failed" % label)
        self.assertEqual(len(results), THREADS)

        # 1. They overlapped rather than queueing.
        self.assertLess(
            elapsed, CONCURRENT_CEILING,
            "%s: %d threads took %.3fs; serialised is ~%.3fs, so they queued"
            % (label, THREADS, elapsed, THREADS * DELAY))
        self.assertEqual(
            in_flight, THREADS,
            "%s: the router only ever saw %d request(s) at once"
            % (label, in_flight))

        # 2. Each thread got back its own reply, over its own identity. This is
        #    the half a fake cannot check: one shared identity still round-trips.
        for index, response in sorted(results.items()):
            self.assertEqual(response.get("request_id"), "probe-%d" % index,
                             "%s: thread %d got another thread's reply"
                             % (label, index))
            self.assertEqual(response.get("echo_thread"), index)
        identities = set(r.get("served_identity") for r in results.values())
        self.assertEqual(
            len(identities), THREADS,
            "%s: %d distinct DEALER identities across %d threads"
            % (label, len(identities), THREADS))

    def test_package_transport_runs_four_threads_concurrently(self):
        from bigqmt_signal_trader.transports.zmq_transport import ZmqTransport
        self._assert_concurrent_and_correctly_routed(
            ZmqTransport, "src/bigqmt_signal_trader/transports")

    def test_no_redis_copy_runs_four_threads_concurrently(self):
        """The copy the single-file builder inlines; it has drifted before."""
        module = _load_no_redis_transport()
        try:
            self._assert_concurrent_and_correctly_routed(
                module.ZmqTransport, "bigqmt_no_redis")
        finally:
            sys.modules.pop("nr_zmq_transport_test", None)


if __name__ == "__main__":
    unittest.main()
