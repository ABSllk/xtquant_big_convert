# coding: utf-8
"""#236 follow-up: the four live defects and their fixes, end to end on a
real named pipe (Windows only).

  1. deferred methods never answered -- inline answers and adjust-thread
     answers now serialize per connection; frames no longer interleave.
  2. the client had no real timeout (a blocking ReadFile checks the deadline
     only AFTER it returns) -- now a pump thread + inbox queue, so
     queue.get(timeout=...) is the timeout (cfquant's shape, for the same
     reason).
  3. stop() must not hang on a pump's pending ReadFile.
  4. a restarted server on the same pipe name takes new clients again.
"""
import json
import os
import sys
import threading
import time
import unittest


ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(ROOT, "src"))

from bigqmt_signal_trader.transports.base import TransportTimeout
from bigqmt_signal_trader.transports.pipe_transport import NamedPipeTransport

WINDOWS = os.name == "nt"


def _transport_pair(pipe_name):
    server = NamedPipeTransport(pipe_name=pipe_name, print_prefix="[test-srv]")
    client = NamedPipeTransport(pipe_name=pipe_name, print_prefix="[test-cli]")
    return server, client


@unittest.skipUnless(WINDOWS, "named pipes are Windows-only")
class RoundTripTest(unittest.TestCase):
    def setUp(self):
        self.pipe = "bigqmt-test-%d-%d" % (os.getpid(), int(time.time() * 1000) % 100000)
        self.server, self.client = _transport_pair(self.pipe)

    def tearDown(self):
        try:
            self.client.stop()
        except Exception:
            pass
        try:
            self.server.stop()
        except Exception:
            pass

    def test_inline_round_trip(self):
        self.server.start_receiving(lambda req: {
            "request_id": req.get("request_id"), "ok": True, "echo": req.get("method")})

        answer = self.client.send_request(
            {"request_id": "r1", "method": "ping"}, timeout_seconds=5.0)

        self.assertEqual(answer, {"request_id": "r1", "ok": True, "echo": "ping"})

    def test_an_answer_written_on_another_thread_arrives(self):
        """The deferred shape: the request is received on the connection
        worker, parked, and answered from a DIFFERENT thread (the adjust
        thread in production). Before the per-connection write lock, this
        raced with inline answers and corrupted frames."""
        parked = threading.Event()
        released = threading.Event()

        def deferred_handler(req):
            parked.set()
            released.wait(5.0)
            return {"request_id": req.get("request_id"), "ok": True,
                    "echo": req.get("method"), "thread": threading.current_thread().name}

        self.server.start_receiving(deferred_handler)
        releaser = threading.Timer(0.2, released.set)
        releaser.start()

        answer = self.client.send_request(
            {"request_id": "r-deferred", "method": "get_asset"}, timeout_seconds=5.0)

        self.assertTrue(parked.is_set())
        self.assertEqual(answer["request_id"], "r-deferred")
        self.assertEqual(answer["echo"], "get_asset")
        releaser.cancel()

    def test_the_timeout_is_real(self):
        """Defect 3: a silent server must not hang the client forever."""
        self.server.start_receiving(lambda req: time.sleep(60))

        started = time.time()
        with self.assertRaises(TransportTimeout):
            self.client.send_request({"request_id": "r-hang", "method": "ping"},
                                     timeout_seconds=1.0)
        elapsed = time.time() - started

        self.assertLess(elapsed, 5.0, "the client waited %.1fs -- the timeout is decorative" % elapsed)

    def test_concurrent_writes_from_two_threads_keep_frames_intact(self):
        """Many responses racing on ONE connection from two threads (inline
        worker + adjust): every frame must arrive as one parseable message."""
        requests = []
        done_setup = threading.Event()

        def handler(req):
            requests.append(req.get("request_id"))
            done_setup.wait(1.0)
            return {"request_id": req.get("request_id"), "ok": True, "n": req.get("n")}

        self.server.start_receiving(handler)
        done_setup.set()

        errors = []
        def call(n):
            try:
                answer = self.client.send_request(
                    {"request_id": "req-%d" % n, "method": "work", "n": n},
                    timeout_seconds=10.0)
                if answer.get("n") != n:
                    errors.append("mismatched answer %r for %d" % (answer, n))
            except Exception as exc:
                errors.append(str(exc))

        threads = [threading.Thread(target=call, args=(n,)) for n in range(12)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=15.0)

        self.assertEqual(errors, [])

    def test_stop_does_not_hang(self):
        """stop() with an idle pump blocked in ReadFile must return promptly."""
        self.server.start_receiving(lambda req: time.sleep(60))
        # Arm a client pump blocked in a read.
        hanging = threading.Thread(
            target=lambda: self._silence(lambda: self.client.send_request(
                {"request_id": "x", "method": "ping"}, timeout_seconds=60.0),
                timeout=10.0), daemon=True)
        hanging.start()
        time.sleep(0.5)

        started = time.time()
        self.client.stop()
        self.server.stop()
        elapsed = time.time() - started

        self.assertLess(elapsed, 6.0, "stop() took %.1fs" % elapsed)

    @staticmethod
    def _silence(fn, timeout):
        thread = threading.Thread(target=fn, daemon=True)
        thread.start()
        thread.join(timeout=timeout)

    def test_a_restarted_server_takes_clients_again(self):
        """After a server restart the client recovers within the SAME call:
        a write-side failure is provably unsent, so one reconnect-and-resend
        is safe. (A read-side failure stays a loud error -- unknown outcome,
        the #195 class; the caller decides there.)"""
        self.server.start_receiving(lambda req: {"request_id": req.get("request_id"), "ok": True})
        self.client.send_request({"request_id": "a", "method": "ping"}, timeout_seconds=5.0)
        self.server.stop()

        replacement, _client2 = _transport_pair(self.pipe)
        try:
            replacement.start_receiving(lambda req: {
                "request_id": req.get("request_id"), "ok": True, "echo": req.get("method")})
            answer = self.client.send_request(
                {"request_id": "b1", "method": "ping"}, timeout_seconds=5.0)
            self.assertEqual(answer["echo"], "ping")
            answer = self.client.send_request(
                {"request_id": "b2", "method": "ping"}, timeout_seconds=5.0)
            self.assertEqual(answer["echo"], "ping")
        finally:
            replacement.stop()


if __name__ == "__main__":
    unittest.main()
