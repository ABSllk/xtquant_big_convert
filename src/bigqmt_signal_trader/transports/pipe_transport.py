# coding: utf-8
"""Windows named-pipe transport (same-host, zero third-party dependencies).

Why this exists
---------------
Some broker QMT builds enforce an import whitelist that rejects ``socket``
(directly and indirectly -- ``logging.handlers`` pulled it in once) and forbid
``pip install`` into the bundled Python. On those terminals neither the Redis
client nor ``pyzmq`` can be installed, so the bridge has no wire at all.

A named pipe needs neither: ``ctypes.WinDLL("kernel32")`` is standard library,
and named pipes are not sockets, so they sit outside the whitelist that blocks
networking. Same trick the cfquant project uses.

What it buys, measured
----------------------
Raw round trip on this machine, 108-byte payload, 500 iterations::

    named pipe    median 0.012ms
    zmq REQ/REP   median 0.109ms
    redis list    median 0.798ms

66x faster than Redis **at the wire**. But the wire is not where the time goes:
end-to-end RPC against the live bridge measures 3-12ms median, so replacing
Redis with a pipe saves ~0.79ms of that -- 25% on the fastest calls, 7% on the
slowest. Pick this transport for the *dependency* story, not the speed story.

Limits
------
Windows only, same host only. No cross-machine, no Linux client, no Docker.
Deployments that need those keep Redis or ZMQ.
"""

import ctypes
import json
import os
import threading
import time

from .base import RpcTransport, TransportError, TransportTimeout
from ..adapters.redis_common import decode_text


DEFAULT_PIPE_NAME = "bigqmt_rpc"
_PIPE_ROOT = "\\\\.\\pipe\\"

# kernel32 constants (winbase.h)
PIPE_ACCESS_DUPLEX = 0x00000003
PIPE_TYPE_MESSAGE = 0x00000004
PIPE_READMODE_MESSAGE = 0x00000002
PIPE_WAIT = 0x00000000
PIPE_UNLIMITED_INSTANCES = 255
GENERIC_READ = 0x80000000
GENERIC_WRITE = 0x40000000
OPEN_EXISTING = 3
INVALID_HANDLE_VALUE = ctypes.c_void_p(-1).value
ERROR_PIPE_CONNECTED = 535
ERROR_PIPE_BUSY = 231
ERROR_MORE_DATA = 234
ERROR_OPERATION_ABORTED = 995
ERROR_INVALID_HANDLE = 6
ERROR_BROKEN_PIPE = 109

_BUFFER_BYTES = 1 << 20      # 1MB: whole-market quote frames are large
_CONNECT_POLL_SECONDS = 0.05


def _kernel32():
    if os.name != "nt":
        raise TransportError(
            "named-pipe transport is Windows-only (os.name=%r). Use redis or "
            "zmq for cross-platform deployments." % os.name)
    from ctypes import wintypes

    dll = ctypes.WinDLL("kernel32", use_last_error=True)
    dll.CreateNamedPipeW.restype = wintypes.HANDLE
    dll.CreateFileW.restype = wintypes.HANDLE
    return dll, wintypes


def pipe_path(name=DEFAULT_PIPE_NAME, account_id=""):
    """Full pipe path. The account id is part of the name so two bridges --
    one live account, one simulated -- never share a wire (the same mistake
    that gave two deployments one log file, #144)."""
    suffix = ("_" + str(account_id)) if account_id else ""
    return _PIPE_ROOT + str(name or DEFAULT_PIPE_NAME) + suffix


class NamedPipeTransport(RpcTransport):
    """Request/response over a Windows named pipe in message mode.

    Message mode (not byte mode) is deliberate: each WriteFile is one message
    and each ReadFile returns exactly one, so there is no framing protocol to
    get wrong. A message larger than the read buffer surfaces as
    ERROR_MORE_DATA rather than a silently truncated payload.
    """

    name = "pipe"

    def __init__(self, account_id="", print_prefix="[bigqmt_rpc]",
                 pipe_name=DEFAULT_PIPE_NAME, connect_timeout_seconds=5.0,
                 **kwargs):
        super(NamedPipeTransport, self).__init__(
            account_id=account_id, print_prefix=print_prefix)
        self.pipe_name = str(pipe_name or DEFAULT_PIPE_NAME)
        self.connect_timeout_seconds = float(connect_timeout_seconds)
        self.path = pipe_path(self.pipe_name, account_id)
        self._k32 = None
        self._wintypes = None
        self._listener = None
        self._server_handles = []
        self._server_lock = threading.RLock()
        # Per-thread client handle. A single shared handle would serialise every
        # caller behind one round trip -- exactly the bug #186 fixed for the ZMQ
        # DEALER, and the reason that fix is not worth repeating here.
        self._client_local = threading.local()
        self._client_handles = []
        # 读缓冲也按线程持有 —— 见 _read_buffer 里的实测数字。
        self._io_local = threading.local()

    # -- ctypes plumbing ---------------------------------------------------
    def _dll(self):
        if self._k32 is None:
            self._k32, self._wintypes = _kernel32()
        return self._k32

    def _last_error(self):
        return ctypes.get_last_error()

    def _write(self, handle, payload):
        dll = self._dll()
        written = self._wintypes.DWORD()
        ok = dll.WriteFile(handle, payload, len(payload),
                           ctypes.byref(written), None)
        if not ok:
            raise TransportError("WriteFile failed (err=%s)" % self._last_error())
        return written.value

    def _read_buffer(self):
        """按线程复用读缓冲。

        原来每次 _read 都 create_string_buffer(1MB) —— 分配并清零 1MB，一次
        往返两侧各一次就是 2MB。实测：裸管道往返 0.012ms、两侧 JSON 合计
        0.010ms，预期总共 0.022ms，而传输层实测中位 0.765ms —— 多出来的
        0.74ms 全在这里。缓冲区按线程持有一份即可，读多少用多少。
        """
        buf = getattr(self._io_local, "buf", None)
        if buf is None:
            buf = ctypes.create_string_buffer(_BUFFER_BYTES)
            self._io_local.buf = buf
        return buf

    def _read(self, handle):
        dll = self._dll()
        buf = self._read_buffer()
        read = self._wintypes.DWORD()
        chunks = []
        while True:
            ok = dll.ReadFile(handle, buf, _BUFFER_BYTES, ctypes.byref(read), None)
            # string_at 只拷实际读到的字节。buf.raw 会先把整个 1MB 缓冲区
            # 复制成 bytes 再切片 —— 和上面那个每次分配 1MB 是同一类错误，
            # 实测占掉往返的一半时间。
            if read.value:
                chunks.append(ctypes.string_at(buf, read.value))
            if ok:
                break
            err = self._last_error()
            if err == ERROR_MORE_DATA:
                continue
            # 停机路径：CancelIoEx 取消了这次读、句柄被关掉、或对端断开。
            # 这些都是正常收尾，不该当成错误抛出去把日志刷满。
            if err in (ERROR_OPERATION_ABORTED, ERROR_INVALID_HANDLE,
                       ERROR_BROKEN_PIPE) or not self._running:
                return b""
            if not chunks or not chunks[0]:
                return b""
            raise TransportError("ReadFile failed (err=%s)" % err)
        return b"".join(chunks)

    # -- client side -------------------------------------------------------
    def _client_handle(self):
        handle = getattr(self._client_local, "handle", None)
        if handle is not None:
            return handle
        dll = self._dll()
        deadline = time.time() + self.connect_timeout_seconds
        while True:
            handle = dll.CreateFileW(self.path, GENERIC_READ | GENERIC_WRITE,
                                     0, None, OPEN_EXISTING, 0, None)
            if handle != INVALID_HANDLE_VALUE:
                break
            err = self._last_error()
            if err != ERROR_PIPE_BUSY or time.time() >= deadline:
                raise TransportError(
                    "cannot connect to %s (err=%s). Is the QMT-side strategy "
                    "running with transport=pipe?" % (self.path, err))
            time.sleep(_CONNECT_POLL_SECONDS)
        mode = self._wintypes.DWORD(PIPE_READMODE_MESSAGE)
        dll.SetNamedPipeHandleState(handle, ctypes.byref(mode), None, None)
        self._client_local.handle = handle
        with self._server_lock:
            self._client_handles.append(handle)
        return handle

    def send_request(self, request, timeout_seconds):
        handle = self._client_handle()
        payload = json.dumps(request, ensure_ascii=False, default=str).encode("utf-8")
        started = time.time()
        try:
            self._write(handle, payload)
            raw = self._read(handle)
        except TransportError:
            # A broken pipe means the bridge restarted. Drop the handle so the
            # next call reconnects instead of failing forever on a dead one.
            self._drop_client_handle()
            raise
        if not raw:
            self._drop_client_handle()
            raise TransportTimeout(
                "named pipe closed while waiting for %s"
                % (request or {}).get("method"))
        if timeout_seconds and (time.time() - started) > float(timeout_seconds):
            raise TransportTimeout(
                "named pipe rpc timeout: %s" % (request or {}).get("method"))
        return json.loads(decode_text(raw))

    def _drop_client_handle(self):
        handle = getattr(self._client_local, "handle", None)
        if handle is None:
            return
        try:
            self._dll().CloseHandle(handle)
        except Exception:
            pass
        self._client_local.handle = None
        with self._server_lock:
            if handle in self._client_handles:
                self._client_handles.remove(handle)

    # -- server side -------------------------------------------------------
    def start_receiving(self, on_request, **kwargs):
        super(NamedPipeTransport, self).start_receiving(on_request)
        self._listener = threading.Thread(
            target=self._accept_loop, name="bigqmt-pipe-accept")
        self._listener.daemon = True
        self._listener.start()

    def _accept_loop(self):
        dll = self._dll()
        while self._running:
            handle = dll.CreateNamedPipeW(
                self.path, PIPE_ACCESS_DUPLEX,
                PIPE_TYPE_MESSAGE | PIPE_READMODE_MESSAGE | PIPE_WAIT,
                PIPE_UNLIMITED_INSTANCES, _BUFFER_BYTES, _BUFFER_BYTES, 0, None)
            if handle == INVALID_HANDLE_VALUE:
                if not self._running:
                    return
                time.sleep(_CONNECT_POLL_SECONDS)
                continue
            with self._server_lock:
                self._server_handles.append(handle)
            connected = dll.ConnectNamedPipe(handle, None)
            if not connected and self._last_error() != ERROR_PIPE_CONNECTED:
                self._close_server_handle(handle)
                continue
            if not self._running:
                self._close_server_handle(handle)
                return
            worker = threading.Thread(
                target=self._serve_connection, args=(handle,),
                name="bigqmt-pipe-conn")
            worker.daemon = True
            worker.start()

    def _serve_connection(self, handle):
        try:
            while self._running:
                try:
                    raw = self._read(handle)
                except TransportError:
                    break
                if not raw:
                    break
                try:
                    request = json.loads(decode_text(raw))
                except Exception:
                    break
                # Remember which handle to answer on. send_response reads it
                # back, so a handler that replies inline reaches the right peer
                # even with several clients connected at once.
                request["_pipe_handle"] = handle
                self.deliver(request)
        finally:
            self._close_server_handle(handle)

    def send_response(self, request, response):
        handle = (request or {}).get("_pipe_handle")
        if handle is None:
            raise TransportError("no pipe handle on the request to reply to")
        payload = json.dumps(response, ensure_ascii=False, default=str).encode("utf-8")
        self._write(handle, payload)

    def _close_server_handle(self, handle):
        # 先把句柄从表里摘掉，摘到的那个线程才负责真正关闭。stop() 和工作线程
        # 的 finally 会同时走到这里，重复 CloseHandle 会关掉一个已被回收复用的
        # 句柄 —— 那种 bug 只会在高并发下偶发，最难查。
        with self._server_lock:
            if handle not in self._server_handles:
                return
            self._server_handles.remove(handle)
        dll = self._dll()
        # **必须先取消挂起的 I/O。** DisconnectNamedPipe 会等同一句柄上挂起的
        # 同步 ReadFile 完成，而工作线程正阻塞在那个 ReadFile 上等下一个请求 ——
        # 谁也等不到谁，主线程和 20 个工作线程一起卡死（实测线程栈确认）。
        # CancelIoEx 让那次读带 ERROR_OPERATION_ABORTED 返回，循环随即退出。
        try:
            dll.CancelIoEx(handle, None)
        except Exception:
            pass
        try:
            dll.DisconnectNamedPipe(handle)
        except Exception:
            pass
        try:
            dll.CloseHandle(handle)
        except Exception:
            pass

    def stop(self):
        self._running = False
        with self._server_lock:
            server = list(self._server_handles)
            clients = list(self._client_handles)
        # Unblock the accept loop: it is parked in ConnectNamedPipe, which only
        # returns once somebody connects. Connect to our own pipe once.
        if server:
            try:
                dll = self._dll()
                handle = dll.CreateFileW(self.path, GENERIC_READ | GENERIC_WRITE,
                                         0, None, OPEN_EXISTING, 0, None)
                if handle != INVALID_HANDLE_VALUE:
                    dll.CloseHandle(handle)
            except Exception:
                pass
        for handle in server:
            self._close_server_handle(handle)
        for handle in clients:
            try:
                self._dll().CloseHandle(handle)
            except Exception:
                pass
        with self._server_lock:
            self._client_handles = []
        self._client_local = threading.local()
        self._io_local = threading.local()
        super(NamedPipeTransport, self).stop()
