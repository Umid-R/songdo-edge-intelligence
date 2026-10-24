"""IPC primitives: BoundedQueue (drop_oldest) and Unix domain socket framing."""
import json
import multiprocessing as mp
import os
import queue
import socket
import struct
import threading
import time

STOP = "__STOP__"   # shutdown sentinel


class StopFlag:
    """Cross-process stop flag with the same API as multiprocessing.Event (set / is_set / wait).

    Why not multiprocessing.Event? If a process is SIGKILLed while blocked in Event.wait(),
    the Event's internal condition keeps a phantom sleeper and a later Event.set() blocks
    forever, so the supervisor could never shut down after a crash test. This flag is a raw
    shared integer: there is no lock to poison.
    """

    def __init__(self, ctx=None):
        ctx = ctx or mp.get_context("spawn")
        self._v = ctx.RawValue("i", 0)

    def set(self):
        self._v.value = 1

    def is_set(self):
        return bool(self._v.value)

    def wait(self, timeout=None):
        end = None if timeout is None else time.monotonic() + timeout
        while not self._v.value:
            remaining = None if end is None else end - time.monotonic()
            if remaining is not None and remaining <= 0:
                break
            time.sleep(0.02 if remaining is None else min(0.02, remaining))
        return bool(self._v.value)


class BoundedQueue:
    """Process-safe queue with a hard cap. When full, the OLDEST item is dropped.

    Bounded memory is the point: an unbounded queue grows RSS until the Linux
    OOM killer steps in.
    """

    def __init__(self, maxsize=500, ctx=None):
        ctx = ctx or mp.get_context("spawn")
        self.maxsize = maxsize
        self._q = ctx.Queue(maxsize)
        self._lock = ctx.Lock()
        self._dropped = ctx.Value("q", 0)

    def put(self, item):
        with self._lock:
            while True:
                try:
                    self._q.put_nowait(item)
                    return
                except queue.Full:
                    try:
                        self._q.get_nowait()          # drop the oldest
                        with self._dropped.get_lock():
                            self._dropped.value += 1
                    except queue.Empty:
                        time.sleep(0.001)             # feeder thread still flushing

    def get(self, timeout=None):
        return self._q.get(timeout=timeout)           # raises queue.Empty

    def qsize(self):
        return self._q.qsize()

    @property
    def dropped(self):
        return self._dropped.value


# ---- Unix domain sockets: length-prefixed JSON frames ----

def send_msg(sock, obj):
    data = json.dumps(obj).encode("utf-8")
    sock.sendall(struct.pack("!I", len(data)) + data)


def _recv_exact(sock, n):
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ConnectionError("socket closed")
        buf += chunk
    return buf


def recv_msg(sock):
    (length,) = struct.unpack("!I", _recv_exact(sock, 4))
    return json.loads(_recv_exact(sock, length).decode("utf-8"))


class UnixSocketServer:
    """Tiny request/response server. handler(obj) -> reply obj."""

    def __init__(self, path, handler):
        self.path = path
        self.handler = handler
        self._stop = threading.Event()
        self._sock = None
        self._thread = None

    def start(self):
        if os.path.exists(self.path):
            os.unlink(self.path)
        self._sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self._sock.bind(self.path)
        self._sock.listen(8)
        self._sock.settimeout(0.2)
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()

    def _loop(self):
        while not self._stop.is_set():
            try:
                conn, _ = self._sock.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            with conn:
                try:
                    send_msg(conn, self.handler(recv_msg(conn)))
                except (ConnectionError, OSError, ValueError):
                    pass

    def stop(self):
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=2)
        if self._sock:
            self._sock.close()
        if os.path.exists(self.path):
            os.unlink(self.path)


def unix_request(path, obj, timeout=2.0):
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as s:
        s.settimeout(timeout)
        s.connect(path)
        send_msg(s, obj)
        return recv_msg(s)
