import time
import typing
import asyncio
import threading
import logging
import aioudp
import queue

from pykcp.pykcp import Kcp


logging.basicConfig(level=logging.DEBUG)


def now_time():
    return int(time.time() * 1000) & 0xFFFFFFFF


def to_mills(v):
    return v / 1000


class MutexLock(object):

    def __init__(self) -> None:
        self._lock = threading.Lock()

    def lock(self):
        self._lock.acquire()

    def unlock(self):
        self._lock.release()


class Locker:

    def __init__(self, lock: MutexLock) -> None:
        self._lock = lock

    def __enter__(self) -> typing.Any:
        self._lock.lock()

    def __exit__(self, *args):
        self._lock.unlock()


class Waker(MutexLock):
    def __init__(self) -> None:
        self._future = None
        super().__init__()

    def set(self, future: asyncio.Future):
        with Locker(self):
            self._future = future

    def wake(self):
        with Locker(self):
            if self._future:
                future = self._future
                self._future = None
                future.set_result(())


class KcpCore(Kcp, MutexLock):

    _read_waker: typing.Optional[asyncio.Future] = None
    _write_waker: typing.Optional[asyncio.Future] = None
    _check_waker: typing.Optional[Waker] = None

    def __init__(self, conv, callback):
        self._last_check = now_time()
        self._lock = threading.Lock()
        self._output_callback = callback
        self._read_waker = None
        self._check_waker = None
        self._write_waker = None
        self._write_waker_lock = threading.Lock()
        self._read_waker_lock = threading.Lock()
        super().__init__(conv)
        self.wndsize(1024, 512)
        self.nodelay(1, 20, 2, 1)

    def update(self, current):
        return super().update(current)

    def check(self, current):
        last_check = self._last_check
        next_check = super().check(current)
        self._last_check = current
        return next_check - last_check

    def input(self, data):
        super().input(data)
        if self.readable():
            self._read_waker_lock.acquire()
            try:
                if self._read_waker:
                    self._read_waker.set_result(None)
                    self._read_waker = None
            finally:
                self._read_waker_lock.release()
        self._check_waker.wake()

    def output(self, buf):
        retval = self._output_callback(buf)
        if self.writeable():
            self._write_waker_lock.acquire()
            try:
                if self._write_waker:
                    self._write_waker.set_result(None)
            finally:
                self._write_waker_lock.release()
        return retval

    def recv(self, ispeek=False):
        with Locker(self):
            return super().recv(ispeek=False)

    def send(self, buf):
        while Locker(self):
            retval = super().send(buf)
            self.update(now_time())
            if self._check_waker:
                self._check_waker.wake()
            return retval

    def readable(self) -> bool:
        with Locker(self):
            return self.peeksize() > 0

    def writeable(self) -> bool:
        return True
    
    def updateable(self, now) -> bool:
        return now > self._last_check

    def set_read_future(self, future):
        self._read_waker_lock.acquire()
        try:
            self._read_waker = future
        finally:
            self._read_waker_lock.release()

    def set_write_future(self, future):
        self._read_waker_lock.acquire()
        try:
            self._write_waker = future
        finally:
            self._write_waker_lock.release()

    def set_check_waker(self, waker):
        self._check_waker = waker


class KcpStream(MutexLock):

    def __init__(self, core: KcpCore) -> None:
        self._core = core
        self._read_lock = asyncio.Lock()
        self._write_lock = asyncio.Lock()
        super().__init__()

    async def read(self) -> bytes:
        future = None

        if not self._core.readable():
            await self._read_lock.acquire()
            try:
                future = asyncio.Future()
                self._core.set_read_future(future)
            finally:
                self._read_lock.release()

        if future:
            await future

        with Locker(self):
            return await self._do_read_internal()

    async def write(self, buf) -> int:
        future = None

        if not self._core.writeable():
            await self._write_lock.acquire()
            try:
                future = asyncio.Future()
                self._core.set_write_future(future)
            finally:
                self._write_lock.release()

        if future:
            await future

        with Locker(self):
            return await self._do_write_internal(buf)

    async def _do_read_internal(self) -> bytes:
        return self._core.recv(ispeek=False)

    async def _do_write_internal(self, buf: bytes) -> int:
        return self._core.send(buf)


class Conv(object):
    def __init__(self) -> None:
        self._val = 1
        self._lock = threading.Lock()

    def next(self, f):
        self._lock.acquire()
        try:
            while not f(self._val):
                self._val += 1
            return self._val
        finally:
            self._lock.release()


class KcpPoller(object):
    _kcps: typing.List[KcpCore]

    def __init__(self) -> None:
        self._kcps = []
        self._check_lock = threading.Lock()
        self._running = None
        self._check_waker = Waker()

    def register(self, kcp: KcpCore):
        self._check_lock.acquire()
        try:
            kcp.set_check_waker(self._check_waker)
            self._kcps.append(kcp)
            self._check_waker.wake()
        finally:
            self._check_lock.release()

    def _calc_next_check(self) -> typing.Optional[float]:
        self._check_lock.acquire()
        next_check = None
        try:
            min_check = None
            for kcp in self._kcps:
                next_check = kcp.check(now_time())
                if min_check:
                    if min_check > next_check:
                        min_check = next_check
                else:
                    min_check = next_check
            return min_check
        finally:
            self._check_lock.release()

    async def run(self):
        async def __start_check_poll():

            logging.debug("kcp check poller started")

            while True:
                future = None
                next_check = self._calc_next_check()

                futures = []

                future = asyncio.Future()

                self._check_waker.set(future)

                if self._kcps:
                    now = to_mills(next_check)
                    # logging.debug("next time {}ms".format(now))
                    futures.append(asyncio.shield(asyncio.sleep(now)))

                futures.append(future)

                await asyncio.wait(futures, return_when=asyncio.FIRST_COMPLETED)

                for fut in futures:
                    if not fut.done():
                        fut.cancel()

                now = now_time()
                for kcp in self._kcps:
                    if not kcp.updateable(now):
                        continue
                    kcp.update(now)

        self._running = asyncio.create_task(__start_check_poll())


class KcpConnector(object):
    _streams: typing.Dict[int, KcpCore]

    def __init__(self, endpoint: aioudp.Endpoint) -> None:
        self._lcok = asyncio.Lock()
        self._streams = {}
        self._poller = KcpPoller()
        self._conv = Conv()
        self._endpoint = endpoint

    def __output_callback(self, data):
        self._endpoint.send(data)

    async def open(self) -> KcpStream:
        await self._lcok.acquire()
        try:
            kcp_core = KcpCore(
                self._conv.next(lambda v: v not in self._streams),
                self.__output_callback,
            )
            self._poller.register(kcp_core)
            self._streams[kcp_core.conv] = kcp_core
            return KcpStream(kcp_core)
        finally:
            self._lcok.release()

    async def __poll_stream_recv(self):

        while True:
            data = await self._endpoint.receive()
            conv = Kcp.getconv(data)
            try:
                await self._lcok.acquire()
                if conv not in self._streams:
                    logging.debug("invalid packet {}".format(data))
                    continue
                self._streams[conv].input(data)
            finally:
                self._lcok.release()

    async def __aenter__(self) -> "KcpConnector":

        await self._poller.run()

        asyncio.create_task(self.__poll_stream_recv())

        return self

    async def __aexit__(self, *args):
        print(args)


class KcpListener(object):
    _streams: typing.Dict[str, typing.Dict[int, KcpCore]]

    def __init__(self, endpoint: aioudp.Endpoint) -> None:
        self._lcok = asyncio.Lock()
        self._poller = KcpPoller()
        self._receiver = asyncio.Queue()
        self._endpoint = endpoint
        self._streams = {}

    async def accept(self) -> KcpStream:
        return await self._receiver.get()

    async def __poll_stream_recv(self):
        while True:
            (data, address) = await self._endpoint.receive()

            kid = hash(address)
            conv = Kcp.getconv(data)

            await self._lcok.acquire()

            try:

                if kid not in self._streams:
                    self._streams[kid] = {}

                streams = self._streams[kid]

                if conv not in streams:

                    def do_send(data):
                        self._endpoint.send(data, address)

                    kcp_core = KcpCore(conv, do_send)
                    streams[conv] = kcp_core

                    self._poller.register(kcp_core)

                    await self._receiver.put(KcpStream(kcp_core))

                streams[conv].input(data)

            finally:
                self._lcok.release()

    async def __aenter__(self) -> "KcpListener":

        await self._poller.run()

        asyncio.create_task(self.__poll_stream_recv())

        return self

    async def __aexit__(self, *args):
        print(args)
