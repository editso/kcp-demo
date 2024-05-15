import time
import typing
import asyncio
import threading
import logging
import aioudp

logging.basicConfig(level=logging.DEBUG)

try:
    import lkcp

    class KcpCompat(lkcp.KcpObj):
        def __init__(self, conv):
            super().__init__(conv, 0, lambda a, b: self.output(b))

        def output(self):
            raise NotImplemented
        
        def recv(self):
            return super().recv()[1]
        
except:
    from pykcp.pykcp import Kcp

    class KcpCompat(Kcp):
        def __init__(self, conv):
            super().__init__(conv)


def now_time():
    return int(time.time() * 1000) & 0xFFFFFFFF

def to_mills(v):
    return v / 1000

def throw_if_non_none(ex, v, exc):
    if ex:
        logging.error(f"{ex.__name__}: {v}", exc_info=exc)


class MutexLock(object):

    def __init__(self) -> None:
        self.__lock_inner = threading.Lock()

    def lock(self):
        self.__lock_inner.acquire()

    def unlock(self):
        self.__lock_inner.release()

class AsyncMutexLock(object):
    def __init__(self) -> None:
        self.__lock_inner = asyncio.Lock()

    async def lock(self):
        await self.__lock_inner.acquire()

    async def unlock(self):
        self.__lock_inner.release()

class Locker:

    def __init__(self, lock: typing.Union[MutexLock, AsyncMutexLock]) -> None:
        self.__mutex_lock = lock

    def __enter__(self) -> typing.Any:
        self.__mutex_lock.lock()

    def __exit__(self, *args):
        self.__mutex_lock.unlock()
        throw_if_non_none(*args)
        
    async def __aenter__(self):
        await self.__mutex_lock.lock()

    async def __aexit__(self, *args):
        await self.__mutex_lock.unlock()
        throw_if_non_none(*args)


class Atomic(MutexLock):

    def __init__(self, val=None) -> None:
        super().__init__()
        self._val = val

    def swap(self, new=None) -> typing.Optional[typing.Any]:
        with Locker(self):
            old = self._val
            self._val = new
            return old

    def take(self) -> typing.Optional[typing.Any]:
        return self.swap()

class Waiter:
    def __init__(self) -> None:
        self._waiter = Atomic()
        super().__init__()

    def set(self, future: asyncio.Future):
        self._waiter.swap(future)

    def wake(self):
        fut: typing.Optional[asyncio.Future] = self._waiter.take()
        if fut and not fut.cancelled():
            fut.set_result(None)

class KcpCore(KcpCompat, MutexLock):

    def __init__(self, conv, callback):
        MutexLock.__init__(self)
        KcpCompat.__init__(self, conv)
        
        self.conv = conv
        self._last_check = now_time()
        self._output_callback = callback
        
        self._read_waiter = Waiter()
        self._write_waiter = Waiter()
        self._check_waiter = Waiter()

        self.snd_wnd = 1024
             
        self.wndsize(self.snd_wnd, 1024)
        self.nodelay(1, 15, 1, 1)

    def update(self, current):
        return super().update(current)

    def check(self, current):
        last_check = self._last_check
        next_check = super().check(current)
        self._last_check = current
        return next_check - last_check

    def input(self, data):
        retval = super().input(data)
        if self.readable():
            self._read_waiter.wake()
        self._check_waiter.wake()
        return retval

    def output(self, buf):
        retval = self._output_callback(buf)
        if self.writeable():
            self._read_waiter.wake()
            self._write_waiter.wake()
        return retval

    def recv(self):
        with Locker(self):
            data = super().recv()
            self._write_waiter.wake()
            self._check_waiter.wake()
            return data

    def send(self, buf):
        while Locker(self):
            retval = super().send(buf)
            self.update(now_time())
            self.flush()
            self._check_waiter.wake()
            return retval

    def readable(self) -> bool:
        with Locker(self):
            return self.peeksize() > 0

    def writeable(self) -> bool:
        with Locker(self):
            return self.waitsnd() < self.snd_wnd * 2
    
    def updateable(self, now) -> bool:
        with Locker(self):
            return now > self._last_check

    def set_read_waiter(self, waiter):
        self._read_waiter.set(waiter)

    def set_write_waiter(self, waiter):
        self._write_waiter.set(waiter)

    def set_check_waiter(self, waiter):
        self._check_waiter = waiter


class KcpStream(AsyncMutexLock):

    def __init__(self, core: KcpCore) -> None:
        super().__init__()
        self._core = core
        self._read_lock = asyncio.Lock()
        self._write_lock = asyncio.Lock()
    
    async def read(self) -> bytes:
        while not self._core.readable():
            waiter = asyncio.Future()
            
            await self._read_lock.acquire()

            self._core.set_read_waiter(waiter)

            try:
                await waiter
            finally:
                self._read_lock.release()
                            
        async with Locker(self):
            return await self._do_read_internal()

    async def write(self, buf) -> int:

        while not self._core.writeable():
            waiter = asyncio.Future()

            await self._write_lock.acquire()

            self._core.set_write_waiter(waiter)
            
            try:   
                await waiter
            finally:
                self._write_lock.release()
                
        async with Locker(self):
            return await self._do_write_internal(buf)
        
    async def _do_read_internal(self) -> bytes:
        return self._core.recv()

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
    _kcp_sessions: typing.List[KcpCore]

    def __init__(self) -> None:
        self._kcp_sessions = []
        self._check_lock = threading.Lock()
        self._check_waiter = Waiter()

    def register(self, kcp: KcpCore):
        self._check_lock.acquire()
        try:
            kcp.set_check_waiter(self._check_waiter)
            self._kcp_sessions.append(kcp)
            self._check_waiter.wake()
        finally:
            self._check_lock.release()

    def _calc_next_check(self) -> typing.Optional[float]:
        self._check_lock.acquire()
        next_check = None
        try:
            min_check = None
            for kcp in self._kcp_sessions:
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
                waiter = None
                next_check = self._calc_next_check()

                futures = []

                waiter = asyncio.Future()

                self._check_waiter.set(waiter)

                if self._kcp_sessions:
                    now = to_mills(next_check)
                    futures.append(asyncio.shield(asyncio.sleep(now)))

                futures.append(waiter)
                                
                await asyncio.wait(futures, return_when=asyncio.FIRST_COMPLETED)
                
                for fut in futures:
                    if not fut.done():
                        fut.cancel()

                now = now_time()

                for kcp in self._kcp_sessions:
                    if kcp.updateable(now):
                        kcp.update(now)

        await __start_check_poll()


class KcpConnector(AsyncMutexLock):
    _tasks: typing.List[asyncio.Task]
    _streams: typing.Dict[int, KcpCore]

    def __init__(self, endpoint: aioudp.Endpoint) -> None:
        AsyncMutexLock.__init__(self)
        
        self._conv = Conv()
        self._tasks = []
        self._poller = KcpPoller()
        self._streams = {}
        self._endpoint = endpoint

    def __output_callback(self, data):
        self._endpoint.send(data)

    async def open(self) -> KcpStream:
       async with Locker(self):
            kcp_core = KcpCore(
                    self._conv.next(lambda v: v not in self._streams),
                    self.__output_callback,
            )
            self._poller.register(kcp_core)
            self._streams[kcp_core.conv] = kcp_core
            return KcpStream(kcp_core)

    async def __poll_stream_recv(self):
        try:
            while True:
                data = await self._endpoint.receive()
                conv = KcpCompat.getconv(data)
                async with Locker(self):
                    if conv not in self._streams:
                        logging.debug("invalid packet {}".format(data))
                        continue
                    self._streams[conv].input(data)
        except Exception as e:
            logging.error("__poll_stream_recv", exc_info=e)

    async def __aenter__(self) -> "KcpConnector":

        fut = asyncio.wait([ asyncio.shield(self._poller.run()), asyncio.shield(self.__poll_stream_recv()) ])

        self._tasks.append(asyncio.create_task(fut))
        
        return self

    async def __aexit__(self, *args):
        for tsk in self._tasks:
            if not tsk.done():
               tsk.cancel()
               
        throw_if_non_none(*args)


class KcpListener(AsyncMutexLock):
    _tasks: typing.List[asyncio.Task]
    _streams: typing.Dict[str, typing.Dict[int, KcpCore]]

    def __init__(self, endpoint: aioudp.Endpoint) -> None:
        AsyncMutexLock.__init__(self)
        
        self._tasks = []
        self._poller = KcpPoller()
        self._receiver = asyncio.Queue()
        self._endpoint = endpoint
        self._streams = {}

    async def accept(self) -> KcpStream:
        return await self._receiver.get()

    async def __poll_stream_recv(self):
        try:
            while True:
                (data, address) = await self._endpoint.receive()
 
                kid = hash(address)
                conv = KcpCompat.getconv(data)
                
                async with Locker(self):
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
        except Exception as e:
            logging.error("__poll_stream_recv", exc_info=e)
                
    async def __aenter__(self) -> "KcpListener":

        fut = asyncio.wait([ asyncio.shield(self._poller.run()), asyncio.shield(self.__poll_stream_recv()) ])
            
        self._tasks.append(asyncio.create_task(fut))
 
        return self

    async def __aexit__(self, *args):
    
        for tsk in self._tasks:
            if not tsk.done():
               tsk.cancel()

        throw_if_non_none(*args)