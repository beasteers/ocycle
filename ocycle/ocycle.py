import time
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from .base import BufferCycle
from .baseio import NumpyIO, ListIO
from .util import truncate_io, FakePool

SERIAL = 'serial'
THREAD = 'thread'
PROCESS = 'process'

Pools = {
    SERIAL: FakePool,
    THREAD: ThreadPoolExecutor,
    PROCESS: ProcessPoolExecutor
}

class BufferEmit(BufferCycle):
    '''Collect bytes in a buffer and call a function in a thread or process once it
    is of a certian size.

    Here's the basics of how it works:
    >>> import ocycle
    >>> def test(x, t0):
    ...:     print('func', x, len(x), t0)
    ...:     time.sleep(2)
    ...:     print(t0, 'done')
    >>> cycle = ocycle.BufferEmit(test, 10)
    >>> while True:
    ...:     b.write(b'asdfq ')
    ...:     time.sleep(1)

    Arguments:
        callback (callable): the function to call when the buffer fills up
        size (int): How big should the buffer be before calling?
        asbuffer (bool): Whether to send the buffer object or the buffer value.
        clip (bool): When a buffer grows greater than `size`, it will pass
            the entire buffer with len(value) >= size. If `clip` is set to True, it
            will truncate at the buffer at `size` and write the remainder to the next
            buffer in the queue. If a sampler is specified, then the remainder will be
            dropped (to avoid jumps in the data).
        sampler (callable, float, optional): By default, the buffers are collected
            continuously. You can provide a function or a static value which can
            return the time between when the callback is called and when it starts
            collecting in the buffer again.
        mode (str): Whether to use a processes vs threads vs serial.
        npool (int): The max workers in the process/thread pool.

    '''
    pool = mode = None
    t0 = pause_until = 0 # is set on the first write
    def __init__(self, callback, size, *a, asbuffer=False, clip=False, sampler=None,
                 mode=SERIAL, npool=10, on_done=None, **kw):
        self.callback = callback
        self.on_done = on_done
        self.size = size
        self.asbuffer = asbuffer
        self.clip_value = clip
        # optional, stochastic silence sampler
        self.sampler = (
            sampler if callable(sampler) else
            (lambda: sampler) if sampler else 0)
        super().__init__(*a, **kw)
        # whether to use a process or a thread
        self.open(npool=npool, mode=mode)

    def __repr__(self):
        return '<{}({}) size={} n={} mode={} asbuffer={} clip={}>'.format(
            self.__class__.__name__, self.callback.__qualname__, self.size, len(self.items),
            self.mode, self.asbuffer, self.clip_value)

    def open(self, npool=None, mode=None, reopen=False):
        if reopen:
            self.close()
        if not self.pool:
            self.mode = mode or self.mode
            self.npool = npool or self.npool
            self.pool = Pools[self.mode](max_workers=self.npool)

    def close(self, wait=True):
        if self.pool:
            self.pool.shutdown(wait=wait)
            self.pool = None

    def __enter__(self):
        self.open()

    def __exit__(self, *a, **kw):
        self.close()

    def __del__(self):
        self.close()

    def __getstate__(self):
        return dict(self.__dict__, pool=None)

    def __call(self):
        # get buffer, and possibly dump the buffer value
        value = buff = self.current
        leftover = truncate_io(buff, self.size) if self.clip_value else None
        if self.sampler: # don't store the leftover if we're going to have a jump in the data.
            leftover = None

        if not self.asbuffer:
            value = buff.getvalue()
            # if we're sending the value, we can reuse the buffer right away
            self.reuse(buff)

        # call the function in a thread/process
        fut = self.pool.submit(self.callback, value, self.t0)
        fut.add_done_callback(self.__on_done)
        if self.asbuffer:
            fut.add_done_callback(lambda fut: self.reuse(buff))

        # ready the next buffer
        self.next(leftover)

    def __on_done(self, fut):
        res = fut.result()
        if self.on_done:
            self.on_done(res)

    @property
    def full(self):
        '''Has the buffer exceeded the specified size?'''
        return int(self.tell()) >= self.size

    @property
    def is_pausing(self):
        '''Are we currently pausing data collection for data sparsity?'''
        return time.time() < self.pause_until

    def write(self, data, t0=None):
        '''Write a frame to the data buffer. Optionally, specify a timestamp for the frame.
        Otherwise, it will use the current timestamp.

        Arguments:
            data (bytes): the data buffer
            t0 (float): the timestamp associated with the frame. Defaults to time.time().
        '''
        t0 = t0 or time.time()
        if t0 < self.pause_until:
            return

        self.t0 = self.t0 or t0 # sets t0 on the first write
        self.current.write(data)
        while self.full: # leftover is cleared if sampler is truthy so, no double buffers when sampling
            self.__call()

            self.t0 = t0
            self.pause_until = t0 + self.sampler() if self.sampler else 0

    def truncate(self, size=None):
        '''Truncate the current buffer to the specified size. Returns the overhang.'''
        return truncate_io(self.current, size or self.size)


class NumpyEmit(BufferEmit):
    new = NumpyIO

class ListEmit(BufferEmit):
    new = ListIO
