import time
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from .base import BuffReCycle
from .util import truncate_io

SERIAL = 'serial'
THREAD = 'thread'
PROCESS = 'process'

class BufferEmit(BuffReCycle):
    '''Collect bytes in a buffer and call a function in a thread or process once it
    is of a certian size.

    Here's the basics of how it works:
    >>> import recycle
    >>> def test(x, t0):
    ...:     print('func', x, len(x), t0)
    ...:     time.sleep(2)
    ...:     print(t0, 'done')
    >>> cycle = recycle.BufferEmit(test, 10)
    >>> while True:
    ...:     b.write(b'asdfq ')
    ...:     time.sleep(1)

    Arguments:
        callback (callable): the function to call when the buffer fills up
        size (int): How big should the buffer be before calling?
        value (bool): Whether to send the buffer value or the entire buffer
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
    pool = None
    def __init__(self, callback, size, *a, value=False, clip=False, sampler=None,
                 mode=SERIAL, npool=10, on_done=None, **kw):
        self.callback = callback
        self.on_done = on_done
        self.size = size
        self.send_value = value
        self.clip_value = clip
        # whether to use a process or a thread
        self.mode = mode
        if mode != SERIAL:
            Pool = ProcessPoolExecutor if mode == PROCESS else ThreadPoolExecutor
            self.pool = Pool(max_workers=npool)

        # optional, stochastic silence sampler
        self.sampler = sampler
        self.t0 = self.pause_until = time.time()
        super().__init__(*a, **kw)

    def __repr__(self):
        return '<{}({}) size={} n={} process={} value={} clip={}>'.format(
            self.__class__.__name__, self.callback.__qualname__, self.size, len(self.items),
            self.process, self.send_value, self.clip_value)

    def __getstate__(self):
        return dict(self.__dict__, pool=None)

    def __call(self):
        # get buffer, and possibly dump the buffer value
        value = buff = self.current
        leftover = truncate_io(buff, self.size) if self.clip_value else None
        if self.sampler: # don't store the leftover if we're going to have a jump in the data.
            leftover = None

        if self.send_value:
            value = buff.getvalue()
            # if we're sending the value, we can reuse the buffer right away
            self.reuse(buff)

        if self.mode == SERIAL:
            res = self.callback(value, self.t0)
            if self.on_done:
                self.on_done(res)
        else:
            # call the function in a thread/process
            fut = self.pool.submit(self.callback, value, self.t0)
            fut.add_done_callback(self.__on_done)
            if not self.send_value:
                fut.add_done_callback(lambda fut: self.reuse(buff))

        # ready the next buffer
        self.next(leftover)

    def __on_done(self, fut):
        res = fut.result() if fut else None
        if self.on_done:
            self.on_done(res)

    def write(self, data, t0=None):
        t0 = t0 or time.time()
        if t0 < self.pause_until:
            return

        self.current.write(data)
        if int(self.tell()) >= self.size:
            self.__call()

            self.t0 = t0
            self.pause_until = t0 + (
                self.sampler() if callable(self.sampler)
                else self.sampler or 0)

    def truncate(self):
        return truncate_io(self.current, self.size)
