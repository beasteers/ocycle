import io
import time
import weakref
import numpy as np
import ocycle


SLEEP = 0.05
ALPHABET_STR = [chr(i) for i in range(65, 91)]
ALPHABET = [bytes(c, 'utf-8') for c in ALPHABET_STR]


def process_(x, t0):
    print('processing', x, t0)
    return x

class BufferEmitTester:
    Cls = ocycle.BufferEmit
    CHARS = ALPHABET

    def __init__(self, n_consume, check=None, process=process_, **kw):
        self.b = self.Cls(process, size=n_consume, on_done=check, **kw)

    def run(self, n, chars=None, **kw):
        for c in chars or self.CHARS:
            m = self.make(c, n)
            # print('writing', m)
            self.b.write(m)
            time.sleep(SLEEP)

    def make(self, src, n):
        return src * n

class StringEmitTester(BufferEmitTester):
    CHARS = ALPHABET_STR
    def __init__(self, *a, **kw):
        super().__init__(*a, new=io.StringIO, **kw)


class NumpyEmitTester(BufferEmitTester):
    Cls = ocycle.NumpyEmit
    CHARS = np.arange(20)
    def make(self, src, n):
        return np.array([[src] * n] * n)

class ListEmitTester(BufferEmitTester):
    Cls = ocycle.ListEmit
    def make(self, src, n):
        return [src] * n


N_EMIT, N_CONSUME = 8, 21




def test_bufferemit():
    def check(res):
        assert isinstance(res, bytes)
    BufferEmitTester(N_CONSUME, check).run(N_EMIT)

    def check(res):
        assert isinstance(res, io.BytesIO)
    BufferEmitTester(N_CONSUME, check, asbuffer=True).run(N_EMIT)

def test_stremit():
    def check(res):
        assert isinstance(res, str)
    StringEmitTester(N_CONSUME, check).run(N_EMIT)

    def check(res):
        assert isinstance(res, io.StringIO)
    StringEmitTester(N_CONSUME, check, asbuffer=True).run(N_EMIT)

def test_npemit():
    def check(res):
        assert isinstance(res, np.ndarray)
        assert len(res) >= N_CONSUME
    NumpyEmitTester(N_CONSUME, check).run(N_EMIT)

    def check(res):
        assert isinstance(res, ocycle.NumpyIO)
        assert len(res.getvalue()) >= N_CONSUME
    NumpyEmitTester(N_CONSUME, check, asbuffer=True).run(N_EMIT)

def test_listemit():
    def check(res):
        assert isinstance(res, list)
        assert len(res) >= N_CONSUME
    ListEmitTester(N_CONSUME, check).run(N_EMIT)

    def check(res):
        assert isinstance(res, ocycle.ListIO)
        assert len(res.getvalue()) >= N_CONSUME
    ListEmitTester(N_CONSUME, check, asbuffer=True).run(N_EMIT)




def test_mode():
    # test that it works in serial, thread, and process mode
    def check(res):
        assert isinstance(res, bytes)
        assert len(res) >= N_CONSUME

    BufferEmitTester(N_CONSUME, check).run(N_EMIT)
    BufferEmitTester(N_CONSUME, check, mode='thread').run(N_EMIT)
    BufferEmitTester(N_CONSUME, check, mode='process').run(N_EMIT)

def test_value():
    # test that a value is returned, not
    def check_val(res):
        assert isinstance(res, io.BytesIO)
        assert len(res.getvalue()) >= N_CONSUME
    BufferEmitTester(N_CONSUME, check_val, asbuffer=True).run(N_EMIT)

def test_clip():
    # Test clipping: if clip=True, len(res) === N_CONSUME
    def check_clip(res):
        assert len(res) == N_CONSUME
    BufferEmitTester(N_CONSUME, check_clip, clip=True).run(N_EMIT)

def test_serialization():
    # Test that the BufferEmit object can be sent over pickle
    BufferEmitTester(N_CONSUME, mode='process').run(N_EMIT)

def test_sampler():
    import random
    BufferEmitTester(N_CONSUME, sampler=1).run(N_EMIT)
    BufferEmitTester(N_CONSUME, sampler=lambda: random.uniform(0, 1.)).run(N_EMIT)

def test_close():
    buf = ocycle.BufferEmit(process_, size=N_CONSUME)
    buf.close()
    assert buf.pool is None

def test_del():
    buf = ocycle.BufferEmit(process_, size=N_CONSUME)
    r = weakref.ref(buf)
    buf.__del__()
    assert r().pool is None
