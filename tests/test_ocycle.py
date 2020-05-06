import io
import time
import weakref
import ocycle


SLEEP = 0.05
ALPHABET_STR = [chr(i) for i in range(65, 91)]
ALPHABET = [bytes(c, 'utf-8') for c in ALPHABET_STR]


def process_(x, t0):
    print('processing', x, t0)
    return x

class BufferEmitTester:
    def __init__(self, n_consume, check=None, process=process_, **kw):
        self.b = ocycle.BufferEmit(process, size=n_consume, on_done=check, **kw)

    def run(self, n_emit, **kw):
        emit_msgs(self.b, n_emit, **kw)

def emit_msgs(b, n_emit, chars=ALPHABET):
    for c in chars:
        m = c*n_emit
        print('writing', m)
        b.write(m)
        time.sleep(SLEEP)




N_EMIT, N_CONSUME = 8, 21

def test_basic():
    # test that it works in serial, thread, and process mode
    def check(res):
        assert isinstance(res, io.BytesIO)
        assert len(res.getvalue()) >= N_CONSUME

    BufferEmitTester(N_CONSUME, check).run(N_EMIT)
    BufferEmitTester(N_CONSUME, check, mode='thread').run(N_EMIT)
    BufferEmitTester(N_CONSUME, check, mode='process').run(N_EMIT)

def test_value():
    # test that a value is returned, not
    def check_val(res):
        assert isinstance(res, bytes)
    BufferEmitTester(N_CONSUME, check_val, value=True).run(N_EMIT)

def test_clip():
    # Test clipping: if clip=True, len(res) === N_CONSUME
    def check_clip(res):
        assert len(res.getvalue()) == N_CONSUME
    BufferEmitTester(N_CONSUME, check_clip, clip=True).run(N_EMIT)

def test_serialization():
    # Test that the BufferEmit object can be sent over pickle
    BufferEmitTester(N_CONSUME, process=None, mode='process').run(N_EMIT)

def test_stringio():
    def check(res):
        assert isinstance(res, io.StringIO)
    BufferEmitTester(N_CONSUME, check, new=io.StringIO).run(N_EMIT, chars=ALPHABET_STR)

    def check_value(res):
        assert isinstance(res, str)
    BufferEmitTester(N_CONSUME, check_value, new=io.StringIO, value=True).run(N_EMIT, chars=ALPHABET_STR)

def test_sampler():
    import random
    def check(res):
        pass
    BufferEmitTester(N_CONSUME, check, sampler=1).run(N_EMIT)
    BufferEmitTester(N_CONSUME, check, sampler=lambda: random.uniform(0, 1.)).run(N_EMIT)

def test_close():
    buf = ocycle.BufferEmit(process_, size=N_CONSUME)
    buf.close()
    assert buf.pool is None

def test_del():
    buf = ocycle.BufferEmit(process_, size=N_CONSUME)
    r = weakref.ref(buf)
    buf.__del__()
    assert r().pool is None
