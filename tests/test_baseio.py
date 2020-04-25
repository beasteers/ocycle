import numpy as np
import ocycle.baseio as bio


def buff_test_read_write(xio, pos, value):
    assert len(xio.getvalue()) == 0, "didn't start out empty"
    xio.write(value)
    assert xio.tell() == len(value), "the cursor was not moved to the end after write."
    assert np.all(xio.getvalue() == value), "values were not written."
    xio.seek(pos)
    assert xio.tell() == pos, "seek didn't move the cursor to the right place."
    assert np.all(xio.read() == value[pos:]), "didn't read from the cursor to the end."
    assert xio.tell() == len(value), "cursor was not moved to the end after read."
    print(xio.getvalue())
    xio.truncate(pos)
    assert np.all(xio.getvalue() == value[:pos])
    return xio

def test_listio():
    buff_test_read_write(bio.ListIO(), 3, list(range(8)))

def test_numpyio():
    xio = bio.NumpyIO()
    pos = 3
    value = np.arange(40).reshape(8, -1)
    buff_test_read_write(xio, pos, value)
    assert xio.getvalue().shape == (pos,) + value.shape[1:]

def test_numpy_buffer_size():
    x = np.arange(10)
    xio = bio.NumpyIO()
