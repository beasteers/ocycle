from collections import deque
import numpy as np


class BaseIO:
    _i = 0
    def read(self, size=None):
        '''Read ``size`` elements from the buffer, starting at ``self.tell()``.'''
        return self.buffer[self._slicenext(size)]

    def truncate(self, i=None):
        '''Truncate buffer to size.'''
        self._i = i if i is not None else self._i
        self.buffer = self.buffer[:self._i]

    def tell(self):
        '''The object cursor.'''
        return self._i

    def size(self):
        '''The current size of the buffer.'''
        return len(self.buffer)

    def seek(self, i):
        '''Move the cursor to a position.'''
        self._i = i if i is not None else self._i

    def _slicenext(self, size=None, inc=True):
        '''Calculate the buffer slice, and optionally update the cursor to the
        end of the slice.'''
        j = self._i + size if size is not None else self.size()
        i, j = self._i, min(j, self.size())
        if inc:
            self._i = j
        return slice(i, j)


class ListIO(BaseIO):
    _type = lambda self, x: list(x)

    def __init__(self):
        self.buffer = []

    def getvalue(self, copy=True):
        '''Get the entire buffer value. If ``copy=False`` is set, the buffer
        will be passed by reference.'''
        return list(self.buffer) if copy else self.buffer

    def write(self, items):
        '''Write items to the buffer.'''
        i, size = self._i, len(self.buffer)
        self.buffer[size:i] = self._create_null(i - size) # fill empty values
        self.buffer[i:i + len(items)] = items # set items
        self._i = i + len(items)

    def _create_null(self, size):
        return [None] * size


class NumpyIO(BaseIO):
    '''A Numpy interface mimicking a BytesIO object.'''
    _size = 0

    def __init__(self, shape=(0), len_lookback=50, padding=0.5):
        # tracking estimated required size
        self._sizes = deque(maxlen=len_lookback)
        self._input_sizes = deque(maxlen=len_lookback)
        self._padding = padding
        self._buffer = np.zeros(shape)

    def getvalue(self, copy=True):
        return np.copy(self.buffer) if copy else self.buffer

    def write(self, data):
        if self._buffer.shape[1:] != data.shape[1:]:
            if not self._buffer.size:
                self._buffer = np.zeros(data.shape)
            else:
                raise ValueError(
                    'Tried to write incorrect shape to a non-empty buffer. '
                    'Buffer is {}, received {}.'.format(
                        self._buffer[1:], data.shape[1:]))

        in_size = len(data)
        size = max(self._i + in_size, self._size)
        self._sizes.append(size)
        self._input_sizes.append(in_size)
        self.readjust()
        self._size = size
        self.buffer[self._slicenext(size)] = data

    def truncate(self, length=None):
        self._i, self._size = 0, length or 0

    def readjust(self):
        # get the relevant sizes
        size, buflen = self._size, len(self._buffer)
        maxsize, max_insize = max(self._sizes), max(self._input_sizes)
        # calculate the new length
        newlen = maxsize + int(max_insize * self._padding)

        # check and resize the buffer
        if buflen < maxsize: # the buffer isn't big enough !
            buff, self._buffer = self._buffer, np.zeros(
                (newlen,) + self._buffer.shape[1:])
            print(buff.shape, self._buffer.shape, self._size)
            self.buffer[:size] = buff[:size]

        elif buflen > maxsize: # the buffer is bigger than you need to be
            self._buffer = self._buffer[:newlen]

    @property
    def buffer(self):
        return self._buffer[:self._size]

    def size(self):
        return self._size
