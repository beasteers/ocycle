import io
from collections import deque
from ocycle.util import reset_io, truncate_io


class ObjectCycle:
    '''Infinitely generate and reuse a set of objects.

    Here's the basics of how it works:
    >>> cycle = ObCycle(np.random.randn)
    >>> assert cycle.current == 0.011874919700215744
    >>> assert cycle.next() == 0.9047307879536564
    >>> assert cycle.current == 0.9047307879536564
    >>> assert cycle.last == 0.011874919700215744
    '''
    last = current = None
    new = None
    def __init__(self, new=None, n=1):
        self.new = new or self.new
        assert self.new is not None
        self.items = deque(self.new() for _ in range(n))
        self.next()

    def __repr__(self):
        return '<{}({}) n_items={}>'.format(self.__class__.__name__, self.new, len(self.items))

    def __getattr__(self, k):
        return getattr(self.current, k)

    def next(self):
        item = self.items.popleft() if self.items else self.new()
        self.last, self.current = self.current, item
        return item

    def reuse(self, item):
        self.items.append(item)


class BufferCycle(ObjectCycle):
    '''Infinitely generate and reuse a set of io.BytesIO objects.'''
    new = io.BytesIO
    def next(self, content=None):
        super().next()
        if content:
            self.write(content)

    def reuse(self, item):
        super().reuse(reset_io(item))

    def reset(self):
        return reset_io(self.current)
