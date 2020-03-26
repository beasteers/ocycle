import io
from collections import deque
from .util import reset_io, truncate_io


class ReCycle:
    '''Infinitely generate and reuse a set of objects.

    Here's the basics of how it works:
    >>> cycle = ReCycle(np.random.randn)
    >>> assert cycle.current == 0.011874919700215744
    >>> assert cycle.next() == 0.9047307879536564
    >>> assert cycle.current == 0.9047307879536564
    >>> assert cycle.last == 0.011874919700215744
    '''
    last = current = None
    def __init__(self, new, n=1):
        self.new = new
        self.items = deque(new() for _ in range(n))
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


class BuffReCycle(ReCycle):
    '''Infinitely generate and reuse a set of io.BytesIO objects.'''
    def __init__(self, new=io.BytesIO, **kw):
        super().__init__(new=new, **kw)

    def next(self, content=None):
        super().next()
        if content:
            self.write(content)

    def reuse(self, item):
        super().reuse(reset_io(item))

    def reset(self):
        return reset_io(self.current)


# class ListIO:
#     def __init__(self):
#         self.items = []
#         self.i = 0
#
#     def __len__(self):
#         return len(self.items)
#
#     def write(self, *items):
#         i, l = self.i, len(self.items)
#         if i < l - 1:
#             self.items[i:l], items = items[:l - i], items[l - i:]
#             i = l
#         self.items.extend((None,) * max(i - l, 0) + items)
#         self.i = i
#
#     def read(self, size=None):
#         return self.items[self.i:self.i + size if self.i is not None else None]
#
#     def seek(self, i):
#         self.i = i if i is not None else self.i
#
#     def tell(self):
#         return self.i
#
#     def getvalue(self):
#         return tuple(self.items)
#
#     def truncate(self, length=None):
#         self.items = self.items[:length if length is not None else self.i]
