

def reset_io(buff):
    buff.seek(0)
    buff.truncate(0)
    return buff

def truncate_io(buff, size):
    # get the remainder value
    buff.seek(size)
    leftover = buff.read()
    # remove the remainder
    buff.seek(0)
    buff.truncate(size)
    return leftover

# def diesattheend(pool):
#     import atexit
#     def close(pool):
#         try:
#             pool.shutdown(wait=True)
#         except OSError: # handle is closed
#             pass # we already closed the pool
#         atexit.unregister(pool.__del__)
#     patch(pool, '__del__', close)
#     atexit.register(pool.__del__)
#     return pool
#
# def patch(obj, name, func):
#     from functools import wraps
#     oldfunc = getattr(obj, name, None) or (lambda: None)
#     def replaced(*a, **kw):
#         if oldfunc:
#             oldfunc(*a, **kw)
#         func(obj, *a, **kw)
#     replaced.__name__ = name
#     setattr(obj, name, wraps(oldfunc)(replaced) if oldfunc else replaced)

class FakePool:
    def __init__(self, max_workers=None):
        pass

    def submit(self, func, *a, **kw):
        return FakeFuture(func(*a, **kw))

    def shutdown(self, wait=None):
        pass

class FakeFuture:
    def __init__(self, result):
        self._result = result

    def result(self):
        return self._result

    def add_done_callback(self, func):
        return func(self)
