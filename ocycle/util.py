

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
