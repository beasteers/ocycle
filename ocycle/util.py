

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
