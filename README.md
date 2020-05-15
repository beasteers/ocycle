# ocycle
Stream buffering and event triggering for unbalanced frame sizes. Data goes in with a certain frame size and comes out with a different size.

It supports:
 - **threaded/multiprocessing callbacks** (in addition to serial execution)
    - a process/thread pool is created that callbacks are sent to.
 - **always available, recycled buffers**
    - when a buffer is passed to an asynchronous callback, a new one is created to be written to while the other is being used.
    - once the callback function is done, it is added back to the stack, ready to be reused.
 - **temporal sampling** - you can pass a function or scalar which will define how long to wait to start writing again after emitting a callback.
    - So for example, if you wanted 50% coverage, you could pass `sampler=size / fs` which will wait for `size / fs` seconds before writing again.

## Example

```python
import ocycle

# this is some random data that represents our
# incoming data buffer
SOME_DATA = 'asdfghjkl;'

# how full should the buffer be before emitting data
CONSUME_SIZE = 29

# a data consumer
def create_consumer(size):
    # this is called when the buffer reaches/exceeds size.
    def callback(value, t0):
        assert len(value) >= CONSUME_SIZE
        print('Emitting:', t0, value)

    return ocycle.BufferEmit(callback, size)

buff = create_consumer(CONSUME_SIZE)

# a generator then writes to the buffer.
# once it is full enough it will call `callback(buff, t0)`
while True:
    print('Writing to buffer:', SOME_DATA)
    buff.write(SOME_DATA)
```

## Example Use Case
### Audio Processing
You want to stream audio from a microphone and perform multiple independent tasks on that stream. Each of these feature extractors have a different audio chunk size required.

Say you want to:
 - extract SPL (every 0.5s)
 - write encrypted flac files (every 10s)
 - run a tensorflow model (every 2s)

Instead of collecting the data with a single chunk size and then manually tracking and spawning a new process every time each individual buffer fills up, `BufferEmit` will handle all of that for you.

## Usage

### Callback

You need to pass in a callback function, `callback`, which will be called once the buffer exceeds size `size`. This will receive the filled buffer and the
timestamp of when the first buffer frame arrived.

You can also pass an additional callback `on_done` which will be called back on the main process and is useful if you need to store a result from the callback on some object in memory.

### Execution Method

You can choose how your callback is executed:
 - `mode='serial'`: the callback will be run in the executing thread/process
 - `mode='thread'`: the callback will be run in a new thread
 - `mode='process'`: the callback will be run in a new process

You can pass e.g. `npool=10` to use a 10 thread/process worker pool.

### Value Type
##### Value Type
By default, the callback will receive a `bytes` and timestamp object. If you pass `asbuffer=True`, it will
pass `io.BytesIO` without calling `buff.getvalue()`.

##### Clipping
By default, the callback will receive the entire buffer after the buffer has exceeded `size`. This is useful if you don't want to clip partial messages, but this also means that `len(buff) >= size`. If you pass `clip=True`, it will return buff in exact chunks of size `size`.

### Sampler

In some cases, you may not want all of the data and you just want to take some subsample over time.

For example, say your machine learning model is not quite real time. You've tried really hard, but you can only get it down to 1.7x. You can define a sampler which will say after you emit a buffer, wait `x` seconds before you start writing to the buffer again. So you could set `sampler=1.0` and it will pause for 1 second before it begins writing to the buffer again.

A sampler can also be any callable that needs no arguments and returns an int/float. This is useful if you want to implement stochastic/dynamic sampling strategies. For example, you could set:

 `sampler=lambda: (np.random.randn() * 1.3 + 4)`

which will delay using a normal distribution of mean 4 and std dev of 1.3.


## Configuration Examples
This shows more of the different configurations that `BufferEmit` can exist as.

TODO: it would be more illustrative with different buffer sizes.... idk what I was thinking lol

```python
import time
import ocycle

# build a list of receivers

buffers = []
SIZE = 15

def on_emit(label): # callback
    return lambda buff, t0: print(
        label, buff.getvalue(), t0)

# default operation
buffers.append(ocycle.BufferEmit(
    on_emit('default'), size=SIZE))

# execute the callback in a thread
buffers.append(ocycle.BufferEmit(
    on_emit('im in a thread!'),
    size=SIZE, mode='thread'))

# the callback for multiprocessing must be pickleable
def on_process_emit(buff, t0): # callback
    x = buff.getvalue()
    print('Im in a process!', x, t0)
    return x[::-1]

# execute the callback in a separate process
# execute `on_done` in the main process
buffers.append(ocycle.BufferEmit(
    on_process_emit,
    on_done=lambda res:
        print('The process returned:', res),
    size=SIZE, mode='process'))

# send data to each receiver

def generate_data(length=4, n=1):
    for _ in range(n):
        for i in range(10):
            yield str(i).encode('utf-8') * length

for x in generate_data(4):
    print('writing', x)
    for b in buffers:
        b.write(x)
    time.sleep(1)
```

Outputs:
```
writing b'0000'
writing b'1111'
writing b'2222'
writing b'3333'
default b'0000111122223333' 1585253921.2961438
im in a thread! b'0000111122223333' 1585253921.296198
gets value, not buffer object b'0000111122223333' 1585253921.2967098
value clipped to size b'000011112222333' 1585253921.296758
Im in a process! b'0000111122223333' 1585253921.296656
The process returned: b'3333222211110000'
writing b'4444'
writing b'5555'
writing b'6666'
writing b'7777'
default b'4444555566667777' 1585253924.3013031
im in a thread! b'4444555566667777' 1585253924.3013911
gets value, not buffer object b'4444555566667777' 1585253924.3263922
value clipped to size b'344445555666677' 1585253924.326594
Im in a process! b'4444555566667777' 1585253924.301939
The process returned: b'7777666655554444'
writing b'8888'
writing b'9999'
writing b'0000'
writing b'1111'
default b'8888999900001111' 1585253928.3309472
im in a thread! b'8888999900001111' 1585253928.331071
gets value, not buffer object b'8888999900001111' 1585253928.3323038
value clipped to size b'778888999900001' 1585253928.33305
Im in a process! b'8888999900001111' 1585253928.332133
The process returned: b'1111000099998888'
writing b'2222'
writing b'3333'
```
