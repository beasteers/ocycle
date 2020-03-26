# ocycle
Stream buffering and event triggering for unbalanced frame sizes.

## Use Case
### Audio Processing
You want to stream audio from a microphone and extract a set of independent features as well as writing audio to file. Each of these feature extractors have a different audio chunk size required.

Instead of manually handling the buffer sizes, you can use this class and it will handle it for you.

## Example

```python
import ocycle

# some data to generate
SOME_DATA = 'asdfghjkl;'

# how full should the buffer be before emitting data
CONSUME_SIZE = 29

# a data consumer
def processor(size):
    # this is called when the buffer reaches/exceeds size.
    def callback(buff, t0):
        value = buff.getvalue()
        assert len(value) >= CONSUME_SIZE
        print('Emitting:', t0, value)

    return ocycle.BufferEmit(callback, size)

proc = processor(CONSUME_SIZE)

# a generator then writes to the buffer.
# once it is full enough it will call
while True:
    print('Writing to buffer:', SOME_DATA)
    proc.write(SOME_DATA)
```
