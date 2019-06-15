from io import BytesIO

import amazon.ion.simpleion as ion
import amazon.ion.reader as ion_reader
from amazon.ion.core import IonEventType
from amazon.ion.reader_managed import managed_reader
from amazon.ion.reader_binary import binary_reader
from amazon.ion.reader import NEXT_EVENT
from amazon.ion.reader import SKIP_EVENT
from amazon.ion.writer import blocking_writer
from amazon.ion.writer_binary import binary_writer
from amazon.ion.writer_text import raw_writer
from amazon.ionhash.hasher import hash_writer

from .util import hash_function_provider


def test_hash_writer():
    ion_str = '[1, 2, {a: 3, b: (4 {c: 5} 6) }, 7]'
    algorithm = "md5"

    # generate events to be used to write the data
    events = _consume(_binary_reader_over(ion_str))

    _run_test(_writer_provider("binary"), events, algorithm)
    _run_test(_writer_provider("text"), events, algorithm)


def _run_test(writer_provider, events, algorithm):
    # capture behavior of an ion-python writer
    w = blocking_writer(writer_provider(), BytesIO())
    expected_write_event_types = _write_to(w, events)

    hw = hash_writer(blocking_writer(writer_provider(), BytesIO()), hash_function_provider(algorithm))
    hw_write_event_types = _write_to(hw, events)

    # assert writer/hash_writer response behavior is identical
    assert hw_write_event_types == expected_write_event_types


def _write_to(w, events):
    write_event_types = []
    for event in events:
        write_event_types.append(w.send(event))
    return write_event_types


def _writer_provider(type):
    def _f():
        if type == "binary":
            return binary_writer()
        elif type == "text":
            return raw_writer()
    return _f


def _binary_reader_over(ion_str):
    value = ion.loads(ion_str)
    _bytes = ion.dumps(value, binary=True)
    return ion_reader.blocking_reader(managed_reader(binary_reader(), None), BytesIO(_bytes))


def _consume(reader, skip_list=[]):
    skip_set = set(skip_list)
    events = []
    i = -1
    while True:
        i += 1
        if i in skip_set:
            event = reader.send(SKIP_EVENT)
        else:
            event = reader.send(NEXT_EVENT)

        events.append(event)
        if event.event_type == IonEventType.STREAM_END:
            break

    return events

