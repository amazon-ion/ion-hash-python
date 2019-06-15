from io import BytesIO

from amazon.ion.writer import blocking_writer
from amazon.ion.writer_binary import binary_writer
from amazon.ion.writer_text import raw_writer
from amazon.ionhash.hasher import hash_writer

from .util import binary_reader_over
from .util import consume
from .util import hash_function_provider


def test_hash_writer():
    ion_str = '[1, 2, {a: 3, b: (4 {c: 5} 6) }, 7]'
    algorithm = "md5"

    # generate events to be used to write the data
    events = consume(binary_reader_over(ion_str))

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

