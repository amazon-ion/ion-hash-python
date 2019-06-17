from io import BytesIO

from amazon.ion.writer import blocking_writer
from amazon.ion.writer_binary import binary_writer
from amazon.ion.writer_text import raw_writer
from amazon.ionhash.hasher import hash_writer

from .util import binary_reader_over
from .util import consume
from .util import hash_function_provider
from .util import hex_string


def test_hash_writer():
    ion_str = '[1, 2, {a: 3, b: (4 {c: 5} 6) }, 7]'
    algorithm = "md5"

    # generate events to be used to write the data
    events = consume(binary_reader_over(ion_str))

    _run_test(_writer_provider("binary"), events, algorithm)
    _run_test(_writer_provider("text"), events, algorithm)


def _run_test(writer_provider, events, algorithm):
    # capture behavior of an ion-python writer
    expected_bytes = BytesIO()
    w = blocking_writer(writer_provider(), expected_bytes)
    expected_write_event_types = _write_to(w, events)

    hw_bytes = BytesIO()
    hw = hash_writer(blocking_writer(writer_provider(), hw_bytes), hash_function_provider(algorithm))
    hw_write_event_types = _write_to(hw, events)

    # assert writer/hash_writer response behavior is identical
    assert hw_write_event_types == expected_write_event_types

    # assert writer/hash_writer produced the same output
    assert hw_bytes.getvalue() == expected_bytes.getvalue()


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

