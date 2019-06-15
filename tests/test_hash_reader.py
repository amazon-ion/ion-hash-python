from io import BytesIO

import amazon.ion.simpleion as ion
import amazon.ion.reader as ion_reader
from amazon.ion.reader_managed import managed_reader
from amazon.ion.reader_binary import binary_reader
from amazon.ion.reader import NEXT_EVENT
from amazon.ion.reader import SKIP_EVENT
from amazon.ion.core import IonEventType
from amazon.ionhash.hasher import hash_reader
from amazon.ionhash.hasher import HashEvent

from .util import hash_function_provider


def test_hash_reader():
    ion_str = '[1, 2, {a: 3, b: (4 {c: 5} 6) }, 7]'
    algorithm = "md5"

    # calculate max # of events
    max_events = _consume(_binary_reader_over(ion_str)).__len__()

    # calculate expected digest
    hr = hash_reader(_binary_reader_over(ion_str), hash_function_provider(algorithm))
    _consume(hr)
    expected_digest = hr.send(HashEvent.DIGEST)

    # verify events produced by reader and hash_reader are identical
    for i in range(0, max_events - 1):
        skip_list = [i] if i > 0 else []

        # verify the hash_reader's digest matches expected digest
        assert _run_test(ion_str, skip_list, algorithm) == expected_digest


def _run_test(ion_str, skip_list, algorithm):
    r = _binary_reader_over(ion_str)
    r_events = _consume(r, skip_list)

    hr = hash_reader(_binary_reader_over(ion_str), hash_function_provider(algorithm))
    hr_events = _consume(hr, skip_list)

    # assert reader/hash_reader response behavior is identical
    assert hr_events == r_events

    return hr.send(HashEvent.DIGEST)


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
