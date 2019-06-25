from amazon.ionhash.hasher import hash_reader
from amazon.ionhash.hasher import HashEvent

from .util import consume
from .util import binary_reader_over
from .util import hash_function_provider


def test_hash_reader():
    ion_str = '[1, 2, {a: 3, b: (4 {c: 5} 6) }, 7]'
    algorithm = "md5"

    # calculate max # of events
    max_events = len(consume(binary_reader_over(ion_str)))

    # calculate expected digest
    hr = hash_reader(binary_reader_over(ion_str), hash_function_provider(algorithm))
    consume(hr)
    expected_digest = hr.send(HashEvent.DIGEST)

    # verify events produced by reader and hash_reader are identical
    for i in range(0, max_events - 1):
        skip_list = [i] if i > 0 else []

        # verify the hash_reader's digest matches expected digest
        assert _run_test(ion_str, skip_list, algorithm) == expected_digest


def _run_test(ion_str, skip_list, algorithm):
    r = binary_reader_over(ion_str)
    r_events = consume(r, skip_list)

    hr = hash_reader(binary_reader_over(ion_str), hash_function_provider(algorithm))
    hr_events = consume(hr, skip_list)

    # assert reader/hash_reader response behavior is identical
    assert hr_events == r_events

    return hr.send(HashEvent.DIGEST)

