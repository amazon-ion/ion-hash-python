import pytest

from amazon.ion.reader import NEXT_EVENT
from amazon.ionhash.hasher import hash_reader
from amazon.ionhash.hasher import HashEvent

from .util import binary_reader_over
from .util import hash_function_provider


def test_digest_too_early():
    ion_str = "{ a: 1, b: 2 }"
    hr = hash_reader(binary_reader_over(ion_str), hash_function_provider("identity"))
    hr.send(NEXT_EVENT)
    with pytest.raises(Exception):
        hr.send(HashEvent.DIGEST)


def test_digest_too_late():
    ion_str = "{ a: 1, b: 2 }"
    reader = binary_reader_over(ion_str)
    reader.send(NEXT_EVENT)

    hr = hash_reader(reader, hash_function_provider("identity"))
    hr.send(NEXT_EVENT)
    hr.send(NEXT_EVENT)
    with pytest.raises(Exception):
        hr.send(NEXT_EVENT)

