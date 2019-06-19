import pytest
from six import StringIO
from io import BytesIO
from os.path import abspath, join

import amazon.ion.simpleion as ion
import amazon.ion.reader as ion_reader
from amazon.ion.reader_managed import managed_reader
from amazon.ion.reader_text import text_reader
from amazon.ion.reader_binary import binary_reader
from amazon.ion.reader import NEXT_EVENT
from amazon.ion.reader import SKIP_EVENT
from amazon.ion.writer import blocking_writer
from amazon.ion.writer_binary import binary_writer
from amazon.ion.writer_text import raw_writer
from amazon.ion.core import IonEventType
from amazon.ion.core import IonEvent
from amazon.ionhash.hasher import hash_reader
from amazon.ionhash.hasher import hash_writer
from amazon.ionhash.hasher import HashEvent

from .util import hash_function_provider
from .util import sexp_to_bytearray


def _test_data(algorithm):
    path = abspath(join(abspath(__file__), '..', '..', 'tests', 'ion_hash_tests.ion'))
    f = open(path)
    ion_tests = ion.loads(f.read(), single_value=False)
    f.close()

    def _has_algorithm(ion_test):
        return algorithm in ion_test['expect']

    return filter(_has_algorithm, ion_tests)


_IVM = "$ion_1_0 "


def _test_name(ion_test):
    if ion_test.ion_annotations.__len__() > 0:
        test_name = ion_test.ion_annotations[0].text
    else:
        test_name = ion.dumps(ion_test['ion'], binary=False).__str__()
        if test_name.startswith(_IVM):
            test_name = test_name[len(_IVM):]

    return " " + test_name


def _to_buffer(ion_test, binary):
    if 'ion' in ion_test:
        v = ion.dumps(ion_test['ion'], binary=binary)

    if '10n' in ion_test:
        v = bytearray([0xE0, 0x01, 0x00, 0xEA])  # $ion_1_0
        for byte in ion_test['10n']:
            v.append(byte)

        if not binary:
            value = ion.load(BytesIO(v))
            v = ion.dumps(value, binary=False)

    if binary:
        return BytesIO(v)
    else:
        return StringIO(v)


def _consumer_provider(reader_provider, buf):
    def _f(algorithm):
        buf.seek(0)
        reader = hash_reader(
            ion_reader.blocking_reader(managed_reader(reader_provider(), None), buf),
            hash_function_provider(algorithm, _actual_updates, _actual_digests))

        _consume(reader)

        return reader.send(HashEvent.DIGEST)

    return _f


def _consume(reader, writer=None):
    event = reader.send(NEXT_EVENT)
    if writer is not None:
        writer.send(event)
    while event.event_type is not IonEventType.STREAM_END:
        event = reader.send(NEXT_EVENT)
        if writer is not None:
            writer.send(event)


def _writer_provider(reader_provider, buf):
    def _f(algorithm):
        buf.seek(0)
        reader = ion_reader.blocking_reader(managed_reader(reader_provider(), None), buf)

        writer = hash_writer(
            blocking_writer(raw_writer(), BytesIO()),
            hash_function_provider(algorithm, _actual_updates, _actual_digests))

        _consume(reader, writer)

        digest = writer.send(HashEvent.DIGEST)
        writer.send(IonEvent(IonEventType.STREAM_END))
        return digest

    return _f


@pytest.mark.parametrize("ion_test", _test_data("identity"), ids=_test_name)
def test_binary(ion_test):
    _run_test(ion_test,
              _consumer_provider(_reader_provider("binary"),
                                 _to_buffer(ion_test, binary=True)))


@pytest.mark.parametrize("ion_test", _test_data("md5"), ids=_test_name)
def test_binary_md5(ion_test):
    _run_test(ion_test,
              _consumer_provider(_reader_provider("binary"),
                                 _to_buffer(ion_test, binary=True)))


@pytest.mark.parametrize("ion_test", _test_data("identity"), ids=_test_name)
def test_text(ion_test):
    _run_test(ion_test,
              _consumer_provider(_reader_provider("text"),
                                 _to_buffer(ion_test, binary=False)))


@pytest.mark.parametrize("ion_test", _test_data("md5"), ids=_test_name)
def test_text_md5(ion_test):
    _run_test(ion_test,
              _consumer_provider(_reader_provider("text"),
                                 _to_buffer(ion_test, binary=False)))


@pytest.mark.parametrize("ion_test", _test_data("identity"), ids=_test_name)
def test_skip_over(ion_test):
    buf = _to_buffer(ion_test, binary=True)

    def skipping_consumer(algorithm):
        buf.seek(0)
        reader = hash_reader(
            ion_reader.blocking_reader(managed_reader(_reader_provider("binary")(), None), buf),
            hash_function_provider(algorithm, _actual_updates, _actual_digests))

        event = reader.send(NEXT_EVENT)
        while event.event_type != IonEventType.STREAM_END:
            if event.event_type == IonEventType.CONTAINER_START:
                event = reader.send(SKIP_EVENT)
            else:
                event = reader.send(NEXT_EVENT)

        return reader.send(HashEvent.DIGEST)

    _run_test(ion_test, skipping_consumer)


@pytest.mark.parametrize("ion_test", _test_data("identity"), ids=_test_name)
def test_writer(ion_test):
    _run_test(ion_test,
              _writer_provider(_reader_provider("text"),
                               _to_buffer(ion_test, binary=False)))


_actual_updates = []
_actual_digests = []


def _run_test(ion_test, digester):
    expect = ion_test['expect']
    for algorithm in expect:
        expected_updates = []
        expected_digests = []
        final_digest = None
        for sexp in expect[algorithm]:
            annot = sexp.ion_annotations[0].text
            if annot == "update":
                expected_updates.append(sexp_to_bytearray(sexp))
                pass
            elif annot == "digest":
                expected_digests.append(sexp_to_bytearray(sexp))
            elif annot == "final_digest":
                final_digest = sexp_to_bytearray(sexp)

        _actual_updates.clear()
        _actual_digests.clear()

        actual_digest_bytes = digester(algorithm)

        if expected_updates.__len__() > 0:
            assert _actual_updates == expected_updates

        if final_digest is not None:
            assert _actual_digests[-1] == final_digest
            assert actual_digest_bytes == final_digest
        else:
            assert _actual_digests == expected_digests
            assert actual_digest_bytes == expected_digests[-1]


def _reader_provider(type):
    def _f():
        if type == "binary":
            return binary_reader()
        elif type == "text":
            return text_reader(is_unicode=True)
    return _f



