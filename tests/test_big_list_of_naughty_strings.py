import base64
import pytest
from io import BytesIO
from os.path import abspath, join

from amazon.ion.reader import NEXT_EVENT
from amazon.ion.writer import blocking_writer
from amazon.ion.writer_text import raw_writer
from amazon.ion.core import IonEventType
from amazon.ionhash.hasher import hash_reader
from amazon.ionhash.hasher import hash_writer
from amazon.ionhash.hasher import HashEvent
from amazon.ionhash.hasher import hashlib_hash_function_provider
from .util import binary_reader_over


def _test_strings():
    path = abspath(join(abspath(__file__), '..', '..', 'tests', 'big-list-of-naughty-strings.txt'))
    file = open(path)
    lines = [line.rstrip('\n') for line in file]
    file.close()

    def _is_test(string):
        return not (string == '' or string[0] == '#')

    return filter(_is_test, lines)


@pytest.mark.parametrize("string", _test_strings())
def test(string):
    tv = _TestValue(string)

    _run_test(tv, tv.symbol())
    _run_test(tv, tv.string())
    _run_test(tv, tv.long_string())
    _run_test(tv, tv.clob())
    _run_test(tv, tv.blob())

    _run_test(tv, tv.symbol() + "::" + tv.symbol())
    _run_test(tv, tv.symbol() + "::" + tv.string())
    _run_test(tv, tv.symbol() + "::" + tv.long_string())
    _run_test(tv, tv.symbol() + "::" + tv.clob())
    _run_test(tv, tv.symbol() + "::" + tv.blob())

    _run_test(tv, tv.symbol() + "::{" + tv.symbol() + ":" + tv.symbol() + "}")
    _run_test(tv, tv.symbol() + "::{" + tv.symbol() + ":" + tv.string() + "}")
    _run_test(tv, tv.symbol() + "::{" + tv.symbol() + ":" + tv.long_string() + "}")
    _run_test(tv, tv.symbol() + "::{" + tv.symbol() + ":" + tv.clob() + "}")
    _run_test(tv, tv.symbol() + "::{" + tv.symbol() + ":" + tv.blob() + "}")

    if tv.valid_ion:
        _run_test(tv, tv.ion)
        _run_test(tv, tv.symbol() + "::" + tv.ion)
        _run_test(tv, tv.symbol() + "::{" + tv.symbol() + ":" + tv.ion + "}")
        _run_test(tv, tv.symbol() + "::{" + tv.symbol() + ":" + tv.symbol() + "::" + tv.ion + "}")

    # list
    _run_test(tv,
        tv.symbol() + "::["
            + tv.symbol() + ", "
            + tv.string() + ", "
            + tv.long_string() + ", "
            + tv.clob() + ", "
            + tv.blob() + ", "
            + (tv.ion if tv.valid_ion else "")
            + "]")

    # sexp
    _run_test(tv,
        tv.symbol() + "::("
            + tv.symbol() + " "
            + tv.string() + " "
            + tv.long_string() + " "
            + tv.clob() + " "
            + tv.blob() + " "
            + (tv.ion if tv.valid_ion else "")
            + ")")

    # multiple annotations
    _run_test(tv, tv.symbol() + "::" + tv.symbol() + "::" + tv.symbol() + "::" + tv.string())


def _run_test(tv, s):
    print("s:", s)
    events = []
    hr_digest = None
    try:
        reader = hash_reader(binary_reader_over(s),
            hashlib_hash_function_provider("md5"))

        while True:
            event = reader.send(NEXT_EVENT)
            events.append(event)
            if event.event_type is IonEventType.STREAM_END:
                break

        hr_digest = reader.send(HashEvent.DIGEST)
    except:
        if tv.valid_ion:
            raise

    hw_digest = None
    try:
        writer = hash_writer(
            blocking_writer(raw_writer(), BytesIO()),
            hashlib_hash_function_provider("md5"))

        for event in events:
            if event.event_type is IonEventType.STREAM_END:
                hw_digest = writer.send(HashEvent.DIGEST)
            writer.send(event)
    except:
        if tv.valid_ion:
            raise

    if tv.valid_ion is None or tv.valid_ion is True:
        assert hr_digest == hw_digest


_ion_prefix = 'ion::'
_invalid_ion_prefix = 'invalid_ion::'


class _TestValue:
    def __init__(self, string):
        self.ion = string
        self.valid_ion = None

        if self.ion.startswith(_ion_prefix):
            self.valid_ion = True
            self.ion = self.ion[_ion_prefix.__len__():]

        if self.ion.startswith(_invalid_ion_prefix):
            self.valid_ion = False
            self.ion = self.ion[_invalid_ion_prefix.__len__():]

    def symbol(self):
        s = self.ion
        s = s.replace("\\", "\\\\")
        s = s.replace("'", "\\'")
        return "\'" + s + "\'"

    def string(self):
        s = self.ion
        s = s.replace("\\", "\\\\")
        s = s.replace("\"", "\\\"")
        return "\"" + s + "\""

    def long_string(self):
        s = self.ion
        s = s.replace("\\", "\\\\")
        s = s.replace("'", "\\'")
        return "'''" + s + "'''"

    def clob(self):
        s = self.string()
        sb = ''
        for c in s:
            if ord(c) >= 128:
                sb += "\\x" + "{0:x}".format(ord(c))
            else:
                sb += c
        return "{{" + sb + "}}"

    def blob(self):
        return "{{" + base64.b64encode(bytes(self.ion, "utf-8")).decode("utf-8") + "}}"

