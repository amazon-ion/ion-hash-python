# Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#  
# Licensed under the Apache License, Version 2.0 (the "License").
# You may not use this file except in compliance with the License.
# A copy of the License is located at
#  
#     http://www.apache.org/licenses/LICENSE-2.0
#  
# or in the "license" file accompanying this file. This file is distributed
# on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
# express or implied. See the License for the specific language governing
# permissions and limitations under the License.

import pytest
from io import BytesIO

from amazon.ion.core import IonEvent
from amazon.ion.core import IonEventType
from amazon.ion.core import IonType
from amazon.ion.symbols import SymbolToken
from amazon.ion.reader import NEXT_EVENT
from amazon.ion.reader import SKIP_EVENT
from amazon.ion.writer import blocking_writer
from amazon.ion.writer_text import raw_writer
from ionhash.hasher import hash_reader
from ionhash.hasher import hash_writer
from ionhash.hasher import HashEvent

from .util import binary_reader_over
from .util import consume
from .util import hash_function_provider


class _TestData:
    def __init__(self, ion_str, expected_digest):
        self.ion_str = ion_str
        self.expected_digest = expected_digest


def _test_data():
    struct_digest = bytearray()
    struct_digest.extend(b'\x0b\xd0')
    struct_digest.extend(  b'\x0c\x0b\x70\x61\x0c\x0e\x0c\x0b\x20\x01\x0c\x0e')
    struct_digest.extend(  b'\x0c\x0b\x70\x62\x0c\x0e\x0c\x0b\x20\x02\x0c\x0e')
    struct_digest.extend(  b'\x0c\x0b\x70\x63\x0c\x0e\x0c\x0b\x20\x03\x0c\x0e')
    struct_digest.extend(b'\x0e')

    return [
        _TestData("null", b'\x0b\x0f\x0e'),
        _TestData("false", b'\x0b\x10\x0e'),
        _TestData("5", b'\x0b\x20\x05\x0e'),
        _TestData("2e0", b'\x0b\x40\x40\x00\x00\x00\x00\x00\x00\x00\x0e'),
        _TestData("1234.500", b'\x0b\x50\xc3\x12\xd6\x44\x0e'),
        _TestData("2017-01-01T00:00:00Z", b'\x0b\x60\x80\x0f\xe1\x81\x81\x80\x80\x80\x0e'),
        _TestData("hi", b'\x0b\x70\x68\x69\x0e'),
        _TestData("\"hi\"", b'\x0b\x80\x68\x69\x0e'),
        _TestData("{{\"hi\"}}", b'\x0b\x90\x68\x69\x0e'),
        _TestData("{{aGVsbG8=}}", b'\x0b\xa0\x68\x65\x6c\x6c\x6f\x0e'),
        _TestData("[1,2,3]", b'\x0b\xb0\x0b\x20\x01\x0e\x0b\x20\x02\x0e\x0b\x20\x03\x0e\x0e'),
        _TestData("(1 2 3)", b'\x0b\xc0\x0b\x20\x01\x0e\x0b\x20\x02\x0e\x0b\x20\x03\x0e\x0e'),
        _TestData("{a:1,b:2,c:3}", struct_digest),
        _TestData("hi::7", b'\x0b\xe0\x0b\x70\x68\x69\x0e\x0b\x20\x07\x0e\x0e'),
    ]


def _test_name(test_data):
    return test_data.ion_str


@pytest.mark.parametrize("test_data", _test_data(), ids=_test_name)
def test_no_fieldname_in_hash(test_data):
    """
    This test verifies a hash_reader/writer that receives field events but did not
    receive the preceeding "begin struct" event DOES NOT include the field_name as
    part of the hash.
    """
    reader = binary_reader_over(test_data.ion_str)
    events = consume(reader)

    buf = BytesIO()
    writer = blocking_writer(raw_writer(), buf)
    writer.send(IonEvent(IonEventType.CONTAINER_START, IonType.STRUCT))

    hw = hash_writer(writer, hash_function_provider("identity"))
    for e in events[:-1]:
        field_name = e.field_name
        if e.depth == 0:
            field_name = SymbolToken("field_name", None)
        new_event = IonEvent(e.event_type, e.ion_type, e.value, field_name, e.annotations, e.depth + 1)
        hw.send(new_event)
    writer.send(IonEvent(IonEventType.CONTAINER_END, IonType.STRUCT))
    writer.send(events[-1])     # send the final event (which should be a STREAM_END event)

    output = buf.getvalue()
    hw_digest = hw.send(HashEvent.DIGEST)
    assert hw_digest == test_data.expected_digest

    reader = binary_reader_over(output)
    hr_digest = b''
    while True:
        event = reader.send(NEXT_EVENT)
        if event.event_type == IonEventType.CONTAINER_START:
            hr = hash_reader(reader, hash_function_provider("identity"))
            for i in range(0, len(events) - 1):
                e = hr.send(NEXT_EVENT)
            hr_digest = hr.send(HashEvent.DIGEST)
        if event.event_type == IonEventType.STREAM_END:
            break
    assert hr_digest == test_data.expected_digest

