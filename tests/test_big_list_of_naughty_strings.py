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

import base64
import pytest
from io import BytesIO
from os.path import abspath, join

from amazon.ion.reader import NEXT_EVENT
from amazon.ion.writer import blocking_writer
from amazon.ion.writer_text import raw_writer
from amazon.ion.core import IonEventType
from ionhash.hasher import hash_reader
from ionhash.hasher import hash_writer
from ionhash.hasher import HashEvent
from ionhash.hasher import hashlib_hash_function_provider
from .util import binary_reader_over


_ion_prefix = 'ion::'
_invalid_ion_prefix = 'invalid_ion::'


class _TestValue:
    def __init__(self, string):
        self.ion = string
        self.valid_ion = None

        if self.ion.startswith(_ion_prefix):
            self.valid_ion = True
            self.ion = self.ion[len(_ion_prefix):]

        if self.ion.startswith(_invalid_ion_prefix):
            self.valid_ion = False
            self.ion = self.ion[len(_invalid_ion_prefix):]

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

    def annotated_symbol(self):
        return self.symbol() + "::" + self.symbol()

    def annotated_string(self):
        return self.symbol() + "::" + self.string()

    def annotated_long_string(self):
        return self.symbol() + "::" + self.long_string()

    def annotated_clob(self):
        return self.symbol() + "::" + self.clob()

    def annotated_blob(self):
        return self.symbol() + "::" + self.blob()

    def __str__(self):
        return self.ion


def _test_strings():
    path = abspath(join(abspath(__file__), '..', '..', 'tests', 'big-list-of-naughty-strings.txt'))
    file = open(path)
    lines = [line.rstrip('\n') for line in file]
    file.close()

    def _is_test(string):
        return not (string == '' or string[0] == '#')

    lines = filter(_is_test, lines)
    strings = []
    for line in lines:
        tv = _TestValue(line)

        strings.append(tv.symbol())
        strings.append(tv.string())
        strings.append(tv.long_string())
        strings.append(tv.clob())
        strings.append(tv.blob())

        strings.append(tv.symbol() + "::" + tv.symbol())
        strings.append(tv.symbol() + "::" + tv.string())
        strings.append(tv.symbol() + "::" + tv.long_string())
        strings.append(tv.symbol() + "::" + tv.clob())
        strings.append(tv.symbol() + "::" + tv.blob())

        strings.append(tv.symbol() + "::{" + tv.symbol() + ":" + tv.symbol() + "}")
        strings.append(tv.symbol() + "::{" + tv.symbol() + ":" + tv.string() + "}")
        strings.append(tv.symbol() + "::{" + tv.symbol() + ":" + tv.long_string() + "}")
        strings.append(tv.symbol() + "::{" + tv.symbol() + ":" + tv.clob() + "}")
        strings.append(tv.symbol() + "::{" + tv.symbol() + ":" + tv.blob() + "}")

        if tv.valid_ion:
            strings.append(tv.ion)
            strings.append(tv.symbol() + "::" + tv.ion)
            strings.append(tv.symbol() + "::{" + tv.symbol() + ":" + tv.ion + "}")
            strings.append(tv.symbol() + "::{" + tv.symbol() + ":" + tv.symbol() + "::" + tv.ion + "}")

        # list
        strings.append(
              tv.symbol() + "::["
                  + tv.symbol() + ", "
                  + tv.string() + ", "
                  + tv.long_string() + ", "
                  + tv.clob() + ", "
                  + tv.blob() + ", "
                  + (tv.ion if tv.valid_ion else "")
                  + "]")

        # sexp
        strings.append(
              tv.symbol() + "::("
                  + tv.symbol() + " "
                  + tv.string() + " "
                  + tv.long_string() + " "
                  + tv.clob() + " "
                  + tv.blob() + " "
                  + (tv.ion if tv.valid_ion else "")
                  + ")")

        # multiple annotations
        strings.append(tv.symbol() + "::" + tv.symbol() + "::" + tv.symbol() + "::" + tv.string())

    return strings


@pytest.mark.parametrize("test_string", _test_strings())
def test(test_string):
    tv = _TestValue(test_string)
    events = []
    hr_digest = None
    try:
        reader = hash_reader(binary_reader_over(test_string),
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

