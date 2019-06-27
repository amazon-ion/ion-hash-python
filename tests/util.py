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

import hashlib
from io import BytesIO

import amazon.ion.simpleion as ion
import amazon.ion.reader as ion_reader
from amazon.ion.core import IonEventType
from amazon.ion.reader_managed import managed_reader
from amazon.ion.reader_binary import binary_reader
from amazon.ion.reader import NEXT_EVENT
from amazon.ion.reader import SKIP_EVENT
from ionhash.hasher import IonHasher


def hash_function_provider(algorithm, updates=[], digests=[]):
    def _f():
        if algorithm == "identity":
            return _IdentityHash(updates, digests)
        elif algorithm == "md5":
            return _MD5Hash(updates, digests)
    return _f


class _IdentityHash(IonHasher):
    def __init__(self, updates, digests):
        self._bytes = bytearray()
        self._updates = updates
        self._digests = digests

    def update(self, _bytes):
        self._updates.append(_bytes)
        self._bytes.extend(_bytes)

    def digest(self):
        _bytes = self._bytes
        self._bytes = bytearray()
        self._digests.append(_bytes)
        return _bytes


class _MD5Hash(IonHasher):
    def __init__(self, updates, digests):
        self._m = hashlib.md5()
        self._updates = updates
        self._digests = digests

    def update(self, _bytes):
        self._m.update(_bytes)
        self._updates.append(_bytes)

    def digest(self):
        digest = self._m.digest()
        self._m = hashlib.md5()
        self._digests.append(digest)
        return digest


def sexp_to_bytearray(sexp):
    ba = bytearray()
    for b in sexp:
        ba.append(b)
    return ba


def hex_string(obj):
    if obj is None:
        return 'None'
    if isinstance(obj, bytes) or isinstance(obj, bytearray):
        return ''.join(' %02x' % x for x in obj)
    return obj.__repr__()


def binary_reader_over(ion_str):
    value = ion.loads(ion_str)
    _bytes = ion.dumps(value, binary=True)
    return ion_reader.blocking_reader(managed_reader(binary_reader(), None), BytesIO(_bytes))


def consume(reader, skip_list=[]):
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

