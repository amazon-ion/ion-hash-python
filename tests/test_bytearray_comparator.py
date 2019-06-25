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

from amazon.ionhash.hasher import _bytearray_comparator


def test_equals():
    assert _bytearray_comparator(b'\x01\x02\x03',
                                 b'\x01\x02\x03') == 0


def test_less_than():
    assert _bytearray_comparator(b'\x01\x02\x03',
                                 b'\x01\x02\x04') == -1


def test_less_than_due_to_length():
    assert _bytearray_comparator(b'\x01\x02\x03',
                                 b'\x01\x02\x03\x04') == -1


def test_greater_than():
    assert _bytearray_comparator(b'\x01\x02\x04',
                                 b'\x01\x02\x03') == 1


def test_greater_than_due_to_length():
    assert _bytearray_comparator(b'\x01\x02\x03\x04',
                                 b'\x01\x02\x03') == 1


def test_unsigned_behavior():
    # verify signed bytes are being correctly handled as unsigned bytes
    assert _bytearray_comparator(b'\x01', b'\x7f') == -1
    assert _bytearray_comparator(b'\x01', b'\x80') == -1
    assert _bytearray_comparator(b'\x01', b'\xff') == -1
    assert _bytearray_comparator(b'\x7f', b'\x01') == 1
    assert _bytearray_comparator(b'\x80', b'\x01') == 1
    assert _bytearray_comparator(b'\xff', b'\x01') == 1
