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

from amazon.ionhash.hasher import _escape


def test_escape():
    _run_test(b'', b'')
    _run_test(b'\x10\x11\x12\x13', b'\x10\x11\x12\x13')
    _run_test(b'\x0b', b'\x0c\x0b')
    _run_test(b'\x0e', b'\x0c\x0e')
    _run_test(b'\x0c', b'\x0c\x0c')
    _run_test(b'\x0b\x0e\x0c', b'\x0c\x0b\x0c\x0e\x0c\x0c')
    _run_test(b'\x0c\x0c', b'\x0c\x0c\x0c\x0c')
    _run_test(b'\x0c\x10\x0c\x11\x0c\x12\x0c', b'\x0c\x0c\x10\x0c\x0c\x11\x0c\x0c\x12\x0c\x0c')


def _run_test(_bytes, expected_bytes):
    assert _escape(_bytes) == expected_bytes

