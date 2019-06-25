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

