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

from ionhash.hasher import hash_reader
from ionhash.hasher import HashEvent
from ionhash.hasher import hashlib_hash_function_provider

from .util import consume
from .util import binary_reader_over
from .util import hash_function_provider


def test_hashlib_hash_function_provider():
    ion_str = '[1, 2, {a: 3, b: (4 {c: 5} 6) }, 7]'
    algorithm = "md5"

    # calculate expected digest
    hr = hash_reader(binary_reader_over(ion_str), hash_function_provider(algorithm))
    consume(hr)
    expected_digest = hr.send(HashEvent.DIGEST)

    # calculate digest using hashlib_hash_function_provider
    hr = hash_reader(binary_reader_over(ion_str), hashlib_hash_function_provider(algorithm))
    consume(hr)
    actual_digest = hr.send(HashEvent.DIGEST)

    assert actual_digest == expected_digest

