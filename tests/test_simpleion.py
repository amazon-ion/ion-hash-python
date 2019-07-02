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

import amazon.ion.simpleion as ion
from .util import hex_string


def test_simpleion_invalid_no_params():
    try:
        ion.loads('blah').ion_hash()
        raise Exception("Expected an exception to be raised")
    except:
        pass


def test_simpleion_invalid_too_many_params():
    def noop_function():
        pass

    try:
        ion.loads('blah').ion_hash("md5", noop_function)
        raise Exception("Expected an exception to be raised")
    except:
        pass


def test_simpleion_with_algorithm():
    assert ion.loads('"hello"').ion_hash("md5") == \
        b'\x9e\xb1\x12\x17\x8d\xfa\x00\x57\xf7\xdc\x79\x44\x67\x9d\x99\xb8'


# remainder of the testing for the ion_hash() extension to simpleion classes is
# covered by test_ion_hash_tests.test_simpleion

