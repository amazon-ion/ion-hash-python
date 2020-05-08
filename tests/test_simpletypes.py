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

from amazon.ion.core import IonType
from amazon.ion.simple_types import IonPyFloat


def test_simpletype_with_annotation():
    ion_float = IonPyFloat.from_value(IonType.FLOAT, 123, (u'abc',))
    assert ion_float.ion_hash('md5') == \
           b'\x48\x87\x5a\x7c\x70\x8f\xfa\x6e\x24\x1b\x60\x35\xb7\xcd\x92\xcd'

