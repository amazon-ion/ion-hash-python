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

from .util import hex_string


# overrides pytest failure message for bytearray assertions
def pytest_assertrepr_compare(op, left, right):
    if isinstance(left, bytearray) and isinstance(right, bytearray) and op == "==":
        return [     "expected: " + hex_string(right),
                "       actual: " + hex_string(left)]

    if isinstance(left, list) and isinstance(right, list) and op == "==":
        result = ["expected: ["]
        for i in right:
            result.append("      " + hex_string(i) + ",")
        result.append("     ]")

        result.append("     actual: [")
        for i in left:
            result.append("      " + hex_string(i) + ",")
        result.append("     ]")

        return result

