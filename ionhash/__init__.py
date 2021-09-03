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

from amazon.ion.simple_types import _IonNature

from ionhash.fast_value_hasher import hash_value
from ionhash.hasher import hashlib_hash_function_provider


# pydoc for this method is DUPLICATED in docs/index.rst
def ion_hash(self, algorithm=None, hash_function_provider=None):
    """Given an algorithm or hash_function_provider, computes the Ion hash
    of this value.

    Args:
        algorithm:
            A string corresponding to the name of a hash algorithm supported
            by the `hashlib` module.

        hash_function_provider:
            A function that returns a new ``IonHasher`` instance when called.

            Note that multiple ``IonHasher`` instances may be required to hash a single value
            (depending on the type of the Ion value).

    Returns:
        `bytes` that represent the Ion hash of this value for the specified algorithm
        or hash_function_provider.
    """
    if algorithm is None and hash_function_provider is None:
        raise Exception("Either 'algorithm' or 'hash_function_provider' must be specified")
    if algorithm is not None and hash_function_provider is not None:
        raise Exception("Either 'algorithm' or 'hash_function_provider' must be specified, not both")

    if algorithm is not None:
        hfp = hashlib_hash_function_provider(algorithm)
    else:
        hfp = hash_function_provider

    return hash_value(self, hfp)


# adds the `ion_hash` method to all simpleion value classes:
_IonNature.ion_hash = ion_hash

