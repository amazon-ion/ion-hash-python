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

import sys
import amazon.ion.simpleion as ion
import ionhash


if len(sys.argv) < 3:
    print("Utility that prints the Ion Hash of the top-level values in a file.");
    print();
    print("Usage:");
    print("  ion-hash [algorithm] [filename]");
    print();
    print("where [algorithm] is a hash function such as sha256");
    print();
    sys.exit(1);


algorithm = sys.argv[1]
input_file = sys.argv[2]

opts = 'r'
with open(input_file, 'rb') as f:
    first_four = f.read(4)
    if first_four == b'\xe0\x01\x00\xea':
        opts = 'rb'

with open(input_file, opts) as f:
    values = ion.loads(f.read(), single_value=False)

for value in values:
    try:
        digest = value.ion_hash('md5')
        print(''.join('%02x ' % x for x in value.ion_hash('md5'))[0:-1])
    except Exception as e:
        print('[unable to digest: ' + str(e) + ']')

