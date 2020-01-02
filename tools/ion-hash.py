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

import fileinput
from six import StringIO
import sys

from amazon.ion.core import IonEventType
import amazon.ion.reader as ion_reader
from amazon.ion.reader import NEXT_EVENT
from amazon.ion.reader_binary import binary_reader
from amazon.ion.reader_managed import managed_reader
from amazon.ion.reader_text import text_reader
import amazon.ion.simpleion as ion
from ionhash.hasher import hash_reader
from ionhash.hasher import HashEvent
from ionhash.hasher import hashlib_hash_function_provider


algorithm = sys.argv[1]
input_file = []
if len(sys.argv) > 2:
    input_file = sys.argv[2]

for line in fileinput.input(input_file):
    try:
        hr = hash_reader(
                ion_reader.blocking_reader(managed_reader(text_reader(is_unicode=True), None), StringIO(line)),
                hashlib_hash_function_provider(algorithm))
        while True:
            event = hr.send(NEXT_EVENT)
            if event.event_type == IonEventType.STREAM_END:
                break

        digest = hr.send(HashEvent.DIGEST)
        print(''.join('%02x ' % x for x in digest)[0:-1])
    except EOFError:
        break
    except Exception as e:
        print('[unable to digest: ' + str(e) + ']')

