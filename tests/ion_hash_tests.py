import amazon.ion.simpleion as ion
import hashlib
import traceback
from six import StringIO
from io import BytesIO
import time

import amazon.ion.reader as ion_reader
from amazon.ion.reader_managed import managed_reader
from amazon.ion.reader_text import text_reader
from amazon.ion.reader_binary import binary_reader
from amazon.ion.reader import NEXT_EVENT
from amazon.ion.core import IonEventType
from amazon.ionhash.hash_reader import hashing_reader
from amazon.ionhash.hash_reader import HashEvent
from amazon.ionhash.hasher import _bytearray_comparator

def hash_function_provider():
    return IdentityHash()


cnt = 0

class IdentityHash:
    def __init__(self):
        self._bytes = bytearray()
        global cnt
        cnt += 1
        self._cnt = cnt

    def update(self, bytes):
        #print " ", self._cnt, "update:", hex_string(bytes)
        self._bytes.extend(bytes)

    def digest(self):
        bytes = self._bytes
        self._bytes = bytearray()
        #print " ", self._cnt, "digest:", hex_string(bytes)
        return bytes


def hex_string(bytes):
    if bytes is None:
        return 'None'
    return ''.join('{:02x} '.format(x) for x in bytes)


def _execute_test(test_id, test):
    print test_id,
    try:
        if 'ion' in test:
            test_name = ion.dumps(test['ion'], binary=False)
            buffer = StringIO(test_name)
            base_reader = text_reader(is_unicode=True)

        if '10n' in test:
            test_name = "10n: (" + hex_string(test['10n'])[:-1] + ")"

            bytes = bytearray([0xE0, 0x01, 0x00, 0xEA])    # $ion_1_0
            for byte in test['10n']:
                bytes.append(byte)

            buffer = BytesIO(bytes)
            base_reader = binary_reader()

        reader = hashing_reader(
            ion_reader.blocking_reader(managed_reader(base_reader, None), buffer),
            hash_function_provider)

        expect = test['expect']
        identity_digest = expect['identity'][-1]
        expected_bytes = bytearray()
        for byte in identity_digest:
            expected_bytes.append(byte)

        reader.next()
        event = reader.send(NEXT_EVENT)
        while event.event_type is not IonEventType.STREAM_END:
            event = reader.send(NEXT_EVENT)

        actual_bytes = reader.send(HashEvent.DIGEST)

        if expected_bytes == actual_bytes:
            print ".", test_name
            return True
        else:
            print "F", test_name
            print "    expected: ", hex_string(expected_bytes)
            print "      actual: ", hex_string(actual_bytes)

    except Exception as e:
        time.sleep(1)
        print "E", test_name
        #print "    Exception: ", e
        traceback.print_exc()
        time.sleep(1)

    return False


test_count = 0
test_pass = 0
test_fail = 0

f = open('ion_hash_tests.ion')
ion_tests = ion.loads(f.read(), single_value=False)
f.close()
for ion_test in ion_tests:
    test_count += 1

    #if test_count == 153:
        #_execute_test(test_count, ion_test)

    passed = _execute_test(test_count, ion_test)
    if passed:
        test_pass += 1
    else:
        test_fail += 1

print
print test_pass, "of", test_count, "tests passed (", test_fail, "failures)"
