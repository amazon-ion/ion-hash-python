from amazon.ionhash.hasher import _bytearray_comparator


def test_equals():
    assert _bytearray_comparator(b'\x01\x02\x03',
                                 b'\x01\x02\x03') == 0


def test_less_than():
    assert _bytearray_comparator(b'\x01\x02\x03',
                                 b'\x01\x02\x04') == -1


def test_less_than_due_to_length():
    assert _bytearray_comparator(b'\x01\x02\x03',
                                 b'\x01\x02\x03\x04') == -1


def test_greater_than():
    assert _bytearray_comparator(b'\x01\x02\x04',
                                 b'\x01\x02\x03') == 1


def test_greater_than_due_to_length():
    assert _bytearray_comparator(b'\x01\x02\x03\x04',
                                 b'\x01\x02\x03') == 1


def test_unsigned_behavior():
    # verify signed bytes are being correctly handled as unsigned bytes
    assert _bytearray_comparator(b'\x01', b'\x7f') == -1
    assert _bytearray_comparator(b'\x01', b'\x80') == -1
    assert _bytearray_comparator(b'\x01', b'\xff') == -1
    assert _bytearray_comparator(b'\x7f', b'\x01') == 1
    assert _bytearray_comparator(b'\x80', b'\x01') == 1
    assert _bytearray_comparator(b'\xff', b'\x01') == 1
