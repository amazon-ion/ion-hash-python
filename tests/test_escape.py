from amazon.ionhash.hasher import _escape


def test_escape():
    _run_test(b'', b'')
    _run_test(b'\x10\x11\x12\x13', b'\x10\x11\x12\x13')
    _run_test(b'\x0b', b'\x0c\x0b')
    _run_test(b'\x0e', b'\x0c\x0e')
    _run_test(b'\x0c', b'\x0c\x0c')
    _run_test(b'\x0b\x0e\x0c', b'\x0c\x0b\x0c\x0e\x0c\x0c')
    _run_test(b'\x0c\x0c', b'\x0c\x0c\x0c\x0c')
    _run_test(b'\x0c\x10\x0c\x11\x0c\x12\x0c', b'\x0c\x0c\x10\x0c\x0c\x11\x0c\x0c\x12\x0c\x0c')


def _run_test(_bytes, expected_bytes):
    assert _escape(_bytes) == expected_bytes

