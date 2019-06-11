# overrides pytest failure message for bytearray assertions
def pytest_assertrepr_compare(op, left, right):
    print "type(left) type(right):", type(left), type(right)
    if isinstance(left, bytearray) and isinstance(right, bytearray) and op == "==":
        return ["bytearray mismatch:",
                "expected: " + _hex_string(right),
                "  actual: " + _hex_string(left)]
    if isinstance(left, list) and isinstance(right, list) and op == "==":
        result = ["bytearray mismatch:"]
        result.append("expected:")
        expected_strs = _hex_strings(right)
        for s in expected_strs:
            result.append("    " + s)

        result.append("  actual:")
        actual_strs = _hex_strings(left)
        for s in actual_strs:
            result.append("    " + s)

        return result

#def _hex_string(bytes):
    #if bytes is None:
        #return 'None'
    #return ''.join('{:02x} '.format(x) for x in bytes)

def _hex_strings(lst):
    strings = []
    for item in lst:
        strings.append(_hex_string(item))
    return strings

def _hex_string(_bytes):
    if _bytes is None:
        return 'None'
    if isinstance(_bytes, bytearray):
        #print "it's a bytearray, len:", _bytes.__len__()
        return ''.join('{:02x} '.format(x) for x in _bytes)
    if isinstance(_bytes, bytes):
        return ' '.join('%02x' % ord(x) for x in _bytes)
    print "unknown type", type(_bytes)
    return _bytes

