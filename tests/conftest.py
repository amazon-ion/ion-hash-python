# overrides pytest failure message for bytearray assertions
def pytest_assertrepr_compare(op, left, right):
    if isinstance(left, bytearray) and isinstance(right, bytearray) and op == "==":
        return [     "expected: " + _hex_string(right),
                "       actual: " + _hex_string(left)]

    if isinstance(left, list) and isinstance(right, list) and op == "==":
        result = ["expected: ["]
        for i in right:
            result.append("      " + _hex_string(i) + ",")
        result.append("     ]")

        result.append("     actual: [")
        for i in left:
            result.append("      " + _hex_string(i) + ",")
        result.append("     ]")

        return result


def _hex_string(_bytes):
    if _bytes is None:
        return 'None'
    if isinstance(_bytes, bytes) or isinstance(_bytes, bytearray):
        return ''.join(' %02x' % x for x in _bytes)
    return _bytes

