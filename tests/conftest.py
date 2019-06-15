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


def _hex_string(element):
    if element is None:
        return 'None'
    if isinstance(element, bytes) or isinstance(element, bytearray):
        return ''.join(' %02x' % x for x in element)
    return element.__repr__()

