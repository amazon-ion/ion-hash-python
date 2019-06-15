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

