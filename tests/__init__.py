import amazon.ion
from amazon.ion.simple_types import _ion_type_for
from collections import OrderedDict


class _CustomOrderedDict(OrderedDict):
    """ion-python's IonPyDict may shuffle the order of struct fields,
    which causes failures when verifying the expected digests
    for some struct tests in ion_hash_tests.ion.

    Installing this custom implementation maintains the order
    and thus allows the tests to pass."""
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def add_item(self, key, value):
        super().__setitem__(key, value)


amazon.ion.simple_types.IonPyDict = _ion_type_for('IonPyDict', _CustomOrderedDict)

