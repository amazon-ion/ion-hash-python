from amazon.ion.core import MultimapValue
from amazon.ion.simple_types import _ion_type_for
from amazon.ion.simple_types import IonPyDict

from collections import MutableMapping, OrderedDict
import six


# ion-python's IonPyDict may shuffle the order of struct fields,
# which causes failures when verifying the expected digests
# for some struct tests in ion_hash_tests.ion.
#
# Installing this custom implementation maintains the order
# and thus allows the tests to pass.
#
# This code is a clone/mutate of amazon.ion.core's Multimap in
# which self.__store is an OrderedDict() instead of a dict.
class _OrderedMultimap(MutableMapping):
    """
    Dictionary that can hold multiple values for the same key

    In order not to break existing customers, getting and inserting elements with ``[]`` keeps the same behaviour
    as the built-in dict. If multiple elements are already mapped to the key, ``[]`  will return
    the newest one.

    To map multiple elements to a key, use the ``add_item`` operation.
    To retrieve all the values map to a key, use ``get_all_values``.
    """

    def __init__(self, *args, **kwargs):
        super().__init__()
        self.__store = OrderedDict()
        if args is not None and len(args) > 0:
            for key, value in six.iteritems(args[0]):
                self.__store[key] = MultimapValue(value)

    def __getitem__(self, key):
        return self.__store[key][len(self.__store[key]) - 1]  # Return only one in order not to break clients

    def __delitem__(self, key):
        del self.__store[key]

    def __setitem__(self, key, value):
        self.__store[key] = MultimapValue(value)

    def __len__(self):
        return sum([len(values) for values in six.itervalues(self.__store)])

    def __iter__(self):
        for key in six.iterkeys(self.__store):
            yield key

    def add_item(self, key, value):
        if key in self.__store:
            self.__store[key].append(value)
        else:
            self.__setitem__(key, value)

    def get_all_values(self, key):
        return self.__store[key]

    def iteritems(self):
        for key in self.__store:
            for value in self.__store[key]:
                yield (key, value)

    def items(self):
        output = []
        for k, v in self.iteritems():
            output.append((k, v))
        return output

    def __repr__(self):
        return self.__store.__repr__()


IonPyDict = _ion_type_for('IonPyDict', _OrderedMultimap)

