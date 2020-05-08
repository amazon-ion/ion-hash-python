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

"""Adds an `ion_hash()` method to all simpleion value classes, and provides
readers/writers that hash Ion values according to the Ion Hash Specification."""

from abc import ABC, abstractmethod
from functools import cmp_to_key
import hashlib

from amazon.ion.core import DataEvent
from amazon.ion.core import IonEvent
from amazon.ion.core import IonEventType
from amazon.ion.core import IonType
from amazon.ion.util import Enum
from amazon.ion.util import coroutine
from amazon.ion.reader import NEXT_EVENT
from amazon.ion.reader import SKIP_EVENT
from amazon.ion.writer_binary_raw import _serialize_blob
from amazon.ion.writer_binary_raw import _serialize_bool
from amazon.ion.writer_binary_raw import _serialize_clob
from amazon.ion.writer_binary_raw import _serialize_decimal
from amazon.ion.writer_binary_raw import _serialize_float
from amazon.ion.writer_binary_raw import _serialize_int
from amazon.ion.writer_binary_raw import _serialize_timestamp


class HashEvent(Enum):
    """Events that may be pushed into a hash_reader or hash_writer coroutine,
    in addition to those allowed by the wrapped reader/writer.

    Attributes:
        DIGEST:  produces `bytes` that represents the hash of the Ion values read/written
    """
    DIGEST = 0


def hashlib_hash_function_provider(algorithm):
    """A hash function provider based on `hashlib`."""
    def _f():
        return _HashlibHash(algorithm)
    return _f


class IonHasher(ABC):
    """Abstract class declaring the hashing methods that must be implemented in order to
    support a hash function for use by `hash_reader` or `hash_writer`."""
    @abstractmethod
    def update(self, _bytes):
        """Updates the hash function with the specified _bytes."""
        pass

    @abstractmethod
    def digest(self):
        """Returns a digest of the accumulated bytes passed to `update`, and resets the `IonHasher`
        to its initial state."""
        pass


class _HashlibHash(IonHasher):
    """Implements the expected hash function methods for the specified algorithm using `hashlib`."""
    def __init__(self, algorithm):
        self._algorithm = algorithm
        self._hasher = hashlib.new(self._algorithm)

    def update(self, _bytes):
        self._hasher.update(_bytes)

    def digest(self):
        digest = self._hasher.digest()
        self._hasher = hashlib.new(self._algorithm)
        return digest


@coroutine
def hash_reader(reader, hash_function_provider):
    """Provides a coroutine that wraps an ion-python reader and adds Ion Hash functionality.

    The given coroutine yields `bytes` when given ``HashEvent.DIGEST``.  Otherwise, the
    couroutine's behavior matches that of the wrapped reader.

    Notes:
        The coroutine translates any amazon.ion.reader.SKIP_EVENTs into a series of amazon.ion.reader.NEXT_EVENTs in order
        to ensure that the hash correctly includes any subsequent or nested values.

    Args:
        reader(couroutine):
            An ion-python reader coroutine.

        hash_function_provider(function):
            A function that returns a new ``IonHasher`` instance when called.

            Note that multiple ``IonHasher`` instances may be required to hash a single value
            (depending on the type of the Ion value).

    Yields:
        bytes:
            The result of hashing.

        other values:
            As defined by the provided reader coroutine.
    """
    return _hasher(_hash_reader_handler, reader, hash_function_provider)


@coroutine
def hash_writer(writer, hash_function_provider):
    """Provides a coroutine that wraps an ion-python writer and adds Ion Hash functionality.

    The given coroutine yields `bytes` when given ``HashEvent.DIGEST``.  Otherwise, the
    couroutine's behavior matches that of the wrapped writer.

    Args:
        writer(coroutine):
            An ion-python writer coroutine.

        hash_function_provider(function):
            A function that returns a new ``IonHasher`` instance when called.

            Note that multiple ``IonHasher`` instances may be required to hash a single value
            (depending on the type of the Ion value).

    Yields:
        bytes:
            The result of hashing.

        other values:
            As defined by the provided writer coroutine.
    """
    return _hasher(_hash_writer_handler, writer, hash_function_provider)


def _hasher(handler, delegate, hash_function_provider):
    """Provides a coroutine that wraps an ion-python reader or writer and adds Ion Hash functionality."""
    hasher = _Hasher(hash_function_provider)
    output = None
    input = yield output
    while True:
        if isinstance(input, HashEvent):
            if input == HashEvent.DIGEST:
                input = yield hasher.digest()
        else:
            output = handler(input, output, hasher, delegate)
            if output is None:
                break
            input = yield output


def _hash_reader_handler(input, output, hasher, reader):
    """Handles input to a reader-based coroutine."""
    if isinstance(input, DataEvent):
        if input == SKIP_EVENT:
            # translate SKIP_EVENTs into the appropriate number
            # of NEXT_EVENTs to ensure hash correctness:
            target_depth = output.depth
            if output.event_type != IonEventType.CONTAINER_START:
                target_depth = output.depth - 1

            output = reader.send(NEXT_EVENT)
            while output.event_type != IonEventType.STREAM_END and output.depth > target_depth:
                _hash_event(hasher, output)
                output = reader.send(NEXT_EVENT)

        else:
            output = reader.send(input)

    if isinstance(output, IonEvent):
        _hash_event(hasher, output)

    return output


def _hash_writer_handler(input, output, hasher, writer):
    """Handles input to a writer-based coroutine."""
    if isinstance(input, IonEvent):
        output = writer.send(input)
        _hash_event(hasher, input)

    return output


def _hash_event(hasher, event):
    """Maps an IonEvent to the appropriate hasher method."""
    if event.event_type is IonEventType.CONTAINER_START:
        hasher.step_in(event)
    elif event.event_type is IonEventType.CONTAINER_END:
        hasher.step_out()
    elif event.event_type is not IonEventType.STREAM_END:
        hasher.scalar(event)


# Ion Hash special bytes:
_BEGIN_MARKER_BYTE = 0x0B
_END_MARKER_BYTE = 0x0E
_ESCAPE_BYTE = 0x0C
_BEGIN_MARKER = bytes([_BEGIN_MARKER_BYTE])
_END_MARKER = bytes([_END_MARKER_BYTE])

# Type/Qualifier byte for each Ion type.  This lookup table is used when
# serializing null.*, container, string, and symbol values.  The TQ byte
# for other values is derived from serialization performed by functions
# from the writer_binary_raw module.
_TQ = {
    IonType.NULL:      0x0F,
    IonType.BOOL:      0x10,
    IonType.INT:       0x20,
    IonType.FLOAT:     0x40,
    IonType.DECIMAL:   0x50,
    IonType.TIMESTAMP: 0x60,
    IonType.SYMBOL:    0x70,
    IonType.STRING:    0x80,
    IonType.CLOB:      0x90,
    IonType.BLOB:      0xA0,
    IonType.LIST:      0xB0,
    IonType.SEXP:      0xC0,
    IonType.STRUCT:    0xD0,
}
_TQ_SYMBOL_SID0 = 0x71
_TQ_ANNOTATED_VALUE = bytes([0xE0])


class _Hasher:
    """Primary driver of the Ion hash algorithm.

    This class maintains a stack of serializers corresponding to the nesting of Ion data
    being hashed.
    """
    def __init__(self, hash_function_provider):
        self._hash_function_provider = hash_function_provider
        self._current_hasher = _Serializer(self._hash_function_provider(), 0)
        self._hasher_stack = [self._current_hasher]

    def scalar(self, ion_event):
        self._current_hasher.scalar(ion_event)

    def step_in(self, ion_event):
        hf = self._current_hasher.hash_function
        if isinstance(self._current_hasher, _StructSerializer):
            hf = self._hash_function_provider()

        if ion_event.ion_type == IonType.STRUCT:
            self._current_hasher = _StructSerializer(hf, self._depth(), self._hash_function_provider)
        else:
            self._current_hasher = _Serializer(hf, self._depth())

        self._hasher_stack.append(self._current_hasher)
        self._current_hasher.step_in(ion_event)

    def step_out(self):
        if self._depth() == 0:
            raise Exception("Hasher cannot step_out any further")
        self._current_hasher.step_out()
        popped_hasher = self._hasher_stack.pop()
        self._current_hasher = self._hasher_stack[-1]

        if isinstance(self._current_hasher, _StructSerializer):
            digest = popped_hasher.digest()
            self._current_hasher.append_field_hash(digest)

    def digest(self):
        if self._depth() != 0:
            raise Exception("A digest may only be provided at the same depth hashing started")
        return self._current_hasher.digest()

    def _depth(self):
        return len(self._hasher_stack) - 1


class _Serializer:
    """Serialization/hashing logic for all Ion types except struct."""
    def __init__(self, hash_function, depth):
        self.hash_function = hash_function
        self._has_container_annotations = False
        self._depth = depth

    def _handle_field_name(self, ion_event):
        if ion_event.field_name is not None and self._depth > 0:
            self._write_symbol(ion_event.field_name)

    def _handle_annotations_begin(self, ion_event, is_container=False):
        if len(ion_event.annotations) > 0:
            self._begin_marker()
            self.hash_function.update(_TQ_ANNOTATED_VALUE)
            for annotation in ion_event.annotations:
                self._write_symbol(annotation)
            if is_container:
                self._has_container_annotations = True

    def _handle_annotations_end(self, ion_event = None, is_container=False):
        if (ion_event is not None and len(ion_event.annotations) > 0) \
                or (is_container and self._has_container_annotations):
            self._end_marker()
            if is_container:
                self._has_container_annotations = False

    def _update(self, _bytes):
        return self.hash_function.update(_bytes)

    def _begin_marker(self):
        return self.hash_function.update(_BEGIN_MARKER)

    def _end_marker(self):
        return self.hash_function.update(_END_MARKER)

    def _write_symbol(self, token):
        self._begin_marker()
        _bytes = _serialize_symbol_token(token)
        [tq, representation] = _scalar_or_null_split_parts(IonType.SYMBOL, _bytes)
        self._update(bytes([tq]))
        if len(representation) > 0:
            self._update(_escape(representation))
        self._end_marker()

    def scalar(self, ion_event):
        self._handle_annotations_begin(ion_event)
        self._begin_marker()
        scalar_bytes = _serializer(ion_event)(ion_event)
        [tq, representation] = _scalar_or_null_split_parts(ion_event.ion_type, scalar_bytes)
        self._update(bytes([tq]))
        if len(representation) > 0:
            self._update(_escape(representation))
        self._end_marker()
        self._handle_annotations_end(ion_event)

    def step_in(self, ion_event):
        self._handle_field_name(ion_event)
        self._handle_annotations_begin(ion_event, is_container=True)
        self._begin_marker()
        self._update(bytes([_TQ[ion_event.ion_type]]))

    def step_out(self):
        self._end_marker()
        self._handle_annotations_end(is_container=True)

    def digest(self):
        return self.hash_function.digest()


class _StructSerializer(_Serializer):
    """Serialization/hashing logic for Ion structs."""
    def __init__(self, hash_function, depth, hash_function_provider):
        super().__init__(hash_function, depth)
        self._scalar_serializer = _Serializer(hash_function_provider(), depth + 1)
        self._field_hashes = []

    def scalar(self, ion_event):
        self._scalar_serializer._handle_field_name(ion_event)
        self._scalar_serializer.scalar(ion_event)
        digest = self._scalar_serializer.digest()
        self.append_field_hash(digest)

    def step_out(self):
        self._field_hashes.sort(key=cmp_to_key(_bytearray_comparator))
        for digest in self._field_hashes:
            self._update(_escape(digest))
        super().step_out()

    def append_field_hash(self, digest):
        self._field_hashes.append(digest)


#
# Serialization functions for scalar types:
#

def _serialize_null(ion_event):
    ba = bytearray()
    ba.append(_TQ[ion_event.ion_type] | _TQ[IonType.NULL])
    return ba


def _serialize_string(ion_event):
    ba = bytearray()
    ba.append(_TQ[IonType.STRING])
    ba.extend(ion_event.value.encode('utf-8'))
    return ba


def _serialize_symbol(ion_event):
    return _serialize_symbol_token(ion_event.value)


def _serialize_symbol_token(token):
    ba = bytearray()
    is_token = hasattr(token, 'sid')
    if is_token and token.sid == 0:
        ba.append(_TQ_SYMBOL_SID0)
    else:
        ba.append(_TQ[IonType.SYMBOL])
        ba.extend(bytearray(token.text if is_token else token, encoding="utf-8"))
    return ba


def _serializer(ion_event):
    if ion_event.value is None:
        return _serialize_null
    return _UPDATE_SCALAR_HASH_BYTES_JUMP_TABLE[ion_event.ion_type]


_UPDATE_SCALAR_HASH_BYTES_JUMP_TABLE = {
    IonType.NULL:      _serialize_null,
    IonType.BOOL:      _serialize_bool,
    IonType.INT:       _serialize_int,
    IonType.FLOAT:     _serialize_float,
    IonType.DECIMAL:   _serialize_decimal,
    IonType.TIMESTAMP: _serialize_timestamp,
    IonType.SYMBOL:    _serialize_symbol,
    IonType.STRING:    _serialize_string,
    IonType.CLOB:      _serialize_clob,
    IonType.BLOB:      _serialize_blob,
}


def _scalar_or_null_split_parts(ion_type, _bytes):
    """Splits scalar bytes into TQ and representation; also handles any special case binary cleanup."""
    offset = 1 + _get_length_length(_bytes)

    # the representation is everything after TL (first byte) and length
    representation = _bytes[offset:]

    tq = _bytes[0]
    if (ion_type != IonType.BOOL
            and ion_type != IonType.SYMBOL
            and tq & 0x0F != 0x0F):      # not a null value
        tq &= 0xF0                       # zero-out the L nibble

    return [tq, representation]


def _get_length_length(_bytes):
    """Returns a count of bytes in an Ion value's `length` field."""
    if (_bytes[0] & 0x0F) == 0x0E:
        # read subsequent byte(s) as the "length" field
        for i in range(1, len(_bytes)):
            if (_bytes[i] & 0x80) != 0:
                return i
        raise Exception("Problem while reading VarUInt!")
    return 0


def _bytearray_comparator(a, b):
    """Implements a comparator using the lexicographical ordering of octets as unsigned integers."""
    a_len = len(a)
    b_len = len(b)
    i = 0
    while i < a_len and i < b_len:
        a_byte = a[i]
        b_byte = b[i]
        if a_byte != b_byte:
            if a_byte - b_byte < 0:
                return -1
            else:
                return 1
        i += 1

    len_diff = a_len - b_len
    if len_diff < 0:
        return -1
    elif len_diff > 0:
        return 1
    else:
        return 0


def _escape(_bytes):
    """If _bytes contains one or more BEGIN_MARKER_BYTEs, END_MARKER_BYTEs, or ESCAPE_BYTEs,
    returns a new bytearray with such bytes preceeded by a ESCAPE_BYTE;  otherwise, returns
    the original _bytes unchanged."
    """
    for b in _bytes:
        if b == _BEGIN_MARKER_BYTE or b == _END_MARKER_BYTE or b == _ESCAPE_BYTE:
            # found a byte that needs to be escaped;  build a new byte array that
            # escapes that byte as well as any others
            escaped_bytes = bytearray()
            for c in _bytes:
                if c == _BEGIN_MARKER_BYTE or c == _END_MARKER_BYTE or c == _ESCAPE_BYTE:
                    escaped_bytes.append(_ESCAPE_BYTE)
                escaped_bytes.append(c)
            return escaped_bytes

    # no escaping needed, return the original _bytes
    return _bytes

