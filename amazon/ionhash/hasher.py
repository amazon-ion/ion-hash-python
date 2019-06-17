from functools import cmp_to_key
import hashlib

from amazon.ion.core import DataEvent
from amazon.ion.core import IonEvent
from amazon.ion.core import IonEventType
from amazon.ion.core import IonType
from amazon.ion.util import Enum
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
    DISABLE_HASHING = 0
    ENABLE_HASHING = 1
    DIGEST = 2


def hashlib_hash_function_provider(algorithm):
    def _f():
        return _HashlibHash(algorithm)
    return _f


class _HashlibHash:
    def __init__(self, algorithm):
        self._algorithm = algorithm
        self._hasher = hashlib.new(self._algorithm)

    def update(self, _bytes):
        self._hasher.update(_bytes)

    def digest(self):
        digest = self._hasher.digest()
        self._hasher = hashlib.new(self._algorithm)
        return digest


def hash_reader(reader, hash_function_provider, hashing_enabled=True):
    hr = _hasher(_hash_reader_handler, reader, hash_function_provider, hashing_enabled)
    next(hr)    # prime the generator
    return hr


def hash_writer(writer, hash_function_provider, hashing_enabled=True):
    hw = _hasher(_hash_writer_handler, writer, hash_function_provider, hashing_enabled)
    next(hw)    # prime the generator
    return hw


def _hasher(handler, delegate, hash_function_provider, hashing_enabled=True):
    hasher = _Hasher(hash_function_provider)
    output = None
    while True:
        input = yield output
        output = handler(input, output, hasher, delegate, hashing_enabled)
        while output is not None:
            if input == HashEvent.DISABLE_HASHING:
                hashing_enabled = False
                input = yield

            elif input == HashEvent.ENABLE_HASHING:
                hashing_enabled = True
                input = yield

            elif input == HashEvent.DIGEST:
                input = yield hasher.digest()

            else:
                input = yield output

            output = handler(input, output, hasher, delegate, hashing_enabled)


def _hash_reader_handler(input, output, hasher, reader, hashing_enabled):
    if isinstance(input, DataEvent):
        if input == SKIP_EVENT and hashing_enabled:
            target_depth = output.depth
            if output.event_type != IonEventType.CONTAINER_START:
                target_depth = output.depth - 1

            output = reader.send(NEXT_EVENT)
            while output.event_type != IonEventType.STREAM_END and output.depth > target_depth:
                _hash_event(hasher, output)
                output = reader.send(NEXT_EVENT)

        else:
            output = reader.send(input)

    if isinstance(output, IonEvent) and hashing_enabled:
        _hash_event(hasher, output)

    return output


def _hash_writer_handler(input, output, hasher, writer, hashing_enabled):
    if isinstance(input, IonEvent):
        output = writer.send(input)
        if hashing_enabled:
            _hash_event(hasher, input)

    return output


def _hash_event(hasher, event):
    if event.event_type is IonEventType.CONTAINER_START:
        hasher.step_in(event)
    elif event.event_type is IonEventType.CONTAINER_END:
        hasher.step_out()
    elif event.event_type is not IonEventType.STREAM_END:
        hasher.scalar(event)


_BEGIN_MARKER_BYTE = 0x0B
_END_MARKER_BYTE = 0x0E
_ESCAPE_BYTE = 0x0C
_BEGIN_MARKER = b'\x0B'
_END_MARKER = b'\x0E'

_TQ_SYMBOL_SID0 = 0x71
_TQ_ANNOTATED_VALUE = b'\xE0'

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


class _Hasher:
    def __init__(self, hash_function_provider):
        self._hash_function_provider = hash_function_provider
        self._current_hasher = _Serializer(self._hash_function_provider())
        self._hasher_stack = [self._current_hasher]

    def scalar(self, ion_event):
        self._current_hasher.scalar(ion_event)

    def step_in(self, ion_event):
        hf = self._current_hasher.hash_function
        if isinstance(self._current_hasher, _StructSerializer):
            hf = self._hash_function_provider()

        if ion_event.ion_type == IonType.STRUCT:
            self._current_hasher = _StructSerializer(hf, self._hash_function_provider)
        else:
            self._current_hasher = _Serializer(hf)

        self._hasher_stack.append(self._current_hasher)
        self._current_hasher.step_in(ion_event)

    def step_out(self):
        self._current_hasher.step_out()
        popped_hasher = self._hasher_stack.pop()
        self._current_hasher = self._hasher_stack[-1]

        if isinstance(self._current_hasher, _StructSerializer):
            digest = popped_hasher.digest()
            self._current_hasher.append_field_hash(_escape(digest))

    def digest(self):
        return self._current_hasher.digest()


class _Serializer:
    def __init__(self, hash_function):
        self.hash_function = hash_function
        self._has_container_annotations = False

    def _handle_field_name(self, ion_event):
        if ion_event.field_name is not None:
            self._write_symbol(ion_event.field_name)

    def _handle_annotations_begin(self, ion_event, is_container=False):
        if ion_event.annotations.__len__() > 0:
            self._begin_marker()
            self.hash_function.update(_TQ_ANNOTATED_VALUE)
            for annotation in ion_event.annotations:
                self._write_symbol(annotation)
            if is_container:
                self._has_container_annotations = True

    def _handle_annotations_end(self, ion_event = None, is_container=False):
        if (ion_event is not None and ion_event.annotations.__len__() > 0) \
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
        [tq, representation] = _scalar_or_null_split_parts(IonEvent(None, IonType.SYMBOL), _bytes)
        self._update(bytes([tq]))
        if representation.__len__() > 0:
            self._update(_escape(representation))
        self._end_marker()

    def scalar(self, ion_event):
        self._handle_annotations_begin(ion_event)
        self._begin_marker()
        scalar_bytes = _serializer(ion_event)(ion_event)
        [tq, representation] = _scalar_or_null_split_parts(ion_event, scalar_bytes)
        self._update(bytes([tq]))
        if representation.__len__() > 0:
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
    def __init__(self, hash_function, hash_function_provider):
        super().__init__(hash_function)
        self._scalar_serializer = _Serializer(hash_function_provider())
        self._field_hashes = []

    def scalar(self, ion_event):
        self._scalar_serializer._handle_field_name(ion_event)
        self._scalar_serializer.scalar(ion_event)
        digest = self._scalar_serializer.digest()
        self.append_field_hash(_escape(digest))

    def step_out(self):
        self._field_hashes.sort(key=cmp_to_key(_bytearray_comparator))
        for digest in self._field_hashes:
            self._update(digest)
        super().step_out()

    def append_field_hash(self, digest):
        self._field_hashes.append(digest)


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
    if token.sid == 0:
        ba.append(_TQ_SYMBOL_SID0)
    else:
        ba.append(_TQ[IonType.SYMBOL])
        ba.extend(bytearray(token.text, encoding="utf-8"))
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


# split scalar bytes into TQ and representation; also handles any special case binary cleanup
def _scalar_or_null_split_parts(ion_event, _bytes):
    offset = 1 + _get_length_length(_bytes)

    # the representation is everything after TL (first byte) and length
    representation = _bytes[offset:]

    tq = _bytes[0]
    if (ion_event.ion_type != IonType.BOOL
            and ion_event.ion_type != IonType.SYMBOL
            and tq & 0x0F != 0x0F):      # not a null value
        tq &= 0xF0                       # zero-out the L nibble

    return [tq, representation]


# returns a count of bytes in the "length" field
def _get_length_length(_bytes):
    if (_bytes[0] & 0x0F) == 0x0E:
        # read subsequent byte(s) as the "length" field
        for i in range(1, len(_bytes)):
            if (_bytes[i] & 0x80) != 0:
                return i
        raise Exception("Problem while reading VarUInt!")
    return 0


def _bytearray_comparator(a, b):
    a_len = a.__len__()
    b_len = b.__len__()
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

