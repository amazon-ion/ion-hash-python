from functools import cmp_to_key

from amazon.ion.core import DataEvent
from amazon.ion.core import IonEvent
from amazon.ion.core import IonEventType
from amazon.ion.core import IonType
from amazon.ion.util import Enum
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


def hasher(reader, hash_function_provider, hashing_enabled = True):
    hasher = _Hasher(hash_function_provider)
    event = None
    while True:
        directive = yield event
        event = reader.send(directive)
        while event is not None:
            if directive == HashEvent.DISABLE_HASHING:
                hashing_enabled = False

            elif directive == HashEvent.ENABLE_HASHING:
                hashing_enabled = True

            elif directive == HashEvent.DIGEST:
                event = hasher.digest()

            elif isinstance(event, IonEvent) and event.event_type is not IonEventType.STREAM_END:
                if hashing_enabled:
                    if event.event_type is IonEventType.CONTAINER_START:
                        hasher.step_in(event)

                    elif event.event_type is IonEventType.CONTAINER_END:
                        hasher.step_out()

                    else:
                        hasher.update(event)

            directive = yield event

            if isinstance(directive, DataEvent):
                event = reader.send(directive)


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
        self._hash_function = self._hash_function_provider()
        self._current_hasher = _BaseSerializer(self._hash_function)
        self._hasher_stack = [self._current_hasher]

    def update(self, ion_event):
        _debug(ion_event)
        self._current_hasher.update(ion_event)

    def step_in(self, ion_event):
        _debug(ion_event)
        if ion_event.ion_type == IonType.STRUCT:
            # TBD fix to avoid _hash_function reference
            self._current_hasher = StructSerializer(self._current_hasher._hash_function, self._hash_function_provider)
        else:
            if isinstance(self._current_hasher, StructSerializer):
                self._current_hasher = _BaseSerializer(self._hash_function_provider())
            else:
                self._current_hasher = _BaseSerializer(self._hash_function)

        self._hasher_stack.append(self._current_hasher)
        self._current_hasher.step_in(ion_event)

    def step_out(self):
        _debug("step_out")
        self._current_hasher.step_out()

        if isinstance(self._hasher_stack[-2], StructSerializer):
            digest = self._current_hasher.digest()
            _debug("hasher.step_out.digest:", _hex_string(digest))
            self._hasher_stack[-2]._field_hashes.append(_escape(digest))      # TBD internalize _field_hashes
            _dump_hashes(self._hasher_stack[-2]._field_hashes, "hasher.step_out.hashes.from_stack")

        self._hasher_stack.pop()

        self._current_hasher = self._hasher_stack[-1]

        if isinstance(self._current_hasher, StructSerializer):
            _dump_hashes(self._current_hasher._field_hashes, "hasher.step_out.hashes")

    def digest(self):
        return self._hash_function.digest()


class _AbstractSerializer:
    def __init__(self, hash_function):
        self._hash_function = hash_function
        self._has_container_annotations = False

    def _handle_field_name(self, hf, ion_event):
        if ion_event.field_name is not None:
            _write_symbol(hf, ion_event.field_name)

    def _handle_annotations_begin(self, hf, ion_event, is_container=False):
        if ion_event.annotations.__len__() > 0:
            hf.update(_BEGIN_MARKER)
            hf.update(_TQ_ANNOTATED_VALUE)
            for annotation in ion_event.annotations:
                _write_symbol(hf, annotation)
            if is_container:
                self._has_container_annotations = True

    def _handle_annotations_end(self, hf, ion_event = None, is_container=False):
        if (ion_event is not None and ion_event.annotations.__len__() > 0) \
                or (is_container and self._has_container_annotations):
            hf.update(_END_MARKER)
            if is_container:
                self._has_container_annotations = False


class _BaseSerializer(_AbstractSerializer):
    def __init__(self, hash_function):
        _AbstractSerializer.__init__(self, hash_function)

    def update(self, ion_event):
        _debug("base.update")
        self._handle_annotations_begin(self._hash_function, ion_event)
        self._write_scalar(self._hash_function, ion_event)
        self._handle_annotations_end(self._hash_function, ion_event)

    def step_in(self, ion_event):
        _debug("base.step_in")
        self._handle_field_name(self._hash_function, ion_event)
        self._handle_annotations_begin(self._hash_function, ion_event, is_container=True)
        self._hash_function.update(_BEGIN_MARKER)
        self._hash_function.update(bytes([_TQ[ion_event.ion_type]]))

    def step_out(self):
        _debug("base.step_out")
        self._hash_function.update(_END_MARKER)
        self._handle_annotations_end(self._hash_function, is_container=True)

    def digest(self):
        return self._hash_function.digest()

    def _write_scalar(self, hf, ion_event):
        hf.update(_BEGIN_MARKER)

        scalar_bytes = _serializer(ion_event)(ion_event)
        [tq, representation] = _scalar_or_null_split_parts(ion_event, scalar_bytes)
        hf.update(bytes([tq]))
        if representation.__len__() > 0:
            hf.update(_escape(representation))

        hf.update(_END_MARKER)


# TBD refactor to extend an _AbstractSerializer class?
class StructSerializer(_BaseSerializer):
    def __init__(self, parent_hash_function, hash_function_provider):
        self._parent_hash_function = parent_hash_function
        _BaseSerializer.__init__(self, hash_function_provider())

        self._field_hashes = []
        self._scalar_serializer = _BaseSerializer(hash_function_provider())

    def update(self, ion_event):
        # fieldname
        self._handle_field_name(self._scalar_serializer._hash_function, ion_event)

        # value
        self._scalar_serializer.update(ion_event)

        digest = self._scalar_serializer._hash_function.digest()
        self._field_hashes.append(_escape(digest))
        _dump_hashes(self._field_hashes, "struct.update")

    def step_in(self, ion_event):
        _dump_hashes(self._field_hashes, "struct.step_in")

        self._handle_field_name(self._parent_hash_function, ion_event)

        self._handle_annotations_begin(self._parent_hash_function, ion_event, is_container=True)
        self._parent_hash_function.update(_BEGIN_MARKER)
        self._parent_hash_function.update(bytes([_TQ[IonType.STRUCT]]))


    def step_out(self):
        _dump_hashes(self._field_hashes, "struct.step_out")

        self._field_hashes.sort(key=cmp_to_key(_bytearray_comparator))
        for digest in self._field_hashes:
            self._parent_hash_function.update(digest)

        self._parent_hash_function.update(_END_MARKER)
        self._handle_annotations_end(self._parent_hash_function, is_container=True)

    def digest(self):
        return self._parent_hash_function.digest()


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
        ba.extend(bytearray(token.text, encoding="utf-8"))           # TBD escape?
    return ba


_symbol_event = IonEvent(None, IonType.SYMBOL)


def _write_symbol(hf, token):
    hf.update(_BEGIN_MARKER)
    _bytes = _serialize_symbol_token(token)
    [tq, representation] = _scalar_or_null_split_parts(_symbol_event, _bytes)
    hf.update(bytes([tq]))
    if representation.__len__() > 0:
        hf.update(_escape(representation))

    hf.update(_END_MARKER)


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


_debug_flag = 0


def _debug(*args):
    if _debug_flag > 0:
        for arg in args:
            print(arg,)
        print


def _hex_string(_bytes):
    if _bytes is None:
        return 'None'
    if isinstance(_bytes, bytes) or isinstance(_bytes, bytearray):
        return ''.join(' %02x' % x for x in _bytes)
    return _bytes


def _dump_hashes(hashes, id):
    if _debug_flag > 0:
        _debug("hashes:", id, ''.join(' {},'.format(_hex_string(h)) for h in hashes))

