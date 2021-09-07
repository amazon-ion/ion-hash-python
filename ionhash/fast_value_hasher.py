from amazon.ion.core import IonType
from amazon.ion.simple_types import IonPyNull

from functools import cmp_to_key

from ionhash.hasher import _bytearray_comparator, _scalar_or_null_split_parts, _serialize_null, \
    _UPDATE_SCALAR_HASH_BYTES_JUMP_TABLE, _BEGIN_MARKER, _TQ, _END_MARKER, \
    _BEGIN_MARKER_BYTE, _END_MARKER_BYTE, _TQ_ANNOTATED_VALUE, _escape, _TQ_SYMBOL_SID0


class _IonEventDuck:
    """Looks like an IonEvent, quacks like an IonEvent...
    Used for sending scalar values to the existing ion_binary_writer serializers.
    """
    def __init__(self, value, ion_type):
        self.value = value
        self.ion_type = ion_type


# H(value) → h(s(value))
def hash_value(value, hfp):
    """An implementation of the [Ion Hash algorithm](https://github.com/amzn/ion-hash/blob/gh-pages/docs/spec.md)
    for the Ion data model that doesn't instantiate any ion_readers or ion_writers.

    Args:
        value: the Ion value to hash
        hfp: hash function provider

    Returns:
        Ion Hash digest of the given Ion value
    """
    hash_fn = hfp()
    hash_fn.update(serialize_value(value, hfp))
    return hash_fn.digest()


# s(value) → serialized bytes
def serialize_value(value, hfp):
    """Transforms an Ion value to its Ion Hash serialized representation.

    Args:
        value: the Ion value to serialize
        hfp: hash function provider

    Returns:
        bytes representing the given Ion value, serialized according to the Ion Hash algorithm
    """
    if value.ion_annotations:
        return _s_annotated_value(value, hfp)
    else:
        return _s_value(value, hfp)


# s(annotated value) → B || TQ || s(annotation1) || s(annotation2) || ... || s(annotationn) || s(value) || E
def _s_annotated_value(value, hfp):
    return _BEGIN_MARKER + _TQ_ANNOTATED_VALUE + b''.join([_write_symbol(a) for a in value.ion_annotations]) \
           + _s_value(value, hfp) + _END_MARKER


# s(struct) → B || TQ || escape(concat(sort(H(field1), H(field2), ..., H(fieldn)))) || E
# s(list) or s(sexp) → B || TQ || s(value1) || s(value2) || ... || s(valuen)) || E
# s(scalar) → B || TQ || escape(representation) || E
def _s_value(value, hfp):
    ion_type = value.ion_type
    is_ion_null = isinstance(value, IonPyNull)
    if ion_type == IonType.STRUCT and not is_ion_null:
        field_hashes = [_h_field(field_name, field_value, hfp) for [field_name, field_value] in value.iteritems()]
        field_hashes.sort(key=cmp_to_key(_bytearray_comparator))
        return _BEGIN_MARKER + bytes([_TQ[IonType.STRUCT]]) + _escape(b''.join(field_hashes)) + _END_MARKER
    elif ion_type in [IonType.LIST, IonType.SEXP] and not is_ion_null:
        return _BEGIN_MARKER + bytes([_TQ[ion_type]]) \
               + b''.join([bytes(serialize_value(child, hfp)) for child in value]) + _END_MARKER
    else:
        serializer = _serialize_null if is_ion_null else _UPDATE_SCALAR_HASH_BYTES_JUMP_TABLE[ion_type]
        scalar_bytes = serializer(_IonEventDuck(None if is_ion_null else value, ion_type))
        [tq, representation] = _scalar_or_null_split_parts(ion_type, scalar_bytes)
        if len(representation) == 0:
            return bytes([_BEGIN_MARKER_BYTE, tq, _END_MARKER_BYTE])
        else:
            return b''.join([_BEGIN_MARKER, bytes([tq]), _escape(representation), _END_MARKER])


# H(field) → h(s(fieldname) || s(fieldvalue))
def _h_field(field_name, field_value, hfp):
    hash_fn = hfp()
    hash_fn.update(_write_symbol(field_name) + serialize_value(field_value, hfp))
    return hash_fn.digest()


# Precomputed bytes for the unknown symbol (sid $0) case of _write_symbol()
_SERIALIZED_SYMBOL_SID0_BYTES = bytes([_BEGIN_MARKER_BYTE, _TQ_SYMBOL_SID0, _END_MARKER_BYTE])


# Function for writing symbol tokens (annotations and field names)
# Has simplified logic compared to regular function because we can make some assumptions about it
# Namely, that this value does not have annotations, it is always type "symbol"
def _write_symbol(text_or_symbol_token):
    text = getattr(text_or_symbol_token, 'text', text_or_symbol_token)
    if text is None:
        return _SERIALIZED_SYMBOL_SID0_BYTES
    else:
        return _BEGIN_MARKER + bytes([_TQ[IonType.SYMBOL]]) \
               + _escape(bytearray(text, encoding="utf-8")) + _END_MARKER
