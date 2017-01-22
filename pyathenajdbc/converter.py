# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals
import logging
import binascii
from datetime import datetime
from decimal import Decimal

import jpype
from future.utils import iteritems, PY3


_logger = logging.getLogger(__name__)


if PY3:
    unicode = str


def _to_none(result_set, index):
    return None


def _to_unicode(result_set, index):
    val = result_set.getString(index)
    was_null = result_set.wasNull()
    if was_null:
        return None
    elif isinstance(val, unicode):
        return val
    else:
        return unicode(val)


def _to_date(result_set, index):
    val = result_set.getDate(index)
    was_null = result_set.wasNull()
    if was_null:
        return None
    return datetime.strptime(val.toString(), '%Y-%m-%d').date()


def _to_datetime(result_set, index):
    val = result_set.getTimestamp(index)
    was_null = result_set.wasNull()
    if was_null:
        return None
    return datetime.strptime(val.toString(), '%Y-%m-%d %H:%M:%S.%f')


def _to_float(result_set, index):
    val = result_set.getDouble(index)
    was_null = result_set.wasNull()
    if was_null:
        return None
    return float(val)


def _to_int(result_set, index):
    val = result_set.getLong(index)
    was_null = result_set.wasNull()
    if was_null:
        return None
    return int(val)


def _to_decimal(result_set, index):
    val = result_set.getString(index)
    was_null = result_set.wasNull()
    if was_null:
        return None
    return Decimal(val)


def _to_boolean(result_set, index):
    val = result_set.getBoolean(index)
    was_null = result_set.wasNull()
    if was_null:
        return None
    elif val:
        return True
    else:
        return False


def _to_array_str(result_set, index):
    val = result_set.getString(index)
    was_null = result_set.wasNull()
    if was_null:
        return None
    return val


def _to_binary(result_set, index):
    val = result_set.getString(index)
    was_null = result_set.wasNull()
    if was_null:
        return None
    return binascii.a2b_hex(''.join(val.split(' ')))


def _to_default(result_set, index):
    val = result_set.getObject(index)
    was_null = result_set.wasNull()
    if was_null:
        return None
    else:
        return val


class JDBCTypeConverter(object):

    def __init__(self):
        types = jpype.java.sql.Types
        self.jdbc_type_mappings = dict()
        for field in types.__javaclass__.getClassFields():
            self.jdbc_type_mappings[field.getName()] = field.getStaticAttribute()
        _logger.debug(self.jdbc_type_mappings)
        self.converter_mappings = dict()
        for k, v in iteritems(_DEFAULT_CONVERTERS):
            type_code = self.jdbc_type_mappings.get(k, None)
            if type_code is not None:
                self.converter_mappings[type_code] = v
            else:
                _logger.warning('%s is not defined java.sql.Types.', k)

    def convert(self, type_code, result_set, index):
        converter = self.converter_mappings.get(type_code, _to_default)
        return converter(result_set, index)

    def register_converter(self, type_name, converter):
        type_code = self.jdbc_type_mappings.get(type_name, None)
        if type_code:
            self.converter_mappings[type_code] = converter
        else:
            _logger.warning('%s is not defined java.sql.Types.', type_name)


_DEFAULT_CONVERTERS = {
    'NULL': _to_none,
    'BOOLEAN': _to_boolean,
    'TINYINT': _to_int,
    'SMALLINT': _to_int,
    'BIGINT': _to_int,
    'INTEGER': _to_int,
    'REAL': _to_float,
    'DOUBLE': _to_float,
    'FLOAT': _to_float,
    'CHAR': _to_unicode,
    'NCHAR': _to_unicode,
    'VARCHAR': _to_unicode,
    'NVARCHAR': _to_unicode,
    'LONGVARCHAR': _to_unicode,
    'LONGNVARCHAR': _to_unicode,
    'DATE': _to_date,
    'TIMESTAMP': _to_datetime,
    'TIMESTAMP_WITH_TIMEZONE': _to_datetime,
    'ARRAY': _to_array_str,
    'DECIMAL': _to_decimal,
    'NUMERIC': _to_decimal,
    'BINARY': _to_binary,
    'VARBINARY': _to_binary,
    'LONGVARBINARY': _to_binary,
    # TODO Converter impl
    # 'TIME': ???,
    # 'BIT': ???,
    # 'CLOB': ???,
    # 'BLOB': ???,
    # 'NCLOB': ???,
    # 'STRUCT': ???,
    # 'JAVA_OBJECT': ???,
    # 'REF_CURSOR': ???,
    # 'REF': ???,
    # 'DISTINCT': ???,
    # 'DATALINK': ???,
    # 'SQLXML': ???,
    # 'OTHER': ???,
    # 'ROWID': ???,
}
