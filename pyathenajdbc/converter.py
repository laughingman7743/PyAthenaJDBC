# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals
import logging
import binascii
from datetime import datetime
from decimal import Decimal

import jpype
from future.utils import iteritems
from past.types import unicode


_logger = logging.getLogger(__name__)


def _to_none(varchar_value):
    return None


def _to_unicode(varchar_value):
    if varchar_value is None:
        return None
    elif isinstance(varchar_value, unicode):
        return varchar_value
    else:
        return unicode(varchar_value)


def _to_date(varchar_value):
    if varchar_value is None:
        return None
    return datetime.strptime(varchar_value, '%Y-%m-%d').date()


def _to_datetime(varchar_value):
    if varchar_value is None:
        return None
    return datetime.strptime(varchar_value, '%Y-%m-%d %H:%M:%S.%f')


def _to_float(varchar_value):
    if varchar_value is None:
        return None
    return float(varchar_value)


def _to_int(varchar_value):
    if varchar_value is None:
        return None
    return int(varchar_value)


def _to_decimal(varchar_value):
    if varchar_value is None:
        return None
    return Decimal(varchar_value)


def _to_boolean(varchar_value):
    if varchar_value is None:
        return None
    elif varchar_value.lower() == 'true':
        return True
    elif varchar_value.lower() == 'false':
        return False
    else:
        return None


def _to_binary(varchar_value):
    if varchar_value is None:
        return None
    return binascii.a2b_hex(''.join(varchar_value.split(' ')))


def _to_default(varchar_value):
    if varchar_value is None:
        return None
    else:
        return varchar_value


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

    def convert(self, type_code, varchar_value):
        converter = self.converter_mappings.get(type_code, _to_default)
        return converter(varchar_value)

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
    'ARRAY': _to_unicode,
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
