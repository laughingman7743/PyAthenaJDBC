# -*- coding: utf-8 -*-
import binascii
import logging
from abc import ABCMeta, abstractmethod
from copy import deepcopy
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Callable, Dict, Optional

import jpype

_logger = logging.getLogger(__name__)  # type: ignore


class JDBCTypeConverter(object, metaclass=ABCMeta):
    def __init__(
        self,
        mappings: Dict[str, Callable[[Any, int], Optional[Any]]],
        default: Callable[[Any, int], Optional[Any]] = None,
    ) -> None:
        modifier = jpype.java.lang.reflect.Modifier
        types = jpype.java.sql.Types
        self._jdbc_type_name_mappings = dict()
        self._jdbc_type_code_mappings = dict()
        for field in types.class_.getFields():
            if modifier.isStatic(field.getModifiers()):
                name = field.getName()
                attr = getattr(types, field.getName())
                self._jdbc_type_name_mappings[name] = attr
                self._jdbc_type_code_mappings[attr] = name
        _logger.debug(self._jdbc_type_name_mappings)

        self._mappings = dict()
        if mappings:
            for k, v in mappings.items():
                type_code = self._jdbc_type_name_mappings.get(k, None)
                if type_code is not None:
                    self._mappings[type_code] = v
                else:
                    _logger.warning("%s is not defined java.sql.Types.", k)
        self._default = default

    @property
    def mappings(self) -> Dict[str, Callable[[Any, int], Optional[Any]]]:
        return self._mappings

    def get(self, type_: str) -> Optional[Callable[[Any, int], Optional[Any]]]:
        return self.mappings.get(type_, self._default)

    def set(self, type_: str, converter: Callable[[Any, int], Optional[Any]]) -> None:
        type_code = self._jdbc_type_name_mappings.get(type_, None)
        if type_code:
            self._mappings[type_code] = converter
        else:
            _logger.warning("%s is not defined java.sql.Types.", type_)

    def remove(self, type_: str) -> None:
        self.mappings.pop(type_, None)

    def update(self, mappings: Dict[str, Callable[[Any, int], Optional[Any]]]) -> None:
        self.mappings.update(mappings)

    @abstractmethod
    def convert(self, type_code: Any, result_set: Any, index: int) -> Optional[Any]:
        raise NotImplementedError  # pragma: no cover

    def get_jdbc_type_code(self, type_name: Any) -> Any:
        return self._jdbc_type_name_mappings.get(type_name, None)

    def get_jdbc_type_name(self, type_code: Any) -> Any:
        return self._jdbc_type_code_mappings.get(type_code, None)


def _to_none(result_set: Any, index: int) -> None:
    return None


def _to_string(result_set: Any, index: int) -> Optional[str]:
    val = result_set.getString(index)
    was_null = result_set.wasNull()
    if was_null:
        return None
    else:
        return str(val)


def _to_date(result_set: Any, index: int) -> Optional[date]:
    val = result_set.getDate(index)
    was_null = result_set.wasNull()
    if was_null:
        return None
    return datetime.strptime(val.toString(), "%Y-%m-%d").date()


def _to_datetime(result_set: Any, index: int) -> Optional[datetime]:
    val = result_set.getTimestamp(index)
    was_null = result_set.wasNull()
    if was_null:
        return None
    return datetime.strptime(val.toString(), "%Y-%m-%d %H:%M:%S.%f")


def _to_float(result_set: Any, index: int) -> Optional[float]:
    val = result_set.getDouble(index)
    was_null = result_set.wasNull()
    if was_null:
        return None
    return float(val)


def _to_int(result_set: Any, index: int) -> Optional[int]:
    val = result_set.getLong(index)
    was_null = result_set.wasNull()
    if was_null:
        return None
    return int(val)


def _to_decimal(result_set: Any, index: int) -> Optional[Decimal]:
    val = result_set.getString(index)
    was_null = result_set.wasNull()
    if was_null:
        return None
    return Decimal(val)


def _to_boolean(result_set: Any, index: int) -> Optional[bool]:
    val = result_set.getBoolean(index)
    was_null = result_set.wasNull()
    if was_null:
        return None
    elif val:
        return True
    else:
        return False


def _to_array_str(result_set: Any, index: int) -> Optional[str]:
    val = result_set.getArray(index)
    was_null = result_set.wasNull()
    if was_null:
        return None
    return str(val.toString())


def _to_binary(result_set: Any, index: int) -> Optional[bytes]:
    val = result_set.getString(index)
    was_null = result_set.wasNull()
    if was_null:
        return None
    return binascii.a2b_hex("".join(val.split(" ")))


def _to_default(result_set: Any, index: int) -> Optional[Any]:
    val = result_set.getObject(index)
    was_null = result_set.wasNull()
    if was_null:
        return None
    else:
        return val


_DEFAULT_JDBC_CONVERTERS: Dict[str, Callable[[Any, int], Optional[Any]]] = {
    "NULL": _to_none,
    "BOOLEAN": _to_boolean,
    "TINYINT": _to_int,
    "SMALLINT": _to_int,
    "BIGINT": _to_int,
    "INTEGER": _to_int,
    "REAL": _to_float,
    "DOUBLE": _to_float,
    "FLOAT": _to_float,
    "CHAR": _to_string,
    "NCHAR": _to_string,
    "VARCHAR": _to_string,
    "NVARCHAR": _to_string,
    "LONGVARCHAR": _to_string,
    "LONGNVARCHAR": _to_string,
    "DATE": _to_date,
    "TIMESTAMP": _to_datetime,
    "TIMESTAMP_WITH_TIMEZONE": _to_datetime,
    "ARRAY": _to_array_str,
    "DECIMAL": _to_decimal,
    "NUMERIC": _to_decimal,
    "BINARY": _to_binary,
    "VARBINARY": _to_binary,
    "LONGVARBINARY": _to_binary,
    "JAVA_OBJECT": _to_string,
    # TODO Converter impl
    # 'TIME': ???,
    # 'BIT': ???,
    # 'CLOB': ???,
    # 'BLOB': ???,
    # 'NCLOB': ???,
    # 'STRUCT': ???,
    # 'REF_CURSOR': ???,
    # 'REF': ???,
    # 'DISTINCT': ???,
    # 'DATALINK': ???,
    # 'SQLXML': ???,
    # 'OTHER': ???,
    # 'ROWID': ???,
}


class DefaultJDBCTypeConverter(JDBCTypeConverter):
    def __init__(self) -> None:
        super(DefaultJDBCTypeConverter, self).__init__(
            mappings=deepcopy(_DEFAULT_JDBC_CONVERTERS), default=_to_default
        )

    def convert(self, type_code: Any, result_set: Any, index: int) -> Optional[Any]:
        converter = self._mappings.get(type_code, _to_default)
        return converter(result_set, index)
