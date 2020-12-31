# -*- coding: utf-8 -*-
import logging
from typing import Any, Dict, List, Optional, Tuple, cast

from pyathenajdbc.converter import JDBCTypeConverter
from pyathenajdbc.error import DatabaseError, ProgrammingError
from pyathenajdbc.formatter import Formatter
from pyathenajdbc.util import attach_thread_to_jvm, synchronized

_logger = logging.getLogger(__name__)  # type: ignore


class Cursor(object):

    DEFAULT_FETCH_SIZE: int = 1000

    def __init__(
        self,
        connection: Any,
        converter: JDBCTypeConverter,
        formatter: Formatter,
    ):
        self._connection = connection
        self._converter = converter
        self._formatter = formatter

        self._rownumber: Optional[int] = None
        self._arraysize: int = self.DEFAULT_FETCH_SIZE

        self._description: Optional[
            List[
                Tuple[
                    Optional[Any],
                    Optional[Any],
                    Optional[Any],
                    None,
                    Optional[Any],
                    Optional[Any],
                    Optional[Any],
                ]
            ]
        ] = None
        self._statement: Any = self.connection.createStatement()
        self._result_set: Optional[Any] = None
        self._meta_data: Optional[Any] = None
        self._update_count: int = -1

    @property
    def connection(self) -> Any:
        return self._connection

    @property
    def arraysize(self) -> int:
        return self._arraysize

    @arraysize.setter
    def arraysize(self, value: int):
        if value <= 0 or value > self.DEFAULT_FETCH_SIZE:
            raise ProgrammingError(
                "MaxResults is more than maximum allowed length {0}.".format(
                    self.DEFAULT_FETCH_SIZE
                )
            )
        self._arraysize = value

    @property
    def rownumber(self) -> Optional[int]:
        return self._rownumber

    @property
    def rowcount(self) -> int:
        """By default, return -1 to indicate that this is not supported."""
        return -1

    @property  # type: ignore
    @attach_thread_to_jvm
    def has_result_set(self) -> bool:
        return (
            self._result_set is not None
            and self._meta_data is not None
            and not self._result_set.isClosed()
        )

    @property  # type: ignore
    @attach_thread_to_jvm
    def description(
        self,
    ) -> Optional[
        List[
            Tuple[
                Optional[Any],
                Optional[Any],
                Optional[Any],
                None,
                Optional[Any],
                Optional[Any],
                Optional[Any],
            ]
        ]
    ]:
        if self._description:
            return self._description
        if not self.has_result_set:
            return None
        meta_data = cast(Any, self._meta_data)
        self._description = [
            (
                meta_data.getColumnName(i),
                self._converter.get_jdbc_type_name(meta_data.getColumnType(i)),
                meta_data.getColumnDisplaySize(i),
                None,
                meta_data.getPrecision(i),
                meta_data.getScale(i),
                meta_data.isNullable(i),
            )
            for i in range(1, meta_data.getColumnCount() + 1)
        ]
        return self._description

    @attach_thread_to_jvm
    @synchronized
    def close(self) -> None:
        self._meta_data = None
        if self._result_set and not self._result_set.isClosed():
            self._result_set.close()
        self._result_set = None
        if self._statement and not self._statement.isClosed():
            self._statement.close()
        self._statement = None
        self._description = None
        self._connection = None

    @property
    def is_closed(self) -> bool:
        return self._connection is None

    def _reset_state(self) -> None:
        self._description = None
        self._result_set = None
        self._meta_data = None
        self._rownumber = 0

    @attach_thread_to_jvm
    @synchronized
    def execute(self, operation: str, parameters: Optional[Dict[str, Any]] = None):
        if self.is_closed:
            raise ProgrammingError("Connection is closed.")

        query = self._formatter.format(operation, parameters)
        _logger.debug(query)
        try:
            self._reset_state()
            has_result_set = self._statement.execute(query)
            if has_result_set:
                self._result_set = self._statement.getResultSet()
                self._result_set.setFetchSize(self._arraysize)
                self._meta_data = self._result_set.getMetaData()
                self._update_count = -1
            else:
                self._update_count = self._statement.getUpdateCount()
        except Exception as e:
            _logger.exception("Failed to execute query.")
            raise DatabaseError(*e.args) from e
        return self

    def executemany(
        self, operation: str, seq_of_parameters: List[Optional[Dict[str, Any]]]
    ):
        for parameters in seq_of_parameters:
            self.execute(operation, parameters)
        # Operations that have result sets are not allowed with executemany.
        self._reset_state()

    @attach_thread_to_jvm
    @synchronized
    def cancel(self) -> None:
        if self.is_closed:
            raise ProgrammingError("Connection is closed.")
        self._statement.cancel()

    @attach_thread_to_jvm
    def _fetch(self):
        if self.is_closed:
            raise ProgrammingError("Connection is closed.")
        if not self.has_result_set:
            raise ProgrammingError("No result set.")

        result_set = cast(Any, self._result_set)
        if not result_set.next():
            return None
        if self._rownumber is None:
            self._rownumber = 0
        self._rownumber += 1
        meta_data = cast(Any, self._meta_data)
        return tuple(
            [
                self._converter.convert(meta_data.getColumnType(i), result_set, i)
                for i in range(1, meta_data.getColumnCount() + 1)
            ]
        )

    @synchronized
    def fetchone(self):
        return self._fetch()

    @synchronized
    def fetchmany(self, size: int = None):
        if not size or size <= 0:
            size = self._arraysize
        rows = []
        for i in range(size):
            row = self._fetch()
            if row:
                rows.append(row)
            else:
                break
        return rows

    @synchronized
    def fetchall(self):
        rows = []
        while True:
            row = self._fetch()
            if row:
                rows.append(row)
            else:
                break
        return rows

    def setinputsizes(self, sizes):
        """Does nothing by default"""
        pass

    def setoutputsize(self, size, column=None):
        """Does nothing by default"""
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def __next__(self):
        row = self.fetchone()
        if row is None:
            raise StopIteration
        else:
            return row

    next = __next__

    def __iter__(self):
        return self
