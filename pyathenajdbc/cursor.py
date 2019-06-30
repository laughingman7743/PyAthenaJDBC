# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import logging

from future.utils import raise_from
from past.builtins.misc import xrange

from pyathenajdbc.error import (DatabaseError, NotSupportedError, ProgrammingError)
from pyathenajdbc.util import (attach_thread_to_jvm, synchronized)

_logger = logging.getLogger(__name__)


class Cursor(object):

    DEFAULT_FETCH_SIZE = 1000

    def __init__(self, connection, converter, formatter):
        self._connection = connection
        self._converter = converter
        self._formatter = formatter

        self._rownumber = None
        self._arraysize = self.DEFAULT_FETCH_SIZE

        self._description = None
        self._statement = self.connection.createStatement()
        self._result_set = None
        self._meta_data = None
        self._update_count = -1

    @property
    def connection(self):
        return self._connection

    @property
    def arraysize(self):
        return self._arraysize

    @arraysize.setter
    def arraysize(self, value):
        if value <= 0 or value > self.DEFAULT_FETCH_SIZE:
            raise ProgrammingError('MaxResults is more than maximum allowed length {0}.'.format(
                self.DEFAULT_FETCH_SIZE))
        self._arraysize = value

    @property
    def rownumber(self):
        return self._rownumber

    @property
    def rowcount(self):
        """By default, return -1 to indicate that this is not supported."""
        return -1

    @property
    @attach_thread_to_jvm
    def has_result_set(self):
        return self._result_set and self._meta_data and not self._result_set.isClosed()

    @property
    @attach_thread_to_jvm
    def description(self):
        if self._description:
            return self._description
        if not self.has_result_set:
            return None
        self._description = [
            (
                self._meta_data.getColumnName(i),
                self._converter.get_jdbc_type_name(self._meta_data.getColumnType(i)),
                self._meta_data.getColumnDisplaySize(i),
                None,
                self._meta_data.getPrecision(i),
                self._meta_data.getScale(i),
                self._meta_data.isNullable(i)
            )
            for i in xrange(1, self._meta_data.getColumnCount() + 1)
        ]
        return self._description

    @attach_thread_to_jvm
    @synchronized
    def close(self):
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
    def is_closed(self):
        return self._connection is None

    def _reset_state(self):
        self._description = None
        self._result_set = None
        self._meta_data = None
        self._rownumber = 0

    @attach_thread_to_jvm
    @synchronized
    def execute(self, operation, parameters=None):
        if self.is_closed:
            raise ProgrammingError('Connection is closed.')

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
            _logger.exception('Failed to execute query.')
            raise_from(DatabaseError(e), e)

    def executemany(self, operation, seq_of_parameters):
        raise NotSupportedError

    @attach_thread_to_jvm
    @synchronized
    def cancel(self):
        if self.is_closed:
            raise ProgrammingError('Connection is closed.')
        self._statement.cancel()

    @attach_thread_to_jvm
    def _fetch(self):
        if self.is_closed:
            raise ProgrammingError('Connection is closed.')
        if not self.has_result_set:
            raise ProgrammingError('No result set.')

        if not self._result_set.next():
            return None
        self._rownumber += 1
        return tuple([
            self._converter.convert(self._meta_data.getColumnType(i), self._result_set, i)
            for i in xrange(1, self._meta_data.getColumnCount() + 1)
        ])

    @synchronized
    def fetchone(self):
        return self._fetch()

    @synchronized
    def fetchmany(self, size=None):
        if not size or size <= 0:
            size = self._arraysize
        rows = []
        for i in xrange(size):
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
