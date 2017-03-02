# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals
import logging

from past.builtins.misc import xrange

from pyathenajdbc.error import ProgrammingError, NotSupportedError
from pyathenajdbc.util import reraise_dbapi_error


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
        self._arraysize = value

    @property
    def rownumber(self):
        return self._rownumber

    @property
    def rowcount(self):
        """By default, return -1 to indicate that this is not supported."""
        return -1

    @property
    def has_result_set(self):
        return self._result_set and self._meta_data and not self._result_set.isClosed()

    @property
    def description(self):
        if self._description:
            return self._description
        if not self.has_result_set:
            return None
        self._description = [
            (
                self._meta_data.getColumnName(i),
                self._meta_data.getColumnType(i),
                self._meta_data.getColumnDisplaySize(i),
                None,
                self._meta_data.getPrecision(i),
                self._meta_data.getScale(i),
                self._meta_data.isNullable(i)
            )
            for i in xrange(1, self._meta_data.getColumnCount() + 1)
        ]
        return self._description

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

    def execute(self, operation, *parameter_args, **parameter_kwargs):
        if self.is_closed:
            raise ProgrammingError('Connection is closed.')

        query = self._formatter.format(operation, *parameter_args, **parameter_kwargs)
        try:
            _logger.debug(query)
            self._description = None
            self._rownumber = 0
            result_set = self._statement.executeQuery(query)
            if result_set:
                self._result_set = result_set
                self._meta_data = result_set.getMetaData()
                self._update_count = -1
            else:
                self._result_set = None
                self._meta_data = None
                self._update_count = self._statement.getUpdatecount()
        except Exception as e:
            _logger.exception('Failed to execute query.')
            reraise_dbapi_error()

    def executemany(self, operation, seq_of_parameters):
        raise NotSupportedError

    def cancel(self):
        # TODO AthenaStatement dose not support cancel operation.
        #      Because ResultSet is not NULL only when query state finished.
        # public void cancel() throws SQLException {
        #     this.checkOpen();
        #     if(!this.isCancelled.get()) {
        #         this.lock.lock();
        #         try {
        #             if(this.currentResult.get() != null) {
        #                 ((AthenaResultSet)this.currentResult.get()).getClient().cancel();
        #                 this.isCancelled.compareAndSet(false, true);
        #             }
        #         } finally {
        #             this.lock.unlock();
        #         }
        #     }
        # }
        if self.is_closed:
            raise ProgrammingError('Connection is closed.')
        self._statement.cancel()

    def _fetch(self, size):
        if self.is_closed:
            raise ProgrammingError('Connection is closed.')
        if not self.has_result_set:
            raise ProgrammingError('No result set.')

        self._result_set.setFetchSize(size)
        if not self._result_set.next():
            return None
        self._rownumber += 1
        return tuple([
            self._converter.convert(self._meta_data.getColumnType(i), self._result_set, i)
            for i in range(1, self._meta_data.getColumnCount() + 1)
        ])

    def fetchone(self):
        return self._fetch(1)

    def fetchmany(self, size=None):
        if not size or size <= 0:
            size = self._arraysize
        rows = []
        for i in range(size):
            row = self._fetch(size)
            if row:
                rows.append(row)
            else:
                break
        return rows

    def fetchall(self):
        rows = []
        while True:
            row = self._fetch(self._arraysize)
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
