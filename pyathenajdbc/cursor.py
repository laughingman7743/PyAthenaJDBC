# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals
import logging

import jpype
from future.utils import raise_from
from past.builtins.misc import xrange

from pyathenajdbc.error import (ProgrammingError, NotSupportedError,
                                DatabaseError, InternalError)
from pyathenajdbc.util import (attach_thread_to_jvm, synchronized,
                               to_datetime, unwrap_exception)


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

        self._output_location = None
        self._completion_date_time = None
        self._submission_date_time = None
        self._data_scanned_in_bytes = None
        self._execution_time_in_millis = None

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

    @property
    @attach_thread_to_jvm
    def query_id(self):
        if not self.has_result_set:
            return None
        return self._result_set.getClient().getQueryExecutionId()

    @property
    def output_location(self):
        return self._output_location

    @property
    def completion_date_time(self):
        return self._completion_date_time

    @property
    def submission_date_time(self):
        return self._submission_date_time

    @property
    def data_scanned_in_bytes(self):
        return self._data_scanned_in_bytes

    @property
    def execution_time_in_millis(self):
        return self._execution_time_in_millis

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

    def _athena_client(self):
        for field in self._connection.__javaclass__.getDeclaredFields():
            if field.name == 'athenaServiceClient':
                field.setAccessible(True)
                return field.get(self._connection).getClient()
        raise InternalError('AthenaServiceClient field not found.')

    def _query_execution(self):
        client = self._athena_client()
        request = jpype.JPackage('com.amazonaws.athena.jdbc.shaded.' +
                                 'com.amazonaws.services.athena.model').GetQueryExecutionRequest()
        result = client.getQueryExecution(request.withQueryExecutionId(self.query_id))
        return result.getQueryExecution()

    def _reset_state(self):
        self._description = None
        self._result_set = None
        self._meta_data = None
        self._rownumber = 0

        self._output_location = None
        self._completion_date_time = None
        self._submission_date_time = None
        self._data_scanned_in_bytes = None
        self._execution_time_in_millis = None

    @attach_thread_to_jvm
    @synchronized
    def execute(self, operation, parameters=None):
        if self.is_closed:
            raise ProgrammingError('Connection is closed.')

        query = self._formatter.format(operation, parameters)
        _logger.debug(query)
        try:
            self._reset_state()
            result_set = self._statement.executeQuery(query)
            if result_set:
                self._result_set = result_set
                self._result_set.setFetchSize(self._arraysize)
                self._meta_data = result_set.getMetaData()
                self._update_count = -1
            else:
                self._update_count = self._statement.getUpdatecount()
            query_execution = self._query_execution()

            result_conf = query_execution.getResultConfiguration()
            self._output_location = result_conf.getOutputLocation()

            status = query_execution.getStatus()
            self._completion_date_time = to_datetime(status.getCompletionDateTime())
            self._submission_date_time = to_datetime(status.getSubmissionDateTime())

            statistics = query_execution.getStatistics()
            self._data_scanned_in_bytes = statistics.getDataScannedInBytes()
            self._execution_time_in_millis = statistics.getEngineExecutionTimeInMillis()
        except Exception as e:
            _logger.exception('Failed to execute query.')
            raise_from(DatabaseError(unwrap_exception(e)), e)

    def executemany(self, operation, seq_of_parameters):
        raise NotSupportedError

    @attach_thread_to_jvm
    @synchronized
    def cancel(self):
        if self.is_closed:
            raise ProgrammingError('Connection is closed.')
        self._statement.cancel()

    def _rows(self):
        for field in self._result_set.__javaclass__.getDeclaredFields():
            if field.name == 'row':
                field.setAccessible(True)
                return field.get(self._result_set).get()
        raise InternalError('Row field not found.')

    def _columns(self):
        for field in self._meta_data.__javaclass__.getDeclaredFields():
            if field.name == 'columnInfo':
                field.setAccessible(True)
                return field.get(self._meta_data).toArray()
        raise InternalError('ColumnInfo field not found.')

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
            self._converter.convert(column.getSQLColumnType(), row.getVarCharValue())
            for column, row in zip(self._columns(), self._rows())
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
