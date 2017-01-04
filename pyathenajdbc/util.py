# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals
import sys

from future.utils import reraise

from pyathenajdbc.error import DatabaseError, Error


def as_pandas(cursor):
    from pandas import DataFrame
    names = [metadata[0] for metadata in cursor.description]
    return DataFrame.from_records(cursor.fetchall(), columns=names)


def reraise_dbapi_error():
    exc_info = sys.exc_info()
    import jpype
    if isinstance(exc_info[1], jpype._jexception.JavaException):
        if issubclass(exc_info[1].__javaclass__, jpype.java.sql.SQLException):
            exc_type = DatabaseError
        else:
            exc_type = Error
    else:
        exc_type = exc_info[0]
    reraise(exc_type, exc_info[1], exc_info[2])
