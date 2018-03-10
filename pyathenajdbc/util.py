# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals
import functools
import threading
from datetime import datetime


def as_pandas(cursor):
    from pandas import DataFrame
    names = [metadata[0] for metadata in cursor.description]
    return DataFrame.from_records(cursor.fetchall(), columns=names)


def synchronized(wrapped):
    """The missing @synchronized decorator

    https://git.io/vydTA"""
    _lock = threading.RLock()

    @functools.wraps(wrapped)
    def _wrapper(*args, **kwargs):
        with _lock:
            return wrapped(*args, **kwargs)
    return _wrapper


def attach_thread_to_jvm(wrapped):
    @functools.wraps(wrapped)
    def _wrapper(*args, **kwargs):
        import jpype
        if not jpype.isThreadAttachedToJVM():
            jpype.attachThreadToJVM()
        return wrapped(*args, **kwargs)
    return _wrapper


def to_datetime(java_date):
    if not java_date:
        return None
    import jpype
    cal = jpype.java.util.Calendar.getInstance()
    cal.setTime(java_date)
    return datetime(cal.get(jpype.java.util.Calendar.YEAR),
                    cal.get(jpype.java.util.Calendar.MONTH) + 1,
                    cal.get(jpype.java.util.Calendar.DAY_OF_MONTH),
                    cal.get(jpype.java.util.Calendar.HOUR_OF_DAY),
                    cal.get(jpype.java.util.Calendar.MINUTE),
                    cal.get(jpype.java.util.Calendar.SECOND))


def unwrap_exception(exc):
    import jpype
    if isinstance(exc, jpype._jexception.JavaException) and \
            issubclass(exc.__javaclass__, jpype.java.sql.SQLException):
        args = exc.args
        if args:
            cause = args[0].cause
            if cause:
                return cause
            else:
                return args[0]
    return exc
