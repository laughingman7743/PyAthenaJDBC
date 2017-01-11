# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import datetime


__version__ = '1.0.5'
__athena_driver_version__ = '1.0.0'


# Globals https://www.python.org/dev/peps/pep-0249/#globals
apilevel = '2.0'
threadsafety = 3
paramstyle = 'pyformat'


ATHENA_JAR = 'AthenaJDBC41-{0}.jar'.format(__athena_driver_version__)
ATHENA_DRIVER_DOWNLOAD_URL = 'https://s3.amazonaws.com/athena-downloads/drivers/{0}'.format(
    ATHENA_JAR)
ATHENA_DRIVER_CLASS_NAME = 'com.amazonaws.athena.jdbc.AthenaDriver'
ATHENA_CONNECTION_STRING = 'jdbc:awsathena://athena.{region}.amazonaws.com:443/'


class DBAPITypeObject:
    """Type Objects and Constructors

    https://www.python.org/dev/peps/pep-0249/#type-objects-and-constructors
    """
    def __init__(self, *values):
        self.values = values

    def __cmp__(self, other):
        if other in self.values:
            return 0
        if other < self.values:
            return 1
        else:
            return -1


STRING = DBAPITypeObject('CHAR', 'NCHAR',
                         'VARCHAR', 'NVARCHAR',
                         'LONGVARCHAR', 'LONGNVARCHAR')
BINARY = DBAPITypeObject('BINARY', 'VARBINARY', 'LONGVARBINARY')
NUMBER = DBAPITypeObject('BOOLEAN', 'TINYINT', 'SMALLINT', 'BIGINT', 'INTEGER',
                         'REAL', 'DOUBLE', 'FLOAT', 'DECIMAL', 'NUMERIC')
DATETIME = DBAPITypeObject('TIMESTAMP')
ROWID = DBAPITypeObject('')


Date = datetime.date
Time = datetime.time
Timestamp = datetime.datetime


def connect(s3_staging_dir=None, access_key=None, secret_key=None,
            region_name=None, profile_name=None, credential_file=None,
            jvm_options=None, converter=None, formatter=None, jvm_path=None,
            **kwargs):
    from pyathenajdbc.connection import Connection
    return Connection(s3_staging_dir, access_key, secret_key,
                      region_name, profile_name, credential_file,
                      jvm_options, converter, formatter, jvm_path,
                      **kwargs)
