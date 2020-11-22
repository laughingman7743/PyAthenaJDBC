# -*- coding: utf-8 -*-
from __future__ import absolute_import, unicode_literals

import datetime

from pyathenajdbc.error import *  # noqa

__version__ = "2.1.1"
__athena_driver_version__ = "2.0.14"

# Globals https://www.python.org/dev/peps/pep-0249/#globals
apilevel = "2.0"
threadsafety = 3
paramstyle = "pyformat"

ATHENA_JAR = "AthenaJDBC42_{0}.jar".format(__athena_driver_version__)
ATHENA_DRIVER_DOWNLOAD_URL = (
    "https://s3.amazonaws.com/athena-downloads/drivers/JDBC/"
    + "SimbaAthenaJDBC_{0}/{1}".format(__athena_driver_version__, ATHENA_JAR)
)
ATHENA_DRIVER_CLASS_NAME = "com.simba.athena.jdbc.Driver"
ATHENA_CONNECTION_STRING = "jdbc:awsathena://AwsRegion={region};"
LOG4J_PROPERTIES = "log4j.properties"


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

    def __eq__(self, other):
        return other in self.values


STRING = DBAPITypeObject(
    "CHAR",
    "NCHAR",
    "VARCHAR",
    "NVARCHAR",
    "LONGVARCHAR",
    "LONGNVARCHAR",
    "ARRAY",
    "JAVA_OBJECT",
)
BINARY = DBAPITypeObject("BINARY", "VARBINARY", "LONGVARBINARY")
BOOLEAN = DBAPITypeObject("BOOLEAN")
NUMBER = DBAPITypeObject(
    "TINYINT",
    "SMALLINT",
    "BIGINT",
    "INTEGER",
    "REAL",
    "DOUBLE",
    "FLOAT",
    "DECIMAL",
    "NUMERIC",
)
DATE = DBAPITypeObject("DATE")
DATETIME = DBAPITypeObject("TIMESTAMP")


Date = datetime.date
Time = datetime.time
Timestamp = datetime.datetime


def connect(*args, **kwargs):
    from pyathenajdbc.connection import Connection

    return Connection(*args, **kwargs)
