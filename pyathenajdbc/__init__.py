# -*- coding: utf-8 -*-
import datetime
from typing import TYPE_CHECKING, FrozenSet, Type

from pyathenajdbc.error import *  # noqa

if TYPE_CHECKING:
    from pyathenajdbc.connection import Connection

__version__: str = "3.0.0"
__athena_driver_version__: str = "2.0.16.1000"

# Globals https://www.python.org/dev/peps/pep-0249/#globals
apilevel: str = "2.0"
threadsafety: int = 3
paramstyle: str = "pyformat"

ATHENA_JAR: str = "AthenaJDBC42.jar"
ATHENA_DRIVER_DOWNLOAD_URL: str = (
    "https://s3.amazonaws.com/athena-downloads/drivers/JDBC/"
    + "SimbaAthenaJDBC-{0}/{1}".format(__athena_driver_version__, ATHENA_JAR)
)
ATHENA_DRIVER_CLASS_NAME: str = "com.simba.athena.jdbc.Driver"
ATHENA_CONNECTION_STRING: str = "jdbc:awsathena://AwsRegion={region};"
LOG4J_PROPERTIES: str = "log4j.properties"


class DBAPITypeObject(FrozenSet[str]):
    """Type Objects and Constructors

    https://www.python.org/dev/peps/pep-0249/#type-objects-and-constructors
    """

    def __eq__(self, other: object):
        if isinstance(other, frozenset):
            return frozenset.__eq__(self, other)
        else:
            return other in self

    def __ne__(self, other: object):
        if isinstance(other, frozenset):
            return frozenset.__ne__(self, other)
        else:
            return other not in self

    def __hash__(self):
        return frozenset.__hash__(self)


STRING: DBAPITypeObject = DBAPITypeObject(
    (
        "CHAR",
        "NCHAR",
        "VARCHAR",
        "NVARCHAR",
        "LONGVARCHAR",
        "LONGNVARCHAR",
        "ARRAY",
        "JAVA_OBJECT",
    )
)
BINARY: DBAPITypeObject = DBAPITypeObject(("BINARY", "VARBINARY", "LONGVARBINARY"))
BOOLEAN: DBAPITypeObject = DBAPITypeObject(("BOOLEAN",))
NUMBER: DBAPITypeObject = DBAPITypeObject(
    (
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
)
DATE: DBAPITypeObject = DBAPITypeObject(("DATE",))
DATETIME: DBAPITypeObject = DBAPITypeObject(("TIMESTAMP",))

Date: Type[datetime.date] = datetime.date
Time: Type[datetime.time] = datetime.time
Timestamp: Type[datetime.datetime] = datetime.datetime


def connect(*args, **kwargs) -> "Connection":
    from pyathenajdbc.connection import Connection

    return Connection(*args, **kwargs)
