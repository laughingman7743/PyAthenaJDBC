# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals
import re

from sqlalchemy.engine.default import DefaultDialect
from sqlalchemy.sql.compiler import IdentifierPreparer, SQLCompiler
from sqlalchemy.sql.sqltypes import (ARRAY, BIGINT, BINARY, BOOLEAN, DATE, DECIMAL,
                                     FLOAT, INTEGER, NULLTYPE, STRINGTYPE, TIMESTAMP)


class UniversalSet(object):
    """UniversalSet

    https://github.com/dropbox/PyHive/blob/master/pyhive/common.py"""
    def __contains__(self, item):
        return True


class AthenaIdentifierPreparer(IdentifierPreparer):
    """PrestoIdentifierPreparer

    https://github.com/dropbox/PyHive/blob/master/pyhive/sqlalchemy_presto.py"""
    reserved_words = UniversalSet()


class AthenaCompiler(SQLCompiler):
    """PrestoCompiler

    https://github.com/dropbox/PyHive/blob/master/pyhive/sqlalchemy_presto.py"""
    def visit_char_length_func(self, fn, **kw):
        return 'length{}'.format(self.function_argspec(fn, **kw))


_TYPE_MAPPINGS = {
    'DOUBLE': FLOAT,
    'SMALLINT': INTEGER,
    'BOOLEAN': BOOLEAN,
    'INTEGER': INTEGER,
    'VARCHAR': STRINGTYPE,
    'TINYINT': INTEGER,
    'DECIMAL': DECIMAL,
    'ARRAY': ARRAY,
    'ROW': STRINGTYPE,  # StructType
    'VARBINARY': BINARY,
    'MAP': STRINGTYPE,
    'BIGINT': BIGINT,
    'DATE': DATE,
    'TIMESTAMP': TIMESTAMP,
}


class AthenaDialect(DefaultDialect):

    name = 'awsathena'
    driver = 'jdbc'
    preparer = AthenaIdentifierPreparer
    statement_compiler = AthenaCompiler
    supports_alter = False
    supports_pk_autoincrement = False
    supports_default_values = False
    supports_empty_insert = False
    supports_unicode_statements = True
    supports_unicode_binds = True
    returns_unicode_strings = True
    description_encoding = None
    supports_native_boolean = True

    @classmethod
    def dbapi(cls):
        import pyathenajdbc
        return pyathenajdbc

    def create_connect_args(self, url):
        # Connection string format:
        #   awsathena+jdbc://
        #   {access_key}:{secret_key}@athena.{region_name}.amazonaws.com:443/
        #   {schema_name}?s3_staging_dir={s3_staging_dir}&driver_path={driver_path}&...
        opts = {
            'access_key': url.username,
            'secret_key': url.password,
            'region_name': re.sub(r'^athena\.([a-z0-9-]+)\.amazonaws\.com$', r'\1', url.host),
            'schema_name': url.database if url.database else 'default'
        }
        opts.update(url.query)
        return [[], opts]

    def get_schema_names(self, connection, **kw):
        query = """
                SELECT schema_name FROM information_schema.schemata
                WHERE schema_name NOT IN ('information_schema')
                """
        return [row.schema_name for row in connection.execute(query).fetchall()]

    def get_table_names(self, connection, schema=None, **kw):
        query = """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = {0}
                """.format(schema if schema else connection.connection.schema_name)
        return [row.table_name for row in connection.execute(query).fetchall()]

    def has_table(self, connection, table_name, schema=None):
        table_names = self.get_table_names(connection, schema)
        if table_name in table_names:
            return True
        return False

    def get_columns(self, connection, table_name, schema=None, **kw):
        query = """
                SELECT
                  column_name,
                  data_type,
                  is_nullable,
                  column_default,
                  ordinal_position,
                  comment
                FROM information_schema.columns
                WHERE table_schema = {0}
                """.format(schema if schema else connection.connection.schema_name)
        return [
            {
                'name': row.column_name,
                'type': _TYPE_MAPPINGS.get(re.sub(r'^([A-Z]+)($|\([A-Z0-9,\s]+\)$)', r'\1',
                                                  row.data_type.upper()), NULLTYPE),
                'nullable': True if row.is_nullable == 'YES' else False,
                'default': row.column_default,
                'ordinal_position': row.ordinal_position,
                'comment': row.comment,
            } for row in connection.execute(query).fetchall()
        ]

    def get_foreign_keys(self, connection, table_name, schema=None, **kw):
        # Athena has no support for foreign keys.
        return []

    def get_pk_constraint(self, connection, table_name, schema=None, **kw):
        # Athena has no support for primary keys.
        return []

    def get_indexes(self, connection, table_name, schema=None, **kw):
        # Athena has no support for indexes.
        return []

    def do_rollback(self, dbapi_connection):
        # No transactions for Athena
        pass

    def _check_unicode_returns(self, connection, additional_tests=None):
        # Requests gives back Unicode strings
        return True

    def _check_unicode_description(self, connection):
        # Requests gives back Unicode strings
        return True
