# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import re

from sqlalchemy.engine import reflection, Engine
from sqlalchemy.engine.default import DefaultDialect
from sqlalchemy.sql.compiler import (BIND_PARAMS, BIND_PARAMS_ESC,
                                     IdentifierPreparer, SQLCompiler)
from sqlalchemy.sql.sqltypes import (BIGINT, BINARY, BOOLEAN, DATE, DECIMAL, FLOAT,
                                     INTEGER, NULLTYPE, STRINGTYPE, TIMESTAMP)

import pyathenajdbc


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
        return 'length{0}'.format(self.function_argspec(fn, **kw))

    def visit_textclause(self, textclause, **kw):
        def do_bindparam(m):
            name = m.group(1)
            if name in textclause._bindparams:
                return self.process(textclause._bindparams[name], **kw)
            else:
                return self.bindparam_string(name, **kw)

        if not self.stack:
            self.isplaintext = True

        if len(textclause._bindparams) == 0:
            # Prevents double escaping of percent character
            return textclause.text
        else:
            # un-escape any \:params
            return BIND_PARAMS_ESC.sub(
                lambda m: m.group(1),
                BIND_PARAMS.sub(
                    do_bindparam,
                    self.post_process_text(textclause.text))
            )


_TYPE_MAPPINGS = {
    'boolean': BOOLEAN,
    'real': FLOAT,
    'float': FLOAT,
    'double': FLOAT,
    'tinyint': INTEGER,
    'smallint': INTEGER,
    'integer': INTEGER,
    'bigint': BIGINT,
    'decimal': DECIMAL,
    'char': STRINGTYPE,
    'varchar': STRINGTYPE,
    'array': STRINGTYPE,
    'row': STRINGTYPE,  # StructType
    'varbinary': BINARY,
    'map': STRINGTYPE,
    'date': DATE,
    'timestamp': TIMESTAMP,
}


class AthenaDialect(DefaultDialect):

    name = 'awsathena'
    driver = 'jdbc'
    preparer = AthenaIdentifierPreparer
    statement_compiler = AthenaCompiler
    default_paramstyle = pyathenajdbc.paramstyle
    supports_alter = False
    supports_pk_autoincrement = False
    supports_default_values = False
    supports_empty_insert = False
    supports_unicode_statements = True
    supports_unicode_binds = True
    returns_unicode_strings = True
    description_encoding = None
    supports_native_boolean = True

    _pattern_column_type = re.compile(r'^([a-zA-Z]+)($|\(.+\)$)')

    @classmethod
    def dbapi(cls):
        return pyathenajdbc

    def _raw_connection(self, connection):
        if isinstance(connection, Engine):
            return connection.raw_connection()
        return connection.connection

    def create_connect_args(self, url):
        # Connection string format:
        #   awsathena+jdbc://
        #   {access_key}:{secret_key}@athena.{region_name}.amazonaws.com:443/
        #   {schema_name}?s3_staging_dir={s3_staging_dir}&driver_path={driver_path}&...
        opts = {
            'access_key': url.username if url.username else None,
            'secret_key': url.password if url.password else None,
            'region_name': re.sub(r'^athena\.([a-z0-9-]+)\.amazonaws\.(com|com.cn)$', r'\1',
                                  url.host),
            'schema_name': url.database if url.database else 'default'
        }
        opts.update(url.query)
        return [[], opts]

    @reflection.cache
    def get_schema_names(self, connection, **kw):
        query = """
                SELECT schema_name
                FROM information_schema.schemata
                WHERE schema_name NOT IN ('information_schema')
                """
        return [row.schema_name for row in connection.execute(query).fetchall()]

    @reflection.cache
    def get_table_names(self, connection, schema=None, **kw):
        raw_connection = self._raw_connection(connection)
        schema = schema if schema else raw_connection.schema_name
        query = """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = '{0}'
                """.format(schema)
        return [row.table_name for row in connection.execute(query).fetchall()]

    def has_table(self, connection, table_name, schema=None):
        table_names = self.get_table_names(connection, schema)
        if table_name in table_names:
            return True
        return False

    @reflection.cache
    def get_columns(self, connection, table_name, schema=None, **kw):
        raw_connection = self._raw_connection(connection)
        schema = schema if schema else raw_connection.schema_name
        query = """
                SELECT
                  table_schema,
                  table_name,
                  column_name,
                  data_type,
                  is_nullable,
                  column_default,
                  ordinal_position,
                  comment
                FROM information_schema.columns
                WHERE table_schema = '{schema}'
                AND table_name = '{table}'
                """.format(schema=schema, table=table_name)
        return [
            {
                'name': row.column_name,
                'type': _TYPE_MAPPINGS.get(self._get_column_type(row.data_type), NULLTYPE),
                'nullable': True if row.is_nullable == 'YES' else False,
                'default': row.column_default,
                'ordinal_position': row.ordinal_position,
                'comment': row.comment,
            } for row in connection.execute(query).fetchall()
        ]

    def _get_column_type(self, type_):
        return self._pattern_column_type.sub(r'\1', type_)

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
