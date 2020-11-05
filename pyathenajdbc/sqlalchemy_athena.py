# -*- coding: utf-8 -*-
from __future__ import absolute_import, unicode_literals

import re

from sqlalchemy import exc, util
from sqlalchemy.engine import Engine, reflection
from sqlalchemy.engine.default import DefaultDialect
from sqlalchemy.sql.compiler import (
    BIND_PARAMS,
    BIND_PARAMS_ESC,
    DDLCompiler,
    GenericTypeCompiler,
    IdentifierPreparer,
    SQLCompiler,
)
from sqlalchemy.sql.sqltypes import (
    BIGINT,
    BINARY,
    BOOLEAN,
    DATE,
    DECIMAL,
    FLOAT,
    INTEGER,
    NULLTYPE,
    STRINGTYPE,
    TIMESTAMP,
)

import pyathenajdbc


class UniversalSet(object):
    """UniversalSet

    https://github.com/dropbox/PyHive/blob/master/pyhive/common.py"""

    def __contains__(self, item):
        """
        Determine if item is contained in the list.

        Args:
            self: (todo): write your description
            item: (str): write your description
        """
        return True


class AthenaDMLIdentifierPreparer(IdentifierPreparer):
    """PrestoIdentifierPreparer

    https://github.com/dropbox/PyHive/blob/master/pyhive/sqlalchemy_presto.py"""

    reserved_words = UniversalSet()


class AthenaDDLIdentifierPreparer(IdentifierPreparer):
    def __init__(
        self,
        dialect,
        initial_quote="`",
        final_quote=None,
        escape_quote="`",
        quote_case_sensitive_collations=True,
        omit_schema=False,
    ):
        """
        Initialize the table.

        Args:
            self: (todo): write your description
            dialect: (str): write your description
            initial_quote: (todo): write your description
            final_quote: (todo): write your description
            escape_quote: (str): write your description
            quote_case_sensitive_collations: (todo): write your description
            omit_schema: (todo): write your description
        """
        super(AthenaDDLIdentifierPreparer, self).__init__(
            dialect=dialect,
            initial_quote=initial_quote,
            final_quote=final_quote,
            escape_quote=escape_quote,
            quote_case_sensitive_collations=quote_case_sensitive_collations,
            omit_schema=omit_schema,
        )


class AthenaStatementCompiler(SQLCompiler):
    """PrestoCompiler
    https://github.com/dropbox/PyHive/blob/master/pyhive/sqlalchemy_presto.py"""

    def visit_char_length_func(self, fn, **kw):
        """
        Returns a function length for a function.

        Args:
            self: (todo): write your description
            fn: (todo): write your description
            kw: (todo): write your description
        """
        return "length{0}".format(self.function_argspec(fn, **kw))

    def visit_textclause(self, textclause, **kw):
        """
        Create a new query

        Args:
            self: (todo): write your description
            textclause: (str): write your description
            kw: (todo): write your description
        """
        def do_bindparam(m):
            """
            Bind a new query to the given string.

            Args:
                m: (todo): write your description
            """
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
                BIND_PARAMS.sub(do_bindparam, self.post_process_text(textclause.text)),
            )


class AthenaTypeCompiler(GenericTypeCompiler):
    def visit_FLOAT(self, type_, **kw):
        """
        Convenience for the given type.

        Args:
            self: (todo): write your description
            type_: (todo): write your description
            kw: (todo): write your description
        """
        return self.visit_REAL(type_, **kw)

    def visit_REAL(self, type_, **kw):
        """
        Return a string representation.

        Args:
            self: (todo): write your description
            type_: (str): write your description
            kw: (str): write your description
        """
        return "DOUBLE"

    def visit_NUMERIC(self, type_, **kw):
        """
        Handles visitor.

        Args:
            self: (todo): write your description
            type_: (todo): write your description
            kw: (todo): write your description
        """
        return self.visit_DECIMAL(type_, **kw)

    def visit_DECIMAL(self, type_, **kw):
        """
        Return a string representation of the type.

        Args:
            self: (todo): write your description
            type_: (todo): write your description
            kw: (todo): write your description
        """
        if type_.precision is None:
            return "DECIMAL"
        elif type_.scale is None:
            return "DECIMAL(%(precision)s)" % {"precision": type_.precision}
        else:
            return "DECIMAL(%(precision)s, %(scale)s)" % {
                "precision": type_.precision,
                "scale": type_.scale,
            }

    def visit_INTEGER(self, type_, **kw):
        """
        Return an astroid. string type.

        Args:
            self: (todo): write your description
            type_: (str): write your description
            kw: (todo): write your description
        """
        return "INTEGER"

    def visit_SMALLINT(self, type_, **kw):
        """
        Return a string.

        Args:
            self: (todo): write your description
            type_: (todo): write your description
            kw: (todo): write your description
        """
        return "SMALLINT"

    def visit_BIGINT(self, type_, **kw):
        """
        Return a string for a : pyobject.

        Args:
            self: (todo): write your description
            type_: (str): write your description
            kw: (todo): write your description
        """
        return "BIGINT"

    def visit_TIMESTAMP(self, type_, **kw):
        """
        R

        Args:
            self: (todo): write your description
            type_: (todo): write your description
            kw: (todo): write your description
        """
        return "TIMESTAMP"

    def visit_DATETIME(self, type_, **kw):
        """
        Handles a type_T.

        Args:
            self: (todo): write your description
            type_: (str): write your description
            kw: (todo): write your description
        """
        return self.visit_TIMESTAMP(type_, **kw)

    def visit_DATE(self, type_, **kw):
        """
        Dynamically formatted string representation of the dATE type.

        Args:
            self: (todo): write your description
            type_: (todo): write your description
            kw: (todo): write your description
        """
        return "DATE"

    def visit_TIME(self, type_, **kw):
        """
        Raise an exception if the type.

        Args:
            self: (todo): write your description
            type_: (todo): write your description
            kw: (todo): write your description
        """
        raise exc.CompileError("Data type `{0}` is not supported".format(type_))

    def visit_CLOB(self, type_, **kw):
        """
        Return an astroid.

        Args:
            self: (todo): write your description
            type_: (todo): write your description
            kw: (todo): write your description
        """
        return self.visit_BINARY(type_, **kw)

    def visit_NCLOB(self, type_, **kw):
        """
        Returns a numpy array.

        Args:
            self: (todo): write your description
            type_: (todo): write your description
            kw: (todo): write your description
        """
        return self.visit_BINARY(type_, **kw)

    def visit_CHAR(self, type_, **kw):
        """
        Return a string representation of the specified type.

        Args:
            self: (todo): write your description
            type_: (todo): write your description
            kw: (todo): write your description
        """
        return self._render_string_type(type_, "CHAR")

    def visit_NCHAR(self, type_, **kw):
        """
        Return a string representing the nCHAR.

        Args:
            self: (todo): write your description
            type_: (todo): write your description
            kw: (todo): write your description
        """
        return self._render_string_type(type_, "CHAR")

    def visit_VARCHAR(self, type_, **kw):
        """
        Return a string representing the string type.

        Args:
            self: (todo): write your description
            type_: (todo): write your description
            kw: (todo): write your description
        """
        return self._render_string_type(type_, "VARCHAR")

    def visit_NVARCHAR(self, type_, **kw):
        """
        : param type_type type_type_type.

        Args:
            self: (todo): write your description
            type_: (todo): write your description
            kw: (todo): write your description
        """
        return self._render_string_type(type_, "VARCHAR")

    def visit_TEXT(self, type_, **kw):
        """
        Return a type_id

        Args:
            self: (todo): write your description
            type_: (todo): write your description
            kw: (todo): write your description
        """
        return "STRING"

    def visit_BLOB(self, type_, **kw):
        """
        Return an astroid.

        Args:
            self: (todo): write your description
            type_: (todo): write your description
            kw: (todo): write your description
        """
        return self.visit_BINARY(type_, **kw)

    def visit_BINARY(self, type_, **kw):
        """
        Return an astroid for the given type.

        Args:
            self: (todo): write your description
            type_: (todo): write your description
            kw: (todo): write your description
        """
        return "BINARY"

    def visit_VARBINARY(self, type_, **kw):
        """
        Check if vARB type.

        Args:
            self: (todo): write your description
            type_: (todo): write your description
            kw: (todo): write your description
        """
        return self.visit_BINARY(type_, **kw)

    def visit_BOOLEAN(self, type_, **kw):
        """
        Return an astroid.

        Args:
            self: (todo): write your description
            type_: (str): write your description
            kw: (todo): write your description
        """
        return "BOOLEAN"


class AthenaDDLCompiler(DDLCompiler):
    @property
    def preparer(self):
        """
        Prepare the stream.

        Args:
            self: (todo): write your description
        """
        return self._preparer

    @preparer.setter
    def preparer(self, value):
        """
        Prepares the given value.

        Args:
            self: (todo): write your description
            value: (todo): write your description
        """
        pass

    def __init__(
        self,
        dialect,
        statement,
        bind=None,
        schema_translate_map=None,
        compile_kwargs=util.immutabledict(),
    ):
        """
        Initialize the sql statement.

        Args:
            self: (todo): write your description
            dialect: (str): write your description
            statement: (todo): write your description
            bind: (int): write your description
            schema_translate_map: (todo): write your description
            compile_kwargs: (dict): write your description
            util: (todo): write your description
            immutabledict: (dict): write your description
        """
        self._preparer = AthenaDDLIdentifierPreparer(dialect)
        super(AthenaDDLCompiler, self).__init__(
            dialect=dialect,
            statement=statement,
            bind=bind,
            schema_translate_map=schema_translate_map,
            compile_kwargs=compile_kwargs,
        )

    def visit_create_table(self, create):
        """
        Return a table statement.

        Args:
            self: (todo): write your description
            create: (bool): write your description
        """
        table = create.element
        preparer = self.preparer

        text = "\nCREATE EXTERNAL "
        text += "TABLE " + preparer.format_table(table) + " "
        text += "("

        separator = "\n"
        for create_column in create.columns:
            column = create_column.element
            try:
                processed = self.process(create_column)
                if processed is not None:
                    text += separator
                    separator = ", \n"
                    text += "\t" + processed
            except exc.CompileError as ce:
                util.raise_from_cause(
                    exc.CompileError(
                        util.u("(in table '{0}', column '{1}'): {2}").format(
                            table.description, column.name, ce.args[0]
                        )
                    )
                )

        const = self.create_table_constraints(
            table,
            _include_foreign_key_constraints=create.include_foreign_key_constraints,
        )
        if const:
            text += separator + "\t" + const

        text += "\n)\n%s\n\n" % self.post_create_table(table)
        return text

    def post_create_table(self, table):
        """
        Post - table.

        Args:
            self: (todo): write your description
            table: (str): write your description
        """
        raw_connection = table.bind.raw_connection()
        # TODO Supports orc, avro, json, csv or tsv format
        text = "STORED AS PARQUET\n"

        location = (
            raw_connection._driver_kwargs["S3Location"]
            if "S3Location" in raw_connection._driver_kwargs
            else raw_connection._driver_kwargs.get("S3OutputLocation")
        )
        if not location:
            raise exc.CompileError(
                "`S3Location` or `S3OutputLocation` parameter is required"
                " in the connection string."
            )
        schema = table.schema if table.schema else raw_connection.schema_name
        text += "LOCATION '{0}{1}/{2}/'\n".format(location, schema, table.name)

        compression = raw_connection._driver_kwargs.get("compression")
        if compression:
            text += "TBLPROPERTIES ('parquet.compress'='{0}')\n".format(
                compression.upper()
            )

        return text


_TYPE_MAPPINGS = {
    "boolean": BOOLEAN,
    "real": FLOAT,
    "float": FLOAT,
    "double": FLOAT,
    "tinyint": INTEGER,
    "smallint": INTEGER,
    "integer": INTEGER,
    "bigint": BIGINT,
    "decimal": DECIMAL,
    "char": STRINGTYPE,
    "varchar": STRINGTYPE,
    "array": STRINGTYPE,
    "row": STRINGTYPE,  # StructType
    "varbinary": BINARY,
    "map": STRINGTYPE,
    "date": DATE,
    "timestamp": TIMESTAMP,
}


class AthenaDialect(DefaultDialect):

    name = "awsathena"
    driver = "jdbc"
    preparer = AthenaDMLIdentifierPreparer
    statement_compiler = AthenaStatementCompiler
    ddl_compiler = AthenaDDLCompiler
    type_compiler = AthenaTypeCompiler
    default_paramstyle = pyathenajdbc.paramstyle
    supports_alter = False
    supports_pk_autoincrement = False
    supports_default_values = False
    supports_empty_insert = False
    supports_multivalues_insert = True
    supports_unicode_statements = True
    supports_unicode_binds = True
    returns_unicode_strings = True
    description_encoding = None
    supports_native_boolean = True
    postfetch_lastrowid = False

    _pattern_column_type = re.compile(r"^([a-zA-Z]+)($|\(.+\)$)")

    @classmethod
    def dbapi(cls):
        """
        Return an instance of the sqlalchemy.

        Args:
            cls: (todo): write your description
        """
        return pyathenajdbc

    def _raw_connection(self, connection):
        """
        Return a raw raw connection.

        Args:
            self: (todo): write your description
            connection: (todo): write your description
        """
        if isinstance(connection, Engine):
            return connection.raw_connection()
        return connection.connection

    def create_connect_args(self, url):
        """
        Create a database connection arguments.

        Args:
            self: (todo): write your description
            url: (str): write your description
        """
        # Connection string format:
        #   awsathena+jdbc://
        #   {User}:{Password}@athena.{AwsRegion}.amazonaws.com:443/
        #   {Schema}?S3OutputLocation={S3OutputLocation}&driver_path={driver_path}&...
        opts = {
            "User": url.username if url.username else None,
            "Password": url.password if url.password else None,
            "AwsRegion": re.sub(
                r"^athena\.([a-z0-9-]+)\.amazonaws\.(com|com.cn)$", r"\1", url.host
            ),
            "Schema": url.database if url.database else "default",
        }
        opts.update(url.query)
        return [[], opts]

    @reflection.cache
    def get_schema_names(self, connection, **kw):
        """
        Returns a list of schema

        Args:
            self: (todo): write your description
            connection: (todo): write your description
            kw: (str): write your description
        """
        query = """
                SELECT schema_name
                FROM information_schema.schemata
                WHERE schema_name NOT IN ('information_schema')
                """
        return [row.schema_name for row in connection.execute(query).fetchall()]

    @reflection.cache
    def get_table_names(self, connection, schema=None, **kw):
        """
        Returns all table names.

        Args:
            self: (todo): write your description
            connection: (todo): write your description
            schema: (todo): write your description
            kw: (todo): write your description
        """
        raw_connection = self._raw_connection(connection)
        schema = schema if schema else raw_connection.schema_name
        query = """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = '{0}'
                """.format(
            schema
        )
        return [row.table_name for row in connection.execute(query).fetchall()]

    def has_table(self, connection, table_name, schema=None):
        """
        Returns true if schema exists.

        Args:
            self: (todo): write your description
            connection: (todo): write your description
            table_name: (str): write your description
            schema: (str): write your description
        """
        table_names = self.get_table_names(connection, schema)
        if table_name in table_names:
            return True
        return False

    @reflection.cache
    def get_columns(self, connection, table_name, schema=None, **kw):
        """
        Return a list of column names.

        Args:
            self: (todo): write your description
            connection: (todo): write your description
            table_name: (str): write your description
            schema: (todo): write your description
            kw: (todo): write your description
        """
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
                """.format(
            schema=schema, table=table_name
        )
        return [
            {
                "name": row.column_name,
                "type": _TYPE_MAPPINGS.get(
                    self._get_column_type(row.data_type), NULLTYPE
                ),
                "nullable": True if row.is_nullable == "YES" else False,
                "default": row.column_default,
                "ordinal_position": row.ordinal_position,
                "comment": row.comment,
            }
            for row in connection.execute(query).fetchall()
        ]

    def _get_column_type(self, type_):
        """
        Return the column type.

        Args:
            self: (todo): write your description
            type_: (str): write your description
        """
        return self._pattern_column_type.sub(r"\1", type_)

    def get_foreign_keys(self, connection, table_name, schema=None, **kw):
        """
        Returns a list of foreign keys.

        Args:
            self: (todo): write your description
            connection: (todo): write your description
            table_name: (str): write your description
            schema: (todo): write your description
            kw: (todo): write your description
        """
        # Athena has no support for foreign keys.
        return []

    def get_pk_constraint(self, connection, table_name, schema=None, **kw):
        """
        Gets pk constraint.

        Args:
            self: (todo): write your description
            connection: (todo): write your description
            table_name: (str): write your description
            schema: (todo): write your description
            kw: (todo): write your description
        """
        # Athena has no support for primary keys.
        return []

    def get_indexes(self, connection, table_name, schema=None, **kw):
        """
        Returns all indexes for a table.

        Args:
            self: (todo): write your description
            connection: (todo): write your description
            table_name: (str): write your description
            schema: (str): write your description
            kw: (str): write your description
        """
        # Athena has no support for indexes.
        return []

    def do_rollback(self, dbapi_connection):
        """
        Rollback a rollback.

        Args:
            self: (todo): write your description
            dbapi_connection: (todo): write your description
        """
        # No transactions for Athena
        pass

    def _check_unicode_returns(self, connection, additional_tests=None):
        """
        Check if the connection is valid.

        Args:
            self: (todo): write your description
            connection: (todo): write your description
            additional_tests: (todo): write your description
        """
        # Requests gives back Unicode strings
        return True

    def _check_unicode_description(self, connection):
        """
        Check if the given connection is valid.

        Args:
            self: (todo): write your description
            connection: (todo): write your description
        """
        # Requests gives back Unicode strings
        return True
