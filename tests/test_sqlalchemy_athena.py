# -*- coding: utf-8 -*-
import re
import unittest
import uuid
from datetime import date, datetime
from decimal import Decimal
from urllib.parse import quote_plus

import numpy as np
import pandas as pd
import sqlalchemy
from sqlalchemy.engine import create_engine
from sqlalchemy.exc import NoSuchTableError
from sqlalchemy.sql import expression
from sqlalchemy.sql.schema import Column, MetaData, Table
from sqlalchemy.sql.sqltypes import (
    BIGINT,
    BINARY,
    BOOLEAN,
    DATE,
    DECIMAL,
    FLOAT,
    INTEGER,
    STRINGTYPE,
    TIMESTAMP,
)

from tests.conftest import ENV, SCHEMA
from tests.util import with_engine


class TestSQLAlchemyAthena(unittest.TestCase):
    """Reference test case is following:

    https://github.com/dropbox/PyHive/blob/master/pyhive/tests/sqlalchemy_test_case.py
    https://github.com/dropbox/PyHive/blob/master/pyhive/tests/test_sqlalchemy_hive.py
    https://github.com/dropbox/PyHive/blob/master/pyhive/tests/test_sqlalchemy_presto.py
    """

    def create_engine(self):
        conn_str = (
            "awsathena+jdbc://athena.{AwsRegion}.amazonaws.com:443/"
            + "{Schema}?S3OutputLocation={S3OutputLocation}&S3Location={S3Location}"
            + "&compression=snappy"
        )
        return create_engine(
            conn_str.format(
                AwsRegion=ENV.region_name,
                Schema=SCHEMA,
                S3OutputLocation=quote_plus(ENV.s3_staging_dir),
                S3Location=quote_plus(ENV.s3_staging_dir),
            )
        )

    @with_engine
    def test_basic_query(self, engine, conn):
        rows = conn.execute("SELECT * FROM one_row").fetchall()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].number_of_rows, 1)
        self.assertEqual(len(rows[0]), 1)

    @with_engine
    def test_reflect_no_such_table(self, engine, conn):
        self.assertRaises(
            NoSuchTableError,
            lambda: Table("this_does_not_exist", MetaData(bind=engine), autoload=True),
        )
        self.assertRaises(
            NoSuchTableError,
            lambda: Table(
                "this_does_not_exist",
                MetaData(bind=engine),
                schema="also_does_not_exist",
                autoload=True,
            ),
        )

    @with_engine
    def test_reflect_table(self, engine, conn):
        one_row = Table("one_row", MetaData(bind=engine), autoload=True)
        self.assertEqual(len(one_row.c), 1)
        self.assertIsNotNone(one_row.c.number_of_rows)

    @with_engine
    def test_reflect_table_with_schema(self, engine, conn):
        one_row = Table("one_row", MetaData(bind=engine), schema=SCHEMA, autoload=True)
        self.assertEqual(len(one_row.c), 1)
        self.assertIsNotNone(one_row.c.number_of_rows)

    @with_engine
    def test_reflect_table_include_columns(self, engine, conn):
        one_row_complex = Table("one_row_complex", MetaData(bind=engine))
        version = float(
            re.search(r"^([\d]+\.[\d]+)\..+", sqlalchemy.__version__).group(1)
        )
        if version <= 1.2:
            engine.dialect.reflecttable(
                conn,
                one_row_complex,
                include_columns=["col_int"],
                exclude_columns=[],
            )
        else:
            engine.dialect.reflecttable(
                conn,
                one_row_complex,
                include_columns=["col_int"],
                exclude_columns=[],
                resolve_fks=True,
            )
        self.assertEqual(len(one_row_complex.c), 1)
        self.assertIsNotNone(one_row_complex.c.col_int)
        self.assertRaises(AttributeError, lambda: one_row_complex.c.col_tinyint)

    @with_engine
    def test_unicode(self, engine, conn):
        unicode_str = "密林"
        one_row = Table("one_row", MetaData(bind=engine))
        returned_str = sqlalchemy.select(
            [expression.bindparam("あまぞん", unicode_str)],
            from_obj=one_row,
        ).scalar()
        self.assertEqual(returned_str, unicode_str)

    @with_engine
    def test_reflect_schemas(self, engine, conn):
        insp = sqlalchemy.inspect(engine)
        schemas = insp.get_schema_names()
        self.assertIn(SCHEMA, schemas)
        self.assertIn("default", schemas)

    @with_engine
    def test_get_table_names(self, engine, conn):
        meta = MetaData()
        meta.reflect(bind=engine)
        print(meta.tables)
        self.assertIn("one_row", meta.tables)
        self.assertIn("one_row_complex", meta.tables)

        insp = sqlalchemy.inspect(engine)
        self.assertIn(
            "many_rows",
            insp.get_table_names(schema=SCHEMA),
        )

    @with_engine
    def test_has_table(self, engine, conn):
        self.assertTrue(Table("one_row", MetaData(bind=engine)).exists())
        self.assertFalse(
            Table("this_table_does_not_exist", MetaData(bind=engine)).exists()
        )

    @with_engine
    def test_get_columns(self, engine, conn):
        insp = sqlalchemy.inspect(engine)
        actual = insp.get_columns(table_name="one_row", schema=SCHEMA)[0]
        self.assertEqual(actual["name"], "number_of_rows")
        self.assertTrue(isinstance(actual["type"], INTEGER))
        self.assertTrue(actual["nullable"])
        self.assertIsNone(actual["default"])
        self.assertEqual(actual["ordinal_position"], 1)
        self.assertIsNone(actual["comment"])

    @with_engine
    def test_char_length(self, engine, conn):
        one_row_complex = Table("one_row_complex", MetaData(bind=engine), autoload=True)
        result = (
            sqlalchemy.select(
                [sqlalchemy.func.char_length(one_row_complex.c.col_string)]
            )
            .execute()
            .scalar()
        )
        self.assertEqual(result, len("a string"))

    @with_engine
    def test_reflect_select(self, engine, conn):
        one_row_complex = Table("one_row_complex", MetaData(bind=engine), autoload=True)
        self.assertEqual(len(one_row_complex.c), 15)
        self.assertIsInstance(one_row_complex.c.col_string, Column)
        rows = one_row_complex.select().execute().fetchall()
        self.assertEqual(len(rows), 1)
        self.assertEqual(
            list(rows[0]),
            [
                True,
                127,
                32767,
                2147483647,
                9223372036854775807,
                0.5,
                0.25,
                "a string",
                datetime(2017, 1, 1, 0, 0, 0),
                date(2017, 1, 2),
                b"123",
                "1, 2",
                "{1=2, 3=4}",
                "{a=1, b=2}",
                Decimal("0.100000"),
            ],
        )
        self.assertIsInstance(one_row_complex.c.col_boolean.type, BOOLEAN)
        self.assertIsInstance(one_row_complex.c.col_tinyint.type, INTEGER)
        self.assertIsInstance(one_row_complex.c.col_smallint.type, INTEGER)
        self.assertIsInstance(one_row_complex.c.col_int.type, INTEGER)
        self.assertIsInstance(one_row_complex.c.col_bigint.type, BIGINT)
        self.assertIsInstance(one_row_complex.c.col_float.type, FLOAT)
        self.assertIsInstance(one_row_complex.c.col_double.type, FLOAT)
        self.assertIsInstance(one_row_complex.c.col_string.type, type(STRINGTYPE))
        self.assertIsInstance(one_row_complex.c.col_timestamp.type, TIMESTAMP)
        self.assertIsInstance(one_row_complex.c.col_date.type, DATE)
        self.assertIsInstance(one_row_complex.c.col_binary.type, BINARY)
        self.assertIsInstance(one_row_complex.c.col_array.type, type(STRINGTYPE))
        self.assertIsInstance(one_row_complex.c.col_map.type, type(STRINGTYPE))
        self.assertIsInstance(one_row_complex.c.col_struct.type, type(STRINGTYPE))
        self.assertIsInstance(one_row_complex.c.col_decimal.type, DECIMAL)

    @with_engine
    def test_reserved_words(self, engine, conn):
        """Presto uses double quotes, not backticks"""
        fake_table = Table(
            "select", MetaData(bind=engine), Column("current_timestamp", STRINGTYPE)
        )
        query = str(fake_table.select(fake_table.c.current_timestamp == "a"))
        self.assertIn('"select"', query)
        self.assertIn('"current_timestamp"', query)
        self.assertNotIn("`select`", query)
        self.assertNotIn("`current_timestamp`", query)

    @with_engine
    def test_get_column_type(self, engine, conn):
        dialect = engine.dialect
        self.assertEqual(dialect._get_column_type("boolean"), "boolean")
        self.assertEqual(dialect._get_column_type("tinyint"), "tinyint")
        self.assertEqual(dialect._get_column_type("smallint"), "smallint")
        self.assertEqual(dialect._get_column_type("integer"), "integer")
        self.assertEqual(dialect._get_column_type("bigint"), "bigint")
        self.assertEqual(dialect._get_column_type("real"), "real")
        self.assertEqual(dialect._get_column_type("double"), "double")
        self.assertEqual(dialect._get_column_type("varchar"), "varchar")
        self.assertEqual(dialect._get_column_type("timestamp"), "timestamp")
        self.assertEqual(dialect._get_column_type("date"), "date")
        self.assertEqual(dialect._get_column_type("varbinary"), "varbinary")
        self.assertEqual(dialect._get_column_type("array(integer)"), "array")
        self.assertEqual(dialect._get_column_type("map(integer, integer)"), "map")
        self.assertEqual(dialect._get_column_type("row(a integer, b integer)"), "row")
        self.assertEqual(dialect._get_column_type("decimal(10,1)"), "decimal")

    @with_engine
    def test_contain_percents_character_query(self, engine, conn):
        query = sqlalchemy.sql.text(
            """
            SELECT date_parse('20191030', '%Y%m%d')
            """
        )
        result = engine.execute(query)
        self.assertEqual(result.fetchall(), [(datetime(2019, 10, 30),)])

    @with_engine
    def test_query_with_parameter(self, engine, conn):
        query = sqlalchemy.sql.text(
            """
            SELECT :word
            """
        )
        result = engine.execute(query, word="cat")
        self.assertEqual(result.fetchall(), [("cat",)])

    @with_engine
    def test_contain_percents_character_query_with_parameter(self, engine, conn):
        query = sqlalchemy.sql.text(
            """
            SELECT date_parse('20191030', '%Y%m%d'), :word
            """
        )
        result = engine.execute(query, word="cat")
        self.assertEqual(result.fetchall(), [(datetime(2019, 10, 30), "cat")])

        query = sqlalchemy.sql.text(
            """
            SELECT col_string FROM one_row_complex
            WHERE col_string LIKE 'a%' OR col_string LIKE :param
            """
        )
        result = engine.execute(query, param="b%")
        self.assertEqual(result.fetchall(), [("a string",)])

    @with_engine
    def test_to_sql(self, engine, conn):
        # TODO Add binary column (After dropping support for Python 2.7)
        table_name = "to_sql_{0}".format(str(uuid.uuid4()).replace("-", ""))
        df = pd.DataFrame(
            {
                "col_int": np.int32([1]),
                "col_bigint": np.int64([12345]),
                "col_float": np.float32([1.0]),
                "col_double": np.float64([1.2345]),
                "col_string": ["a"],
                "col_boolean": np.bool_([True]),
                "col_timestamp": [datetime(2020, 1, 1, 0, 0, 0)],
                "col_date": [date(2020, 12, 31)],
            }
        )
        # Explicitly specify column order
        df = df[
            [
                "col_int",
                "col_bigint",
                "col_float",
                "col_double",
                "col_string",
                "col_boolean",
                "col_timestamp",
                "col_date",
            ]
        ]
        df.to_sql(
            table_name,
            engine,
            schema=SCHEMA,
            index=False,
            if_exists="replace",
            method="multi",
        )

        table = Table(table_name, MetaData(bind=engine), autoload=True)
        self.assertEqual(
            table.select().execute().fetchall(),
            [
                (
                    1,
                    12345,
                    1.0,
                    1.2345,
                    "a",
                    True,
                    datetime(2020, 1, 1, 0, 0, 0),
                    date(2020, 12, 31),
                )
            ],
        )
