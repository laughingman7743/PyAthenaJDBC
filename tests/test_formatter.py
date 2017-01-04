# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals
import unittest
from datetime import date, datetime
from decimal import Decimal

from pyathenajdbc.formatter import ParameterFormatter


class TestParameterFormatter(unittest.TestCase):

    # TODO More DDL statement test case & Complex parameter format test case

    FORMATTER = ParameterFormatter()

    def format(self, operation, *args, **kwargs):
        return self.FORMATTER.format(operation, *args, **kwargs)

    def test_add_partition(self):
        expected = """
        ALTER TABLE test_table
        ADD PARTITION (dt='2017-01-01', hour=1)
        """.strip()

        actual1 = self.format("""
        ALTER TABLE test_table
        ADD PARTITION (dt={0}, hour={1})
        """, date(2017, 1, 1), 1)
        self.assertEqual(actual1, expected)

        actual2 = self.format("""
        ALTER TABLE test_table
        ADD PARTITION (dt={dt}, hour={hour})
        """, dt=date(2017, 1, 1), hour=1)
        self.assertEqual(actual2, expected)

    def test_drop_partition(self):
        expected = """
        ALTER TABLE test_table
        DROP PARTITION (dt='2017-01-01', hour=1)
        """.strip()

        actual1 = self.format("""
        ALTER TABLE test_table
        DROP PARTITION (dt={0}, hour={1})
        """, date(2017, 1, 1), 1)
        self.assertEqual(actual1, expected)

        actual2 = self.format("""
        ALTER TABLE test_table
        DROP PARTITION (dt={dt}, hour={hour})
        """, dt=date(2017, 1, 1), hour=1)
        self.assertEqual(actual2, expected)

    def test_format_none(self):
        expected = """
        SELECT *
        FROM test_table
        WHERE col is null
        """.strip()

        actual1 = self.format("""
        SELECT *
        FROM test_table
        WHERE col is {0}
        """, None)
        self.assertEqual(actual1, expected)

        actual2 = self.format("""
        SELECT *
        FROM test_table
        WHERE col is {param}
        """, param=None)
        self.assertEqual(actual2, expected)

    def test_format_datetime(self):
        expected = """
        SELECT *
        FROM test_table
        WHERE col_timestamp >= timestamp'2017-01-01 12:00:00.000'
          AND col_timestamp <= timestamp'2017-01-02 06:00:00.000'
        """.strip()

        actual1 = self.format("""
        SELECT *
        FROM test_table
        WHERE col_timestamp >= {0}
          AND col_timestamp <= {1}
        """, datetime(2017, 1, 1, 12, 0, 0), datetime(2017, 1, 2, 6, 0, 0))
        self.assertEqual(actual1, expected)

        actual2 = self.format("""
        SELECT *
        FROM test_table
        WHERE col_timestamp >= {start}
          AND col_timestamp <= {end}
        """, start=datetime(2017, 1, 1, 12, 0, 0), end=datetime(2017, 1, 2, 6, 0, 0))
        self.assertEqual(actual2, expected)

    def test_format_date(self):
        expected = """
        SELECT *
        FROM test_table
        WHERE col_date between date'2017-01-01' and date'2017-01-02'
        """.strip()

        actual1 = self.format("""
        SELECT *
        FROM test_table
        WHERE col_date between {0} and {1}
        """, date(2017, 1, 1), date(2017, 1, 2))
        self.assertEqual(actual1, expected)

        actual2 = self.format("""
        SELECT *
        FROM test_table
        WHERE col_date between {start} and {end}
        """, start=date(2017, 1, 1), end=date(2017, 1, 2))
        self.assertEqual(actual2, expected)

    def test_format_int(self):
        expected = """
        SELECT *
        FROM test_table
        WHERE col_int = 1
        """.strip()

        actual1 = self.format("""
        SELECT *
        FROM test_table
        WHERE col_int = {0}
        """, 1)
        self.assertEqual(actual1, expected)

        actual2 = self.format("""
        SELECT *
        FROM test_table
        WHERE col_int = {param}
        """, param=1)
        self.assertEqual(actual2, expected)

    def test_format_float(self):
        expected = """
        SELECT *
        FROM test_table
        WHERE col_float >= 0.1
        """.strip()

        actual1 = self.format("""
        SELECT *
        FROM test_table
        WHERE col_float >= {0:.1f}
        """, 0.1)
        self.assertEqual(actual1, expected)

        actual2 = self.format("""
        SELECT *
        FROM test_table
        WHERE col_float >= {param:.1f}
        """, param=0.1)
        self.assertEqual(actual2, expected)

    def test_format_decimal(self):
        expected = """
        SELECT *
        FROM test_table
        WHERE col_decimal <= 0.0000000001
        """.strip()

        actual1 = self.format("""
        SELECT *
        FROM test_table
        WHERE col_decimal <= {0:f}
        """, Decimal('0.0000000001'))
        self.assertEqual(actual1, expected)

        actual2 = self.format("""
        SELECT *
        FROM test_table
        WHERE col_decimal <= {param:f}
        """, param=Decimal('0.0000000001'))
        self.assertEqual(actual2, expected)

    def test_format_bool(self):
        expected = """
        SELECT *
        FROM test_table
        WHERE col_boolean = True
        """.strip()

        actual1 = self.format("""
        SELECT *
        FROM test_table
        WHERE col_boolean = {0}
        """, True)
        self.assertEqual(actual1, expected)

        actual2 = self.format("""
        SELECT *
        FROM test_table
        WHERE col_boolean = {param}
        """, param=True)
        self.assertEqual(actual2, expected)

    def test_format_str(self):
        expected = """
        SELECT *
        FROM test_table
        WHERE col_string = 'amazon athena'
        """.strip()

        actual1 = self.format("""
        SELECT *
        FROM test_table
        WHERE col_string = {0}
        """, 'amazon athena')
        self.assertEqual(actual1, expected)

        actual2 = self.format("""
        SELECT *
        FROM test_table
        WHERE col_string = {param}
        """, param='amazon athena')
        self.assertEqual(actual2, expected)

    def test_format_unicode(self):
        expected = """
        SELECT *
        FROM test_table
        WHERE col_string = '密林 女神'
        """.strip()

        actual1 = self.format("""
        SELECT *
        FROM test_table
        WHERE col_string = {0}
        """, '密林 女神')
        self.assertEqual(actual1, expected)

        actual2 = self.format("""
        SELECT *
        FROM test_table
        WHERE col_string = {param}
        """, param='密林 女神')
        self.assertEqual(actual2, expected)

    def test_format_none_list(self):
        expected = """
        SELECT *
        FROM test_table
        WHERE col IN (null,null)
        """.strip()

        actual1 = self.format("""
        SELECT *
        FROM test_table
        WHERE col IN {0}
        """, [None, None])
        self.assertEqual(actual1, expected)

        actual2 = self.format("""
        SELECT *
        FROM test_table
        WHERE col IN {param}
        """, param=[None, None])
        self.assertEqual(actual2, expected)

    def test_format_datetime_list(self):
        expected = """
        SELECT *
        FROM test_table
        WHERE col_timestamp IN
        (timestamp'2017-01-01 12:00:00.000',timestamp'2017-01-02 06:00:00.000')
        """.strip()

        actual1 = self.format("""
        SELECT *
        FROM test_table
        WHERE col_timestamp IN
        {0}
        """, [datetime(2017, 1, 1, 12, 0, 0), datetime(2017, 1, 2, 6, 0, 0)])
        self.assertEqual(actual1, expected)

        actual2 = self.format("""
        SELECT *
        FROM test_table
        WHERE col_timestamp IN
        {param}
        """, param=[datetime(2017, 1, 1, 12, 0, 0), datetime(2017, 1, 2, 6, 0, 0)])
        self.assertEqual(actual2, expected)

    def test_format_date_list(self):
        expected = """
        SELECT *
        FROM test_table
        WHERE col_date IN (date'2017-01-01',date'2017-01-02')
        """.strip()

        actual1 = self.format("""
        SELECT *
        FROM test_table
        WHERE col_date IN {0}
        """, [date(2017, 1, 1), date(2017, 1, 2)])
        self.assertEqual(actual1, expected)

        actual2 = self.format("""
        SELECT *
        FROM test_table
        WHERE col_date IN {param}
        """, param=[date(2017, 1, 1), date(2017, 1, 2)])
        self.assertEqual(actual2, expected)

    def test_format_int_list(self):
        expected = """
        SELECT *
        FROM test_table
        WHERE col_int IN (1,2)
        """.strip()

        actual1 = self.format("""
        SELECT *
        FROM test_table
        WHERE col_int IN {0}
        """, [1, 2])
        self.assertEqual(actual1, expected)

        actual2 = self.format("""
        SELECT *
        FROM test_table
        WHERE col_int IN {param}
        """, param=[1, 2])
        self.assertEqual(actual2, expected)

    def test_format_float_list(self):
        # default precision is 6
        expected = """
        SELECT *
        FROM test_table
        WHERE col_float IN (0.100000,0.200000)
        """.strip()

        actual1 = self.format("""
        SELECT *
        FROM test_table
        WHERE col_float IN {0}
        """, [0.1, 0.2])
        self.assertEqual(actual1, expected)

        actual2 = self.format("""
        SELECT *
        FROM test_table
        WHERE col_float IN {param}
        """, param=[0.1, 0.2])
        self.assertEqual(actual2, expected)

    def test_format_decimal_list(self):
        expected = """
        SELECT *
        FROM test_table
        WHERE col_decimal IN (0.0000000001,99.9999999999)
        """.strip()

        actual1 = self.format("""
        SELECT *
        FROM test_table
        WHERE col_decimal IN {0}
        """, [Decimal('0.0000000001'), Decimal('99.9999999999')])
        self.assertEqual(actual1, expected)

        actual2 = self.format("""
        SELECT *
        FROM test_table
        WHERE col_decimal IN {param}
        """, param=[Decimal('0.0000000001'), Decimal('99.9999999999')])
        self.assertEqual(actual2, expected)

    def test_format_bool_list(self):
        expected = """
        SELECT *
        FROM test_table
        WHERE col_boolean IN (True,False)
        """.strip()

        actual1 = self.format("""
        SELECT *
        FROM test_table
        WHERE col_boolean IN {0}
        """, [True, False])
        self.assertEqual(actual1, expected)

        actual2 = self.format("""
        SELECT *
        FROM test_table
        WHERE col_boolean IN {param}
        """, param=[True, False])
        self.assertEqual(actual2, expected)

    def test_format_str_list(self):
        expected = """
        SELECT *
        FROM test_table
        WHERE col_string IN ('amazon','athena')
        """.strip()

        actual1 = self.format("""
        SELECT *
        FROM test_table
        WHERE col_string IN {0}
        """, ['amazon', 'athena'])
        self.assertEqual(actual1, expected)

        actual2 = self.format("""
        SELECT *
        FROM test_table
        WHERE col_string IN {param}
        """, param=['amazon', 'athena'])
        self.assertEqual(actual2, expected)

    def test_format_unicode_list(self):
        expected = """
        SELECT *
        FROM test_table
        WHERE col_string IN ('密林','女神')
        """.strip()

        actual1 = self.format("""
        SELECT *
        FROM test_table
        WHERE col_string IN {0}
        """, ['密林', '女神'])
        self.assertEqual(actual1, expected)

        actual2 = self.format("""
        SELECT *
        FROM test_table
        WHERE col_string IN {param}
        """, param=['密林', '女神'])
        self.assertEqual(actual2, expected)

    def test_format_unpack_list(self):
        expected = """
        SELECT *
        FROM test_table
        WHERE col_date between date'2017-01-01' and date'2017-01-02'
        """.strip()
        params = [date(2017, 1, 1), date(2017, 1, 2)]

        actual1 = self.format("""
        SELECT *
        FROM test_table
        WHERE col_date between {0} and {1}
        """, *params)
        self.assertEqual(actual1, expected)

    def test_format_unpack_dict(self):
        expected = """
        SELECT *
        FROM test_table
        WHERE col_date between date'2017-01-01' and date'2017-01-02'
        """.strip()
        param_dict = {'start': date(2017, 1, 1), 'end': date(2017, 1, 2)}

        actual1 = self.format("""
        SELECT *
        FROM test_table
        WHERE col_date between {start} and {end}
        """, **param_dict)
        self.assertEqual(actual1, expected)

    def test_format_unpack_list_dict(self):
        expected = """
        SELECT *
        FROM test_table
        WHERE col_date between date'2017-01-01' and date'2017-01-02'
        AND col_int = 1
        AND col_float = 0.000001
        """.strip()
        params = [date(2017, 1, 1), date(2017, 1, 2)]
        param_dict = {'params1': 1, 'params2': 0.000001}

        actual1 = self.format("""
        SELECT *
        FROM test_table
        WHERE col_date between {0} and {1}
        AND col_int = {params1:d}
        AND col_float = {params2:f}
        """, *params, **param_dict)
        self.assertEqual(actual1, expected)
