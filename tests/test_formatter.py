# -*- coding: utf-8 -*-
from __future__ import absolute_import, unicode_literals

import textwrap
import unittest
from datetime import date, datetime
from decimal import Decimal

from pyathenajdbc.error import ProgrammingError
from pyathenajdbc.formatter import ParameterFormatter


class TestParameterFormatter(unittest.TestCase):

    # TODO More DDL statement test case & Complex parameter format test case

    FORMATTER = ParameterFormatter()

    def format(self, operation, parameters=None):
        """
        Formats operation.

        Args:
            self: (todo): write your description
            operation: (str): write your description
            parameters: (todo): write your description
        """
        return self.FORMATTER.format(operation, parameters)

    def test_add_partition(self):
        """
        Add test test test.

        Args:
            self: (todo): write your description
        """
        expected = textwrap.dedent(
            """
            ALTER TABLE test_table
            ADD PARTITION (dt=DATE '2017-01-01', hour=1)
            """
        ).strip()

        actual = self.format(
            textwrap.dedent(
                """
                ALTER TABLE test_table
                ADD PARTITION (dt=%(dt)s, hour=%(hour)d)
                """
            ).strip(),
            {"dt": date(2017, 1, 1), "hour": 1},
        )
        self.assertEqual(actual, expected)

    def test_drop_partition(self):
        """
        Test if the test.

        Args:
            self: (todo): write your description
        """
        expected = textwrap.dedent(
            """
            ALTER TABLE test_table
            DROP PARTITION (dt=DATE '2017-01-01', hour=1)
            """
        ).strip()

        actual = self.format(
            textwrap.dedent(
                """
                ALTER TABLE test_table
                DROP PARTITION (dt=%(dt)s, hour=%(hour)d)
                """
            ).strip(),
            {"dt": date(2017, 1, 1), "hour": 1},
        )
        self.assertEqual(actual, expected)

    def test_format_none(self):
        """
        Assigns the first element of the expected value.

        Args:
            self: (todo): write your description
        """
        expected = textwrap.dedent(
            """
            SELECT *
            FROM test_table
            WHERE col is null
            """
        ).strip()

        actual = self.format(
            textwrap.dedent(
                """
                SELECT *
                FROM test_table
                WHERE col is %(param)s
                """
            ).strip(),
            {"param": None},
        )
        self.assertEqual(actual, expected)

    def test_format_datetime(self):
        """
        Format datetime hashed.

        Args:
            self: (todo): write your description
        """
        expected = textwrap.dedent(
            """
            SELECT *
            FROM test_table
            WHERE col_timestamp >= TIMESTAMP '2017-01-01 12:00:00.000'
              AND col_timestamp <= TIMESTAMP '2017-01-02 06:00:00.000'
            """
        ).strip()

        actual = self.format(
            textwrap.dedent(
                """
                SELECT *
                FROM test_table
                WHERE col_timestamp >= %(start)s
                  AND col_timestamp <= %(end)s
                """
            ).strip(),
            {
                "start": datetime(2017, 1, 1, 12, 0, 0),
                "end": datetime(2017, 1, 2, 6, 0, 0),
            },
        )
        self.assertEqual(actual, expected)

    def test_format_date(self):
        """
        Format the date of the date.

        Args:
            self: (todo): write your description
        """
        expected = textwrap.dedent(
            """
            SELECT *
            FROM test_table
            WHERE col_date between DATE '2017-01-01' and DATE '2017-01-02'
            """
        ).strip()

        actual = self.format(
            textwrap.dedent(
                """
                SELECT *
                FROM test_table
                WHERE col_date between %(start)s and %(end)s
                """
            ).strip(),
            {"start": date(2017, 1, 1), "end": date(2017, 1, 2)},
        )
        self.assertEqual(actual, expected)

    def test_format_int(self):
        """
        Format the intwrap.

        Args:
            self: (todo): write your description
        """
        expected = textwrap.dedent(
            """
            SELECT *
            FROM test_table
            WHERE col_int = 1
            """
        ).strip()

        actual = self.format(
            textwrap.dedent(
                """
                SELECT *
                FROM test_table
                WHERE col_int = %(param)s
                """
            ).strip(),
            {"param": 1},
        )
        self.assertEqual(actual, expected)

    def test_format_float(self):
        """
        Format the float as a float.

        Args:
            self: (todo): write your description
        """
        expected = textwrap.dedent(
            """
            SELECT *
            FROM test_table
            WHERE col_float >= 0.1
            """
        ).strip()

        actual = self.format(
            textwrap.dedent(
                """
                SELECT *
                FROM test_table
                WHERE col_float >= %(param).1f
                """
            ).strip(),
            {"param": 0.1},
        )
        self.assertEqual(actual, expected)

    def test_format_decimal(self):
        """
        Format the decimal and decimal.

        Args:
            self: (todo): write your description
        """
        expected = textwrap.dedent(
            """
            SELECT *
            FROM test_table
            WHERE col_decimal <= DECIMAL '0.0000000001'
            """
        ).strip()

        actual = self.format(
            textwrap.dedent(
                """
                SELECT *
                FROM test_table
                WHERE col_decimal <= %(param)s
                """
            ).strip(),
            {"param": Decimal("0.0000000001")},
        )
        self.assertEqual(actual, expected)

    def test_format_bool(self):
        """
        Format the test text.

        Args:
            self: (todo): write your description
        """
        expected = textwrap.dedent(
            """
            SELECT *
            FROM test_table
            WHERE col_boolean = True
            """
        ).strip()

        actual = self.format(
            textwrap.dedent(
                """
                SELECT *
                FROM test_table
                WHERE col_boolean = %(param)s
                """
            ).strip(),
            {"param": True},
        )
        self.assertEqual(actual, expected)

    def test_format_str(self):
        """
        Format the test string as a string.

        Args:
            self: (todo): write your description
        """
        expected = textwrap.dedent(
            """
            SELECT *
            FROM test_table
            WHERE col_string = 'amazon athena'
            """
        ).strip()

        actual = self.format(
            textwrap.dedent(
                """
                SELECT *
                FROM test_table
                WHERE col_string = %(param)s
                """
            ).strip(),
            {"param": "amazon athena"},
        )
        self.assertEqual(actual, expected)

    def test_format_unicode(self):
        """
        Format the test text.

        Args:
            self: (todo): write your description
        """
        expected = textwrap.dedent(
            """
            SELECT *
            FROM test_table
            WHERE col_string = '密林 女神'
            """
        ).strip()

        actual = self.format(
            textwrap.dedent(
                """
                SELECT *
                FROM test_table
                WHERE col_string = %(param)s
                """
            ).strip(),
            {"param": "密林 女神"},
        )
        self.assertEqual(actual, expected)

    def test_format_none_list(self):
        """
        Ensure that the last element of the list.

        Args:
            self: (todo): write your description
        """
        expected = textwrap.dedent(
            """
            SELECT *
            FROM test_table
            WHERE col IN (null, null)
            """
        ).strip()

        actual = self.format(
            textwrap.dedent(
                """
                SELECT *
                FROM test_table
                WHERE col IN %(param)s
                """
            ).strip(),
            {"param": [None, None]},
        )
        self.assertEqual(actual, expected)

    def test_format_datetime_list(self):
        """
        Convert datetime.

        Args:
            self: (todo): write your description
        """
        expected = textwrap.dedent(
            """
            SELECT *
            FROM test_table
            WHERE col_timestamp IN
            (TIMESTAMP '2017-01-01 12:00:00.000', TIMESTAMP '2017-01-02 06:00:00.000')
            """
        ).strip()

        actual = self.format(
            textwrap.dedent(
                """
                SELECT *
                FROM test_table
                WHERE col_timestamp IN
                %(param)s
                """
            ).strip(),
            {"param": [datetime(2017, 1, 1, 12, 0, 0), datetime(2017, 1, 2, 6, 0, 0)]},
        )
        self.assertEqual(actual, expected)

    def test_format_date_list(self):
        """
        Format the date ascii format. datetime.

        Args:
            self: (todo): write your description
        """
        expected = textwrap.dedent(
            """
            SELECT *
            FROM test_table
            WHERE col_date IN (DATE '2017-01-01', DATE '2017-01-02')
            """
        ).strip()

        actual = self.format(
            textwrap.dedent(
                """
                SELECT *
                FROM test_table
                WHERE col_date IN %(param)s
                """
            ).strip(),
            {"param": [date(2017, 1, 1), date(2017, 1, 2)]},
        )
        self.assertEqual(actual, expected)

    def test_format_int_list(self):
        """
        Convert the int intwrap.

        Args:
            self: (todo): write your description
        """
        expected = textwrap.dedent(
            """
            SELECT *
            FROM test_table
            WHERE col_int IN (1, 2)
            """
        ).strip()

        actual = self.format(
            textwrap.dedent(
                """
                SELECT *
                FROM test_table
                WHERE col_int IN %(param)s
                """
            ).strip(),
            {"param": [1, 2]},
        )
        self.assertEqual(actual, expected)

    def test_format_float_list(self):
        """
        Takes a float to float.

        Args:
            self: (todo): write your description
        """
        # default precision is 6
        expected = textwrap.dedent(
            """
            SELECT *
            FROM test_table
            WHERE col_float IN (0.100000, 0.200000)
            """
        ).strip()

        actual = self.format(
            textwrap.dedent(
                """
                SELECT *
                FROM test_table
                WHERE col_float IN %(param)s
                """
            ).strip(),
            {"param": [0.1, 0.2]},
        )
        self.assertEqual(actual, expected)

    def test_format_decimal_list(self):
        """
        Takes a list of decimal strings.

        Args:
            self: (todo): write your description
        """
        expected = textwrap.dedent(
            """
            SELECT *
            FROM test_table
            WHERE col_decimal IN (DECIMAL '0.0000000001', DECIMAL '99.9999999999')
            """
        ).strip()

        actual = self.format(
            textwrap.dedent(
                """
                SELECT *
                FROM test_table
                WHERE col_decimal IN %(param)s
                """
            ).strip(),
            {"param": [Decimal("0.0000000001"), Decimal("99.9999999999")]},
        )
        self.assertEqual(actual, expected)

    def test_format_bool_list(self):
        """
        Takes a list of strings.

        Args:
            self: (todo): write your description
        """
        expected = textwrap.dedent(
            """
            SELECT *
            FROM test_table
            WHERE col_boolean IN (True, False)
            """
        ).strip()

        actual = self.format(
            textwrap.dedent(
                """
                SELECT *
                FROM test_table
                WHERE col_boolean IN %(param)s
                """
            ).strip(),
            {"param": [True, False]},
        )
        self.assertEqual(actual, expected)

    def test_format_str_list(self):
        """
        Format a list of strings as a string.

        Args:
            self: (todo): write your description
        """
        expected = textwrap.dedent(
            """
            SELECT *
            FROM test_table
            WHERE col_string IN ('amazon', 'athena')
            """
        ).strip()

        actual = self.format(
            textwrap.dedent(
                """
                SELECT *
                FROM test_table
                WHERE col_string IN %(param)s
                """
            ).strip(),
            {"param": ["amazon", "athena"]},
        )
        self.assertEqual(actual, expected)

    def test_format_unicode_list(self):
        """
        Format a comma - separated list.

        Args:
            self: (todo): write your description
        """
        expected = textwrap.dedent(
            """
            SELECT *
            FROM test_table
            WHERE col_string IN ('密林', '女神')
            """
        ).strip()

        actual = self.format(
            textwrap.dedent(
                """
                SELECT *
                FROM test_table
                WHERE col_string IN %(param)s
                """
            ).strip(),
            {"param": ["密林", "女神"]},
        )
        self.assertEqual(actual, expected)

    def test_format_bad_parameter(self):
        """
        Format bad bad bad bad parameter is set.

        Args:
            self: (todo): write your description
        """
        self.assertRaises(
            ProgrammingError,
            lambda: self.format(
                textwrap.dedent(
                    """
                    SELECT *
                    FROM test_table
                    where col_int = $(param)d
                    """
                ).strip(),
                1,
            ),
        )

        self.assertRaises(
            ProgrammingError,
            lambda: self.format(
                textwrap.dedent(
                    """
                    SELECT *
                    FROM test_table
                    where col_string = $(param)s
                    """
                ).strip(),
                "a string",
            ),
        )

        self.assertRaises(
            ProgrammingError,
            lambda: self.format(
                textwrap.dedent(
                    """
                    SELECT *
                    FROM test_table
                    where col_string in $(param)s
                    """
                ).strip(),
                ["a string"],
            ),
        )
