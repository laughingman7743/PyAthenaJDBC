#!/usr/bin/env python
# -*- coding: utf-8 -*-
import unittest
from datetime import datetime

from pyathenajdbc.util import to_datetime


class TestUtil(unittest.TestCase):
    def test_to_datetime(self):
        self.assertIsNone(to_datetime(None))
        import jpype

        cal = jpype.java.util.Calendar.getInstance()
        cal.set(jpype.java.util.Calendar.YEAR, 2017)
        cal.set(jpype.java.util.Calendar.MONTH, 0)
        cal.set(jpype.java.util.Calendar.DAY_OF_MONTH, 1)
        cal.set(jpype.java.util.Calendar.HOUR_OF_DAY, 2)
        cal.set(jpype.java.util.Calendar.MINUTE, 3)
        cal.set(jpype.java.util.Calendar.SECOND, 4)
        java_date = cal.getTime()
        py_datetime = to_datetime(java_date)
        self.assertEqual(py_datetime, datetime(2017, 1, 1, 2, 3, 4))
