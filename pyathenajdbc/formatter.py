# -*- coding: utf-8 -*-
from __future__ import absolute_import, unicode_literals

import logging
from datetime import date, datetime
from decimal import Decimal

from future.utils import iteritems
from past.types import long, unicode

from pyathenajdbc.error import ProgrammingError

_logger = logging.getLogger(__name__)


def _escape_presto(val):
    """ParamEscaper

    https://github.com/dropbox/PyHive/blob/master/pyhive/common.py"""
    return "'{0}'".format(val.replace("'", "''"))


def _escape_hive(val):
    """HiveParamEscaper

    https://github.com/dropbox/PyHive/blob/master/pyhive/hive.py"""
    return "'{0}'".format(
        val.replace("\\", "\\\\")
        .replace("'", "\\'")
        .replace("\r", "\\r")
        .replace("\n", "\\n")
        .replace("\t", "\\t")
    )


def _format_none(formatter, escaper, val):
    """
    Format the given formatter.

    Args:
        formatter: (todo): write your description
        escaper: (todo): write your description
        val: (float): write your description
    """
    return "null"


def _format_default(formatter, escaper, val):
    """
    Format the default value for a formatter.

    Args:
        formatter: (todo): write your description
        escaper: (todo): write your description
        val: (float): write your description
    """
    return val


def _format_date(formatter, escaper, val):
    """
    Formats a date.

    Args:
        formatter: (todo): write your description
        escaper: (todo): write your description
        val: (todo): write your description
    """
    return "DATE '{0}'".format(val.strftime("%Y-%m-%d"))


def _format_datetime(formatter, escaper, val):
    """
    Formats a datetime.

    Args:
        formatter: (todo): write your description
        escaper: (todo): write your description
        val: (todo): write your description
    """
    return "TIMESTAMP '{0}'".format(val.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3])


def _format_bool(formatter, escaper, val):
    """
    Format a boolean value.

    Args:
        formatter: (todo): write your description
        escaper: (str): write your description
        val: (float): write your description
    """
    return str(val)


def _format_str(formatter, escaper, val):
    """
    Format a string with the given formatter.

    Args:
        formatter: (todo): write your description
        escaper: (str): write your description
        val: (float): write your description
    """
    return escaper(val)


def _format_seq(formatter, escaper, val):
    """
    Formats a sequence of results.

    Args:
        formatter: (todo): write your description
        escaper: (str): write your description
        val: (int): write your description
    """
    results = []
    for v in val:
        func = formatter.get_formatter(v)
        formatted = func(formatter, escaper, v)
        if not isinstance(
            formatted,
            (
                str,
                unicode,
            ),
        ):
            # force string format
            if isinstance(
                formatted,
                (
                    float,
                    Decimal,
                ),
            ):
                formatted = "{0:f}".format(formatted)
            else:
                formatted = "{0}".format(formatted)
        results.append(formatted)
    return "({0})".format(", ".join(results))


def _format_decimal(formatter, escaper, val):
    """
    Format a decimal value.

    Args:
        formatter: (todo): write your description
        escaper: (list): write your description
        val: (float): write your description
    """
    return "DECIMAL {0}".format(escaper("{0:f}".format(val)))


class ParameterFormatter(object):
    def __init__(self):
        """
        Initialize mappings.

        Args:
            self: (todo): write your description
        """
        self.mappings = _DEFAULT_FORMATTERS

    def get_formatter(self, val):
        """
        Return the formatter for the given value.

        Args:
            self: (str): write your description
            val: (str): write your description
        """
        func = self.mappings.get(type(val), None)
        if not func:
            raise TypeError("{0} is not defined formatter.".format(type(val)))
        return func

    def format(self, operation, parameters=None):
        """
        Formats an operation.

        Args:
            self: (todo): write your description
            operation: (str): write your description
            parameters: (todo): write your description
        """
        if not operation or not operation.strip():
            raise ProgrammingError("Query is none or empty.")
        operation = operation.strip()

        if operation.upper().startswith("SELECT") or operation.upper().startswith(
            "WITH"
        ):
            escaper = _escape_presto
        else:
            escaper = _escape_hive

        kwargs = dict()
        if parameters:
            if isinstance(parameters, dict):
                for k, v in iteritems(parameters):
                    func = self.get_formatter(v)
                    kwargs.update({k: func(self, escaper, v)})
            else:
                raise ProgrammingError(
                    "Unsupported parameter "
                    + "(Support for dict only): {0}".format(parameters)
                )

        return (operation % kwargs).strip() if kwargs else operation.strip()

    def register_formatter(self, type_, formatter):
        """
        Registers a formatter.

        Args:
            self: (todo): write your description
            type_: (todo): write your description
            formatter: (todo): write your description
        """
        self.mappings[type_] = formatter


_DEFAULT_FORMATTERS = {
    type(None): _format_none,
    date: _format_date,
    datetime: _format_datetime,
    int: _format_default,
    float: _format_default,
    long: _format_default,
    Decimal: _format_decimal,
    bool: _format_bool,
    str: _format_str,
    unicode: _format_str,
    list: _format_seq,
    set: _format_seq,
    tuple: _format_seq,
}
