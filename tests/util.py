# -*- coding: utf-8 -*-
from __future__ import absolute_import, unicode_literals

import codecs
import contextlib
import functools
import os


class Env(object):
    def __init__(self):
        """
        Initialize s3 env.

        Args:
            self: (todo): write your description
        """
        self.region_name = os.getenv("AWS_DEFAULT_REGION", None)
        assert (
            self.region_name
        ), "Required environment variable `AWS_DEFAULT_REGION` not found."
        self.s3_staging_dir = os.getenv("AWS_ATHENA_S3_STAGING_DIR", None)
        assert (
            self.s3_staging_dir
        ), "Required environment variable `AWS_ATHENA_S3_STAGING_DIR` not found."


def with_cursor(fn):
    """
    Return a function that creates a cursor.

    Args:
        fn: (todo): write your description
    """
    @functools.wraps(fn)
    def wrapped_fn(self, *args, **kwargs):
        """
        Wraps the wrapped function.

        Args:
            self: (todo): write your description
        """
        with contextlib.closing(self.connect()) as conn:
            with conn.cursor() as cursor:
                fn(self, cursor, *args, **kwargs)

    return wrapped_fn


def with_engine(fn):
    """
    Create a new engine.

    Args:
        fn: (todo): write your description
    """
    @functools.wraps(fn)
    def wrapped_fn(self, *args, **kwargs):
        """
        Wrapper around engine.

        Args:
            self: (todo): write your description
        """
        engine = self.create_engine()
        try:
            with contextlib.closing(engine.connect()) as conn:
                fn(self, engine, conn, *args, **kwargs)
        finally:
            engine.dispose()

    return wrapped_fn


def read_query(path):
    """
    Read query string query string from the query

    Args:
        path: (str): write your description
    """
    with codecs.open(path, "rb", "utf-8") as f:
        query = f.read()
    return [q.strip() for q in query.split(";") if q and q.strip()]
