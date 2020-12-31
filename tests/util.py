# -*- coding: utf-8 -*-
import codecs
import contextlib
import functools
import os


class Env(object):
    def __init__(self):
        self.region_name = os.getenv("AWS_DEFAULT_REGION", None)
        assert (
            self.region_name
        ), "Required environment variable `AWS_DEFAULT_REGION` not found."
        self.s3_staging_dir = os.getenv("AWS_ATHENA_S3_STAGING_DIR", None)
        assert (
            self.s3_staging_dir
        ), "Required environment variable `AWS_ATHENA_S3_STAGING_DIR` not found."


def with_cursor(**opts):
    def _with_cursor(fn):
        @functools.wraps(fn)
        def wrapped_fn(self, *args, **kwargs):
            with contextlib.closing(self.connect(**opts)) as conn:
                with conn.cursor() as cursor:
                    fn(self, cursor, *args, **kwargs)

        return wrapped_fn

    return _with_cursor


def with_engine(**opts):
    def _with_engine(fn):
        @functools.wraps(fn)
        def wrapped_fn(self, *args, **kwargs):
            engine = self.create_engine(**opts)
            try:
                with contextlib.closing(engine.connect()) as conn:
                    fn(self, engine, conn, *args, **kwargs)
            finally:
                engine.dispose()

        return wrapped_fn

    return _with_engine


def read_query(path):
    with codecs.open(path, "rb", "utf-8") as f:
        query = f.read()
    return [q.strip() for q in query.split(";") if q and q.strip()]
