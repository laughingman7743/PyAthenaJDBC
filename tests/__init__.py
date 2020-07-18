# -*- coding: utf-8 -*-
import os
import random
import string

from past.builtins.misc import xrange
from sqlalchemy.dialects import registry

registry.register("awsathena.jdbc", "pyathenajdbc.sqlalchemy_athena", "AthenaDialect")

BASE_PATH = os.path.dirname(os.path.abspath(__file__))
S3_PREFIX = "test_pyathena_jdbc"
WORK_GROUP = "test-pyathena-jdbc"
SCHEMA = "test_pyathena_jdbc_" + "".join(
    [random.choice(string.ascii_lowercase + string.digits) for i in xrange(10)]
)


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


ENV = Env()


class WithConnect(object):
    def connect(self, work_group=None):
        from pyathenajdbc import connect

        return connect(Schema=SCHEMA, Workgroup=work_group)
