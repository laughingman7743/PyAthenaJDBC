# -*- coding: utf-8 -*-
from sqlalchemy.dialects import registry

registry.register("awsathena.jdbc", "pyathenajdbc.sqlalchemy_athena", "AthenaDialect")
