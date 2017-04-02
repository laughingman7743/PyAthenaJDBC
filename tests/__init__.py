# -*- coding: utf-8 -*-
from future.utils import PY26


if PY26:
    import unittest2 as unittest
else:
    import unittest  # noqa: F401
