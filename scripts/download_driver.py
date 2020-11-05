#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import print_function

import os
import sys

import pyathenajdbc

if sys.version_info[0] == 2:
    from urllib import urlretrieve
else:
    from urllib.request import urlretrieve

_PACKAGE_DIR = "pyathenajdbc"
_BASE_PATH = os.path.dirname(os.path.abspath(os.path.join(__file__, os.pardir)))


def download():
    """
    Download a file from google drive.

    Args:
    """
    dest = os.path.join(_BASE_PATH, _PACKAGE_DIR, pyathenajdbc.ATHENA_JAR)
    if os.path.exists(dest):
        print("The jdbc driver already exists: {0}".format(dest))
    else:
        print(
            "Downloading JDBC driver from: {0}".format(
                pyathenajdbc.ATHENA_DRIVER_DOWNLOAD_URL
            )
        )
        urlretrieve(pyathenajdbc.ATHENA_DRIVER_DOWNLOAD_URL, dest)


if __name__ == "__main__":
    download()
