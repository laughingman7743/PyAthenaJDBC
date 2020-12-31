#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
from urllib.request import urlretrieve

import pyathenajdbc

_PACKAGE_DIR: str = "pyathenajdbc"
_BASE_PATH: str = os.path.dirname(os.path.abspath(os.path.join(__file__, os.pardir)))


def download() -> None:
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
