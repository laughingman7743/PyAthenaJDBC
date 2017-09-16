#!/usr/bin/env python
#  -*- coding: utf-8 -*-
from __future__ import print_function
import codecs
import os
import sys

from setuptools import setup, Command
from setuptools.command.install import install

import pyathenajdbc


if sys.version_info[0] == 2:
    from urllib import urlretrieve
else:
    from urllib.request import urlretrieve


_PACKAGE_NAME = 'PyAthenaJDBC'
_BASE_PATH = os.path.dirname(os.path.abspath(__file__))


class DriverDownloader(object):

    def __init__(self, on_completion):
        self.on_completion = on_completion

    def _download(self):
        dest = os.path.join(_BASE_PATH, _PACKAGE_NAME.lower(), pyathenajdbc.ATHENA_JAR)
        if os.path.exists(dest):
            print('skipping driver download')
        else:
            print('running driver download from {0}'.format(
                pyathenajdbc.ATHENA_DRIVER_DOWNLOAD_URL))
            urlretrieve(pyathenajdbc.ATHENA_DRIVER_DOWNLOAD_URL, dest)

    def download(self):
        self._download()
        self.on_completion()


class DriverDownloadCommand(Command):

    description = 'Download Amazon Athena JDBC driver'
    user_options = []

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self):
        downloader = DriverDownloader(lambda: None)
        downloader.download()


class InstallWithDriver(install):

    def _run(self):
        install.run(self)

    def run(self):
        downloader = DriverDownloader(self._run)
        downloader.download()


commands = {
    'driver_download': DriverDownloadCommand,
    'install': InstallWithDriver,
}


try:
    from wheel.bdist_wheel import bdist_wheel

    class BdistWheelWithDriver(bdist_wheel):

        def _run(self):
            bdist_wheel.run(self)

        def run(self):
            downloader = DriverDownloader(self._run)
            downloader.download()

    commands['bdist_wheel'] = BdistWheelWithDriver
except ImportError:
    pass


with codecs.open('README.rst', 'rb', 'utf-8') as readme:
    long_description = readme.read()


setup(
    name=_PACKAGE_NAME,
    version=pyathenajdbc.__version__,
    description='Python DB API 2.0 (PEP 249) compliant wrapper for Amazon Athena JDBC driver',
    long_description=long_description,
    url='https://github.com/laughingman7743/PyAthenaJDBC/',
    author='laughingman7743',
    author_email='laughingman7743@gmail.com',
    license='MIT License',
    packages=[_PACKAGE_NAME.lower()],
    package_data={
        '': ['*.rst'],
        _PACKAGE_NAME.lower(): ['*.jar'],
    },
    install_requires=[
        'future',
        'jpype1>=0.6.0',
        'botocore>=1.0.0'
    ],
    extras_require={
        'Pandas': ['pandas>=0.19.0'],
        'SQLAlchemy': ['SQLAlchemy>=1.0.0'],
    },
    tests_require=[
        'futures',
        'SQLAlchemy>=1.0.0',
        'pytest',
        'pytest-cov',
        'pytest-flake8',
        'pytest-catchlog',
    ],
    cmdclass=commands,
    entry_points={
        'sqlalchemy.dialects': [
            'awsathena.jdbc = pyathenajdbc.sqlalchemy_athena:AthenaDialect',
        ],
    },
    zip_safe=False,
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Topic :: Database :: Front-Ends',
        'Programming Language :: Java',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
    ],
)
