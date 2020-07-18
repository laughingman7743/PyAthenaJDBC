# -*- coding: utf-8 -*-
from __future__ import absolute_import, unicode_literals

import logging
import os

import jpype
from future.utils import iteritems

from pyathenajdbc import (
    ATHENA_CONNECTION_STRING,
    ATHENA_DRIVER_CLASS_NAME,
    ATHENA_JAR,
    LOG4J_PROPERTIES,
)
from pyathenajdbc.converter import JDBCTypeConverter
from pyathenajdbc.cursor import Cursor
from pyathenajdbc.error import NotSupportedError, ProgrammingError
from pyathenajdbc.formatter import ParameterFormatter
from pyathenajdbc.util import attach_thread_to_jvm, synchronized

_logger = logging.getLogger(__name__)


class Connection(object):

    _ENV_S3_STAGING_DIR = "AWS_ATHENA_S3_STAGING_DIR"
    _ENV_S3_OUTPUT_LOCATION = "AWS_ATHENA_S3_OUTPUT_LOCATION"
    _ENV_WORK_GROUP = "AWS_ATHENA_WORK_GROUP"
    _BASE_PATH = os.path.dirname(os.path.abspath(__file__))

    def __init__(
        self,
        jvm_path=None,
        jvm_options=None,
        converter=None,
        formatter=None,
        driver_path=None,
        log4j_conf=None,
        **driver_kwargs
    ):
        self._start_jvm(jvm_path, jvm_options, driver_path, log4j_conf)
        self._driver_kwargs = driver_kwargs
        props = self._build_driver_args()
        jpype.JClass(ATHENA_DRIVER_CLASS_NAME)
        self._jdbc_conn = jpype.java.sql.DriverManager.getConnection(
            ATHENA_CONNECTION_STRING.format(
                region=self._driver_kwargs.get('AwsRegion')
            ),
            props,
        )

        self._converter = converter if converter else JDBCTypeConverter()
        self._formatter = formatter if formatter else ParameterFormatter()

    @classmethod
    @synchronized
    def _start_jvm(cls, jvm_path, jvm_options, driver_path, log4j_conf):
        if jvm_path is None:
            jvm_path = jpype.get_default_jvm_path()
        if driver_path is None:
            driver_path = os.path.join(cls._BASE_PATH, ATHENA_JAR)
        if log4j_conf is None:
            log4j_conf = os.path.join(cls._BASE_PATH, LOG4J_PROPERTIES)
        if not jpype.isJVMStarted():
            _logger.debug("JVM path: %s", jvm_path)
            args = [
                "-server",
                "-Djava.class.path={0}".format(driver_path),
                "-Dlog4j.configuration=file:{0}".format(log4j_conf),
            ]
            if jvm_options:
                args.extend(jvm_options)
            _logger.debug("JVM args: %s", args)
            if jpype.__version__.startswith("0.6"):
                jpype.startJVM(jvm_path, *args)
            else:
                jpype.startJVM(
                    jvm_path, *args, ignoreUnrecognized=True, convertStrings=True
                )
            cls.class_loader = (
                jpype.java.lang.Thread.currentThread().getContextClassLoader()
            )
        if not jpype.isThreadAttachedToJVM():
            jpype.attachThreadToJVM()
            if not cls.class_loader:
                cls.class_loader = (
                    jpype.java.lang.Thread.currentThread().getContextClassLoader()
                )
            class_loader = jpype.java.net.URLClassLoader.newInstance(
                [jpype.java.net.URL("jar:file:{0}!/".format(driver_path))],
                cls.class_loader,
            )
            jpype.java.lang.Thread.currentThread().setContextClassLoader(class_loader)

    def _build_driver_args(self):
        props = jpype.java.util.Properties()
        props.setProperty(
            "AwsCredentialsProviderClass",
            "com.simba.athena.amazonaws.auth.DefaultAWSCredentialsProviderChain",
        )
        s3_staging_dir = os.getenv(self._ENV_S3_STAGING_DIR, None)
        if s3_staging_dir:
            props.setProperty("S3OutputLocation", s3_staging_dir)
        s3_output_location = os.getenv(self._ENV_S3_OUTPUT_LOCATION, None)
        if s3_output_location:
            props.setProperty("S3OutputLocation", s3_output_location)
        work_group = os.getenv(self._ENV_WORK_GROUP, None)
        if work_group:
            props.setProperty("Workgroup", work_group)
        for k, v in iteritems(self._driver_kwargs):
            if k and v:
                props.setProperty(k, v)
        return props

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    @attach_thread_to_jvm
    def cursor(self):
        if self.is_closed:
            raise ProgrammingError("Connection is closed.")
        return Cursor(self._jdbc_conn, self._converter, self._formatter)

    @attach_thread_to_jvm
    @synchronized
    def close(self):
        if not self.is_closed:
            self._jdbc_conn.close()
            self._jdbc_conn = None

    @property
    @attach_thread_to_jvm
    def is_closed(self):
        return self._jdbc_conn is None or self._jdbc_conn.isClosed()

    def commit(self):
        """Athena JDBC connection is only supported for auto-commit mode."""
        pass

    def rollback(self):
        raise NotSupportedError(
            "Athena JDBC connection is only supported for auto-commit mode."
        )
