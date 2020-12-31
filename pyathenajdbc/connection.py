# -*- coding: utf-8 -*-
import logging
import os
from typing import Any, List, Optional

import jpype

from pyathenajdbc import (
    ATHENA_CONNECTION_STRING,
    ATHENA_DRIVER_CLASS_NAME,
    ATHENA_JAR,
    LOG4J_PROPERTIES,
)
from pyathenajdbc.converter import DefaultJDBCTypeConverter, JDBCTypeConverter
from pyathenajdbc.cursor import Cursor
from pyathenajdbc.error import NotSupportedError, ProgrammingError
from pyathenajdbc.formatter import DefaultParameterFormatter, Formatter
from pyathenajdbc.util import attach_thread_to_jvm, synchronized

_logger = logging.getLogger(__name__)  # type: ignore


class Connection(object):

    _ENV_S3_STAGING_DIR: str = "AWS_ATHENA_S3_STAGING_DIR"
    _ENV_S3_OUTPUT_LOCATION: str = "AWS_ATHENA_S3_OUTPUT_LOCATION"
    _ENV_WORK_GROUP: str = "AWS_ATHENA_WORK_GROUP"
    _BASE_PATH: str = os.path.dirname(os.path.abspath(__file__))

    _class_loader = None

    def __init__(
        self,
        jvm_path: Optional[str] = None,
        jvm_options: Optional[List[str]] = None,
        converter: Optional[JDBCTypeConverter] = None,
        formatter: Optional[Formatter] = None,
        driver_path: Optional[str] = None,
        log4j_conf: Optional[str] = None,
        **driver_kwargs
    ) -> None:
        self._start_jvm(jvm_path, jvm_options, driver_path, log4j_conf)
        self._driver_kwargs = driver_kwargs
        self.region_name = self._driver_kwargs.get(
            "AwsRegion", os.getenv("AWS_DEFAULT_REGION", None)
        )
        self.schema_name = self._driver_kwargs.get("Schema", "default")
        self.work_group = self._driver_kwargs.get(
            "Workgroup", os.getenv(self._ENV_WORK_GROUP, None)
        )
        props = self._build_driver_args()
        jpype.JClass(ATHENA_DRIVER_CLASS_NAME)
        if self.region_name:
            self._jdbc_conn = jpype.java.sql.DriverManager.getConnection(
                ATHENA_CONNECTION_STRING.format(region=self.region_name), props
            )
        else:
            self._jdbc_conn = jpype.java.sql.DriverManager.getConnection()
        self._converter = converter if converter else DefaultJDBCTypeConverter()
        self._formatter = formatter if formatter else DefaultParameterFormatter()

    @classmethod
    @synchronized
    def _start_jvm(
        cls,
        jvm_path: Optional[str],
        jvm_options: Optional[List[str]],
        driver_path: Optional[str],
        log4j_conf: Optional[str],
    ) -> None:
        if jvm_path is None:
            jvm_path = jpype.getDefaultJVMPath()
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
            jpype.startJVM(
                jvm_path, *args, ignoreUnrecognized=True, convertStrings=True
            )
            cls._class_loader = (
                jpype.java.lang.Thread.currentThread().getContextClassLoader()
            )
        if not jpype.java.lang.Thread.isAttached():
            jpype.java.lang.Thread.attach()
            if not cls._class_loader:
                cls._class_loader = (
                    jpype.java.lang.Thread.currentThread().getContextClassLoader()
                )
            class_loader = jpype.java.net.URLClassLoader.newInstance(
                [jpype.java.net.URL("jar:file:{0}!/".format(driver_path))],
                cls._class_loader,
            )
            jpype.java.lang.Thread.currentThread().setContextClassLoader(class_loader)

    def _build_driver_args(self) -> Any:
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
        if self.region_name:
            props.setProperty("AwsRegion", self.region_name)
        if self.schema_name:
            props.setProperty("Schema", self.schema_name)
        if self.work_group:
            props.setProperty("Workgroup", self.work_group)
        for k, v in self._driver_kwargs.items():
            if k and v:
                props.setProperty(k, v)
        return props

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    @attach_thread_to_jvm
    def cursor(self) -> Cursor:
        if self.is_closed:
            raise ProgrammingError("Connection is closed.")
        return Cursor(self._jdbc_conn, self._converter, self._formatter)

    @attach_thread_to_jvm
    @synchronized
    def close(self) -> None:
        if not self.is_closed:
            self._jdbc_conn.close()
            self._jdbc_conn = None

    @property  # type: ignore
    @attach_thread_to_jvm
    def is_closed(self) -> bool:
        return self._jdbc_conn is None or self._jdbc_conn.isClosed()

    def commit(self) -> None:
        """Athena JDBC connection is only supported for auto-commit mode."""
        pass

    def rollback(self) -> None:
        raise NotSupportedError(
            "Athena JDBC connection is only supported for auto-commit mode."
        )
