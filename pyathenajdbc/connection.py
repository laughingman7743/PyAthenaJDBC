# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals
import logging
import os

import jpype
from future.utils import iteritems

from pyathenajdbc import (ATHENA_DRIVER_CLASS_NAME,
                          ATHENA_CONNECTION_STRING,
                          ATHENA_JAR)
from pyathenajdbc.converter import JDBCTypeConverter
from pyathenajdbc.cursor import Cursor
from pyathenajdbc.error import ProgrammingError, NotSupportedError
from pyathenajdbc.formatter import ParameterFormatter


_logger = logging.getLogger(__name__)


class Connection(object):

    _ENV_S3_STAGING_DIR = 'AWS_ATHENA_S3_STAGING_DIR'

    def __init__(self, s3_staging_dir=None, access_key=None, secret_key=None,
                 region_name=None, profile_name=None, credential_file=None,
                 jvm_options=None, converter=None, formatter=None, jvm_path=None,
                 **driver_args):
        if s3_staging_dir:
            self.s3_staging_dir = s3_staging_dir
        else:
            self.s3_staging_dir = os.getenv(self._ENV_S3_STAGING_DIR, None)
        assert self.s3_staging_dir, 'Required argument `s3_staging_dir` not found.'

        if credential_file:
            self.access_key = None
            self.secret_key = None
            self.token = None
            self.credential_file = credential_file
            assert self.credential_file, 'Required argument `credential_file` not found.'
            self.region_name = region_name
            assert self.region_name, 'Required argument `region_name` not found.'
        else:
            import botocore.session
            session = botocore.session.get_session()
            if access_key and secret_key:
                session.set_credentials(access_key, secret_key)
            if profile_name:
                session.set_config_variable('profile', profile_name)
            if region_name:
                session.set_config_variable('region', region_name)
            credentials = session.get_credentials()
            self.access_key = credentials.access_key
            assert self.access_key, 'Required argument `access_key` not found.'
            self.secret_key = credentials.secret_key
            assert self.secret_key, 'Required argument `secret_key` not found.'
            self.token = credentials.token
            self.credential_file = None
            self.region_name = session.get_config_variable('region')
            assert self.region_name, 'Required argument `region_name` not found.'

        self._start_jvm(jvm_options, jvm_path)

        props = self._build_driver_args(**driver_args)
        jpype.JClass(ATHENA_DRIVER_CLASS_NAME)
        self._jdbc_conn = jpype.java.sql.DriverManager.getConnection(
            ATHENA_CONNECTION_STRING.format(region=self.region_name), props)

        self._converter = converter if converter else JDBCTypeConverter()
        self._formatter = formatter if formatter else ParameterFormatter()

    @classmethod
    def _start_jvm(cls, options, jvm_path):
        if jvm_path is None:
            jvm_path = jpype.get_default_jvm_path()
        if not jpype.isJVMStarted():
            _logger.debug('JVM path: %s', jvm_path)
            args = ['-server', '-Djava.class.path={0}'.format(
                os.path.join(os.path.dirname(__file__), ATHENA_JAR))]
            if options:
                args.extend(options)
            _logger.debug('JVM args: %s', args)
            jpype.startJVM(jvm_path, *args)
        if not jpype.isThreadAttachedToJVM():
            jpype.attachThreadToJVM()

    def _build_driver_args(self, **kwargs):
        props = jpype.java.util.Properties()
        if self.credential_file:
            props.setProperty('aws_credentials_provider_class',
                              'com.amazonaws.athena.jdbc.shaded.' +
                              'com.amazonaws.auth.PropertiesFileCredentialsProvider')
            props.setProperty('aws_credentials_provider_arguments',
                              self.credential_file)
        elif self.token:
            props.setProperty('aws_credentials_provider_class',
                              'com.amazonaws.athena.jdbc.shaded.' +
                              'com.amazonaws.auth.InstanceProfileCredentialsProvider')
        else:
            props.setProperty('user', self.access_key)
            props.setProperty('password', self.secret_key)
        props.setProperty('s3_staging_dir', self.s3_staging_dir)
        for k, v in iteritems(kwargs):
            if k and v:
                props.setProperty(k, v)
        return props

    def __enter__(self):
        return self.cursor()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def cursor(self):
        if self.is_closed:
            raise ProgrammingError('Connection is closed.')
        return Cursor(self._jdbc_conn, self._converter, self._formatter)

    def close(self):
        if not self.is_closed:
            self._jdbc_conn.close()
            self._jdbc_conn = None

    @property
    def is_closed(self):
        return self._jdbc_conn is None or self._jdbc_conn.isClosed()

    def commit(self):
        """Athena JDBC connection is only supported for auto-commit mode."""
        pass

    def rollback(self):
        raise NotSupportedError('Athena JDBC connection is only supported for auto-commit mode.')
