# -*- coding: utf-8 -*-
import json

from redash.utils import JSONEncoder
from redash.query_runner import (BaseQueryRunner, register,
                                 TYPE_DATETIME, TYPE_DATE, TYPE_STRING,
                                 TYPE_BOOLEAN, TYPE_FLOAT, TYPE_INTEGER)

import logging
logger = logging.getLogger(__name__)

try:
    from pyathenajdbc import connect
    enabled = True
except ImportError:
    enabled = False


_ATHENA_TYPES_MAPPING = {
    -6: TYPE_INTEGER,
    5: TYPE_INTEGER,
    4: TYPE_INTEGER,
    -5: TYPE_INTEGER,
    8: TYPE_FLOAT,
    16: TYPE_BOOLEAN,
    -16: TYPE_STRING,
    91: TYPE_DATE,
    93: TYPE_DATETIME,
}


class Athena(BaseQueryRunner):
    noop_query = 'SELECT 1'

    @classmethod
    def name(cls):
        return "Amazon Athena"

    @classmethod
    def configuration_schema(cls):
        return {
            'type': 'object',
            'properties': {
                'region': {
                    'type': 'string',
                    'title': 'AWS Region'
                },
                'aws_access_key': {
                    'type': 'string',
                    'title': 'AWS Access Key'
                },
                'aws_secret_key': {
                    'type': 'string',
                    'title': 'AWS Secret Key'
                },
                's3_staging_dir': {
                    'type': 'string',
                    'title': 'S3 Staging Path'
                },
                'schema': {
                    'type': 'string',
                    'title': 'Schema'
                },
                'jvm_path': {
                    'type': 'string',
                    'title': 'JVM Path'
                },
                'jvm_options': {
                    'type': 'string',
                    'title': 'JVM Options'
                }
            },
            'required': ['region', 's3_staging_dir'],
            'secret': ['aws_secret_key']
        }

    def get_schema(self, get_stats=False):
        schema = {}
        query = """
        SELECT table_schema, table_name, column_name
        FROM information_schema.columns
        WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
        """

        results, error = self.run_query(query, None)
        if error is not None:
            raise Exception("Failed getting schema.")

        results = json.loads(results)
        for row in results['rows']:
            table_name = '{}.{}'.format(row['table_schema'], row['table_name'])
            if table_name not in schema:
                schema[table_name] = {'name': table_name, 'columns': []}
            schema[table_name]['columns'].append(row['column_name'])

        return schema.values()

    def run_query(self, query, user):
        conn = connect(s3_staging_dir=self.configuration['s3_staging_dir'],
                       region_name=self.configuration['region'],
                       access_key=self.configuration.get('aws_access_key', None),
                       secret_key=self.configuration.get('aws_secret_key', None),
                       schema_name=self.configuration.get('schema', 'default'),
                       jvm_path=self.configuration.get('jvm_path', None),
                       jvm_options=self.configuration.get['jvm_options'].split(',')
                       if self.configuration.get('jvm_options', None) else None)
        try:
            with conn.cursor() as cursor:
                cursor.execute(query)
                column_tuples = [(i[0], _ATHENA_TYPES_MAPPING.get(i[1], None)) for i in cursor.description]
                columns = self.fetch_columns(column_tuples)
                rows = [dict(zip(([c['name'] for c in columns]), r)) for i, r in enumerate(cursor.fetchall())]
                data = {'columns': columns, 'rows': rows}
                json_data = json.dumps(data, cls=JSONEncoder)
                error = None
        except Exception as e:
            json_data = None
            error = e.message
        finally:
            if conn:
                conn.close()

        return json_data, error

register(Athena)
