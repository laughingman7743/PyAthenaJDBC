.. image:: https://img.shields.io/pypi/pyversions/PyAthenaJDBC.svg
    :target: https://pypi.python.org/pypi/PyAthenaJDBC/

.. image:: https://travis-ci.org/laughingman7743/PyAthenaJDBC.svg?branch=master
    :target: https://travis-ci.org/laughingman7743/PyAthenaJDBC

.. image:: https://codecov.io/gh/laughingman7743/PyAthenaJDBC/branch/master/graph/badge.svg
    :target: https://codecov.io/gh/laughingman7743/PyAthenaJDBC

.. image:: https://img.shields.io/pypi/l/PyAthenaJDBC.svg
    :target: https://github.com/laughingman7743/PyAthenaJDBC/blob/master/LICENSE


PyAthenaJDBC
============

PyAthenaJDBC is a Python `DB API 2.0 (PEP 249)`_ compliant wrapper for `Amazon Athena JDBC driver`_.

.. _`DB API 2.0 (PEP 249)`: https://www.python.org/dev/peps/pep-0249/
.. _`Amazon Athena JDBC driver`: http://docs.aws.amazon.com/athena/latest/ug/connect-with-jdbc.html

Requirements
------------

* Python

  - CPython 2,7, 3,4, 3.5, 3.6

* Java

  - Java >= 8 (JDBC 4.2)

JDBC driver compatibility
-------------------------

+---------------+---------------------+-------------------------------------------------------------------------------+
| Version       | JDBC driver version | Vendor                                                                        |
+===============+=====================+===============================================================================+
| < 2.0.0       | == 1.1.0            | AWS (Early released jdbc driver. It is incompatible with Simba's JDBC driver) |
+---------------+---------------------+-------------------------------------------------------------------------------+
| >= 2.0.0      | >= 2.0.5            | Simba                                                                         |
+---------------+---------------------+-------------------------------------------------------------------------------+

Installation
------------

.. code:: bash

    $ pip install PyAthenaJDBC

Extra packages:

+---------------+------------------------------------------+----------+
| Package       | Install command                          | Version  |
+===============+==========================================+==========+
| Pandas        | ``pip install PyAthenaJDBC[Pandas]``     | >=0.19.0 |
+---------------+------------------------------------------+----------+
| SQLAlchemy    | ``pip install PyAthenaJDBC[SQLAlchemy]`` | >=1.0.0  |
+---------------+------------------------------------------+----------+

Usage
-----

Basic usage
~~~~~~~~~~~

.. code:: python

    from pyathenajdbc import connect

    conn = connect(s3_staging_dir='s3://YOUR_S3_BUCKET/path/to/',
                   region_name='us-west-2')
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
            SELECT * FROM one_row
            """)
            print(cursor.description)
            print(cursor.fetchall())
    finally:
        conn.close()

Cursor iteration
~~~~~~~~~~~~~~~~

.. code:: python

    from pyathenajdbc import connect

    conn = connect(s3_staging_dir='s3://YOUR_S3_BUCKET/path/to/',
                   region_name='us-west-2')
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
            SELECT * FROM many_rows LIMIT 10
            """)
            for row in cursor:
                print(row)
    finally:
        conn.close()

Query with parameter
~~~~~~~~~~~~~~~~~~~~

Supported `DB API paramstyle`_ is only ``PyFormat``.
``PyFormat`` only supports `named placeholders`_ with old ``%`` operator style and parameters specify dictionary format.

.. code:: python

    from pyathenajdbc import connect

    conn = connect(s3_staging_dir='s3://YOUR_S3_BUCKET/path/to/',
                   region_name='us-west-2')
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
            SELECT col_string FROM one_row_complex
            WHERE col_string = %(param)s
            """, {'param': 'a string'})
            print(cursor.fetchall())
    finally:
        conn.close()

if ``%`` character is contained in your query, it must be escaped with ``%%`` like the following:

.. code:: sql

    SELECT col_string FROM one_row_complex
    WHERE col_string = %(param)s OR col_string LIKE 'a%%'

.. _`DB API paramstyle`: https://www.python.org/dev/peps/pep-0249/#paramstyle
.. _`named placeholders`: https://pyformat.info/#named_placeholders

JVM options
~~~~~~~~~~~

In the connect method or connection object, you can specify JVM options with a string array.

You can increase the JVM heap size like the following:

.. code:: python

    from pyathenajdbc import connect

    conn = connect(s3_staging_dir='s3://YOUR_S3_BUCKET/path/to/',
                   region_name='us-west-2',
                   jvm_options=['-Xms1024m', '-Xmx4096m'])
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
            SELECT * FROM many_rows
            """)
            print(cursor.fetchall())
    finally:
        conn.close()

JDBC 4.1
~~~~~~~~

If you want to use JDBC 4.1, download the corresponding JDBC driver
and specify the path of the downloaded JDBC driver as the argument ``driver_path`` of the connect method or connection object.

* The `AthenaJDBC41-2.0.5.jar`_ is compatible with JDBC 4.1 and requires JDK 7.0 or later.

.. _`AthenaJDBC41-2.0.5.jar`: https://s3.amazonaws.com/athena-downloads/drivers/JDBC/SimbaAthenaJDBC_2.0.5/AthenaJDBC41_2.0.5.jar

.. code:: python

    from pyathenajdbc import connect

    conn = connect(s3_staging_dir='s3://YOUR_S3_BUCKET/path/to/',
                   region_name='us-west-2',
                   driver_path='/path/to/AthenaJDBC41_2.0.5.jar')

JDBC driver configuration options
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The connect method or connection object pass keyword arguments as options to the JDBC driver.
If you want to change the behavior of the JDBC driver,
specify the option as a keyword argument in the connect method or connection object.

.. code:: python

    from pyathenajdbc import connect

    conn = connect(s3_staging_dir='s3://YOUR_S3_BUCKET/path/to/',
                   region_name='us-west-2',
                   LogPath='/path/to/pyathenajdbc/log/',
                   LogLevel='6')

For details of the JDBC driver options refer to the official documentation.

* `JDBC Driver Installation and Configuration Guide`_.

.. _`JDBC Driver Installation and Configuration Guide`: JDBC Driver Installation and Configuration Guide.

NOTE: Option names and values are case-sensitive. The option value is specified as a character string.

SQLAlchemy
~~~~~~~~~~

Install SQLAlchemy with ``pip install SQLAlchemy>=1.0.0`` or ``pip install PyAthenaJDBC[SQLAlchemy]``.
Supported SQLAlchemy is 1.0.0 or higher.

.. code:: python

    import contextlib
    from urllib.parse import quote_plus  # PY2: from urllib import quote_plus
    from sqlalchemy.engine import create_engine
    from sqlalchemy.sql.expression import select
    from sqlalchemy.sql.functions import func
    from sqlalchemy.sql.schema import Table, MetaData

    conn_str = 'awsathena+jdbc://{access_key}:{secret_key}@athena.{region_name}.amazonaws.com:443/'\
               '{schema_name}?s3_staging_dir={s3_staging_dir}'
    engine = create_engine(conn_str.format(
        access_key=quote_plus('YOUR_ACCESS_KEY'),
        secret_key=quote_plus('YOUR_SECRET_ACCESS_KEY'),
        region_name='us-west-2',
        schema_name='default',
        s3_staging_dir=quote_plus('s3://YOUR_S3_BUCKET/path/to/')))
    try:
        with contextlib.closing(engine.connect()) as conn:
            many_rows = Table('many_rows', MetaData(bind=engine), autoload=True)
            print(select([func.count('*')], from_obj=many_rows).scalar())
    finally:
        engine.dispose()

The connection string has the following format:

.. code:: python

    awsathena+jdbc://{access_key}:{secret_key}@athena.{region_name}.amazonaws.com:443/{schema_name}?s3_staging_dir={s3_staging_dir}&driver_path={driver_path}&...

NOTE: ``s3_staging_dir`` requires quote. If ``access_key``, ``secret_key`` and other parameter contain special characters, quote is also required.

Pandas
~~~~~~

Minimal example for Pandas DataFrame:

.. code:: python

    from pyathenajdbc import connect
    import pandas as pd

    conn = connect(access_key='YOUR_ACCESS_KEY_ID',
                   secret_key='YOUR_SECRET_ACCESS_KEY',
                   s3_staging_dir='s3://YOUR_S3_BUCKET/path/to/',
                   region_name='us-west-2',
                   jvm_path='/path/to/jvm')  # optional, as used by JPype
    df = pd.read_sql("SELECT * FROM many_rows LIMIT 10", conn)

As Pandas DataFrame:

.. code:: python

    import contextlib
    from pyathenajdbc import connect
    from pyathenajdbc.util import as_pandas

    with contextlib.closing(
            connect(s3_staging_dir='s3://YOUR_S3_BUCKET/path/to/'
                    region_name='us-west-2'))) as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
            SELECT * FROM many_rows
            """)
            df = as_pandas(cursor)
    print(df.describe())

Examples
--------

Redash_ query runner example
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

See `examples/redash/athena.py`_

.. _Redash: https://github.com/getredash/redash
.. _`examples/redash/athena.py`: examples/redash/athena.py

Credential
----------

Support `AWS CLI credentials`_, `Properties file credentials`_ and `AWS credentials provider chain`_.

.. _`AWS CLI credentials`: http://docs.aws.amazon.com/cli/latest/userguide/cli-chap-getting-started.html
.. _`Properties file credentials`: http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/auth/PropertiesFileCredentialsProvider.html
.. _`AWS credentials provider chain`: http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/auth/DefaultAWSCredentialsProviderChain.html

Credential files
~~~~~~~~~~~~~~~~

~/.aws/credentials

.. code:: cfg

    [default]
    aws_access_key_id=YOUR_ACCESS_KEY_ID
    aws_secret_access_key=YOUR_SECRET_ACCESS_KEY

~/.aws/config

.. code:: cfg

    [default]
    region=us-west-2
    output=json

Environment variables
~~~~~~~~~~~~~~~~~~~~~

.. code:: bash

    $ export AWS_ACCESS_KEY_ID=YOUR_ACCESS_KEY_ID
    $ export AWS_SECRET_ACCESS_KEY=YOUR_SECRET_ACCESS_KEY
    $ export AWS_DEFAULT_REGION=us-west-2

Additional environment variable:

.. code:: bash

    $ export AWS_ATHENA_S3_STAGING_DIR=s3://YOUR_S3_BUCKET/path/to/

Properties file credentials
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Create a property file of the following format.

/path/to/AWSCredentials.properties

.. code:: properties

    accessKeyId:YOUR_ACCESS_KEY_ID
    secretKey:YOUR_SECRET_ACCESS_KEY

Specify the property file path with ``credential_file`` of the connect method or connection object.

.. code:: python

    from pyathenajdbc import connect

    conn = connect(credential_file='/path/to/AWSCredentials.properties',
                   s3_staging_dir='s3://YOUR_S3_BUCKET/path/to/',
                   region_name='us-west-2')

PyAthenaJDBC uses the property file to authenticate Amazon Athena.

AWS credentials provider chain
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

See `AWS credentials provider chain`_

    AWS credentials provider chain that looks for credentials in this order:

        * Environment Variables - AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY (RECOMMENDED since they are recognized by all the AWS SDKs and CLI except for .NET), or AWS_ACCESS_KEY and AWS_SECRET_KEY (only recognized by Java SDK)
        * Java System Properties - aws.accessKeyId and aws.secretKey
        * Credential profiles file at the default location (~/.aws/credentials) shared by all AWS SDKs and the AWS CLI
        * Credentials delivered through the Amazon EC2 container service if AWS_CONTAINER_CREDENTIALS_RELATIVE_URI" environment variable is set and security manager has permission to access the variable,
        * Instance profile credentials delivered through the Amazon EC2 metadata service

In the connect method or connection object, you can connect by specifying at least ``s3_staging_dir`` and ``region_name``.
It is not necessary to specify ``access_key`` and ``secret_key``.

.. code:: python

    from pyathenajdbc import connect

    conn = connect(s3_staging_dir='s3://YOUR_S3_BUCKET/path/to/',
                   region_name='us-west-2')

Terraform_ Instance profile example:

See `examples/terraform/`_

.. _Terraform: https://github.com/hashicorp/terraform
.. _`examples/terraform/`: examples/terraform/


Testing
-------

Depends on the following environment variables:

.. code:: bash

    $ export AWS_ACCESS_KEY_ID=YOUR_ACCESS_KEY_ID
    $ export AWS_SECRET_ACCESS_KEY=YOUR_SECRET_ACCESS_KEY
    $ export AWS_DEFAULT_REGION=us-west-2
    $ export AWS_ATHENA_S3_STAGING_DIR=s3://YOUR_S3_BUCKET/path/to/

Run test
~~~~~~~~

.. code:: bash

    $ pip install pipenv
    $ pipenv install --dev
    $ pipenv run scripts/test_data/upload_test_data.sh
    $ pipenv run pytest
    $ pipenv run scripts/test_data/delete_test_data.sh

Run test multiple Python versions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code:: bash

    $ pip install pipenv
    $ pipenv install --dev
    $ pipenv run scripts/test_data/upload_test_data.sh
    $ pyenv local 3.6.5 3.5.5 3.4.8 2.7.14
    $ pipenv run tox
    $ pipenv run scripts/test_data/delete_test_data.sh

License
-------

The license of all Python code except JDBC driver is `MIT license`_.

.. _`MIT license`: LICENSE

JDBC driver
~~~~~~~~~~~

For the license of JDBC driver, please check the following link.

* `JDBC driver release notes`_
* `JDBC driver license`_
* `JDBC driver notices`_

.. _`JDBC driver release notes`: https://s3.amazonaws.com/athena-downloads/drivers/JDBC/SimbaAthenaJDBC_2.0.5/docs/release-notes.txt
.. _`JDBC driver License`: https://s3.amazonaws.com/athena-downloads/drivers/JDBC/SimbaAthenaJDBC_2.0.5/docs/LICENSE.txt
.. _`JDBC driver notices`: https://s3.amazonaws.com/athena-downloads/drivers/JDBC/SimbaAthenaJDBC_2.0.5/docs/NOTICES.txt
