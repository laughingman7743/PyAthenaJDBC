.. image:: https://img.shields.io/pypi/pyversions/PyAthenaJDBC.svg
    :target: https://pypi.org/project/PyAthenaJDBC/

.. image:: https://github.com/laughingman7743/PyAthenaJDBC/workflows/test/badge.svg
    :target: https://github.com/laughingman7743/PyAthenaJDBC/actions

.. image:: https://img.shields.io/pypi/l/PyAthenaJDBC.svg
    :target: https://github.com/laughingman7743/PyAthenaJDBC/blob/master/LICENSE

.. image:: https://pepy.tech/badge/pyathenajdbc/month
    :target: https://pepy.tech/project/pyathenajdbc

.. image:: https://img.shields.io/badge/code%20style-black-000000.svg
    :target: https://github.com/psf/black

PyAthenaJDBC
============

PyAthenaJDBC is an `Amazon Athena JDBC driver`_ wrapper for the Python `DB API 2.0 (PEP 249)`_.

.. _`DB API 2.0 (PEP 249)`: https://www.python.org/dev/peps/pep-0249/
.. _`Amazon Athena JDBC driver`: https://docs.aws.amazon.com/athena/latest/ug/connect-with-jdbc.html

Requirements
------------

* Python

  - CPython 3.6, 3.7, 3.8, 3.9

* Java

  - Java >= 8 (JDBC 4.2)

JDBC driver compatibility
-------------------------

+---------------+---------------------+-------------------------------------------------------------------------------+
| Version       | JDBC driver version | Vendor                                                                        |
+===============+=====================+===============================================================================+
| < 2.0.0       | == 1.1.0            | AWS (Early released JDBC driver. It is incompatible with Simba's JDBC driver) |
+---------------+---------------------+-------------------------------------------------------------------------------+
| >= 2.0.0      | >= 2.0.5            | Simba                                                                         |
+---------------+---------------------+-------------------------------------------------------------------------------+

Installation
------------

.. code:: bash

    $ pip install PyAthenaJDBC

Extra packages:

+---------------+------------------------------------------+-----------------+
| Package       | Install command                          | Version         |
+===============+==========================================+=================+
| Pandas        | ``pip install PyAthenaJDBC[Pandas]``     | >=1.0.0         |
+---------------+------------------------------------------+-----------------+
| SQLAlchemy    | ``pip install PyAthenaJDBC[SQLAlchemy]`` | >=1.0.0, <2.0.0 |
+---------------+------------------------------------------+-----------------+

Usage
-----

Basic usage
~~~~~~~~~~~

.. code:: python

    from pyathenajdbc import connect

    conn = connect(S3OutputLocation="s3://YOUR_S3_BUCKET/path/to/",
                   AwsRegion="us-west-2")
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

    conn = connect(S3OutputLocation="s3://YOUR_S3_BUCKET/path/to/",
                   AwsRegion="us-west-2")
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

    conn = connect(S3OutputLocation="s3://YOUR_S3_BUCKET/path/to/",
                   AwsRegion="us-west-2")
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
            SELECT col_string FROM one_row_complex
            WHERE col_string = %(param)s
            """, {"param": "a string"})
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

    conn = connect(S3OutputLocation="s3://YOUR_S3_BUCKET/path/to/",
                   AwsRegion="us-west-2",
                   jvm_options=["-Xms1024m", "-Xmx4096m"])
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

* The `AthenaJDBC41-2.0.7.jar`_ is compatible with JDBC 4.1 and requires JDK 7.0 or later.

.. _`AthenaJDBC41-2.0.7.jar`: https://s3.amazonaws.com/athena-downloads/drivers/JDBC/SimbaAthenaJDBC_2.0.7/AthenaJDBC41_2.0.7.jar

.. code:: python

    from pyathenajdbc import connect

    conn = connect(S3OutputLocation="s3://YOUR_S3_BUCKET/path/to/",
                   AwsRegion="us-west-2",
                   driver_path="/path/to/AthenaJDBC41_2.0.7.jar")

JDBC driver configuration options
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The connect method or connection object pass keyword arguments as options to the JDBC driver.
If you want to change the behavior of the JDBC driver,
specify the option as a keyword argument in the connect method or connection object.

.. code:: python

    from pyathenajdbc import connect

    conn = connect(S3OutputLocation="s3://YOUR_S3_BUCKET/path/to/",
                   AwsRegion="us-west-2",
                   LogPath="/path/to/pyathenajdbc/log/",
                   LogLevel="6")

For details of the JDBC driver options refer to the official documentation.

* `JDBC Driver Installation and Configuration Guide`_.

.. _`JDBC Driver Installation and Configuration Guide`: https://s3.amazonaws.com/athena-downloads/drivers/JDBC/SimbaAthenaJDBC_2.0.7/docs/Simba+Athena+JDBC+Driver+Install+and+Configuration+Guide.pdf

NOTE: Option names and values are case-sensitive. The option value is specified as a character string.

SQLAlchemy
~~~~~~~~~~

Install SQLAlchemy with ``pip install SQLAlchemy>=1.0.0`` or ``pip install PyAthenaJDBC[SQLAlchemy]``.
Supported SQLAlchemy is 1.0.0 or higher and less than 2.0.0.

.. code:: python

    import contextlib
    from urllib.parse import quote_plus
    from sqlalchemy.engine import create_engine
    from sqlalchemy.sql.expression import select
    from sqlalchemy.sql.functions import func
    from sqlalchemy.sql.schema import Table, MetaData

    conn_str = "awsathena+jdbc://{User}:{Password}@athena.{AwsRegion}.amazonaws.com:443/"\
               "{Schema}?S3OutputLocation={S3OutputLocation}"
    engine = create_engine(conn_str.format(
        User=quote_plus("YOUR_ACCESS_KEY"),
        Password=quote_plus("YOUR_SECRET_ACCESS_KEY"),
        AwsRegion="us-west-2",
        Schema="default",
        S3OutputLocation=quote_plus("s3://YOUR_S3_BUCKET/path/to/")))
    try:
        with contextlib.closing(engine.connect()) as conn:
            many_rows = Table("many_rows", MetaData(bind=engine), autoload=True)
            print(select([func.count("*")], from_obj=many_rows).scalar())
    finally:
        engine.dispose()

The connection string has the following format:

.. code:: text

    awsathena+jdbc://{User}:{Password}@athena.{AwsRegion}.amazonaws.com:443/{Schema}?S3OutputLocation={S3OutputLocation}&driver_path={driver_path}&...

If you do not specify ``User`` (i.e. AWSAccessKeyID) and ``Password`` (i.e. AWSSecretAccessKey) using instance profile credentials or credential profiles file:

.. code:: text

    awsathena+jdbc://:@athena.{Region}.amazonaws.com:443/{Schema}?S3OutputLocation={S3OutputLocation}&driver_path={driver_path}&...

NOTE: ``S3OutputLocation`` requires quote. If ``User``, ``Password`` and other parameter contain special characters, quote is also required.

Pandas
~~~~~~

As DataFrame
^^^^^^^^^^^^

You can use the `pandas.read_sql`_ to handle the query results as a `DataFrame object`_.

.. code:: python

    from pyathenajdbc import connect
    import pandas as pd

    conn = connect(User="YOUR_ACCESS_KEY_ID",
                   Password="YOUR_SECRET_ACCESS_KEY",
                   S3OutputLocation="s3://YOUR_S3_BUCKET/path/to/",
                   AwsRegion="us-west-2",
                   jvm_path="/path/to/jvm")
    df = pd.read_sql("SELECT * FROM many_rows LIMIT 10", conn)

The ``pyathena.util`` package also has helper methods.

.. code:: python

    import contextlib
    from pyathenajdbc import connect
    from pyathenajdbc.util import as_pandas

    with contextlib.closing(
            connect(S3OutputLocation="s3://YOUR_S3_BUCKET/path/to/"
                    AwsRegion="us-west-2"))) as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
            SELECT * FROM many_rows
            """)
            df = as_pandas(cursor)
    print(df.describe())

.. _`pandas.read_sql`: https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_sql.html
.. _`DataFrame object`: https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html

To SQL
^^^^^^

You can use `pandas.DataFrame.to_sql`_ to write records stored in DataFrame to Amazon Athena.
`pandas.DataFrame.to_sql`_ uses `SQLAlchemy`_, so you need to install it.

.. code:: python

    import pandas as pd
    from urllib.parse import quote_plus
    from sqlalchemy import create_engine
    conn_str = "awsathena+jdbc://:@athena.{AwsRegion}.amazonaws.com:443/"\
               "{Schema}?S3OutputLocation={S3OutputLocation}&S3Location={S3Location}&compression=snappy"
    engine = create_engine(conn_str.format(
        AwsRegion="us-west-2",
        Schema_name="YOUR_SCHEMA",
        S3OutputLocation=quote_plus("s3://YOUR_S3_BUCKET/path/to/"),
        S3Location=quote_plus("s3://YOUR_S3_BUCKET/path/to/")))
    df = pd.DataFrame({"a": [1, 2, 3, 4, 5]})
    df.to_sql("YOUR_TABLE", engine, schema="YOUR_SCHEMA", index=False, if_exists="replace", method="multi")

The location of the Amazon S3 table is specified by the ``S3Location`` parameter in the connection string.
If ``S3Location`` is not specified, ``S3OutputLocation`` parameter will be used. The following rules apply.

.. code:: text

    s3://{S3Location or S3OutputLocation}/{schema}/{table}/

The data format only supports Parquet. The compression format is specified by the ``compression`` parameter in the connection string.

.. _`pandas.DataFrame.to_sql`: https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.to_sql.html

Credential
----------

AWS credentials provider chain
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

See `Supplying and retrieving AWS credentials`_

    https://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/auth/DefaultAWSCredentialsProviderChain.html

    AWS credentials provider chain that looks for credentials in this order:

        * Environment Variables - AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY (RECOMMENDED since they are recognized by all the AWS SDKs and CLI except for .NET), or AWS_ACCESS_KEY and AWS_SECRET_KEY (only recognized by Java SDK)
        * Java System Properties - aws.accessKeyId and aws.secretKey
        * Web Identity Token credentials from the environment or container
        * Credential profiles file at the default location (~/.aws/credentials) shared by all AWS SDKs and the AWS CLI
        * Credentials delivered through the Amazon EC2 container service if AWS_CONTAINER_CREDENTIALS_RELATIVE_URI" environment variable is set and security manager has permission to access the variable,
        * Instance profile credentials delivered through the Amazon EC2 metadata service

In the connect method or connection object, you can connect by specifying at least ``S3OutputLocation`` and ``AwsRegion``.
``User`` and ``Password`` are not required if environment variables, credential files, or instance profiles have been set.

.. code:: python

    from pyathenajdbc import connect

    conn = connect(S3OutputLocation="s3://YOUR_S3_BUCKET/path/to/",
                   AwsRegion="us-west-2")

.. _`Supplying and retrieving AWS credentials`: https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/credentials.html

Testing
-------

Depends on the following environment variables:

.. code:: bash

    $ export AWS_ACCESS_KEY_ID=YOUR_ACCESS_KEY_ID
    $ export AWS_SECRET_ACCESS_KEY=YOUR_SECRET_ACCESS_KEY
    $ export AWS_DEFAULT_REGION=us-west-2
    $ export AWS_ATHENA_S3_STAGING_DIR=s3://YOUR_S3_BUCKET/path/to/

And you need to create a workgroup named ``test-pyathena-jdbc``.

Run test
~~~~~~~~

.. code:: bash

    $ pip install poetry
    $ poetry install -v
    $ poetry run scripts/test_data/upload_test_data.sh
    $ poetry run pytest
    $ poetry run scripts/test_data/delete_test_data.sh

Run test multiple Python versions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code:: bash

    $ pip install poetry
    $ poetry install -v
    $ poetry run scripts/test_data/upload_test_data.sh
    $ pyenv local 3.9.0 3.8.6 3.7.9 3.6.12
    $ poetry run tox
    $ poetry run scripts/test_data/delete_test_data.sh

Code formatting
---------------

The code formatting uses `black`_ and `isort`_.

Appy format
~~~~~~~~~~~

.. code:: bash

    $ make fmt

Check format
~~~~~~~~~~~~

.. code:: bash

    $ make chk

.. _`black`: https://github.com/psf/black
.. _`isort`: https://github.com/timothycrosley/isort

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
* `JDBC driver third-party licenses`_

.. _`JDBC driver release notes`: jdbc/release-notes.txt
.. _`JDBC driver License`: jdbc/LICENSE.txt
.. _`JDBC driver notices`: jdbc/NOTICES.txt
.. _`JDBC driver third-party licenses`: jdbc/third-party-licenses.txt
