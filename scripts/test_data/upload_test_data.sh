#!/bin/bash -xe

[[ "${AWS_ATHENA_S3_STAGING_DIR:(-1)}" == '/' ]] || { echo "Please add trailing '/' to your AWS_ATHENA_S3_STAGING_DIR."; exit 1; }

aws s3 cp $(dirname $0)/one_row.tsv ${AWS_ATHENA_S3_STAGING_DIR}test_pyathena_jdbc/one_row/one_row.tsv
aws s3 cp $(dirname $0)/one_row_complex.tsv ${AWS_ATHENA_S3_STAGING_DIR}test_pyathena_jdbc/one_row_complex/one_row_complex.tsv
aws s3 cp $(dirname $0)/many_rows.tsv ${AWS_ATHENA_S3_STAGING_DIR}test_pyathena_jdbc/many_rows/many_rows.tsv
