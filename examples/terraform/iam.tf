resource "aws_iam_role" "athena_access_role" {
  name = "athena-access-role"
  assume_role_policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "ec2.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
EOF
}

resource "aws_iam_role_policy" "athena_access_policy" {
  name = "athena-access-policy"
  role =  "${aws_iam_role.athena_access_role.id}"
  policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "athena:*"
      ],
      "Resource": [
        "*"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetBucketLocation",
        "s3:GetObject",
        "s3:ListBucket",
        "s3:ListBucketMultipartUploads",
        "s3:ListMultipartUploadParts",
        "s3:AbortMultipartUpload",
        "s3:CreateBucket",
        "s3:PutObject"
      ],
      "Resource": [
        "arn:aws:s3:::aws-athena-query-results-*",
        "arn:aws:s3:::${var.s3_staging_dir}",
        "arn:aws:s3:::${var.s3_awsome_log_data}"
      ]
    }
  ]
}
EOF
}

resource "aws_iam_instance_profile" "athena_access_profile" {
  name = "${aws_iam_role.athena_access_role.name}"
  roles = ["${aws_iam_role.athena_access_role.name}"]
}
