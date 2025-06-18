#!/bin/bash
#
# TO-DO: run the following command and observe the JSON output: 
# airflow connections get aws_credentials -o json 
# 
#[{"id": "1", 
# "conn_id": "aws_credentials",
# "conn_type": "aws", 
# "description": "", 
# "host": "", 
# "schema": "", 
# "login": "secret_login", 
# "password": "secret_password", 
# "port": null, 
# "is_encrypted": "False", 
# "is_extra_encrypted": "False", 
# "extra_dejson": {}, 
# "get_uri": "aws://secret_login:secret_password@"
#}]
#
# Copy the value after "get_uri":
#
# For example: aws://secret_login:secret_password@
#
# TO-DO: Update the following command with the URI and un-comment it:
#
# airflow connections add aws_credentials --conn-uri 'aws://secret_login:secret_password@'
#
#
# TO-DO: run the following command and observe the JSON output: 
# airflow connections get redshift -o json
# 
# [{"id": "3", 
# "conn_id": "redshift", 
# "conn_type": "redshift", 
# "description": "", 
# "host": "default-workgroup.718733939656.us-east-1.redshift-serverless.amazonaws.com", 
# "schema": "dev", 
# "login": "awsuser", 
# "password": "R3dsh1ft", 
# "port": "5439", 
# "is_encrypted": "False", 
# "is_extra_encrypted": "False", 
# "extra_dejson": {}, 
# "get_uri": "redshift://awsuser:R3dsh1ft@default-workgroup.718733939656.us-east-1.redshift-serverless.amazonaws.com:5439/dev"}]
#
# Copy the value after "get_uri":
#
# For example: redshift://awsuser:R3dsh1ft@default-workgroup.718733939656.us-east-1.redshift-serverless.amazonaws.com:5439/dev
#
# TO-DO: Update the following command with the URI and un-comment it:
#
airflow connections add redshift --conn-uri 'redshift://awsuser:R3dsh1ft@default-workgroup.718733939656.us-east-1.redshift-serverless.amazonaws.com:5439/dev'
#
# TO-DO: update the following bucket name to match the name of your S3 bucket and un-comment it:
#
airflow variables set s3_bucket udacity-dend
#
# TO-DO: un-comment the below line:
#
airflow variables set s3_prefix song-data
