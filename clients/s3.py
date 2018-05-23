# -*- coding: utf-8 -*-
import boto3, io
from os import getenv

client = boto3.client(
    's3',
    aws_access_key_id=getenv('S3_ACCESS_KEY'),
    aws_secret_access_key=getenv('S3_SECRET_KEY'),
)

def get(filename):
    obj = client.get_object(Bucket='dataviva-etl', Key=filename)

    return io.BytesIO(obj['Body'].read())

def put(filename, object):
    obj = client.put_object(
        Bucket=getenv('S3_BUCKET'),
        Key=filename,
        Body=object,
        ContentType='application/json',
    )
