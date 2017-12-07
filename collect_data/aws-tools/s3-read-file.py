import boto3
s3 = boto3.resource('s3')
bucket = "tier"
objs = s3.Bucket(bucket).objects
