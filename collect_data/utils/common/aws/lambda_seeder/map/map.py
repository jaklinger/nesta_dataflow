import boto3
import os
import sys

s3 = boto3.resource('s3')
os.environ["LD_LIBRARY_PATH"] = os.getcwd()


def run(event, context):
    sys.path.append(os.getcwd())
    trigger_file = event["Records"][0]["s3"]["object"]
    key = trigger_file["key"]
    bucket = "datapipeline-triggers"
    print("Got file", key)
    obj = s3.Bucket(bucket).Object(key)
    destination_bucket_name = "_".join(key.split("_")[1:])
    destination_bucket = s3.Bucket(destination_bucket_name)
    for file_name in obj.get()['Body'].read().decode('utf-8').split("\n"):
        print("Creating file", file_name, "in", destination_bucket_name)
        destination_bucket.put_object(Key=file_name, Body='')
    obj.delete()
