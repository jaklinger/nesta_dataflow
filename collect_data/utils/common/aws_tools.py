import boto3
import json


def get_all_by_prefix(prefix, bucket_name, max_get=None):
    data = []
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_name)
    for obj in bucket.objects.filter(Prefix=prefix):
        _data = obj.get()['Body'].read().decode("utf-8")
        js = json.loads(_data)
        if type(js) == list:
            data += js
        else:
            data.append(js)
        if max_get is not None and len(data) > max_get:
            break
    return data
