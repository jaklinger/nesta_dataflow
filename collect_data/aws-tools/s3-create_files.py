import boto3
import os
import time

bucket_name = "datapipeline-triggers"
#file_name = "event-meetup_tier-0-inputs"
startswith = "event-member-meetup"
file_name = startswith + "_tier-0-inputs"

for _file_name in os.listdir("."):
    if not _file_name.startswith(startswith):
        continue
    #    if any("meetup"+str(x) in _file_name for x in (0, 10, 11, 12, 13)):
    #        continue
    print(_file_name)
    body = open(_file_name, 'rb')
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_name)
    bucket.put_object(Key=file_name, Body=body)
    time.sleep(60)
