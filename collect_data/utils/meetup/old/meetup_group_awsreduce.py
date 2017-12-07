import boto3
import logging
import json

from utils.common.datapipeline import DataPipeline


def run(config=None):
    data = []
    s3 = boto3.resource('s3')
    bucket = s3.Bucket('tier-0')
    for obj in bucket.objects.filter(Prefix='group_meetup_'):
        _data = obj.get()['Body'].read().decode("utf-8")
        data.append(json.loads(_data))
        # if len(data) >= 100:
        #    break
    logging.info("\tGot %s groups.", len(data))

    # Write data
    logging.info("\tWriting to table")
    with DataPipeline(config) as dp:
        for row in data:
            dp.insert(row)
