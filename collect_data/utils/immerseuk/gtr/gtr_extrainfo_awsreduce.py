import logging
from utils.common.datapipeline import DataPipeline
import boto3
import json
from copy import deepcopy

s3 = boto3.resource('s3')
bucket = s3.Bucket('tier-0')

def run(config=None):

    orgs = []
    for obj in bucket.objects.all():
        key = str(obj.key)
        if len(key.split("_")) != 3:
            continue
        data = obj.get()['Body'].read().decode("utf-8")
        orgs += json.loads(data)
#        if len(orgs) >= 1000:
#            break
    logging.info("\tGot %s organisations.",len(orgs))

    output = []
    for org in orgs:
        for r in org["results"]:
            row = deepcopy(org)
            row.pop("results")               
            row = dict(**row,**r)
            if row not in output:
                output.append(row)
    
    # Write data
    logging.info("\tWriting to table")
    with DataPipeline(config) as dp:
        for row in output:
            dp.insert(row)

if __name__ == "__main__":
    #run()

    #import numpy as np
    #all_numbers = list(np.arange(0,37242,6))
    #all_numbers.append(37242)

    print(len(open("not_done").read().split()))
    
    n = 0
    for obj in bucket.objects.all():
        n += int(len(obj.key.split("_")) == 3)
        #if key not in all_numbers:
        #    continue
            #print(key,"!!")
        #else:
        #    all_numbers.remove(key)
    print(n)
    # with open("not_done","w") as f:
    #     for n in all_numbers:
    #         print("-->",n,"<--")
    #         f.write(str(n)+" ")
        #data = obj.get()['Body'].read().decode("utf-8")
        #orgs += json.loads(data)
