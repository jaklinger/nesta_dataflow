import logging
import requests
from utils.common.datapipeline import DataPipeline

def run(config):
    # Run until out of pages
    orgs = []
    page = 0
    while True:
        page += 1
        r = requests.get(config["parameters"]["src"],
                         params=dict(page=page,fetchSize=1000))
        # Assume that this means we're out of pages
        if r.status_code != 200:
            break
        # Append the organisations
        js = r.json()
        orgs += js["organisation"]
    logging.info("\tGot %s organisations.",len(orgs))

    # Write data
    logging.info("\tWriting to table")
    with DataPipeline(config) as dp:
        for row in orgs:
            dp.insert(row)
