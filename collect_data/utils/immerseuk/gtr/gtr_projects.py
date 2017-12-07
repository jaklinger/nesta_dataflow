import logging
import requests
from utils.common.datapipeline import DataPipeline


def run(config):
    columns = config["parameters"]["columns"].split(",")
    # Run until out of pages
    page = 795
    while True:
        projs = []
        logging.info("Page %s", page)
        page += 1
        r = requests.get(config["parameters"]["src"],
                         params=dict(p=page, s=100))
        # Assume that this means we're out of pages
        if r.status_code != 200:
            break
        # Append the organisations
        js = r.json()
        for row in js["project"]:
            for link in row["links"]["link"]:
                _row = {k: row[k] for k in columns}
                if link['rel'].endswith("_ORG"):
                    _row["org_api_href"] = link['href']
                    projs.append(_row)

        logging.info("\tGot %s projects.", len(projs))

        # Write data
        logging.info("\tWriting to table")
        with DataPipeline(config, write_google=True) as dp:
            for row in projs:
                dp.insert(row)
