import requests
import logging
import time
import ratelim

# Local imports
from utils.common.db_utils import read_all_results
from utils.common.datapipeline import DataPipeline

RATELIM_DUR = 50 * 60
RATELIM_QUERIES = 8500


@ratelim.patient(RATELIM_QUERIES, RATELIM_DUR)
def get_members(urlname, api_key, offset=0, max_results=200):
    # Set the offset parameter and make the request
    params = dict(offset=offset, page=max_results, key=api_key)
    r = requests.get("https://api.meetup.com/{}/members".format(urlname),
                     params=params)
    r.raise_for_status()
    # If no response is found
    if len(r.text) == 0:
        time.sleep(5)
        print("Got a bad response, so retrying page", offset)
        return get_members(urlname, api_key, offset=offset)
    # Extract results in the country of interest (bonus countries
    # can enter the fold because of the radius parameter)
    data = r.json()
    return [row['id'] for row in data]


def run(config):
    groups = read_all_results(config, 'input_db', 'input_table')
    # Filter groups matching target country/category
    groups = [row for row in groups
              if row['country_name'] == config['parameters']['country']]
    groups = [row for row in groups
              if row['category_id'] == int(config['parameters']['category'])]

    # Collect group info
    groups = set(row['urlname'] for row in groups)
    logging.info("Got %s distinct groups from database", len(groups))

    api_key = config["Meetup"]["api-key"]
    max_results = 200
    output = []
    for urlname in groups:
        member_ids = []
        offset = 0
        while True:
            _results = get_members(urlname, api_key, offset, max_results)
            print(offset, len(_results))
            member_ids += _results
            if len(_results) < max_results:
                break
            offset += 1

        for member_id in set(member_ids):
            row = dict(member_id=member_id, group_urlname=urlname)
            output.append(row)
        break

    logging.info("Got %s rows of data", len(output))
    with DataPipeline(config) as dp:
        for row in output:
            dp.insert(row)
