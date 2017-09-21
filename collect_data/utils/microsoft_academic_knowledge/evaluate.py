import requests
import urllib.parse as urlparse
from sqlalchemy.engine.url import URL
from sqlalchemy import create_engine
from sqlalchemy.sql.expression import insert as sql_insert
from sqlalchemy.sql.expression import text as sql_text
from sqlalchemy.schema import Table
from sqlalchemy.schema import MetaData
import json

'''Gets data from Microsoft Academic Knowledge'''
def get_data(config):
    # Join the URL together from the config
    split_url = urlparse.SplitResult(scheme=config["MAK"]["scheme"],
                                     netloc=config["MAK"]["netloc"],
                                     path=config["MAK-evaluate"]["path"],
                                     query=None,fragment=None)
    url = urlparse.urlunsplit(split_url)
    
    # Launch a POST request with config parameters
    results = []
    count = 1 # dummy value
    while count > 0:
        r = requests.post(url,data=dict(config["parameters"]),
                          headers=dict(config["MAK-evaluate-headers"]))
        r.raise_for_status()
        # Get the results, and increment the offset
        js = r.json()
        results += js["entities"]
        count = len(js["entities"])
        _offset = int(config["parameters"]["offset"])
        config["parameters"]["offset"] = str(_offset + count)
        break
    # Reset the offset
    config["parameters"]["offset"] = "0"
    return results

'''Dump json data in a tier-0 table'''
def dump_json(js,config):
    db_cnf = URL(drivername='mysql+pymysql',
                 query={'read_default_file':config["DEFAULT"]["tier-0.cnf"]})
    engine = create_engine(name_or_url=db_cnf)
    conn = engine.connect()
    # Insert data into table
    metadata = MetaData(bind=engine)
    table = Table(sql_text('example_with_json'),metadata, autoload=True)    
    for row in js:
        conn.execute(sql_insert(table).values(json_data=json.dumps(row)))
    conn.close()
    
'''The hook to get_raw_data. This is the "main" function.'''
def run(config):
    js = get_data(config)
    dump_json(js,config)
