'''
adcc
----

'''

import logging
import pandas as pd
from fuzzywuzzy import fuzz
import numpy as np
from copy import deepcopy
import requests
import re

from sqlalchemy.engine.url import URL
from sqlalchemy import create_engine
from sqlalchemy.sql.expression import text as sql_text
from sqlalchemy.sql.expression import select as sql_select
from sqlalchemy.schema import MetaData
from sqlalchemy.schema import Table
from collections import Counter
from selenium.common.exceptions import NoSuchElementException

# Local imports
from utils.common.browser import SelfClosingBrowser
from utils.common.datapipeline import DataPipeline
from utils.common.timer import timer


def remove_stops(word,stops):    
    _words = word.split()
    for stop in stops:
        if stop in _words:
            _words.remove(stop)
    # Only return if not empty
    if len(_words) > 0:
        return " ".join(_words)
    logging.debug("\t%s is empty after stop removal, "
                  "just using the whole word instead",word)
    return word

def comb_score(a,b,stops):
    if pd.isnull(a):
        return 0
    if pd.isnull(b):
        return 0
    # Remove stops and residual spaces
    a = remove_stops(a,stops)
    b = remove_stops(b,stops)
    
    score_1 = fuzz.token_sort_ratio(a,b)
    score_2 = fuzz.partial_ratio(a,b)
    return np.sqrt(score_1*score_1 + score_2*score_2)/np.sqrt(2)                    

''''''
def get_project_info(b,project,org_url,org_name,stops):
    timer.restart()

    # Dummy data container
    _data = dict(score=0,project_name=project["text"],
                 project_url=project["url"])
    
    # 
    required_fields = ['Organisation','Sector','Country']
    bonus_fields = ["Department"]
    
    # Go to the project info page
    b.get(project["url"])
    timer.stamp("Clicked project link")
    
    # Iterate through tables on the page in order to find
    # the relevant table (fuzzy match on name)
    try:
        outcomes = b.find_element_by_css_selector("#tabOutcomesCol")
        all_tables = outcomes.find_elements_by_class_name("table")
    except NoSuchElementException:
        return _data
    timer.stamp("Found element ids")
    
    for table in all_tables:
        timer.restart()        
        table,_df = b.get_pandas_table(table)#,thorough=True)
        
        # Rotate the tables around to make columns of fields
        _df = _df.T.reset_index()
        _df.columns = _df.iloc[0].values
        _df = _df.drop(0)
        timer.stamp("Wrangled table")
        
        # This field must exist, but needn't be collected
        if "Description" not in _df.columns:
            continue
        # These fields must exists, and will be collects
        if not all(field in _df.columns
                   for field in required_fields):
            continue
        # Additionally append any bonus fields to the requireds
        _required_fields = deepcopy(required_fields)
        for field in bonus_fields:
            if field in _df.columns:
                _required_fields.append(field)
        timer.stamp("Prepared fields")        
                
        # Extract the specified columns of data
        records = _df[_required_fields].to_dict('records')
        timer.stamp("Extracted fields")        
        if len(records) > 1:
            raise RuntimeError("More than one record found.")
        
        # Combine fuzzy scores (convert to float to help sqlalchemy)
        score = comb_score(org_name,records[0]["Organisation"],stops=stops)
        score = float(score)
        timer.stamp("Got score")
        #print(org_name,table_id,score)
        if score > _data["score"]:
            _data["score"] = score            
            for k,v in records[0].items():
                _data[k] = v
        timer.stamp("Appended data")
    return _data    

''''''
def run(config):

    # DB parameters
    input_db = config["parameters"]["input_db"]
    input_table = config["parameters"]["input_table"]
    output_db = config["parameters"]["output_db"]    
    output_table = config["parameters"]["table_name"]

    #
    db_cnf = URL(drivername="mysql+pymysql",
                 query={'read_default_file':config["DEFAULT"][input_db]})
    engine = create_engine(name_or_url=db_cnf)
    conn = engine.connect()
    md = MetaData(engine, reflect=True)
    table = Table(input_table, md, autoload=True, autoload_with=engine)
    results = conn.execute(sql_select([table])).fetchall()

    # Generate a list of "stop words" with more than n counts
    n = 50
    stops=[]
    for row in results:
        stops += row["name"].split()
    stops = [x for x,count in Counter(stops).most_common() if count > n]
    logging.info("\tWill ignore any of %s most common words.",len(stops))

    print(stops)
    return
    
    # Get connection to previously acquired data
    out_db_cnf = URL(drivername="mysql+pymysql",
                 query={'read_default_file':config["DEFAULT"][output_db]})
    out_engine = create_engine(name_or_url=out_db_cnf)
    out_conn = out_engine.connect()
    out_md = MetaData(out_engine, reflect=True)
    out_table = Table(output_table, out_md, autoload=True, autoload_with=engine)
    
    #
    data = {}
    flush_lim = 50
    with SelfClosingBrowser(top_url=config["parameters"]["src"],
                            load_time=1) as b:
        for irow,row in enumerate(results):
            # Get the relevant row information and load the URL
            org_name = row["name"] #Melbourne School of Health Sciences"
            org_id = row["id"]
            org_url = row["url"] #"http://gtr.rcuk.ac.uk/projects?ref=MC_U135097130"

            # Check if the result has already been found
            _select = sql_select([out_table])
            _select = _select.where(out_table.c.id == sql_text(":org_id"))
            _previous = out_conn.execute(_select,org_id=org_id)
            if len(_previous.fetchall()) > 0:
                continue
            
            # Get each project's data
            timer.restart()
            b.get(org_url)
            timer.stamp("Got outer URL")

            # Get the project links (with id=resultProjectLink<i>)
            projects  = b.find_elements_by_css_selector("a[id^=resultProjectLink]")
            project_texts = [dict(url=p.get_attribute("href"),text=p.text)
                             for p in projects]            
            timer.stamp("Got projects")
            data[org_id] = [get_project_info(b,project,org_url,org_name,stops)
                            for project in project_texts]

            if len(data) >= flush_lim:
                # logging.info("Flushing the results")
                # with DataPipeline(config) as dp:        
                #     for org,_data in data.items():
                #         for row in _data:
                #             row["id"] = org
                #             dp.insert(row)
                # data = {}
                # b.refresh()
                # b.delete_all_cookies()
                # logging.info("...refreshed after flushing")
                break
    # for org,d in data.items():
    #     print(org)
    #     for r in d:
    #         print("------")
    #         for k,v in r.items():
    #             print("\t",k," --> ",v)
    #     print("\n===============\n")

    timer.output()
        
    # Write data
    logging.info("\tWriting to table")
    config["GoogleDrive"]["target-repo"] = "tier-1"
    with DataPipeline(config) as dp:        
        for org,_data in data.items():
            for row in _data:
                row["id"] = org
                dp.insert(row)
