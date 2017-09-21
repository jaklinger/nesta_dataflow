'''
'''

import json
import pandas as pd
from sqlalchemy import create_engine
import os

def write_json(conn,table_name,destination=""):
    '''
    '''
    chunks = []
    for chunk in pd.read_sql_table(table_name,conn,chunksize=1000):
        chunks.append(chunk)

    destination = os.path.join(destination,table_name+".json")
    with open(destination,"w") as f:
        df = pd.concat(chunks)
        js = df.to_json(orient='records',lines=True)        
        f.write("["+js.replace("\n",",\n")+"]")
    return destination
