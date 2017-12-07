'''
'''

import pandas as pd
import os
import json
import numpy


class Encoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, numpy.integer):
            return int(obj)
        elif isinstance(obj, numpy.floating):
            return float(obj)
        elif isinstance(obj, numpy.ndarray):
            return obj.tolist()
        else:
            return super().default(obj)


def write_json(conn, table_name, destination=""):
    '''
    '''
    chunks = []
    for chunk in pd.read_sql_table(table_name, conn, chunksize=1000):
        chunks.append(chunk)
    destination = os.path.join(destination, table_name+".json")
    df = pd.concat(chunks)
    _js = df.to_dict(orient='records')
    js = []
    for line in _js:
        for k, v in line.items():
            if k.startswith("timestamp"):
                line[k] = v.to_pydatetime().strftime('%Y-%m-%d %H:%M:%S')
        js.append(line)
    with open(destination, "w") as f:
        json.dump(js, f, indent=4, cls=Encoder)
    return destination
