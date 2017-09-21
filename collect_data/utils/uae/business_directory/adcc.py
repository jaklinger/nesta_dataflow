'''
adcc
----

'''

from bs4 import BeautifulSoup
import logging
import pandas as pd

# Local imports
from utils.common.browser import SelfClosingBrowser
from utils.common.datapipeline import DataPipeline

''''''
def run(config):
    
    data = []
    top_url = config["parameters"]["src"]
    with SelfClosingBrowser(top_url=top_url,load_time=2.5) as b:
        found_result = True
        page_number = 1
        while found_result:
            # Click the next page and get the table
            if page_number > 1:
                found_result = b.find_and_click_link(str(page_number))

            # Increment the page number and get table data
            page_number += 1
            table,_df = b.get_pandas_table_by_id("Table2")

            # Drop any missing companies
            condition = _df["Company Name"] == "-"
            _df = _df.loc[~condition]
            _df["url"] = None        

            # Process the soup for a tags in the table
            html = table.get_attribute('outerHTML')
            soup = BeautifulSoup(html,"lxml")
            for tag in soup.find_all("small"):
                anchors = tag.find_all("a")
                if len(anchors) != 1:
                    continue
                a = anchors[0]
                href = a["href"]
                if "@" in href:
                    continue
                condition = _df["Company Name"] == a.text
                _df.loc[condition,"url"] = href

            # Append the data
            data.append(_df[["Company Name","City","url"]])
        
    # Join the data
    df = pd.concat(data)
    df.columns = map(lambda x : "_".join(x.lower().split()),df.columns)
    logging.info("\tGot %s columns",len(df))    

    # Write data
    logging.info("\tWriting to table")
    with DataPipeline(config) as dp:
        for row in df.to_dict("records"):
            dp.insert(row)
