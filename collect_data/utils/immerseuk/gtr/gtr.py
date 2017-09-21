'''
adcc
----

'''

from bs4 import BeautifulSoup
import logging
import pandas as pd
from fuzzywuzzy import fuzz

# Local imports
from utils.common.browser import SelfClosingBrowser
#from utils.common.datapipeline import DataPipeline

''''''
def run(config):

    org_name = "Melbourne School of Health Sciences"
    org_id = ""
    org_url = "http://gtr.rcuk.ac.uk/projects?ref=MC_U135097130"

    data = {}
    required_fields = ['Organisation', 'Department', 'Sector', 'Country']
    with SelfClosingBrowser(top_url=org_url,load_time=2.5) as b:        
        data[org_id] = (0,{})
        for e in b.find_elements_by_css_selector("table"):
            table_id = e.get_attribute("id")
            if not table_id:
                continue
            table,_df = b.get_pandas_table_by_id(table_id)
            # Rotate the tables around to make columns of fields
            _df = _df.T.reset_index()
            _df.columns = _df.iloc[0].values
            _df = _df.drop(0)
            if "Description" not in _df.columns:
                continue
            if not all(field in _df.columns
                       for field in required_fields):
                continue
            records = _df[required_fields].to_dict('records')
            score = fuzz.token_sort_ratio(org_name,records[0]["Organisation"])
            if score > data[org_id][0]:
                data[org_id] = (score,records[0])                
    print(data)
    
        #print(b.page_source)
    #     found_result = True
    #     page_number = 1
    #     while found_result:
    #         # Click the next page and get the table
    #         if page_number > 1:
    #             found_result = b.find_and_click_link(str(page_number))

    #         # Increment the page number and get table data
    #         page_number += 1
    #         table,_df = b.get_pandas_table_by_id("Table2")

    #         # Drop any missing companies
    #         condition = _df["Company Name"] == "-"
    #         _df = _df.loc[~condition]
    #         _df["url"] = None        

    #         # Process the soup for a tags in the table
    #         html = table.get_attribute('outerHTML')
    #         soup = BeautifulSoup(html,"lxml")
    #         for tag in soup.find_all("small"):
    #             anchors = tag.find_all("a")
    #             if len(anchors) != 1:
    #                 continue
    #             a = anchors[0]
    #             href = a["href"]
    #             if "@" in href:
    #                 continue
    #             condition = _df["Company Name"] == a.text
    #             _df.loc[condition,"url"] = href

    #         # Append the data
    #         data.append(_df[["Company Name","City","url"]])
        
    # # Join the data
    # df = pd.concat(data)
    # df.columns = map(lambda x : "_".join(x.lower().split()),df.columns)
    # logging.info("\tGot %s columns",len(df))    

    # # Write data
    # logging.info("\tWriting to table")
    # with DataPipeline(config) as dp:
    #     for row in df.to_dict("records"):
    #         dp.insert(row)
