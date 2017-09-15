'''
adcc
----


'''

from bs4 import BeautifulSoup
import logging
import pandas as pd
from sqlalchemy.engine.url import URL
from sqlalchemy import create_engine
from sqlalchemy.sql.expression import insert as sql_insert
from sqlalchemy.sql.expression import text as sql_text
from sqlalchemy.schema import Table
from sqlalchemy.schema import MetaData

# Local imports
from utils.browser.browser import SelfClosingBrowser

''''''
def run(config):
    
    data = []
    top_url = "http://online.abudhabichamber.ae/aspApp2012/online_information/adcd/majorcat_list_companies.asp?catCode=20&listType=2"
    with SelfClosingBrowser(top_url=top_url,load_time=2.5) as b:
        found_result = True
        page_number = 1
        while found_result:
            # Click the next page and get the table
            if page_number > 1:
                found_result = b.find_and_click_link(str(page_number))
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
    
    # Connect to the database
    db_cnf = URL(drivername='mysql+pymysql',
                 query={'read_default_file':config["DEFAULT"]["tier-0.cnf"]})
    engine = create_engine(name_or_url=db_cnf)
    conn = engine.connect()

    # Insert data into table
    logging.info("\tWriting to table")
    metadata = MetaData(bind=engine)
    table = Table(sql_text('uae_business_directory'),metadata,
                  autoload=True,mysql_charset='utf8',)
    for row in df.to_dict("records"):
        conn.execute(sql_insert(table).prefix_with("IGNORE").values(**row))
    conn.close()
