'''
dcc
----

'''

from bs4 import BeautifulSoup
import logging
import pandas as pd

import requests
import time

# Local imports
from utils.common.browser import SelfClosingBrowser
from utils.common.datapipeline import DataPipeline

''''''
def get_field_from_box(field,box):
    for row in box.find("ul"):
        # Accept rows containing spans
        try:
            spans = row.find_all("span")
        except AttributeError:
            continue
        # Match the first span to the field name
        if spans[0].text != field:
            continue
        # Return the field data
        return spans[1].text
    raise ValueError("Could not find field "+field)

'''
Returns response if no ConnectionError exception
'''
def get_response_from_url(url,max_tries=3):
    n_tries = 0
    while True:
        n_tries += 1
        # Try to get the URL
        try:
            r = requests.get(url)
            return r
        # Allow connection error, then retry
        except requests.exceptions.ConnectionError as err:
            if n_tries == max_tries:
                raise err
            logging.warning("Connection error to %s",(url))
            time.sleep(10)


''''''
def run(config):

    # Fire up a browser at the top url
    top_url = config["parameters"]["src"]
    cat_pages = {}
    with SelfClosingBrowser(top_url=top_url) as b:
        # Scrape pages until no page found
        found_page = True
        while found_page:
            # Get the category web pages
            html_list = b.find_element_by_class_name("dcci_cat")
            list_items = html_list.find_elements_by_tag_name("li")
            for item in list_items:
                link = item.find_element_by_tag_name("a")
                cat_pages[link.text] = link.get_attribute('href')
            # Click the next page and get the table
            found_page = b.find_and_click_link("Next »")
            
    # Process each category's URL to find companies
    data = {}
    for cat,url in cat_pages.items():
        r = get_response_from_url(url)
        # No bad statuses
        if r.status_code != 200:
            continue        
        # Loop over text boxes in the soup
        soup = BeautifulSoup(r.text,"lxml")
        boxes = soup.find_all("div",class_="result_box")
        for box in boxes:
            # Get the company name
            title_box = box.find("div",class_="title")
            title_link = title_box.find("a")
            company_name = title_link.text

            # Get the website
            company_url = get_field_from_box("Website",box)
            city = get_field_from_box("City",box)
                
            # Check whether this URL has been processed before
            if company_name not in data:
                data[company_name] = dict(category=[cat],url=company_url,
                                          city=city,company_name=company_name)
            else:
                data[company_name]["category"].append(cat)    
    logging.info("\tGot %s rows",len(data))
    
    # Write data
    logging.info("\tWriting to table")
    with DataPipeline(config) as dp:
        for _,row in data.items():
            row["category"] = ";".join(row["category"]) # Pretty hacky
            dp.insert(row)
