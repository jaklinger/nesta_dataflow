from bs4 import BeautifulSoup
import logging
import requests
import sys
from urllib.parse import urljoin
from urllib.parse import urlparse
from urllib.parse import urldefrag
from utils.common.datapipeline import DataPipeline
from utils.common.browser import SelfClosingBrowser

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions

class Crawler:
    '''
    '''
    def __init__(self,top_url=None,max_depth=4,
                 driver=None,wait_condition=""):
        #print(driver)
        # Crawler settings
        self.max_depth = max_depth # Maximum search depth

        # Get the logger
        self.logger = logging.getLogger(__name__)
        self.top_url = top_url
        self.current_url = self.top_url
        
        # Record the top URL stub, in order to identify external sites
        parse_result = urlparse(self.top_url)
        self.top_url_stub = ".".join(parse_result.netloc.split(".")[-3:])

        self.urls = set()
        self.all_urls = set()
        self.driver = driver
        self.wait_condition = wait_condition
        
    '''The "main" crawling method'''        
    def crawl(self,url=None,depth=1):
        tabs = "".join(["\t"]*depth)
        parent_url = self.current_url
        
        # 1) Break out if reached max_depth
        if depth > self.max_depth:
            #self.logger.debug("Breaking out")
            return False
        # 2) Set the url if None
        if url is None:
            url = self.top_url        
        # 3) Set the current URL
        if not url.startswith("http"):
            url = urljoin(self.current_url,url)
        self.current_url = url.rstrip("/")
        self.logger.debug(tabs+"Trying %s",(self.current_url))
        
        # Get this page's HTML
        if self.driver is None:
            try:            
                r = requests.get(self.current_url)
                r.raise_for_status()
            except (requests.exceptions.HTTPError,
                requests.exceptions.InvalidSchema,
                requests.exceptions.MissingSchema,
                requests.exceptions.ConnectionError) as err:
                return False
            # Update the URL (in case of forwarding)            
            self.current_url = r.url
            try:
                html = "\n".join(x.decode("utf-8") for x in r.iter_lines())
            except UnicodeDecodeError:
                self.logger.debug("Ignoring suspicious path %s",(self.current_url))
                return False            
        else:
            try:
                # Get the URL
                self.driver.get(self.current_url)
                if self.wait_condition is not "":
                    wait_for = self.wait_condition 
                    condition = expected_conditions.presence_of_element_located((By.XPATH,wait_for))
                    element = WebDriverWait(self.driver,10).until(condition)                
                # Update the URL (in case of forwarding)            
                self.current_url = self.driver.current_url
                html = self.driver.page_source                                                        
            except Exception as err:
                print("\t"*depth,"Couldn't retrieve",self.current_url)
                #raise err
                return False
        
        # Get the request text (not using r.text since
        # PDF and binary files hang on r.text call)
        if any(self.current_url.endswith(x)
               for x in ("pdf","jpg","jpeg")):
            return False

        # Add the page to the list of "good" URLs
        #if "raduat" in self.current_url:
        print("\t"*depth,">>> Got",self.current_url)#,"from",parent_url)
        self.urls.add(self.current_url) #= True
        
        # Iterate through links on the page to calculate the score
        soup = BeautifulSoup(html,"lxml")        
        new_urls = set()
        anchors = soup.find_all("a")
        for a in anchors:
            # If not a link, skip
            if "href" not in a.attrs:
                continue            
            # Ignore '#' fragments or internal links
            link_url = urldefrag(a["href"])[0].rstrip("/")
            if not link_url:
                continue
            if link_url == self.current_url or link_url.strip() == '':
                continue
            if link_url.startswith("mailto") or link_url.startswith("tel"):
                continue            
            # Ignore external links
            parse_result = urlparse(link_url)
            if parse_result.netloc != '':
                if self.top_url_stub not in parse_result.netloc:
                    continue
            else:
                link_url = urljoin(url,link_url)                
            link_url = link_url.split("?")[0]
            # Get new links, if not already found
            if (link_url not in self.all_urls) and (link_url not in self.urls):
                new_urls.add(link_url)
                self.all_urls.add(link_url)

        # Dive a level deeper
        for url in new_urls:
            self.crawl(url=url,depth=depth+1)
        return True

''''''
def run(config):

    # Set the logging level
    logging.getLogger("requests").setLevel(logging.WARNING)
    logging.basicConfig(stream=sys.stderr,level=logging.INFO)
    
    # Instantiate the crawler
    top_url = config["parameters"]["top_url"]

    # Run with browser
    if config["parameters"]["selenium"] == "True":
        print("Running in selenium mode")
        with SelfClosingBrowser(load_time=10) as driver:
            crawler = Crawler(top_url=top_url,
                              max_depth=int(config["parameters"]["max_depth"]),
                              driver=driver,
                              wait_condition=config["parameters"]["wait_condition"]) #'//*[@id="navbar"]/ul/li[1]/a'
            crawler.crawl()
    # Run with browser
    else:
        crawler = Crawler(top_url=top_url,
                          max_depth=int(config["parameters"]["max_depth"]))
        crawler.crawl()

    print("Got",len(crawler.urls))
        
    # Write data
    logging.info("\tWriting to table")
    with DataPipeline(config) as dp:
        for url in crawler.urls:
            row = dict(top_url=top_url,url=url,success=True)
            dp.insert(row)
    
#________________________
#if __name__ == "__main__":


    #                       
    # crawler.crawl()
    #for url,success in crawler.urls.items():
    #    print(url,success)
            
#run(logging_level=logging.DEBUG,max_depth=5,
#    top_url='http://www.uowdubai.ac.ae')

