import logging
import pandas as pd
from pyvirtualdisplay import Display
import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions

class SelfClosingBrowser(webdriver.Firefox):

    """ Initialise constants, Firefox download settings and start up 
    the webdriver and display.
    """
    def __init__(self,top_url=None,
                 visible=0,display_size=(800,600),
                 load_time=1.5,**kwargs):
        self.load_time = load_time
        
        # Prepare Firefox download settings
        fp = webdriver.FirefoxProfile()
        for k,v in kwargs.items():
            fp.set_preference(k,v)
        
        # Start display and web driver
        self.display = Display(visible=visible,size=display_size)
        self.display.start()
        super().__init__(firefox_profile=fp)

        if top_url is not None:
            self.get(top_url)
        
    '''Dummy method for with'''
    def __enter__(self):
        return self

    """ Close and stop driver and display. This is particularly important in 
    the event of a failure in 'get_chd'.
    """        
    def __exit__(self, type, value, traceback):
        self.close()
        self.display.stop()
        logging.info("\tClosed and stopped driver and display")
        

    def find_and_click_link(self,text):
        all_texts = []
        for link in self.find_elements_by_tag_name('a'):
            all_texts.append(link.text)
            if link.text == text:                
                link.click()
                time.sleep(self.load_time)
                return True
        return False

    def get(self,url):
        logging.info("\tGoing to %s",url)
        result = super().get(url)
        time.sleep(self.load_time)
        return result

    def wait_for_presence_by_id(self,name):
        logging.debug("\tWaiting for %s",name)
        by = (By.ID,name)
        condition = expected_conditions.presence_of_element_located(by)
        return WebDriverWait(self,self.load_time).until(condition)        

    def get_pandas_table_by_id(self,table_id):
        table = self.wait_for_presence_by_id(table_id)
        html = table.get_attribute('outerHTML')
        df = pd.read_html(html,header=0)[0]
        return table,df
            

