'''
browser
-------

'''

import logging
import pandas as pd
from pyvirtualdisplay import Display
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions
from utils.common.timer import timer
from retrying import retry


class SelfClosingBrowser(webdriver.Chrome):
    """ Initialise constants, Firefox download settings and start up
    the webdriver and display.
    """
    def __init__(self, top_url=None, headless=True,
                 visible=0, display_size=(800, 600),
                 load_time=1.5, **kwargs):
        # self.load_time = load_time        
        # Prepare Firefox download settings
        # fp = webdriver.FirefoxProfile()
        # for k,v in kwargs.items():
        #     print(k,v)
        #     fp.set_preference(k,v)
        #
        # Start display and web driver
        self.display = Display(visible=visible, size=display_size)
        self.display.start()
        # super().__init__()#firefox_profile=fp)

        chrome_options = webdriver.ChromeOptions()
        if headless:
            chrome_options.add_argument("--headless")
        super().__init__(chrome_options=chrome_options)
        self.implicitly_wait(load_time)
        if top_url is not None:
            self.get(top_url)

    def __enter__(self):
        '''Dummy method for with'''
        return self

    def __exit__(self, type, value, traceback):
        """ Close and stop driver and display. This is particularly important in
        the event of a failure in 'get_chd'.
        """
        self.close()
        self.display.stop()
        logging.info("\tClosed and stopped driver and display")

    def find_and_click_link(self, text):
        '''
        '''
        all_texts = []
        for link in self.find_elements_by_tag_name('a'):
            all_texts.append(link.text)
            if link.text == text:
                url = link.get_attribute("href")
                if url is not None:
                    self.get(url)
                else:
                    link.click()
                    #time.sleep(self.load_time)
                return True
        return False

    #@retry(wait_exponential_multiplier=5000,stop_max_attempt_number=2,
    #       wrap_exception=True)
    def get(self, url):
        '''
        '''
        logging.debug("\tGoing to %s", url)
        result = super().get(url)
        # time.sleep(self.load_time)
        return result

    def wait_for_presence_by_id(self, name):
        '''
        '''
        logging.debug("\tWaiting for %s", name)
        by = (By.ID, name)
        condition = expected_conditions.presence_of_element_located(by)
        return WebDriverWait(self, self.load_time).until(condition)

    def get_pandas_table_by_id(self, table_id, thorough=True):
        '''
        '''
        if thorough:
            table = self.wait_for_presence_by_id(table_id)
        else:
            table = self.find_element_by_id(table_id)
        timer.stamp("Found table by id")
        self.get_pandas_table(table)

    def get_pandas_table(self, table):
        html = table.get_attribute('outerHTML')
        timer.stamp("Got table html")        
        df = pd.read_html(html, header=0)[0]
        timer.stamp("Converted to pandas")
        return table, df


if __name__ == "__main__":
    
    with SelfClosingBrowser() as driver:
        driver.get("http://www.pi.ac.ae/en")
        pass
