
from bs4 import BeautifulSoup

from utils.common.browser import SelfClosingBrowser
from utils.common.nlp import tokenize_alphanum
from utils.common.datapipeline import DataPipeline

def run(config):
    top_url = config["parameters"]["src"]
    ucas = set()
    with SelfClosingBrowser(top_url=top_url,load_time=10) as driver:
        while True:
            results = driver.find_elements_by_class_name("course-details")
            for r in results:
                words = [w for w in tokenize_alphanum(r.text)]
                ucas.add(" ".join(words))
            button = driver.find_element_by_class_name('pagination__link--next')
            disabled = button.get_attribute("disabled")
            if disabled is not None:
                break
            button.click()

    print("Got",len(ucas))
    # Write data
    with DataPipeline(config) as dp:
        for words in ucas:
            print("\t",words)
            dp.insert(dict(words=words))
            
