import requests
from bs4 import BeautifulSoup

from utils.common.nlp import tokenize_alphanum
from utils.common.datapipeline import DataPipeline

def run(config):
    r = requests.get(config["parameters"]["src"])
    ucas = []
    soup = BeautifulSoup(r.text,"lxml")
    for a in soup.find_all("a"):
        if "href" not in a.attrs:
            continue
        if not a.attrs["href"].startswith("/subject/"):
            continue
        words = [w for w in tokenize_alphanum(a.text)]
        ucas.append(" ".join(words))

    # Write data
    logging.info("\tWriting to table")
    with DataPipeline(config) as dp:
        for words in ucas:            
            dp.insert(dict(words=words))
            
