from bs4 import BeautifulSoup
from collections import Counter
import re
import requests
from fuzzywuzzy import fuzz
from nltk.corpus import stopwords
import wikipedia

from utils.common.nlp import tokenize_alphanum
from utils.common.datapipeline import DataPipeline

_stops = set(stopwords.words("english"))

'''Get all qualifications on a wikipedia page'''
def get_wiki_quals(url,tokenize=True):
    r = requests.get(url)
    soup = BeautifulSoup(r.text,"lxml")
    quals = []
    for a in soup.find_all("a"):
        if "href" not in a.attrs:
            continue
        href = a.attrs["href"]
        if not href.startswith("/wiki/"):
            continue
        text_elements = a.text.split()
        if len(text_elements) < 1:
            continue
        if text_elements[0].lower() not in href.lower():
            continue
        if tokenize:
            words = [w for w in tokenize_alphanum(a.text)
                     if w not in _stops]
        else:
            words = a.text.split()
        quals.append(" ".join(words))
    return set(quals)

'''Remove dodgy characters'''
def tidy(x,except_=""):
    return re.sub(r'[^a-zA-Z0-9'+except_+']','',x)

'''Guess the acronym for a '''
def acronym_guess(name,n):
    acronym = []
    for i,w in enumerate(name.split()):
        if w in _stops:
            continue
        if i == 0:
            acronym.append(w[0])
        else:
            acronym.append(w[0:n])
    acronym = ".".join(acronym)
    
    return acronym

'''Find the qualification associated with a given '''
def find_qualification(name):
    p = wikipedia.page(name)
    words = {}    
    # Consider two forms of acronym
    for i in [1,2]:
        # Generate acronym and remove dodgy characters
        acronym = acronym_guess(name,i)
        acronym = tidy(acronym)
        if len(acronym) == 0:
            continue
        # Iterate through words in the summary
        for word in p.summary.split():
            # Tidy the word
            _word = tidy(word)
            if _word in _stops:
                continue
            if len(_word) == 0:
                continue
            # Require that the first letter of the word to match
            # that of the acronym
            if _word[0] != acronym[0]:
                continue
            # Match on fuzziness * length_ratio
            score = fuzz.ratio(acronym,_word)
            if score == 0:
                continue
            length_ratio = len(acronym)/len(_word)
            if length_ratio > 1:
                length_ratio = 1/length_ratio
            score = score * length_ratio
            # Tidy again, allowing for full-stops
            # Then exclude if already found with a better score
            _w = tidy(word,except_=".")           
            if _w in words:
                if words[_w] > score:
                    continue
            # Record the word
            words[_w] = score
    # Generate the list of qualifications if the score is good
    output = []
    for qual,score in Counter(words).most_common():
        if score > 50:
            output.append(qual)
    return output


def run(config):
    # Get Wikipedia qualifications
    wiki = set()
    for src in config["parameters"]["src"].split(","):    
        _wiki = get_wiki_quals(src,False)
        wiki = wiki.symmetric_difference(_wiki)
        
    # Generate a mapping of Wikipedia qualifications
    qual_map = {}
    for w in wiki:
        if not any(w.startswith(x) for x in ("Ma","Ba","Dip","Doc","PhD")):
            continue
        if w.strip() == "":
            continue
        quals = find_qualification(w)
        for abbrv in quals:
            if abbrv not in qual_map:
                qual_map[abbrv] = set()
            qual_map[abbrv].add(w)

    # Finally, write to table
    with DataPipeline(config) as dp:
        for abbrv,stds in qual_map.items():
            for std in stds:
                row = dict(standard_name=std,
                           abbreviation=abbrv)
                dp.insert(row)
                
