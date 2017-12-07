import requests
from bs4 import BeautifulSoup
from bs4.element import Comment
import re
from utils.common.nlp import tokenize_alphanum
from utils.common.datapipeline import DataPipeline
from utils.common.browser import SelfClosingBrowser
from utils.common.db_utils import read_all_results

_pattern = re.compile('[\W_]+')


def split_and_strip(text):
    return [_pattern.sub('', t.lower())
            for t in text.split()]


def tag_visible(element):
    if element.parent.name in ['style', 'script', 'head',
                               'title', 'meta', '[document]']:
        return False
    if isinstance(element, Comment):
        return False
    return True


def len_sentence(x):
    try:
        return len(tokenize_alphanum(x))
    except TypeError as err:
        print("Error with", x)
        raise err


def get_sentences(url, return_url=False, selenium=False):
    html = ''
    if selenium:
        with SelfClosingBrowser() as driver:
            driver.get(url)
            html = driver.page_source
    else:
        r = requests.get(url)
        if r.status_code != 200:
            print(url, "not found")
            return []
        html = r.text
    soup = BeautifulSoup(html, "lxml")
    texts = soup.findAll(text=True)
    visible_texts = filter(tag_visible, texts)
    if not return_url:
        return [t.strip() for t in visible_texts
                if len_sentence(t) > 0]

    # Otherwise, find corresponding URL
    sentences = []
    anchors = [a for a in soup.findAll("a")
               if tag_visible(a) and
               len_sentence(a.text) > 0]
    anchor_text = [a.text for a in anchors]
    for t in visible_texts:        
        if len_sentence(t) == 0:
            continue
        _url = ''
        if t in anchor_text:
            a = list(filter(lambda a: t == a.text, anchors))[0]
            if "href" in a.attrs:
                _url = a.attrs["href"]
        sentences.append(dict(go_to_url=_url, text=t, found_on_url=url))
    return sentences


def clean_text(t, qual_map):
    # Remove strings containing numbers
    if any(char.isdigit() for char in t):
        return None
    # Replace bad chars
    bad_chars = ["\n", "\r"]
    for c in bad_chars:
        t = t.replace(c, "")
    # Strip space
    while "  " in t:
        t = t.replace("  ", " ")
    t = t.lstrip()
    t = t.rstrip()
    words = t.split()
    # Remove long words
    if len(words) > 11:
        return None
    # Standardise qualifications
    for w in t.split():
        if w in qual_map:
            t = t.replace(w, min(qual_map[w], key=len))
    # Program is a misleading word, remove it
    if "program" in t.lower():
        return None
    # Require the standardised string to contain one of these
    if not any(x in split_and_strip(t)
               for x in ["bachelor", "master", "phd", "doctor", "diploma"]):
        return None
    return t


def flush(url_courses, config):
    n_flush = 0
    with DataPipeline(config) as dp:
        for top_url, rows in url_courses.items():
            for row in rows:
                n_flush += 1
                row["top_url"] = top_url
                dp.insert(row)
    print("flushed", n_flush)


def run(config):
    # Read qualifications
    qual_map = {}
    _results = read_all_results(config, "input_db", "input_table")
    for std, abbrv in _results:
        if abbrv not in qual_map:
            qual_map[abbrv] = set()
        qual_map[abbrv].add(std)

    # Read urls which have already been done
    _results = read_all_results(config, "output_db", "table_name")
    already_done_urls = set(url for _, url, _, _ in _results)
    print("Already found", len(already_done_urls), "previous urls")

    # Read urls
    _results = read_all_results(config, "input_db", "input_table_urls")
    kws = ["program", "graduate", "admission", "phd", "ma", "ba", "bsc", "msc",
           "dip", " doc"]
    _kws = ["tel:", "news", "mailto", "/events", "calendar", "jobs.", "upload",
            ".pdf", ".jpg", "bulletin", "/email", "/tel/"]
    urls_to_try = {}
    n_skip = 0
    n_todo = 0
    for top_url, url, _ in _results:
        if not any(kw in url.lower() for kw in kws):
            continue
        if any(kw in url.lower() for kw in _kws):
            continue
        if url in already_done_urls:
            n_skip += 1
            continue
        if top_url not in urls_to_try:
            urls_to_try[top_url] = []
        n_todo += 1
        urls_to_try[top_url].append(url)

    print("Skipping", n_skip, ", doing", n_todo)
    # Read UCAS courses
    ucas_results = read_all_results(config, "input_db", "input_table_ucas")
    ucas_courses = set(x[0] for x in ucas_results)

    # Filter out long courses
    ucas_courses = list(filter(lambda x: len(x.split()) < 6, ucas_courses))

    # Check which URLs require selenium
    selenium_urls = {url: wait_for for url, wait_for in
                     read_all_results(config, "external_db",
                                      "input_table_selenium")}
    # Only add if not already found
    url_courses = {}
    flush_lim = 100
    iflush = 0
    for top_url, urls in urls_to_try.items():
        print(top_url, len(urls))
        selenium = False
        if top_url in selenium_urls:
            print("===> Using selenium")
            selenium = True
        url_courses[top_url] = []
        for url in set(urls):
            # Generate results
            n_results = 0
            _results = get_sentences(url, return_url=True, selenium=selenium)

            results = []
            unique_texts = set()
            for data in _results:
                data["text"] = clean_text(data["text"], qual_map)
                if data["text"] is None:
                    continue
                if data["text"] not in unique_texts:
                    unique_texts.add(data["text"])
                    results.append(data)

            for data in results:
                url_courses[top_url].append(data)
                n_results += 1
            if n_results == 0:
                data = dict(text="", go_to_url="", found_on_url=url)
                url_courses[top_url].append(data)
            iflush += n_results
            # Flush if required
            if iflush >= flush_lim:
                iflush = 0
                flush(url_courses, config)
                for k, _ in url_courses.items():
                    url_courses[k] = []

    # Final flush
    flush(url_courses, config)
