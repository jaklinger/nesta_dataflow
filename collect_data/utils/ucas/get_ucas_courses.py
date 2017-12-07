import requests
from bs4 import BeautifulSoup
from urllib.parse import urlsplit
from urllib.parse import urlunsplit
from urllib.parse import urljoin
from utils.common.datapipeline import DataPipeline
import time


def tidy(text):
    text = text.strip()
    text = text.replace("\n", "")
    while "  " in text:
        text = text.replace("  ", " ")
    return text


def get_ucas_data(top_url, page_number):

    u = urlsplit(top_url)
    base_url = urlunsplit(list(u[0:2])+['', '', ''])
    row_class = "content-columns__column"
    btn_class = "button button--small js-searchResults-view"

    row_cols = {1: "course_qualification",
                2: "course_format",
                3: "course_location",
                4: "start_date"}
    title_cols = {"search-result__result-provider": "institute_name",
                  "search-result__result-title": "course_name"}

    output = []
    r = requests.get(top_url, params=dict(pageNumber=page_number))
    soup = BeautifulSoup(r.text, "html5lib")
    articles = soup.find_all('article', {"class": "article search-result"})
    for article in articles:
        # Get broad course info
        row = {}
        headers = article.find_all("h3")
        for h3 in headers:
            attr_name = h3.attrs["class"][1]
            row[title_cols[attr_name]] = tidy(h3.text)
        # Get individual course format info
        i = -1
        _row = None
        _output = []
        divs = article.find_all("div", {"class": row_class})
        for div in divs:
            i += 1
            if i not in row_cols:
                _row = row.copy()
                continue
            _row[row_cols[i]] = tidy(div.text)
            # Refresh the counter for embedded table
            if i == max(row_cols.keys()):
                i = -2
                _output.append(_row)
        # Get url to bonus information
        anchors = article.find_all("a", {"class": btn_class})
        for _row, a in zip(_output, anchors):
            _row["ucas_url"] = urljoin(base_url, a["href"])
            output.append(_row)

    # Enrich the output from the bonus UCAS url
    date_like = ['year', 'month']
    for i, row in enumerate(output):
        r = requests.get(row["ucas_url"])
        soup = BeautifulSoup(r.text, "html5lib")
        anchor = soup.find("a", {"id": "ProviderCourseUrl"})
        # Get course URL if exists
        if anchor is not None:
            row["provider_url"] = anchor["href"]
        # Get course duration
        paragraphs = soup.find_all("p", {"class": "impact impact--medium"})
        for p in paragraphs:
            if any(d in p.text for d in date_like) and p.text[0].isnumeric():
                row["duration"] = tidy(p.text)
                break
        # Get course location info
        address_span = soup.find("span", {"class": "adr"})
        address_info = {}
        if address_span is None:
            print(row)
        else:
            for span in address_span.find_all("span"):
                _name = span.attrs["class"][0]
                if _name not in address_info:
                    address_info[_name] = []
                address_info[_name].append(tidy(span.text))
            for k, v in address_info.items():
                row[k.replace("-", "_")] = ", ".join(address_info[k])                
        # Get unistats URL if it exists
        anchor = soup.find("a", {"id": "unistats-url"})
        if anchor is not None:
            row["unistats_url"] = anchor["href"]
        output[i] = row
    # Return
    return output


def run(config):

    # Scroll through pages until done
    top_url = config["parameters"]["src"]
    page = 1890
    while True:
        page += 1
        result = None
        while result is None:
            try:
                result = get_ucas_data(top_url, page)
            except Exception as err:
                print("Failed", page, "with", err)
                time.sleep(60)
                print("==> retrying")
        print(page, len(result))
        if len(result) == 0:
            break
        output = result
        print("-->", len(output))
        # Write data
        with DataPipeline(config, write_google=False) as dp:
            for row in output:
                dp.insert(row)

    with DataPipeline(config, write_google=True) as dp:
        pass
