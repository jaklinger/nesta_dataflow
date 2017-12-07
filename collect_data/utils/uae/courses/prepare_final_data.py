import pandas as pd
from utils.common.db_utils import read_all_results
from utils.common.nlp import tokenize_alphanum
from utils.common.datapipeline import DataPipeline
import re

_pattern = re.compile('[\W_]+')


def remove_dodgy_chars(text):
    return " ".join([_pattern.sub('', t)
                     for t in text.split()])


def remove_acronyms(text, ignore=[]):
    _text = []
    just_skipped = False
    for word in text.split():
        if word.lower() in ignore:
            just_skipped = False
            _text.append(word)
            continue
        n_caps = sum(char.isupper() for char in word)
        if n_caps > 1 and len(word) < 6:
            just_skipped = True
            continue
        elif just_skipped and len(word) < 4 and n_caps > 0:
            just_skipped = True
            continue
        just_skipped = False
        if word.lower() == "degree":
            continue
        _text.append(word)
    if len(_text) < 3:
        return None
    return " ".join(_text)


def level_checker(query, levels=["bachelor", "master",
                                 "phd", "diploma", "doctor"]):
    # Check if the qualification type is in the query
    for lvl in levels:
        if lvl in query:
            return lvl
    return None


def get_qual_type(query, qual_map):
    _q = tokenize_alphanum(query)
    lvl = level_checker(_q)
    if lvl is not None:
        return lvl
    # Otherwise, search for the acronym
    for word in _q:
        for qual, acronyms in qual_map.items():
            if word in acronyms:
                return level_checker(qual)
    return None


def assign_category(x, kws):
    for label, kw in kws:
        if any(k in x.lower() for k in kw):
            return label


def assign_category_split(x, kws):
    for label, kw in kws:
        for k in kw:
            if k is None:
                continue
            if all(w in x.lower() for w in k.split()):
                return label
    return "Other"


def run(config):

    # Read qualifications
    qual_map = {}
    _results = read_all_results(config, "quals_db", "input_table_quals")
    for std, abbrv in _results:
        if abbrv not in qual_map:
            qual_map[abbrv] = set()
        qual_map[abbrv].add(std)

    _results = read_all_results(config, "input_db", "input_table")
    output = []
    for top_url, found_on_url, go_to_url, text in _results:
        # Reject courses which don't contain these basic words
        # (they should since they've been standardised)
        text = remove_dodgy_chars(text)
        if not any(r in text.split()
                   for r in ["of", "in"]):
            continue
        if any(r in text.lower().split()
               for r in ["university", "college", "usa", "uk",
                         "canada", "france", "spain"]):
            continue
        if "school of" in text.lower():
            continue
        text = remove_acronyms(text, ignore=["phd", "it"])
        if text is None:
            continue
        if len(text.split()) > 11:
            continue
        # Try to get the qualification from the text
        qual = get_qual_type(text,qual_map)
        # Otherwise, try to get the qualification from the URL
        if qual is None and go_to_url is not None:
            qual = get_qual_type(go_to_url,qual_map)
        # Finally, if a qualification has been found, require
        # require the text starts with the qualification
        if qual is not None:
            if not text.lower().startswith(qual):
                qual = None
        if qual is None:
            continue
        row = dict(top_url=top_url,
                   found_on_url=found_on_url,
                   go_to_url=go_to_url,
                   course_name=text,
                   qualification=qual)
        output.append(row)

    df = pd.DataFrame(output)
    df = df.drop_duplicates(["go_to_url", "course_name"])

    # Broad category-keyword matching
    kws = list((("Education", ('education', 'teaching',)),
                ("Science", ('bio',)),
                ("Comp/IT", ('comp', ' it ', 'info',
                             'network', 'software', 'security')),
                ("Engineering", ('eng',)),
                ("Medical", ('health', 'medic', 'nursing', 'pharma',)),
                ("Business/Finance", ('business', 'manag', 'admin',
                                      'financ', 'commerce', 'account',)),
                ("Science", ('science',)),
                ("Other", ('',))))
    df["broad_category"] = df.course_name.apply(assign_category, kws=kws)

    kws = list((("Education", ('education', 'teaching',)),
                ("Water", ('water',)),
                ("Renewable and Clean Energy", ('energy', 'renewable',
                                                'climate', 'environ',)),
                ("Space", ('space', 'astro', 'aero',)),
                ("Technology", ('electric',)),
                ("Transportation", ('transport', 'mechanical',)),
                ("Technology", ('comp', ' it ', 'info', 'network',
                                'software', 'eng', 'security',)),
                ("Health", ('health', 'medic', 'nursing', 'pharma',)),
                ("Technology", ('tech',)),
                ("Other", ('',))))
    df["priority_sector"] = df.course_name.apply(assign_category, kws=kws)

    kws = list((("Education Innovation and Technology",
                 ("education policy", "education tech", "online education")),
                ("Aerospace Advanced Materials, Manufacturing, Maintenance and Testing",
                ("aerospace", )),
                ("Health Information Technology and Bioinformatics",
                 ("medical", "health technology")),
                ("Public Health, Non-Communicable Diseases and Wellness",
                 ("public health", "health admin", "health management")),
                ("Biotechnology and Genomics",
                 ("biotech", "biology", "biochemistry")),
                ("Water Management and Economics",
                 ("water", "agribusiness")),
                ("Solar and Alternative Energy Technology Systems",
                 (None,)),
                ("Space Sciences",
                 ("space",)),
                ("Cubesats and Nanosatellites",
                 (None,)),
                ("Financial Services Technology",
                 ("ebusiness",)),
                ("Cybersecurity",
                 ("cyber", "information technology", "system security",
                  "information security")),
                ("Internet of Things and Big Data",
                ("network",)),
                ("Semiconductor Process Development",
                 ("elec engineering", "physics")),
                ("Robotics and Artificial Intelligence",
                 (None,)),
                ("Smart City Applications and Solutions",
                 ("health safety", "sustainable infrastructure")),
                ("Architecture and Urban Design",
                 ("urban planning",
                  "logical design", "building design", "architect")),
                ("Arabic Digital Technology",
                (None,)),
                ("Petroleum Geosciences",
                 ("petrol",)),
                ("Additive Manufacturing (3D Printing)",
                ("material",)),
                ("Advanced Building and Construction Materials",
                ("civil engineering",)),
                ("Food Security",
                ("food science",)),
                ("Transportation Logistics, Analytics and Security",
                ("logistics",)),
                ("Commercial Unmanned Aerial Vehicles",
                ("aviation",)),
                ("Autonomous Vehicles",
                (None,))))

    df["focus_area"] = df.course_name.apply(assign_category_split, kws=kws)

    config["GoogleDrive"]["target-repo"] = "tier-2"
    with DataPipeline(config) as dp:
        for row in df.to_dict(orient="records"):
            dp.insert(row)
