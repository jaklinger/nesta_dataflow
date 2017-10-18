from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import NoSuchElementException

import json
import os
import sys
from fuzzywuzzy import fuzz
import pandas as pd
from copy import deepcopy
import boto3

s3 = boto3.resource('s3')
os.environ["LD_LIBRARY_PATH"] = os.getcwd()

# Preprocessed stopwords 
stops = ['Limited', 'Ltd', 'of', 'and', 'University', 'Research', 'Institute', 'for', '&', 'Centre', 'UK', 'The', 'Hospital', 'Technology', 'National', 'Group', 'Health', 'School', 'Trust', 'Sciences', 'Systems', 'International', 'Science', 'Technologies', 'Foundation', 'Engineering', 'Medical', 'Council', 'Services', 'Energy', 'Association', 'Ltd.', 'Solutions', 'NHS', 'Sch', '(UK)', 'the', 'Company', 'College', 'Design', 'Society', 'Social', 'Museum', 'Development', 'British', 'Studies', 'Arts', 'Plc', 'Royal', 'Center', '-', 'Office', 'Inst', 'de', 'Management', 'London', 'in', 'Medicine', 'Network', 'European', 'Power', 'City', 'Cancer', 'Faculty', 'Education', 'Europe', 'Inc', 'Partnership', 'Department', 'History', 'Art', 'North', 'Corporation', 'Care', 'Innovation', 'Business', 'West', 'Consulting', 'South', 'Community', 'Global', 'Media', 'Environmental', 'Water', 'Laboratory', 'Associates', 'Scottish', 'Advanced', 'Scotland', 'Agency', 'East', 'Clinical', 'GmbH', 'Public', 'Service', 'Applied', 'Academy', 'Cambridge', 'New', 'Marine', 'Oxford', 'Products', 'Scientific', 'Physics', 'Environment', 'Software', 'State', 'Government', 'Central', 'Healthcare', 'Life', 'Materials', 'Housing', 'Digital', 'Unit', 'Molecular', 'plc', 'Information', 'Ministry', 'St', 'Instruments', 'on', 'General', 'Natural', 'Theatre', 'MRC', 'Biology', 'Tech', 'Project', 'Human', 'Food', 'Music', 'Industries', 'Alliance', 'Hospitals', 'Policy', 'Division', 'Electronics', 'China', 'Northern', 'Space', 'Creative', 'Res', 'Consortium', 'Computer', 'Technical', 'Agricultural', 'World', 'United', 'Manchester', 'Wales', 'Data', 'Carbon', 'Resources', 'Co', 'Control', 'Sci', 'Consultants', 'Library', 'Study', 'Industrial', 'County', 'Holdings', 'Authority', 'Economics', 'Law', 'Ireland', 'Conservation', 'Computing', 'English', 'Inc.', 'Regional', 'Biomedical', 'Forum', 'Manufacturing', 'Biological', 'Psychology', 'Oncology', 'Llp', 'Ctr', 'Humanities', 'Innovations', 'Consultancy', 'Construction', 'England', 'at', 'Primary', 'Transport', 'Green', 'Aerospace', 'Partners', 'High', 'Yorkshire', 'Federal', 'Action', 'Cultural', 'Enterprise', 'Learning', 'Genetics', 'Architects', 'Film', 'Communications', 'Diagnostics', 'Chemistry', 'Earth', 'Therapeutics', 'Gallery', 'Climate', 'Borough', 'Heritage', 'Laboratories', 'Festival', 'Bristol', 'Cell', 'Veterinary', 'Support', 'Foods', 'Animal', 'Sustainable', 'Heart', 'Park', 'Architecture', 'Pharmaceuticals', 'AG', 'Local', 'Building', 'UNLISTED', 'Mathematics', 'Board', 'Nuclear', 'Agriculture', 'Security', 'House', 'Plant', 'Finance', 'Birmingham', 'Precision', 'African', 'York', 'Chemical', 'Leeds', 'Photonics', 'Commission', 'Languages', 'Ocean', 'Industry', 'San']

def remove_stops(word):
    _words = word.split()
    for stop in stops:
        if stop in _words:
            _words.remove(stop)
    # Only return if not empty
    if len(_words) > 0:
        return " ".join(_words)
    #logging.debug("\t%s is empty after stop removal, "
    #              "just using the whole word instead",word)
    return word

def comb_score(a,b):
    if pd.isnull(a):
        return 0
    if pd.isnull(b):
        return 0
    # Remove stops and residual spaces
    a = remove_stops(a)
    b = remove_stops(b)
    
    score_1 = fuzz.token_sort_ratio(a,b)
    score_2 = fuzz.partial_ratio(a,b)
    return math.sqrt(score_1*score_1 + score_2*score_2)/math.sqrt(2)

''''''
def get_project_info(b,project,org_url,org_name):

    # Dummy data container
    _data = dict(score=0,project_name=project["text"],
                 project_url=project["url"])
    
    # 
    required_fields = ['Organisation','Sector','Country']
    bonus_fields = ["Department"]
    
    # Go to the project info page
    b.get(project["url"])
    
    # Iterate through tables on the page in order to find
    # the relevant table (fuzzy match on name)
    try:
        outcomes = b.find_element_by_css_selector("#tabOutcomesCol")
        all_tables = outcomes.find_elements_by_class_name("table")
    except NoSuchElementException:
        return _data
    
    for table in all_tables:
        html = table.get_attribute('outerHTML')
        _df = pd.read_html(html,header=0)[0]
        
        # Rotate the tables around to make columns of fields
        _df = _df.T.reset_index()
        _df.columns = _df.iloc[0].values
        _df = _df.drop(0)
        
        # This field must exist, but needn't be collected
        if "Description" not in _df.columns:
            continue
        # These fields must exists, and will be collects
        if not all(field in _df.columns
                   for field in required_fields):
            continue
        # Additionally append any bonus fields to the requireds
        _required_fields = deepcopy(required_fields)
        for field in bonus_fields:
            if field in _df.columns:
                _required_fields.append(field)
                
        # Extract the specified columns of data
        records = _df[_required_fields].to_dict('records')
        if len(records) > 1:
            raise RuntimeError("More than one record found.")
        
        # Combine fuzzy scores (convert to float to help sqlalchemy)
        score = comb_score(org_name,records[0]["Organisation"])
        score = float(score)
        #print(org_name,table_id,score)
        if score > _data["score"]:
            _data["score"] = score            
            for k,v in records[0].items():
                _data[k] = v
    return _data    

def run(event,context):
    sys.path.append(os.getcwd())

    print("starting driver")
    driver = webdriver.PhantomJS(os.path.join(os.getcwd(),'phantomjs'),
                                 service_log_path='/tmp/ghostdriver.log',
                                 service_args=['--ssl-protocol=any'])
    driver.implicitly_wait(10)

    # Replace these with inputs
    #org_name = "Melbourne School of Health Sciences"
    #org_id = "123"
    #org_url = "http://gtr.rcuk.ac.uk/organisation/C4145C68-58B8-441E-86DD-A4079B65B478"

    data = []
    for line in open('input.tsv'):
        org_name,org_id,org_url = line.replace("\n","").split("\t")
        print('name',repr(org_name))
        print('id',repr(org_id))
        print('url',repr(org_url))
        fetch100 = "?term=&selectedFacets=&fields=&type=&page=1&selectedSortableField=score&selectedSortOrder=DESC&fetchSize=100"

        # Fetch top 100 results (never expect more than 100)
        print("getting top url")
        driver.get(org_url+fetch100)

        # Get all project links
        print("Going to project link")    
        projects  = driver.find_elements_by_css_selector("a[id^=resultProjectLink]")    
        project_texts = [dict(url=p.get_attribute("href"),text=p.text)
                         for p in projects]
        print("got",len(projects),"projects")

        # Generate results
        results=[get_project_info(driver,project,org_url,org_name)
                 for project in project_texts]

        # Get data ready for storage
        output = dict(org_name=org_name,org_id=org_id,
                      org_url=org_url,results=[])
        for result in results:
            if result["score"] < 50:
                continue        
            output["results"].append(result)
        print("Got",len(output["results"]),"results")

        data.append(output)

    # Finished with the driver
    driver.quit() 

    # MAYBE TEST A BASIC LAMBDA JOB FOR THIS BIT
    # Copy to S3 bucket
    bucket = s3.Bucket('tier-0')
    with open(org_id+".json",'w') as f:
        json.dump(data,f,sort_keys=True,indent=4)
        bucket.upload_fileobj(data,"gtr_"+org_id)

if __name__ == "__main__":
    run(None,None)
