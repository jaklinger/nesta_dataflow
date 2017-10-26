from utils.common.db_utils import read_all_results
from utils.common.datapipeline import DataPipeline
import wikipedia

def wiki_summary(q,retry=False):
    try:
        p = wikipedia.page(q)
    except wikipedia.exceptions.PageError as err:
        if retry:
            return None
        try_again = str(err).split('"')[1]
        return wiki_summary(try_again,retry=True)
    except wikipedia.exceptions.DisambiguationError as err:
        if retry:
            return None        
        for option in err.options:
            if "studies" in option.lower():
                return wiki_summary(option,retry=True)
        #print("Speculatively trying '",err.options[0],"' for '",q,"'")
        #return wiki_summary(err.options[0],retry=True)
        return None
    return p.summary

def split_courses(_courses,split):    
    courses = []
    for course in _courses:
        words = course.split()
        if len(words) > 3 and split in words:
            print(course.split(" "+split+" "))
            courses += course.split(" "+split+" ")
            continue
        courses.append(course)
    return courses

def everything_after(word,split):
    return split.join(word.split(split)[1:])

def run(config):

    # Read qualifications
    _results = read_all_results(config,"input_db","input_table")
    # Extract course names
    courses = list(course_name for _,_,_,course_name,_,_,_ in _results)
    print(len(courses))

    #    autocomp = AutoCompounder(max_context=8,beta=0.01)
    #    autocomp.process_sentences([everything_after(course," in ") for course in courses])
    #    autocomp.print_sorted_compounds()

    courses = [everything_after(course," of ") for course in courses]
    courses = [everything_after(course," in ") for course in courses]
    courses = [x for x in courses if x != '']
    courses = split_courses(courses,"and")
    courses = split_courses(courses,"with")        

    courses = set(courses)
    print(len(courses))
    summaries = []
    for course in courses:
        try:
            s = wiki_summary(course)            
            summaries.append(dict(course_name=course,summary=s))
        except wikipedia.exceptions.PageError as err:
            print(course,"--->",err)
        
    with DataPipeline(config) as dp:
        for row in summaries:
            dp.insert(row)
