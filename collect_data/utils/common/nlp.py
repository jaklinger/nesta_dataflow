import re
from nltk.stem import WordNetLemmatizer

lemmatizer = WordNetLemmatizer()

'''
Split words by tokens, including numbers as tokens. Useful for splitting URLs.
'''
def tokenize_alphanum(text):
    words = list(filter(('').__ne__, re.split('[^a-zA-Z]',text)))
    _words = []
    for w in words:
        w = w.lower()
        w = lemmatizer.lemmatize(w)
        _words.append(w)
    return _words
