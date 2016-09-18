
import dask.bag as db

import json
from itertools import combinations
import re
import dateutil.parser
from nltk.tokenize import RegexpTokenizer
from nltk.corpus import stopwords
tw_stopwords = stopwords.words() + ['rt', '@']


def get_word_cooccurrences(json_message):
    """
    Identifies word combinations in a single message.
    Drops stopwords and punctuation.

    Parameters
    ----------
    json_message: basestring
        a single line from a raw twitter json file

    Returns
    -------
    sets: list of tuples
        each tuple contains the date and two words (in alphabetical order) of a pair

    """
    try:
        parsed_json = json.loads(json_message)
        text = parsed_json['text'].lower()

    except KeyError:  # There is no 'Text' in json body
        return []

    # remove hyperlinks
    text = re.sub(r'\w+:\/{2}[\d\w-]+(\.[\d\w-]+)*(?:(?:\/[^\s/]*))*', '', text)

    # tokenize, dropping punctuation
    tokenizer = RegexpTokenizer(r'\w+')
    tokens = tokenizer.tokenize(text)

    # drop stopwords
    tokens = filter(lambda x: x not in tw_stopwords, tokens)

    date = dateutil.parser.parse(parsed_json['created_at']).strftime("%Y%m%d")

    sets = [tuple([date] + sorted(pair)) for pair in combinations(tokens, 2)]

    return sets


def parse_tw_file(tw_files):
    """

    Parameters
    ----------
    tw_files: Absolute or relative filepath, globstring, or list of strings
        The raw twitter JSON files to be processed, either individually or in a list

    Returns
    -------

    """

    cooccurrences = db.read_text(tw_files).map(get_word_cooccurrences).concat()
    frequencies = cooccurrences.frequencies()
    dicts = frequencies.map(
        lambda x: {'Date': x[0][0], 'W1': x[0][1], 'W2': x[0][2], 'Count': x[1]})
    df = dicts.to_dataframe()














