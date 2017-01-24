
import dask.bag as db
import glob

import json
from itertools import combinations
import re
import dateutil.parser
from nltk.tokenize import RegexpTokenizer


import os
import pickle

__location__ = os.path.realpath(
    os.path.join(os.getcwd(), os.path.dirname(__file__)))

sw_file = os.path.join(__location__, "stopwords.pickle")
tw_stopwords = pickle.load(open(sw_file, "rb"))


def get_message_cooccurences(json_message, hashtags_only=False):
    """
    Identifies word combinations in a single twitter message.
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

    # todo: make the hashtags_only parameter operable
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


def get_weighted_edgelist(tw_files, hashtags_only=False):
    """

    Parameters
    ----------
    tw_files: Absolute or relative filepath, globstring, or list of strings
        The raw twitter JSON files to be processed, either individually or in a list

    Returns
    -------
    df: dask dataframe
        weighted edgelist, columns ['Date', 'W1', 'W2', 'Count']

    """

    cooccurences = db.read_text(tw_files).map(get_message_cooccurences).concat()
    frequencies = cooccurences.frequencies()
    dicts = frequencies.map(
        lambda x: {'Date': int(x[0][0]),
                   'W1': str(x[0][1]),
                   'W2': str(x[0][2]),
                   'Count': int(x[1])})
    df = dicts.to_dataframe()
    return df


def build_weighted_edgelist_db(tw_file_globstring,
                               output_globstring,
                               hashtags_only=False):
    files = glob.glob(tw_file_globstring)  # 'tests/resources/testfile*.gz'
    weighted_edgelist = get_weighted_edgelist(files,
                                              hashtags_only=hashtags_only)

    weighted_edgelist.to_hdf(output_globstring, '/weighted_edge_list', dropna=True)  # 'working/testdb_*.hdf'
    return weighted_edgelist













