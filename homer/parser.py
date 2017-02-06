import dask.bag as db
import glob

import ujson
from itertools import combinations
import re
import dateutil.parser
from nltk.tokenize import RegexpTokenizer

import os
import pickle
import pandas as pd

__location__ = os.path.realpath(
    os.path.join(os.getcwd(), os.path.dirname(__file__)))

sw_file = os.path.join(__location__, "stopwords.pickle")
tw_stopwords = pickle.load(open(sw_file, "rb"))

tokenizer = RegexpTokenizer(r'\w+')


def find_pairs(parsed_json):
    text = parsed_json['text'].lower()

    # remove hyperlinks
    text = re.sub(r'\w+:\/{2}[\d\w-]+(\.[\d\w-]+)*(?:(?:\/[^\s/]*))*', '', text)

    # tokenize, dropping punctuation
    tokens = tokenizer.tokenize(text)  # this should release GIL - be ok for parallelization

    # drop stopwords
    tokens = filter(lambda x: x not in tw_stopwords, tokens)

    date = dateutil.parser.parse(parsed_json['created_at']).strftime("%Y%m%d")

    sets = [tuple([date] + sorted(pair)) for pair in combinations(tokens, 2)]
    return sets


#
# def get_message_cooccurences(json_message, languages=[], hashtags_only=False):
#     """
#     Identifies word combinations in a single twitter message.
#     Drops stopwords and punctuation.
#
#     Parameters
#     ----------
#     json_message: basestring
#         a single line from a raw twitter json file
#
#     Returns
#     -------
#     sets: list of tuples
#         each tuple contains the date and two words (in alphabetical order) of a pair
#
#     """
#
#     # todo: make the hashtags_only parameter operable
#     try:
#         parsed_json = json.loads(json_message)
#         assert parsed_json['lang'] in languages
#         text = parsed_json['text'].lower()
#
#     except KeyError:  # There is no 'text', or no 'lang' in json body
#         return []
#
#     except AssertionError:  # Not in the target language
#         return []
#
#     # remove hyperlinks
#     text = re.sub(r'\w+:\/{2}[\d\w-]+(\.[\d\w-]+)*(?:(?:\/[^\s/]*))*', '', text)
#
#     # tokenize, dropping punctuation
#     tokenizer = RegexpTokenizer(r'\w+')
#     tokens = tokenizer.tokenize(text)
#
#     # drop stopwords
#     tokens = filter(lambda x: x not in tw_stopwords, tokens)
#
#     date = dateutil.parser.parse(parsed_json['created_at']).strftime("%Y%m%d")
#
#     sets = [tuple([date] + sorted(pair)) for pair in combinations(tokens, 2)]
#
#     return sets
#




def get_weighted_edgelist(tw_file_globstring,
                          languages,
                          save_threshold=3,
                          hashtags_only=False):
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
    messages = db.read_text(tw_file_globstring, compression='gzip').map(ujson.loads)

    def selector(msg):
        return 'lang' in msg and 'text' in msg and msg['lang'] in languages

    selection = messages.filter(selector)
    sets = selection.map(find_pairs).concat()
    frequencies = sets.frequencies()
    over_threshold = frequencies.filter(lambda x: x[1] >= save_threshold)
    expand = over_threshold.map(lambda x: (x[0][0], x[0][1], x[0][2], x[1]))

    template = pd.DataFrame([{'Date': 20170121, 'W1': 'Toad', 'W2': u'Bug', 'Count': 21}],
                            columns=['Date', 'W1', 'W2', 'Count'])

    df = expand.to_dataframe(template)

    return df


def build_weighted_edgelist_db(tw_file_globstring,
                               output_globstring,
                               hashtags_only=False,
                               languages=['en'],
                               save_threshold=3):
    weighted_edgelist = get_weighted_edgelist(tw_file_globstring,
                                              languages=languages,
                                              save_threshold=save_threshold,
                                              hashtags_only=hashtags_only)

    weighted_edgelist.to_hdf(output_globstring, '/weighted_edge_list', dropna=True)
    return weighted_edgelist
