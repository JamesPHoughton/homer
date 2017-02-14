import dask.bag as db
from dask.diagnostics import ProgressBar
from dask import delayed
import dask

import collections
import ujson
from itertools import combinations
import re
import dateutil.parser
from nltk.tokenize import RegexpTokenizer

import os
import pickle
import pandas as pd
import time
import glob


# Code for processing a single tw file into a corresponding weighted edge list


__location__ = os.path.realpath(
    os.path.join(os.getcwd(), os.path.dirname(__file__)))

sw_file = os.path.join(__location__, "stopwords.pickle")
tw_stopwords = pickle.load(open(sw_file, "rb"))

tokenizer = RegexpTokenizer(r'\w+')
matcher = re.compile(r'\w+:\/{2}[\d\w-]+(\.[\d\w-]+)*(?:(?:\/[^\s/]*))*')


def fold_pairs(counter, json_string, languages):
    parsed_json = ujson.loads(json_string)
    if 'lang' in parsed_json and 'text' in parsed_json and parsed_json['lang'] in languages:
        text = parsed_json['text'].lower()

        # remove hyperlinks
        text = matcher.sub('', text)

        # tokenize, dropping punctuation
        tokens = tokenizer.tokenize(text)  # this should release GIL - be ok for parallelization

        # drop stopwords
        tokens = filter(lambda x: x not in tw_stopwords, tokens)

        date = int(dateutil.parser.parse(parsed_json['created_at']).strftime("%Y%m%d"))

        if date not in counter:
            counter[date] = collections.Counter()
        for pair in combinations(tokens, 2):
            counter[date][tuple(sorted(pair))] += 1

    return counter


def merge_folds(a, b, outupt_globstring):
    outfiles = []

    for fold in a, b:
        if isinstance(fold, dict):
            for date, c in fold.items():
                if len(c):
                    outfile_name = outupt_globstring.replace(
                        '*', date + '_' + str(time.time()).replace('.', '_'))
                    pd.Series(c).to_csv(outfile_name)
                    outfiles.append(outfile_name)

        elif isinstance(fold, list):
            outfiles = outfiles + fold

    return outfiles


def process_raw_tw(tw_file_globstring,
                   output_globstring,
                   languages,
                   dates):
    """

    Parameters
    ----------
    tw_file_globstring
    output_globstring
    languages
    dates

    Returns
    -------
    outfiles: list of strings
        filenames of the intermediate files
        one file per input file.

    """
    lines = db.read_text(tw_file_globstring, compression='gzip', collection=True)

    initial_counter = {date: collections.Counter() for date in dates}
    counters = lines.fold(binop=lambda count, json: fold_pairs(count, json, languages),
                          combine=lambda a, b: merge_folds(a, b, output_globstring),
                          initial=initial_counter)

    with ProgressBar():
        outfiles = counters.compute()

    return outfiles


# code for combining multiple weighted edgelist files into a file for each day


def sum_files(files):
    collector = pd.DataFrame(columns=['W1', 'W2', 'Count']).set_index(['W1', 'W2'])
    for csv_file in files:
        df = pd.read_csv(csv_file, names=['W1', 'W2', 'Count']).set_index(['W1', 'W2'])
        collector = collector.add(df, fill_value=0)
    return collector.astype(int)


def handle_day(input_filenames, output_filename, threshold=1):

    totals = sum_files(input_filenames)
    keepers = totals[totals['Count'] >= threshold]
    keepers.to_csv(output_filename)
    return output_filename


lazy_handle_day = delayed(handle_day)


def process_count_files(infiles_globstring,
                        outfiles_globstring,
                        dates,
                        threshold=1,
                        ):
    collector = []
    for date in dates:
        date_globstring = infiles_globstring.replace('*', date+'*')
        input_filenames = glob.glob(date_globstring)
        output_filename = outfiles_globstring.replace('*', date)
        collector.append(lazy_handle_day(input_filenames,
                                         output_filename,
                                         threshold))
    with ProgressBar():
        output_filenames = dask.compute(collector)

    return output_filenames


def build_weighted_edgelist_db(tw_file_globstring,
                               intermediate_files_globstring,
                               output_files_globstring,
                               languages,
                               dates,
                               threshold=1,
                               hashtags_only=False,
                               ):

    intermediate_files = process_raw_tw(tw_file_globstring,
                                        intermediate_files_globstring,
                                        languages,
                                        dates)
    output_files = process_count_files(intermediate_files_globstring,
                                       output_files_globstring,
                                       dates,
                                       threshold)

    return output_files