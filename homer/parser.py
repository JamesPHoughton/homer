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


def fold_pairs(counter, json_string, languages, hashtags_only):
    parsed_json = ujson.loads(json_string)
    if 'lang' in parsed_json and 'text' in parsed_json and parsed_json['lang'] in languages:
        if not hashtags_only:
            text = parsed_json['text'].lower()

            # remove hyperlinks
            text = matcher.sub('', text)

            # tokenize, dropping punctuation
            tokens = tokenizer.tokenize(text)  # this should release GIL - be ok for parallelization

            # drop stopwords
            tokens = filter(lambda x: x not in tw_stopwords, tokens)

        else:
            if ('entities' in parsed_json and
                        'hashtags' in parsed_json['entities'] and
                        len(parsed_json['entities']['hashtags']) >= 2):
                tokens = [entry['text'].lower() for entry in
                          parsed_json['entities']['hashtags']]
            else:
                tokens = []

        date = int(dateutil.parser.parse(parsed_json['created_at']).strftime("%Y%m%d"))

        if date not in counter:
            counter[date] = collections.Counter()
        for pair in combinations(tokens, 2):
            counter[date][tuple(sorted(pair))] += 1

    return counter


def merge_folds(a, b, outupt_globstring):
    """
    For each input file, writes an output file for each date
    within that input file, separating dates from one another so
    they can be processed separately later on.
    """
    outfiles = []

    for fold in a, b:
        if isinstance(fold, dict):
            for date, c in fold.items():
                if len(c):
                    outfile_name = outupt_globstring.replace(
                        '*', str(date) + '_' + str(time.time()).replace('.', '_'))
                    pd.Series(c).to_csv(outfile_name)
                    outfiles.append(outfile_name)

        elif isinstance(fold, list):
            outfiles = outfiles + fold

    return outfiles


def process_raw_tw(tw_file_globstring,
                   output_globstring,
                   languages,
                   dates,
                   hashtags_only):
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
    counters = lines.fold(binop=lambda count, json: fold_pairs(count, json,
                                                               languages, hashtags_only),
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
        date_globstring = infiles_globstring.replace('*', str(date)+'*')
        input_filenames = glob.glob(date_globstring)
        output_filename = outfiles_globstring.replace('*', str(date))
        collector.append(lazy_handle_day(input_filenames,
                                         output_filename,
                                         threshold))
    with ProgressBar():
        output_filenames = dask.compute(*collector)

    return output_filenames


def build_weighted_edgelist_db(tw_file_globstring,
                               intermediate_files_globstring,
                               output_files_globstring,
                               languages,
                               dates,
                               threshold=1,
                               hashtags_only=False,
                               ):
    """

    Parameters
    ----------
    tw_file_globstring: globstring
        path and name to files
    intermediate_files_globstring: string
        csv file name with an astrisk
        '/parsing/posts_int_*.csv'
    output_files_globstring
        '/parsing/posts_*.csv'
    languages
    dates: list of integers 20150615 style
        this is to initialize the counters, so we don't have to infer dates (which can take time
        if there are a lot of them)
    threshold
    hashtags_only

    Returns
    -------

    Outputs
    -------
    Two progress bars show, one for creating the intermediate files,
    the other for compiling them

    """

    # Todo: should create intermediate and output directories if none exist

    intermediate_files = process_raw_tw(tw_file_globstring,
                                        intermediate_files_globstring,
                                        languages=languages,
                                        dates=dates,
                                        hashtags_only=hashtags_only)
    output_files = process_count_files(intermediate_files_globstring,
                                       output_files_globstring,
                                       dates,
                                       threshold)

    return output_files