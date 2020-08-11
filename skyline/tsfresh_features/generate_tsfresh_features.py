"""
This script determines what the known tsfresh feature names are and extracts
features from a small timeseries sample of utils/data.json

Then the known TSFRESH_FEATURES are compared to the feature names that were
determined from the feature extraction of the sample data set.  The script
reports changes and outputs a new TSFRESH_FEATURES list.

This is very important and Skyline is using the tsfresh feature names in a
relational manner and any changes in tsfresh names or additions must be
considered appropriately.  Further some tsfresh feature name strings contain
spaces and commas so they are not suitable to convert into variables without
string manipulation, therefore the TSFRESH_FEATURES array is declared and
once feature names are declared they should be treated as immutable objects due
to the relational nature of feature profiles.

Run the script with for example:

.. code-block:: bash

    PYTHON_MAJOR_VERSION="3.8"
    PYTHON_VIRTUALENV_DIR="/opt/python_virtualenv"
    PROJECT="skyline-py383"

    cd "${PYTHON_VIRTUALENV_DIR}/projects/${PROJECT}"
    source bin/activate
    bin/python${PYTHON_MAJOR_VERSION} skyline/tsfresh_features/generate_tsfresh_features.py
    deactivate

"""

import os
import sys
import datetime
import csv
from ast import literal_eval

import traceback
import json
from termcolor import colored
from timeit import default_timer as timer

import numpy as np
import pandas as pd

from tsfresh import extract_features, extract_relevant_features, select_features

current_dir = os.path.dirname(os.path.realpath(__file__))
parent_dir = os.path.join(os.path.dirname(os.path.realpath(current_dir)))
root_dir = os.path.join(os.path.dirname(os.path.realpath(parent_dir)))

skyline_dir = parent_dir
sys.path.append(skyline_dir)

import settings
import skyline_version
from tsfresh_feature_names import TSFRESH_FEATURES

skyline_version = skyline_version.__absolute_version__
# @added 20200808 - Bug #3666: Failing algorithm_tests on Python 3.8.3
python_version = int(sys.version_info[0])

if __name__ == '__main__':

    print('current_dir :: %s' % current_dir)
    print('parent_dir :: %s' % parent_dir)
    print('skyline_dir :: %s' % skyline_dir)
    print('root_dir :: %s' % root_dir)

    anomaly_json = '%s/utils/data.json' % root_dir

    if os.path.isfile(anomaly_json):
        with open(anomaly_json, 'r') as f:
            timeseries = json.loads(f.read())

    with open(anomaly_json, 'r') as f:
        timeseries_json = json.loads(f.read())

    # @modified 20200808 - Bug #3666: Failing algorithm_tests on Python 3.8.3
    # timeseries_str = str(timeseries_json).replace('{u\'results\': ', '').replace('}', '')
    if python_version == 2:
        timeseries_str = str(timeseries_json).replace('{u\'results\': ', '').replace('}', '')
    if python_version == 3:
        timeseries_str = str(timeseries_json).replace('{\'results\': ', '').replace('}', '')

    full_timeseries = literal_eval(timeseries_str)
    timeseries = full_timeseries[:60]

    fname_in = anomaly_json
    tmp_csv = '%s.tmp' % anomaly_json
    if os.path.isfile(tmp_csv):
        os.remove(tmp_csv)

    for ts, value in timeseries:
        metric = 'test'
        timestamp = int(ts)
        value = str(float(value))
        utc_ts_line = '%s,%s,%s\n' % (metric, str(timestamp), value)
        with open(tmp_csv, 'a') as fh:
            fh.write(utc_ts_line)
    # TO HERE

    start = timer()

    df = pd.read_csv(tmp_csv, delimiter=',', header=None, names=['metric', 'timestamp', 'value'])

    if os.path.isfile(tmp_csv):
        os.remove(tmp_csv)

    df.columns = ['metric', 'timestamp', 'value']
    start_feature_extraction = timer()
    try:
        df_features = extract_features(df, column_id='metric', column_sort='timestamp', column_kind=None, column_value=None)
    except:
        print(traceback.print_exc())
        print(colored('error: extracting features with tsfresh', 'red'))
        sys.exit(1)

    end_feature_extraction = timer()
    print(colored('notice: extracting features with tsfresh took %.6f seconds', 'cyan') % (end_feature_extraction - start_feature_extraction))

    # write to disk
    fname_out = fname_in + '.features.csv'
    df_features.to_csv(fname_out)

    # Transpose
    df_t = df_features.transpose()
    t_fname_out = fname_in + '.features.transposed.csv'
    df_t.to_csv(t_fname_out)

#    from __future__ import print_function
#    with open(fname_out, 'r') as f:
#        print(f.read(), end="")

    end = timer()

    print(colored('notice: features saved to %s', 'cyan') % (fname_out))
    print(colored('notice: transposed features saved to %s', 'cyan') % (t_fname_out))
    print(colored('notice: completed in %.6f seconds', 'cyan') % (end - start))

    feature_names = []
    count_id = 0
    # @modified 20200808 - Bug #3666: Failing algorithm_tests on Python 3.8.3
    # with open(t_fname_out, 'rb') as fr:
    with open(t_fname_out, 'rt') as fr:
        reader = csv.reader(fr, delimiter=',')
        for i, line in enumerate(reader):
            if str(line[0]) != '':
                if ',' in line[0]:
                    feature_name = '"%s"' % str(line[0])
                else:
                    feature_name = str(line[0])
                count_id += 1
                feature_names.append([count_id, feature_name])

    max_known_id = int(TSFRESH_FEATURES[-1][0])
    max_seen_id = int(feature_names[-1][0])

#    print(feature_names)
    if feature_names == TSFRESH_FEATURES:
        print(colored('notice: The generated feature names match TSFRESH_FEATURES', 'cyan'))
    else:
        print(colored('warning: The generated feature names do not match TSFRESH_FEATURES', 'yellow'))
        for nid, nname in feature_names:
            if int(nid) > max_known_id:
                print(colored('NEW ID AND NAME TS: %s fn: %s - %s' % (str(None), str(nid), str(nname)), 'yellow'))
            for id, name in TSFRESH_FEATURES:
                if int(id) == int(nid):
                    if str(name) != str(nname):
                        print('mismatch TS: %s fn: %s' % (str(name), str(nname)))

        print(colored('info: The newly generated feature names for TSFRESH_FEATURES list for skyline/tsfresh_feature_names.py is:', 'green'))
        print('TSFRESH_FEATURES = [')
        for nid, nname in feature_names:
            print('    [%s, \'%s\'],' % (str(nid), str(nname)))
        print(']')
