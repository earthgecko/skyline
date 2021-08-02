"""
Assign a Graphite single tiemseries metric csv file to tsfresh to process and
calculate the features for.

:param path_to_your_graphite_csv: the full path and filename to your Graphite
    single metric timeseries file saved from a Graphite request with &format=csv
:type path_to_your_graphite_csv: str
:param pytz_tz: [OPTIONAL] defaults to UTC or pass as your pytz timezone string.
    For a list of all pytz timezone strings see
    http://earthgecko-skyline.readthedocs.io/en/latest/development/pytz.html
    and find yours.
:type string: str

Run the script with:

```
python tsfresh/scripts/tsfresh_graphite_csv.py path_to_your_graphite_csv [pytz_timezone]
```

Where path_to_your_graphite_csv.csv is a single metric timeseries that has been
from retrieved from Graphite with the &format=csv request parameter and saved to
a file.

The single metric timeseries could be the result of a graphite function on
multiple timeseries, as long as it is a single timeseries.  This does not handle
multiple timeseries data, meaning a Graphite csv with more than one data set
will not be suitable for this script.

This will output 2 files:

- path_to_your_graphite_csv.features.csv (default tsfresh column wise format)
- path_to_your_graphite_csv.features.transposed.csv (human friendly row wise
  format) you look at this csv :)

Your timeseries features.

.. warning:: Please note that if your timeseries are recorded in a daylight savings
    timezone, this has not been tested the DST changes.

"""

import os
import sys
import datetime
import csv

from termcolor import colored
import traceback

from timeit import default_timer as timer

from pytz import timezone
import pytz

import numpy as np
import pandas as pd

from tsfresh import extract_features, extract_relevant_features, select_features


def set_date(self, d):
    try:
        self.date = d.astimezone(pytz.utc)
    except:
        self.date = pytz.utc.localize(d)


if __name__ == '__main__':

    start = timer()

    # Read the Graphite csv file for a single timeseries that is passed
    fname_in = str(sys.argv[1])
    if os.path.isfile(fname_in):
        print(colored('notice: processing %s', 'cyan') % (fname_in))
        # @added 20210504  - Task #4030: refactoring
        file_path = os.path.dirname(fname_in)
        file_name = os.path.basename(fname_in)
        open_file = '%s/%s' % (file_path, file_name)
    else:
        print(colored('error: file not found - %s', 'red') % (fname_in))

    # If the user passes a pytz timezone string try and set the pytz timezone
    if sys.argv[2]:
        pytz_tz = str(sys.argv[2])
        try:
            test_tz = timezone(pytz_tz)
            print(colored('notice: the passed pytz_tz argument is OK - %s', 'cyan') % (pytz_tz))
        except:
            print(traceback.print_exc())
            print(colored('error: the passed pytz_tz argument is not a valid pytz timezone string - %s', 'red') % (pytz_tz))
            print(colored('notice: to find the valid pytz timezone string for your timezone see', 'cyan') % (pytz_tz))
            print(colored('notice: http://earthgecko-skyline.readthedocs.io/en/latest/development/pytz.html', 'cyan') % (pytz_tz))
    else:
        pytz_tz = 'UTC'

    local = pytz.timezone(pytz_tz)

    # Convert the Graphite csv %Y-%m-%d %H:%M:%S datetime to UTC epoch
    # timestamped csv
    print(colored('notice: converting the Graphite csv %Y-%m-%d %H:%M:%S datetime to UTC epoch', 'cyan'))

    # @modified 20210504  - Task #4030: refactoring
    # tmp_csv = '%s.tmp' % fname_in
    tmp_csv = '%s.tmp' % open_file

    if os.path.isfile(tmp_csv):
        os.remove(tmp_csv)

    # @modified 20210504  - Task #4030: refactoring
    # with open(fname_in, 'rb') as fr:
    with open(open_file, 'rb') as fr:
        reader = csv.reader(fr, delimiter=',')
        for i, line in enumerate(reader):
            metric = str(line[0])
            dt = str(line[1])
            value = str(line[2])

            naive = datetime.datetime.strptime(dt, '%Y-%m-%d %H:%M:%S')
            local_dt = local.localize(naive, is_dst=None)
            utc_dt = local_dt.astimezone(pytz.utc)
            timestamp = utc_dt.strftime('%s')
            utc_ts_line = '%s,%s,%s\n' % (metric, str(timestamp), value)
            with open(tmp_csv, 'a') as fh:
                fh.write(utc_ts_line)

    df = pd.read_csv(tmp_csv, delimiter=',', header=None, names=['metric', 'timestamp', 'value'])

    # if os.path.isfile(tmp_csv):
    #     os.remove(tmp_csv)

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
