####    20210814.POC.task4236.test.m66.with.TCPDBench.ipynb.py    ####
#
# @author Gary Wilson (@earthgecko)
# @created 20210814 - Task #4236: Test m66 with TCPDBench
# @license MIT
# @source mc11:/home/gary/Documents/daily_files/20210814.POC.task4236.test.m66.with.TCPDBench.ipynb.py
# @local mc11:/opt/python_virtualenv/projects/TCPDBench-py386/20210814.POC.task4236.test.m66.with.TCPDBench.ipynb
# @jupyter - http://mc11:8888/notebooks/20210814.POC.task4236.test.m66.with.TCPDBench.ipynb
get_ipython().run_line_magic('load_ext', 'autoreload')
get_ipython().run_line_magic('autoreload', '2')
import sys
from time import time
from timeit import default_timer as timer
sys.path.insert(0, '/opt/python_virtualenv/projects/TCPDBench-py386/TCPDBench/execs/python')
from cpdbench_utils import load_dataset, exit_success


# In[6]:


# Let us look at this data
# data is the raw dataset dictionary, mat is a T x d matrix of observations
input_file = '/opt/python_virtualenv/projects/TCPDBench-py386/TCPDBench/datasets/apple/apple.json'
data, mat = load_dataset(input_file)


# In[7]:


data


# In[8]:


mat


# In[16]:


# Let us look at this data
# data is the raw dataset dictionary, mat is a T x d matrix of observations
input_file = '/opt/python_virtualenv/projects/TCPDBench-py386/TCPDBench/datasets/bitcoin/bitcoin.json'
data, mat = load_dataset(input_file)
data


# In[17]:


mat


# In[13]:


import numpy as np
mat = np.zeros((data["n_obs"], data["n_dim"]))
mat


# In[14]:


for j, series in enumerate(data["series"]):
    mat[:, j] = series["raw"]
mat


# In[15]:


# We normalize to avoid numerical errors.
mat = (mat - np.nanmean(mat)) / np.sqrt(np.nanvar(mat))
mat


# In[18]:


# Do not normalise, Min-Max scale
test_mat = np.zeros((data["n_obs"], data["n_dim"]))
for j, series in enumerate(data["series"]):
    test_mat[:, j] = series["raw"]
x_np = test_mat
test_data = (x_np - x_np.min()) / (x_np.max() - x_np.min())
test_data




DATADIR = '/opt/python_virtualenv/projects/TCPDBench-py386/TCPDBench/datasets'
DATASETS = [
    "apple",
    "bank",
    "bee_waggle_6",
    "bitcoin",
    "brent_spot",
    "businv",
    "centralia",
    "children_per_woman",
    "co2_canada",
    "construction",
    "debt_ireland",
    "gdp_argentina",
    "gdp_croatia",
    "gdp_iran",
    "gdp_japan",
    "global_co2",
    "homeruns",
    "iceland_tourism",
    "jfk_passengers",
    "lga_passengers",
    "measles",
    "nile",
    "occupancy",
    "ozone",
    "quality_control_1",
    "quality_control_2",
    "quality_control_3",
    "quality_control_4",
    "quality_control_5",
    "rail_lines",
    "ratner_stock",
    "robocalls",
    "run_log",
    "scanline_126007",
    "scanline_42049",
    "seatbelts",
    "shanghai_license",
    "uk_coal_employ",
    "unemployment_nl",
    "usd_isk",
    "us_population",
    "well_log",
]
DATASET_FILES = []
for dataset in DATASETS:
    json_file = '%s/%s/%s.json' % (DATADIR, dataset, dataset)
    DATASET_FILES.append(json_file)

import bottleneck as bn
import pandas as pd
from adtk.data import validate_series
from adtk.visualization import plot
current_pid = 1

from datetime import datetime
time_format = '%Y-%m-%d'
time_string = '2018-04-20'
datetime_object = datetime.strptime(time_string, time_format)
datetime_object

timestamp = int(datetime.strptime(time_string, time_format).timestamp())
timestamp

def strictly_increasing_monotonicity(timeseries):
    """
    This function is used to determine whether timeseries is strictly increasing
    monotonically, it will only return True if the values are strictly
    increasing, an incrementing count.
    """

    import numpy as np

    test_ts = []
    for timestamp, datapoint in timeseries:
        # This only identifies and handles positive, strictly increasing
        # monotonic timeseries
        if datapoint < 0.0:
            return False
        test_ts.append(datapoint)

    # Exclude timeseries that are all the same value, these are not increasing
    if len(set(test_ts)) == 1:
        return False

    # Exclude timeseries that sum to 0, these are not increasing
    ts_sum = sum(test_ts[1:])
    if ts_sum == 0:
        return False

    diff_ts = np.asarray(test_ts)
    return np.all(np.diff(diff_ts) >= 0)


def nonNegativeDerivative(timeseries):
    """
    This function is used to convert an integral or incrementing count to a
    derivative by calculating the delta between subsequent datapoints.  The
    function ignores datapoints that trend down and is useful for metrics that
    increase over time and then reset.
    This based on part of the Graphite render function nonNegativeDerivative at:
    https://github.com/graphite-project/graphite-web/blob/1e5cf9f659f5d4cc0fa53127f756a1916e62eb47/webapp/graphite/render/functions.py#L1627
    """

    derivative_timeseries = []
    prev = None

    for timestamp, datapoint in timeseries:
        if None in (prev, datapoint):
            # derivative_timeseries.append((timestamp, None))
            prev = datapoint
            continue

        diff = datapoint - prev
        if diff >= 0:
            derivative_timeseries.append((timestamp, diff))
        # else:
        #    derivative_timeseries.append((timestamp, None))

        prev = datapoint

    return derivative_timeseries


import os
from adtk.visualization import plot
nth_median = 6
n_sigma = 3
window = 2
shift_to_start_of_window = True

for json_file in DATASET_FILES:
    data = None
    mat = None
    try:
        data, mat = load_dataset(json_file)
    except Exception as err:
        print('error :: could not load json_file: %s - %s' % (json_file, err))
        continue

    file_name = os.path.basename(json_file)
    base_name = file_name.split('.')[0]
    if base_name == 'measles':
        continue

    try:
        time_format = data['time']['format']
    except KeyError:
        time_format = None
    except Exception as err:
        print(json_file, err)
        break

    try:
        time_strings = data['time']['raw']
    except KeyError:
        time_strings = data['time']['index']
    except Exception as err:
        print(json_file, err)
        break

    timestamps = []
    for time_string in time_strings:
        if time_format is not None:
            try:
                timestamp = int(datetime.strptime(time_string, time_format).timestamp())
                timestamps.append(timestamp)
            except Exception as err:
                print(json_file, err)
                break
        else:
            timestamps.append(int(time_string))

    if len(data["series"]) > 1:
        print('%s has more than one series %s' % (base_name, str(len(data["series"]))))

    timeseries = []
    for index, timestamp in enumerate(timestamps):
        value = data["series"][0]["raw"][index]
        if value is None:
            value = np.nan
        timeseries.append([timestamp, value])

    if strictly_increasing_monotonicity(timeseries):
        timeseries = nonNegativeDerivative(timeseries)
        print('monotonic', base_name)

    try:
        x_np = np.array([value for ts, value in timeseries])
        # m66 does not normalize, it uses Min-Max scaling so recreate mat
        mat = (x_np - x_np.min()) / (x_np.max() - x_np.min())
    except Exception as err:
        print(json_file, err)
        break

    try:
        # m66 - calculate to nth_median
        median_count = 0
        while median_count < nth_median:
            median_count += 1
            rolling_median_s = bn.move_median(mat, window=window)
            median = rolling_median_s.tolist()
            mat = median
            if median_count == nth_median:
                break
        # m66 - calculate the moving standard deviation for the
        # nth_median array
        rolling_std_s = bn.move_std(mat, window=window)
        std_nth_median_array = np.nan_to_num(rolling_std_s, copy=False, nan=0.0, posinf=None, neginf=None)
        std_nth_median = std_nth_median_array.tolist()
        # m66 - calculate the standard deviation for the entire nth_median
        # array
        metric_stddev = np.std(std_nth_median)
        std_nth_median_n_sigma = []
        anomalies_found = False
        for value in std_nth_median:
            # m66 - if the value in the 6th median array is > six-sigma of
            # the metric_stddev the datapoint is anomalous
            if value > (metric_stddev * n_sigma):
                std_nth_median_n_sigma.append(1)
                anomalies_found = True
            else:
                std_nth_median_n_sigma.append(0)
    except Exception as err:
        print(err)

    anomalies = []
    # m66 - only label anomalous if the n_sigma triggers are persisted
    # for (window / 2)
    current_triggers = []
    for index, item in enumerate(timeseries):
        if std_nth_median_n_sigma[index] == 1:
            current_triggers.append(index)
        else:
            if len(current_triggers) > int(window / 2):
                for trigger_index in current_triggers:
                    # Shift the anomaly back to the beginning of the
                    # window
                    if shift_to_start_of_window:
                        anomalies.append(timeseries[(trigger_index - (window * int((nth_median / 2))))])
                    else:
                        anomalies.append(timeseries[trigger_index])
            current_triggers = []
    # Process any remaining current_triggers
    if len(current_triggers) > int(window / 2):
        for trigger_index in current_triggers:
            # Shift the anomaly back to the beginning of the
            # window
            if shift_to_start_of_window:
                anomalies.append(timeseries[(trigger_index - (window * int((nth_median / 2))))])
            else:
                anomalies.append(timeseries[trigger_index])

    try:
        df = pd.DataFrame(timeseries, columns=['date', 'value'])
        df['date'] = pd.to_datetime(df['date'], unit='s')
    except Exception as err:
        print(json_file, err)
    datetime_index = pd.DatetimeIndex(df['date'].values)
    df = df.set_index(datetime_index)
    df.drop('date', axis=1, inplace=True)
    s = validate_series(df)

    anomalies_data = []
    anomaly_timestamps = [int(item[0]) for item in anomalies]
    for item in timeseries:
        if int(item[0]) in anomaly_timestamps:
            anomalies_data.append(1)
        else:
            anomalies_data.append(0)
    df['anomalies'] = anomalies_data
    plot(s, anomaly=df['anomalies'], anomaly_color='red', title=base_name)
#    break



import json

filename = '/opt/python_virtualenv/projects/TCPDBench-py386/TCPDBench/analysis/annotations/annotations.json'
with open(filename, "r") as fp:
    annotations = json.load(fp)
annotations

anno_sample = {
    'apple': {'6': [319],
        '7': [319],
        '8': [319],
        '9': [53, 90, 197, 276, 319, 403, 463, 535],
        '10': [319]},
    'bank': {'6': [], '7': [], '8': [], '10': [], '12': []},
    'bee_waggle_6': {'6': [], '7': [], '8': [], '9': [182, 246], '12': []},
}

# @added 20210815
# Could run as service to do all those abed jobs

annotators = {}
for base_name in list(annotations.keys()):
    for annotator in list(annotations[base_name].keys()):
        if annotator not in list(annotators.keys()):
            annotators[annotator] = {}
        annotators[annotator][base_name] = len(annotations[base_name][annotator])
annotators_list = list(annotators.keys())
annotators_list
['6', '7', '8', '9', '10', '12', '13', '14']


import os
import numpy as np
from adtk.visualization import plot
nth_median = 6
n_sigma = 3
window = 2
#shift_to_start_of_window = True
shift_to_start_of_window = False

for json_file in DATASET_FILES:
    data = None
    mat = None
    try:
        data, mat = load_dataset(json_file)
    except Exception as err:
        print('error :: could not load json_file: %s - %s' % (json_fil, err))
        continue

    file_name = os.path.basename(json_file)
    base_name = file_name.split('.')[0]
    if base_name == 'measles':
        continue

    try:
        time_format = data['time']['format']
    except KeyError:
        time_format = None
    except Exception as err:
        print(json_file, err)
        break

    try:
        time_strings = data['time']['raw']
    except KeyError:
        time_strings = data['time']['index']
    except Exception as err:
        print(json_file, err)
        break

    timestamps = []
    for time_string in time_strings:
        if time_format is not None:
            try:
                timestamp = int(datetime.strptime(time_string, time_format).timestamp())
                timestamps.append(timestamp)
            except Exception as err:
                print(json_file, err)
                break
        else:
            timestamps.append(int(time_string))

    if len(data["series"]) > 1:
        print('%s has more than one series %s' % (base_name, str(len(data["series"]))))

    timeseries = []
    for index, timestamp in enumerate(timestamps):
        value = data["series"][0]["raw"][index]
        if value is None:
            value = np.nan
        timeseries.append([timestamp, value])

    if strictly_increasing_monotonicity(timeseries):
        timeseries = nonNegativeDerivative(timeseries)
        print('monotonic', base_name)

    try:
        x_np = np.array([value for ts, value in timeseries])
        # m66 does not normalize, it uses Min-Max scaling so recreate mat
        mat = (x_np - x_np.min()) / (x_np.max() - x_np.min())
    except Exception as err:
        print(json_file, err)
        break

    try:
        # m66 - calculate to nth_median
        median_count = 0
        while median_count < nth_median:
            median_count += 1
            rolling_median_s = bn.move_median(mat, window=window)
            median = rolling_median_s.tolist()
            mat = median
            if median_count == nth_median:
                break
        # m66 - calculate the moving standard deviation for the
        # nth_median array
        rolling_std_s = bn.move_std(mat, window=window)
        std_nth_median_array = np.nan_to_num(rolling_std_s, copy=False, nan=0.0, posinf=None, neginf=None)
        std_nth_median = std_nth_median_array.tolist()
        # m66 - calculate the standard deviation for the entire nth_median
        # array
        metric_stddev = np.std(std_nth_median)
        std_nth_median_n_sigma = []
        anomalies_found = False
        for value in std_nth_median:
            # m66 - if the value in the 6th median array is > six-sigma of
            # the metric_stddev the datapoint is anomalous
            if value > (metric_stddev * n_sigma):
                std_nth_median_n_sigma.append(1)
                anomalies_found = True
            else:
                std_nth_median_n_sigma.append(0)
    except Exception as err:
        print(err)

    anomalies = []
    # m66 - only label anomalous if the n_sigma triggers are persisted
    # for (window / 2)
    current_triggers = []
    for index, item in enumerate(timeseries):
        if std_nth_median_n_sigma[index] == 1:
            current_triggers.append(index)
        else:
            if len(current_triggers) > int(window / 2):
                for trigger_index in current_triggers:
                    # Shift the anomaly back to the beginning of the
                    # window
                    if shift_to_start_of_window:
                        anomalies.append(timeseries[(trigger_index - (window * int((nth_median / 2))))])
                    else:
                        anomalies.append(timeseries[trigger_index])
            current_triggers = []
    # Process any remaining current_triggers
    if len(current_triggers) > int(window / 2):
        for trigger_index in current_triggers:
            # Shift the anomaly back to the beginning of the
            # window
            if shift_to_start_of_window:
                anomalies.append(timeseries[(trigger_index - (window * int((nth_median / 2))))])
            else:
                anomalies.append(timeseries[trigger_index])

    try:
        df = pd.DataFrame(timeseries, columns=['date', 'value'])
        df['date'] = pd.to_datetime(df['date'], unit='s')
    except Exception as err:
        print(json_file, err)
    datetime_index = pd.DatetimeIndex(df['date'].values)
    df = df.set_index(datetime_index)
    df.drop('date', axis=1, inplace=True)
    s = validate_series(df)

    anomalies_data = []
    anomaly_timestamps = [int(item[0]) for item in anomalies]
    for item in timeseries:
        if int(item[0]) in anomaly_timestamps:
            anomalies_data.append(1)
        else:
            anomalies_data.append(0)
    df['anomalies'] = anomalies_data
    plot(s, anomaly=df['anomalies'], anomaly_color='red', title=base_name)
#    break


# Why is us_population not converting to nonNegativeDerivative
json_file = '/opt/python_virtualenv/projects/TCPDBench-py386/TCPDBench/datasets/us_population/us_population.json'
file_name = os.path.basename(json_file)
base_name = file_name.split('.')[0]
data = None
mat = None
data, mat = load_dataset(json_file)
try:
    time_format = data['time']['format']
except KeyError:
    time_format = None
try:
    time_strings = data['time']['raw']
except KeyError:
    time_strings = data['time']['index']
timestamps = []
for time_string in time_strings:
    if time_format is not None:
        try:
            timestamp = int(datetime.strptime(time_string, time_format).timestamp())
            timestamps.append(timestamp)
        except Exception as err:
            print(json_file, err)
    else:
        timestamps.append(int(time_string))
timeseries = []
for index, timestamp in enumerate(timestamps):
    value = data["series"][0]["raw"][index]
    if value is None:
        value = np.nan
    timeseries.append([timestamp, value])
timeseries

strictly_increasing_monotonicity(timeseries)
# False
# So the us_population dropped off at some point?

test_ts = [item[1] for item in timeseries]
diff_ts = np.asarray(test_ts)
#np.all(np.diff(diff_ts) >= 0)

np.all(np.diff(diff_ts) >= 0)
# False

# Right the us_population decreased at some point and crashed
# a unicorn monotonic metric ...
# We do not really see those in the machine and apllication metrics space
monotonic_unicorns = {}
diff = []
last_value = None
for index, value in enumerate(test_ts):
    if last_value is None:
        last_value = value
        continue
    if (value - last_value) < 0:
        monotonic_unicorns[(index - 1)] = {'value': value, 'last_value': last_value, 'diff': (value - last_value)}
    last_value = value
    diff.append((value - last_value))
monotonic_unicorns
#{698: {'value': 309191211.0, 'last_value': 309212000.0, 'diff': -20789.0}}

# Same as function
[[index, value] for index, value in enumerate(np.diff(diff_ts)) if value < 0]
# [[698, -20789.0]]


monotonic_unicorns = {}
for json_file in DATASET_FILES:
    file_name = os.path.basename(json_file)
    base_name = file_name.split('.')[0]
    if base_name == 'measles':
        continue
    data, mat = load_dataset(json_file)
    if len(data["series"]) > 1:
        continue

    time_indices = data['time']['index']
    timestamps = []
    for time_index in time_indices:
        timestamps.append(time_index)
    timeseries = []
    for index, timestamp in enumerate(timestamps):
        value = data["series"][0]["raw"][index]
        if value is None:
            value = np.nan
        timeseries.append([timestamp, value])
    timeseries

    monotonic_unicorns[base_name] = {}
    test_ts = []
    not_monotonic = False
    for item in timeseries:
        # This only identifies and handles positive, strictly increasing
        # monotonic timeseries
        if item[1] < 0.0:
            not_monotonic = True
            break
        test_ts.append(item[1])

    if not_monotonic:
        print('%s monotonic_unicorns: %s' % (base_name, len(monotonic_unicorns[base_name])))
        continue

    # Right the us_population decreased at some point and crashed
    # a unicorn monotonic metric ...
    # We do not really see those in the machine and apllication metrics space
    last_value = None
    for index, value in enumerate(test_ts):
        if last_value is None:
            last_value = value
            continue
        if (value - last_value) < 0:
            monotonic_unicorns[base_name][(index - 1)] = {'value': value, 'last_value': last_value, 'diff': (value - last_value)}
        last_value = value
    print('%s monotonic_unicorns: %s' % (base_name, len(monotonic_unicorns[base_name])))

# In[85]:


filename = '/opt/python_virtualenv/projects/TCPDBench-py386/TCPDBench/analysis/output/tables/best_cover_uni_full.json'
with open(filename, "r") as fp:
    data = json.load(fp)
df_best_cover_uni_full = pd.DataFrame.from_dict(data, orient='index')
df_best_cover_uni_full


# In[88]:


filename = '/opt/python_virtualenv/projects/TCPDBench-py386/TCPDBench/analysis/output/tables/best_cover_uni_avg.json'
with open(filename, "r") as fp:
    data = json.load(fp)
df_best_cover_uni_avg = pd.DataFrame.from_dict(data, orient='index')
df_best_cover_uni_avg


# In[113]:


filename = '/opt/python_virtualenv/projects/TCPDBench-py386/TCPDBench/analysis/output/tables/default_cover_uni_full.json'
with open(filename, "r") as fp:
    data = json.load(fp)
df_default_cover_uni_full = pd.DataFrame.from_dict(data, orient='index')
df_default_cover_uni_full.loc['Total']= df_default_cover_uni_full.sum()
df_default_cover_uni_full.loc['Average']= df_default_cover_uni_full.sum() / df_default_cover_uni_full.count()
df_default_cover_uni_full.style.highlight_max(color = 'lightgreen', axis = 1)


# In[114]:


# OK


# In[115]:


# Not bad


# In[116]:


# Better, not a wasted weekend.


# In[ ]:





# In[155]:


import pdflatex
filename = '/opt/python_virtualenv/projects/TCPDBench-py386/TCPDBench/analysis/output/tables/best_f1_uni_full.json'
with open(filename, "r") as fp:
    data = json.load(fp)
df_best_f1_uni_full = pd.DataFrame.from_dict(data, orient='index')
#df_best_f1_uni_full.loc['Total']= df_best_f1_uni_full.sum()
#df_best_f1_uni_full.loc['Average']= df_best_f1_uni_full.sum() / df_best_f1_uni_full.count()
#df_best_f1_uni_full
#df_best_f1_uni_full.style.highlight_max(color = 'lightgreen', axis = 1)

#latex_str = df_best_f1_uni_full.style.highlight_max(color = 'lightgreen', axis = 1).to_latex('/opt/python_virtualenv/projects/TCPDBench-py386/TCPDBench/analysis/output/tables/best_f1_uni_full.tex')
#latex_bin = ''.join(format(i, '08b') for i in bytearray(latex_str, encoding='utf-8'))
#pdfl = pdflatex.PDFLaTeX.from_binarystring(latex_bin, '/opt/python_virtualenv/projects/TCPDBench-py386/TCPDBench/analysis/output/tables/best_f1_uni_full.pdf')
#pdf, log, cp = pdfl.create_pdf(keep_pdf_file=True)

df_best_f1_uni_full.style.highlight_max(color = 'lightgreen', axis = 1).to_latex('/opt/python_virtualenv/projects/TCPDBench-py386/TCPDBench/analysis/output/tables/best_f1_uni_full.pdf.tex')
pdfl = pdflatex.PDFLaTeX.from_texfile('/opt/python_virtualenv/projects/TCPDBench-py386/TCPDBench/analysis/output/tables/best_f1_uni_full.pdf.tex', params)
pdf, log, cp = pdfl.create_pdf(keep_pdf_file=False, keep_log_file=True)


# In[158]:


import os
import platform
import subprocess

# TeX source filename
tex_filename = '/opt/python_virtualenv/projects/TCPDBench-py386/TCPDBench/analysis/output/tables/best_f1_uni_full.pdf.tex'
filename, ext = os.path.splitext(tex_filename)
# the corresponding PDF filename
pdf_filename = filename + '.pdf'

# compile TeX file
subprocess.run(['pdflatex', '-interaction=nonstopmode', tex_filename])

# check if PDF is successfully generated
if not os.path.exists(pdf_filename):
    raise RuntimeError('PDF output not found')

# open PDF with platform-specific command
if platform.system().lower() == 'darwin':
    subprocess.run(['open', pdf_filename])
elif platform.system().lower() == 'windows':
    os.startfile(pdf_filename)
elif platform.system().lower() == 'linux':
    subprocess.run(['xdg-open', pdf_filename])
else:
    raise RuntimeError('Unknown operating system "{}"'.format(platform.system()))


# In[159]:


import matplotlib.backends.backend_pdf
import matplotlib.pyplot as plt
import pandas as pd

table = df_best_f1_uni_full
output_pdf = '/opt/python_virtualenv/projects/TCPDBench-py386/TCPDBench/analysis/output/tables/best_f1_uni_full.pdf'

fig = plt.figure()

ax=fig.add_subplot(111)

cell_text = []
for row in range(len(table)):
    cell_text.append(table.iloc[row])

ax.table(cellText=cell_text, colLabels=table.columns, loc='center')
ax.axis('off')

pdf = matplotlib.backends.backend_pdf.PdfPages(output_pdf)
pdf.savefig(fig)
pdf.close()


# In[179]:


from xhtml2pdf import pisa             # import python module

#df_best_f1_uni_full.style.highlight_max(color = 'lightgreen', axis = 1)
df_best_f1_uni_full_rounded = df_best_f1_uni_full.round(decimals=3)
df = df_best_f1_uni_full_rounded.copy()

def bold_max_value_in_series(series):
    highlight = 'font-weight: bold;'
    default = ''

#df.style.apply(bold_max_value_in_series, axis=1)


source_html = df.style.highlight_max(color = 'lightgreen', axis = 1).to_html()

style_html = """
<style>
    @page {
        size: A4 landscape;
        margin: 1cm;
    }
</style>
"""

html = style_html + source_html
# Define your data
output_filename = output_pdf

# Utility function
# open output file for writing (truncated binary)
result_file = open(output_filename, "w+b")

# convert HTML to PDF
pisa_status = pisa.CreatePDF(
#        source_html,                # the HTML to convert
        html,                # the HTML to convert
        dest=result_file)           # file handle to recieve result

# close output file
result_file.close()                 # close output file


# In[182]:


#df_best_f1_uni_full_rounded
#df_best_f1_uni_full_rounded.style.highlight_max(color = 'lightgreen', axis = 1)
#df
#df.round(decimals=3).style.highlight_max(color = 'lightgreen', axis = 1)
subset = list(df.columns.values)
df.style.apply(bold_max_value_in_series, axis=1, subset=subset)


# In[126]:


filename = '/opt/python_virtualenv/projects/TCPDBench-py386/TCPDBench/analysis/output/tables/best_cover_uni_avg.json'
with open(filename, "r") as fp:
    data = json.load(fp)
df_best_cover_uni_avg = pd.DataFrame.from_dict(data, orient='index')
df_best_cover_uni_avg = df_best_cover_uni_avg.sort_values(0, ascending=False)
df_best_cover_uni_avg.rename(columns={0: "best_cover_avg"})


# In[125]:


df_best_cover_uni_avg.info()


# In[127]:


filename = '/opt/python_virtualenv/projects/TCPDBench-py386/TCPDBench/analysis/output/tables/best_f1_uni_avg.json'
with open(filename, "r") as fp:
    data = json.load(fp)
df_best_f1_uni_avg = pd.DataFrame.from_dict(data, orient='index')
df_best_f1_uni_avg = df_best_f1_uni_avg.sort_values(0, ascending=False)
df_best_f1_uni_avg.rename(columns={0: "best_f1_avg"})


# In[128]:


filename = '/opt/python_virtualenv/projects/TCPDBench-py386/TCPDBench/analysis/output/tables/default_cover_uni_avg.json'
with open(filename, "r") as fp:
    data = json.load(fp)
df_default_cover_uni_avg = pd.DataFrame.from_dict(data, orient='index')
df_default_cover_uni_avg = df_default_cover_uni_avg.sort_values(0, ascending=False)
df_default_cover_uni_avg.rename(columns={0: "default_cover_avg"})


# In[129]:


filename = '/opt/python_virtualenv/projects/TCPDBench-py386/TCPDBench/analysis/output/tables/default_f1_uni_avg.json'
with open(filename, "r") as fp:
    data = json.load(fp)
df_default_f1_uni_avg = pd.DataFrame.from_dict(data, orient='index')
df_default_f1_uni_avg = df_default_f1_uni_avg.sort_values(0, ascending=False)
df_default_f1_uni_avg.rename(columns={0: "default_f1_avg"})

"""
# RESULTS
# m66 is a LEGIT changepoint detection algorithm

best_cover_avg
m66	0.795455
bocpd	0.789368
segneigh	0.783592
binseg	0.780245
amoc	0.745989
bocpdms	0.743791
pelt	0.725448
ecp	0.720113
kcpa	0.625981
zero	0.578767
prophet	0.576222
cpnp	0.552106
wbs	0.428036
rfpop	0.414154


best_f1_avg
bocpd	0.879675
binseg	0.855901
segneigh	0.854902
m66	0.841738
amoc	0.798912
ecp	0.796599
pelt	0.787211
kcpa	0.683167
cpnp	0.665980
zero	0.662375
bocpdms	0.620328
prophet	0.534355
wbs	0.532729
rfpop	0.530942

default_cover_avg
binseg	0.705799
amoc	0.701605
pelt	0.688798
segneigh	0.676410
bocpd	0.636019
bocpdms	0.633351
rbocpdms	0.628623
zero	0.582727
m66	0.582623
prophet	0.539869
cpnp	0.535341
ecp	0.522748
rfpop	0.392427
wbs	0.330206
kcpa	0.061955

default_f1_avg
binseg	0.744400
pelt	0.709992
amoc	0.703711
bocpd	0.689622
segneigh	0.675545
zero	0.668967
m66	0.648660
cpnp	0.606694
ecp	0.597710
bocpdms	0.507137
rfpop	0.499453
prophet	0.487742
rbocpdms	0.446714
wbs	0.411706
kcpa	0.111007
"""

# abed_results.20210815.good.final
