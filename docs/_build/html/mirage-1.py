# A bit of a contrived example...
import numpy as np

# @added 20161119 - Task #1758: Update deps in Ionosphere
# The update to Sphinx or matplotlib is causing a user error warning
# This call to matplotlib.use() has no effect
# because the backend has already been chosen;
# matplotlib.use() must be called *before* pylab, matplotlib.pyplot,
# or matplotlib.backends is imported for the first time.
# Seen in adding Redis data plot to the alerters, fixes like this
import matplotlib
matplotlib.use('Agg')

import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.patches as mpatches
import csv
import string
import os
import datetime as dt
# @modified 20200808 - Task #3608: Update Skyline to Python 3.8.3 and deps
# import urllib2
from urllib.request import urlopen

# Department for Education real-time energy data: October 2015
# https://www.gov.uk/government/uploads/system/uploads/attachment_data/file/476574/Real_time_energy_data_October.csv
datafile = '../examples/data/Real_time_energy_data_October.csv'
if not os.path.exists(datafile):
    datafile = '/tmp/Real_time_energy_data_October.csv'
    url = 'https://www.gov.uk/government/uploads/system/uploads/attachment_data/file/476574/Real_time_energy_data_October.csv'
    # @modified 20200808 - Task #3608: Update Skyline to Python 3.8.3 and deps
    # Change to urlopen and added nosec for bandit [B310:blacklist] Audit
    # url open for permitted schemes. Allowing use of file:/ or custom
    # schemes is often unexpected.
    # response = urllib2.urlopen(url)
    if url.lower().startswith('http'):
        response = urlopen(url)  # nosec
    else:
        response = None

    with open(datafile, 'w') as fw:
        fw.write(response.read())

values = []
# @modified 20200808 - Task #3608: Update Skyline to Python 3.8.3 and deps
# with open(datafile, 'rb') as csvfile:
with open(datafile, 'rt') as csvfile:
    reader = csv.reader(csvfile, delimiter=',')
    for row in reader:
        values_row = ', '.join(row)
        # @modified 20200808 - Task #3608: Update Skyline to Python 3.8.3 and deps
        # values_only_string = string.replace(values_row, ' ', '')
        values_only_string = values_row.replace(' ', '')
        values_list = values_only_string.split(',')
        values.append(values_list)

hours = []
current_index = 2
for index, value in enumerate(values):
    if value[1] == 'Day/Time':
        while current_index < 50:
            hours.append(value[current_index])
            current_index += 1
two_weeks = '01/10/2015 02/10/2015 03/10/2015 04/10/2015 05/10/2015 06/10/2015 07/10/2015 08/10/2015 09/10/2015 10/10/2015 11/10/2015 12/10/2015 13/10/2015 14/10/2015'
data = []
for index, value in enumerate(values):
    # if value[0] != 'Site' and value[1] != '31/10/2015':
    if value[1] in two_weeks:
        current_index = 2
        current_hour = 0
        while current_index < 50:
            date = '%s %s' % (value[1], hours[current_hour])
            line_data = [date, value[current_index]]
            data.append(line_data)
            current_index += 1
            current_hour += 1

tmp_datafile = '/tmp/skyline.docs.mirage.energy_data.csv'
if os.path.exists(tmp_datafile):
    os.remove(tmp_datafile)

for element in data:
    ts_line = '%s, %.2f\n' % (element[0], float(element[1]))
    with open(tmp_datafile, 'a') as fw:
        fw.write(ts_line)

# @added 20200808 - Task #3608: Update Skyline to Python 3.8.3 and deps
def bytespdate2num(b):
    return mdates.datestr2num(b)

hours, consumption = np.loadtxt(
    tmp_datafile, unpack=True,
    delimiter=',',
    # @modified 20200808 - Task #3608: Update Skyline to Python 3.8.3 and deps
    # converters={0: mdates.strpdate2num('%d/%m/%Y %H:%M')},
    # converters={0: mdates.datestr2num('%d/%m/%Y %H:%M')},
    converters={0: bytespdate2num},
    encoding='latin1')

if os.path.exists(tmp_datafile):
    os.remove(tmp_datafile)

fig = plt.figure(figsize=(14, 5))

x_anno1 = dt.datetime.strptime('02/10/2015 06:00', '%d/%m/%Y %H:%M')
x_anno2 = dt.datetime.strptime('03/10/2015 06:00', '%d/%m/%Y %H:%M')
plt.annotate(
    'Analyzer at 86400\nFULL_DURATION\nwould probably fire\naround here',
    xy=(x_anno2, 130), xycoords='data',
    xytext=(0.2, 0.5), textcoords='axes fraction',
    arrowprops=dict(facecolor='red', shrink=0.01),
    horizontalalignment='right', verticalalignment='top')

plt.axvspan(x_anno1, x_anno2, alpha=0.4, color='pink')
analyzer_full_duration = mpatches.Patch(color='pink', label='Analyzer FULL_DURATION')
plt.legend(handles=[analyzer_full_duration])

x_anno3 = dt.datetime.strptime('09/10/2015 06:00', '%d/%m/%Y %H:%M')
x_anno4 = dt.datetime.strptime('10/10/2015 06:00', '%d/%m/%Y %H:%M')
plt.annotate(
    'Analyzer at 86400\nFULL_DURATION\nwould probably fire\naround here',
    xy=(x_anno4, 130), xycoords='data',
    xytext=(0.7, 0.5), textcoords='axes fraction',
    arrowprops=dict(facecolor='red', shrink=0.01),
    horizontalalignment='right', verticalalignment='top')

plt.axvspan(x_anno3, x_anno4, alpha=0.4, color='pink')

x_anno5 = dt.datetime.strptime('03/10/2015 06:00', '%d/%m/%Y %H:%M')
x_anno6 = dt.datetime.strptime('09/10/2015 06:00', '%d/%m/%Y %H:%M')
plt.axvspan(x_anno5, x_anno6, alpha=0.4, color='blue')
x_anno7 = dt.datetime.strptime('02/10/2015 04:00', '%d/%m/%Y %H:%M')
x_anno8 = dt.datetime.strptime('02/10/2015 06:00', '%d/%m/%Y %H:%M')
plt.axvspan(x_anno7, x_anno8, alpha=0.4, color='blue')
x_anno9 = dt.datetime.strptime('10/10/2015 06:00', '%d/%m/%Y %H:%M')
x_anno10 = dt.datetime.strptime('10/10/2015 08:00', '%d/%m/%Y %H:%M')
plt.axvspan(x_anno9, x_anno10, alpha=0.4, color='blue')

mirage_full_duration = mpatches.Patch(color='blue', label='Mirage FULL_DURATION')

plt.annotate(
    '', xy=(x_anno7, 370), xycoords='data',
    xytext=(x_anno10, 370), textcoords='data',
    arrowprops={'arrowstyle': '<->'})
plt.text(x_anno1, 375, 'Mirage FULL_DURATION period')

plt.annotate(
    '', xy=(x_anno2, 310), xycoords='data',
    xytext=(x_anno1, 310), textcoords='data',
    arrowprops={'arrowstyle': '<->'})
plt.text(x_anno1, 310, 'Analyzer FULL_DURATION period')

plt.annotate(
    '', xy=(x_anno3, 310), xycoords='data',
    xytext=(x_anno4, 310), textcoords='data',
    arrowprops={'arrowstyle': '<->'})
plt.text(x_anno3, 310, 'Analyzer FULL_DURATION period')

plt.legend(handles=[analyzer_full_duration, mirage_full_duration])

plt.title('Department of Education Sanctuary Buildings - energy consumption\nAn example of Skyline Analyzer and Mirage data views')
plt.figtext(0.99, 0.01, 'Sample data from https://www.gov.uk/government/uploads/system/uploads/attachment_data/file/476574/Real_time_energy_data_October.csv', horizontalalignment='right')
plt.plot_date(x=hours, y=consumption, markersize=1.3)
plt.gcf().autofmt_xdate()

plt.show()