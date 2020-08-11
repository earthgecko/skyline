"""
This script is used during the local build purpose and automatically generates
the docs/development/pytz.rst file to provide a list of all pytz timezones.

This may be a little unconventional but it probably beats trying to do it via
Sphinx support custom extensions, without using generates or includes or Jinga
templating, which may or may not work with readthedocs.

Its usage is documentated in and a part of the documentation build see Building
documentation (see docs/development/building-documentation.md)

"""

import os
from sys import version_info
from termcolor import colored

import pkg_resources

from pytz import timezone
import pytz

import traceback

python_version = int(version_info[0])
pytz_version = pkg_resources.get_distribution('pytz').version
# pytz_all_timezones = list(pytz.all_timezones)
pytz_all_timezones = list(pytz.all_timezones)

print(colored('notice: skyline/tsfresh/scripts/make-pytz.all_timezones-rst.py is creating docs/development/pytz.rst', 'cyan'))
# Note: when developing with jupyter the __file__ is non-existent e.g.
# not hasattr __file_, so
try:
    script_path = os.path.dirname(__file__)
except:
    print('You are running this in jupyter?')
    print(colored('warning: You are running this in jupyter?', 'yellow'))

    script_path = '/home/gary/sandbox/of/github/earthgecko/skyline/ionosphere/skyline/skyline/tsfresh/scripts'

print('script_path: %s' % script_path)

doc_path = script_path.replace('skyline/tsfresh/scripts', 'docs/development')
print('doc_path: %s' % doc_path)

write_to_file = '%s/pytz.rst' % doc_path
print('write_to_file: %s' % write_to_file)

page_text = '''******************
Development - pytz
******************

.. warning:: This pytz.rst file is automatically generated DO NOT edit this page
    directly, edit its source skyline/tsfresh/scripts/make-pytz.all_timezones-rst.py
    and see `Building documentation <building-documentation.html>`__)

pytz.all_timezones
==================

**pytz version: %s**

This is an automatically generated list of all pytz timezones for the specified
pytz version generated using pytz.all_timezones.  It is generated so there is an
easy reference for users to find their relevant pytz timezone string.

It is specifically related to Skyline Ionosphere, but it is also
applicable more generally to working with Graphite timeseries data, different
timezones, pytz and formatting the Graphite timeseries data in UTC for feature
extraction using tsfresh.

In terms of Ionosphere or generally working with Graphite timeseries data, this
is only applicable for anyone want who has Graphite implemented in a timezone
other than UTC and wishes to pass a timezone to extract the features of a metric
timeseries with tsfresh.

The user should not have to fire up pytz locally or on their server to find
their applicable pytz timezone, they are all listed here below, which is yours?
Or in more detail - https://en.wikipedia.org/wiki/Tz_database

It is hoped that the methods used handle daylight savings timezones (DST), the
methods implemented has been taken from some best practices, however they are
not tested on DST changes, so the outcome is somewhat unknown.

**pytz version: %s**

Timezones list for pytz version
-------------------------------
''' % (str(pytz_version), str(pytz_version))

# print(page_text)
# print '\n'.join(map(str, pytz.all_timezones))
pytz_all_timezones = '\n\n'.join(map(str, pytz.all_timezones))

full_page_text = '''%s
%s
''' % (str(page_text), pytz_all_timezones)

# Test that the text is as expected
if 'automatically generated' in full_page_text:
    test_text = True
    print(colored('notice: the expected automatically generated text was found in the full_page_text - OK', 'cyan'))
else:
    test_text = False
    print(colored('warning: the expected automatically generated text was not found in the full_page_text', 'yellow'))

if 'Greenwich' in full_page_text:
    test_pytz_text = True
    print(colored('notice: the expected pytz timezone text was found in the full_page_text - OK', 'cyan'))
else:
    test_pytz_text = False
    print(colored('warning: the expected pytz timezone text was not found in the full_page_text', 'yellow'))

try:
    with open(write_to_file, 'w') as fh:
        fh.write(full_page_text)
    print(colored('info: docs/development/pytz.rst created - OK', 'green'))
except:
    print(traceback.format_exc())
    print(colored('error: an error was encountered creating docs/development/pytz.rst, please review.', 'red'))
