# CHANGES.md

## Panorama

- mysql-connector-python added
- rebrow added
- Webapp UI changes
- Added Panorama module files and related Webapp UI changes
- Upgraded Webapp UI jquery, dygraph, bootstrap
- Tested all Skyline components on Python 2.7.11 and 2.7.12

## Crucible

- Added a variant of @astanway crucible into the skyline src
- Added a crucible app so that ad-hoc timeseries can be feed to it and analyzed
- Restructured the skyline layout to be more inline with a Python package and
  setuptools with a merge of @languitar changes for setuptools and more pythonic
  structure that was submitted as per:
  https://github.com/etsy/skyline/pull/93
  https://github.com/etsy/skyline/issues/91
  Provide a setuptools-based build infrastructure #93 - etsy#91
- Not totally setuptools compliant yet
- Added sphinx docs and documentation build pattern
- More documentation
- Serve sphinx documentation via webapp /static/docs/
- Handle pandas versions changes to - PANDAS_VERSION
- Attempted to handle logging without log overwrites, not pretty but works
- Performance profiled - pprofile, RunSnakeRun, cProfile, vmprof, snakeviz
- deroomba - kill any lingering vacuum processes - ROOMBA_TIMEOUT
- Self monitor Analyzer spin_process threads and terminate if any spin_process
  has run for longer than 180 seconds - MAX_ANALYZER_PROCESS_RUNTIME
- Optimizations to Analyzer workflow logic - RUN_OPTIMIZED_WORKFLOW
- Some general code optimizations based on profiling results
- Addition of algorithm_breakdown metrics - ENABLE_ALGORITHM_RUN_METRICS and
  SKYLINE_TMP_DIR, tmpfs over multiprocessing Value
- Updated current requirements
- Patterned and tested in Python virtualenv for ease of python version, package
  management and dependencies management

## boundary - Nov 24, 2015

- boundary - hipchat - wildcard metric namespaces
- Merged the boundary branch and functionality to master, this supercedes the
  detect_drop_off_cliff-algorithm branch which was not really fit for purpose
  in so much as it was too much modification of analyzer. boundary is more fit
  for purposes and adds a lot of functionality and another dimension to skyline.
  This commit reverts skyline analyzer back to the mirage branch version and
  extends through boundary.
- Refactored other skyline code to pep8 (mostly bar E501 and E402)

## detect_drop_off_cliff - Nov 12, 2015

- Update fork info From etsy/skyline to earthgecko/skylineMerge Modified: readme.md
- Merge detect_drop_off_cliff-algorithm branch and functionality to master

## mirage - Nov 12, 2015

- Merge mirage branch and functionality to master
- Handle connection error to Graphite
- Break the loop when connection closes.
- Add ability to embed graphite graphs in emails - @mikedougherty As per https://github.com/etsy/skyline/pull/76
- multiple_skylines_graphite_namespace - merge
- wildcard_alert_patterns - merge
- Fixing #92 - Satisfying missing module dependencies for SafeUnpickler class
- Fixing #77 - Patched in the SafeUnpickler from Graphite Carbon

## skyline.analyzer.metrics - Jun 11, 2014

- Added functionality to analyzer.py for skyline to feed all of its own metrics
  back to graphite. This results in skyline analyzing its own metrics for free.

## alert_syslog - Jun 10, 2014
- Added new alerter syslog alert_syslog to write anomalous metrics to the
  syslog at LOG_LOCAL4, with Anomalous metric: %s (value: %s) so LOG_WARN
  priority this creates in local log and ships to any remote syslog as well, so
  that it can be used further down a data pipeline

## etsy - original
- Fixing #83 - correcting algorithm docs
- Doh.
- pep8 rule tweak
- Use SVG for the Travis badge in the README
- added restart option
- whitespace cleanup for pep8
- update language of GRAPHITE_HOST a little, could still use some love
- Add GRAPH_URL config option so that GRAPHITE_HOST is just the Graphite Host or ip.
- Added python-simple-hipchat to requirements and added a comment to README file
- Support for multiple alert recipients
- Process series under FULL_DURATION anyway. Closes #63
- Rename GRAPHITE_PORT to CARBON_PORT
- Update settings.py
- Expand the Contributions section of the README
- missed settings file in my initial pep8 cleanup
- Update travis config to search all code instead of just src
- pep8 cleanup
- Indent to PEP8 standards
- Move dicts out of class variables
- Clean up debugging statements
- Remove shared dictionaries
- UX on verify_alerts.py
- Add script to test/verify alert configuration
- PEP8 spacing on seed_data.py
- update seed_data to respect namespace settings
- Expand the Contributions section of the README
- Missed one
- pep8 cleanup to pass travis tests
- missed settings file in my initial pep8 cleanup
- Update travis config to search all code instead of just src
- pep8 cleanup
- Indent to PEP8 standards
- Move dicts out of class variables
- Clean up debugging statements
- Remove shared dictionaries
- UX on verify_alerts.py
- Add script to test/verify alert configuration
- PEP8 spacing on seed_data.py
- Removing dependency on Oculus
- update seed_data to respect namespace settings
- Removing dependency on Oculus
- Removing dependency on Oculus
- seed_data: Delay Redis connection until we need it
- seed_data: Don't catch all Exceptions
- seed_data: Simplify settings import.
- seed_data: Reorganize imports per PEP8.
- seed_data: Move into function.
- seed_data: Allow running from arbitrary directories.
- seed_data: Make executable.
- Rename simple_stddev; better UX around broken algorithms
- Log only on canary: https://github.com/etsy/skyline/pull/56
- Graphite canary; pids on all logs. Fixes #55
- That time we actually wrote this ourselves
- Add BOREDOM_SET_SIZE setting
- Raise boredom level to 2 by default; ignore binary series
- Fix tests
- Dump RDB to /var/dump by default
- Turn second order anomalies
- Fixes #53
- Second order anomalies
- Trailing comma
- Update .travis.yml for working Anaconda URL
- Update readme.md
- Abstracting alerters and adding HipChat and PagerDuty
- Markdown
- Update readme for alerts
- [API BREAKING]: Email alerts
- Get rid of Bootstrap folder
- feed enough data into ks and adf tests
- Update defaults for Kolmogorov-Smirnov
- fix non-ascii in description
- add 2 sample Kolmogorov-Smirnov test
- Add some messaging for start scripts' run command
- Add usage for run in analyzer.d
- Add run to usage help in webapp.d
- Add support for running in non-daemon mode
- Compare median of deviations instead of true median
- Median absolute deviation algorithm
- Add space in canary log metric
- Append namespace to canary metric
- Update readme.md
- Add example statsd exclusion namespaces
- Refactor Graphite sending, add GRAPHITE_PORT setting
- Allow start/stop scripts to be run from anywhere Add -f to rm in start/stop scripts to suppress no such file warnings
- Actual link
- Link to mailing list
- Add HORIZON_IP to sample settings. Change Listen to use said variable, defaulting to 127.0.0.1 if it does not exist
- Link travis build status image to build details
- Disable email notifications
- Add Travis status
- Fixup _addSkip() - https://github.com/etsy/skyline/pull/27#issuecomment-19858932
- unittest2 and mock in requirements.txt
- Unit tests (single commit). See https://github.com/etsy/skyline/pull/11
- removed extra lines
- added ROOMBA_GRACE_TIME setting for cleanup process
- Bugfix: TypeError: unhashable type: 'list'. Resolution: Include only the data points from timeseries in the set.
- Update example settings
- A data set is boring if "the last MAX_TOLERABLE_BOREDOM values are the same number, not that they sum to zero."
- Readme
- Ignore settings.py for public repo. You should Chef out your local settings.py if you're using this in production.
- Removing unused imports from src/horizon/*
- Call super() in Thread/Process subclasses in src/horizon/
- Bugfix: missed a trailing :
- Minor idiomatic cleanup: - Call super().__init__() in Thread subclass - Use context manager to close out file when dumping anomalous metrics - Use identity comparison for None - Simplified unpacker -> timeseries fixup
- Minor refactoring of return values to avoid redundant if blocks
- Removing unused imports
- Removing unused imports
- Cleaning up continuity.py a bit: - "tuple" is reserved - Calculate total_sum as the sum of the last 50 items via slice notation - None is singleton. Compare via identity, not equality
- Various doc comments
- Update readme.md
- PID location for the webapp
- License
- Readme
- First
