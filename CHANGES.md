# CHANGES.md

## Ionosphere - v1.1.4-beta-ionosphere - Jun 19, 2017

- Some debug choices for daylight savings countdown issue
  (Feature 1876: Ionosphere - training_data learn countdown)
- Handle change in monotonicity with a check if a metric has its own Redis
  z.derivative_metric key that has not expired.
  (Bug 2050: analyse_derivatives - change in monotonicity)
- Added D1 layer to features_profiles.html
- Added D1 layer is required to ensure that layers do not inadvertently silence
  true anomalies due the D layer being a singular value, a D1 layer is required
  to allow the operator to mitigate against this, by setting a min and max
  threshold to skip the layer. (Feature 2048: D1 ionosphere layer)
- Allow the operator to save a training_data set with save_training_data_dir in
  ionosphere_backend, etc (Feature 2054: ionosphere.save.training_data)
- Updated docs with layers and misc modifications
- Ionosphere - rate limiting profile learning.Rate limit Ionosphere to only
  learn one profile per resolution per metric per hour. This is to stop
  Ionosphere LEARNT from iteratively learning bad if something becomes anomalous
  and Skyline creates lots of training data sets for a metric.  Ionosphere learn
  could come along and start to incrementally increase the range in LEARNT
  features profiles, with each iteration increasing a bit and going to the next
  training data set another profile to match.  A spawn.c incident where
  Ionosphere learnt bad and badly.
  (Feature 2010: Ionosphere learn - rate limiting profile learning)
- Ionosphere - analyse_derivatives (Feature 2034: analyse_derivatives). Add
  functionality that enables Skyline apps to convert a timeseries to the
  nonNegativeDerivative too properly analyse incremented count metrics. This will
  allow for Ionosphere to learn these types of metrics too, as previously it could
  only learn step changes at static rates as the features profiles change at every
  step. NON_DERIVATIVE_MONOTONIC_METRICS setting.
- Added matched_greater_than query to the search features profile page.  At the
  moment this only covers features profiles matches not layers matches.
  (Feature 1996: Ionosphere - matches page)
- validated features profiles. Added a validated component to the features
  profiles (Feature 2000: Ionosphere - validated)

## Ionosphere - v1.1.3-beta-ionosphere - Apr 24, 2017

- Added a validated component to the features profiles
- keying training data.  The addition of each training_dir and data set is now
  Redis keyed to allow for an increase efficiency in terms of disk I/O for
  ionosphere.py and making keyed data available for each training_dir data set
  so that transient matched data can be surfaced for the webapp along with
  directory paths, etc

## Ionosphere - v1.1.2-beta-ionosphere - Apr 2, 2017

- Added ionosphere_layers table to DB related resources
- Added preliminary Ionosphere layers functionality
- v1.1.2-alpha
- Allow for the declaration of a DO_NOT_SKIP_LIST in the worker context
- Allow Ionosphere to send Panorama checks for ionosphere_metrics, not Mirage
- Display the original anomalous datapoint value in the Redis plot alert image

## Ionosphere - v1.1.1-beta-ionosphere - Feb 25, 2017

Added learning via features extracted using https://github.com/blue-yonder/tsfresh/releases/tag/v0.4.0
Dedicated to my father, Derek, a man of numbers if there ever was one.

- Corrected sum_common_values column name
- Remove needs_sphinx for RTD
- needs_sphinx = '1.3.5' for RTD
- 0 should be None as this was causing an error if the full_duration is not
  in the features_profile_details_file, which it was not for some reason.
- Added metric_check_file and ts_full_duration is needed to be determined
  and added the to features_profile_details_file as it was not added here on
  the 20170104 when it was added the webapp and ionosphere - so added to
  features_profile.py
- Added all_fp_ids and lists of ionosphere_smtp_alerter_metrics and
  ionosphere_non_smtp_alerter_metrics to ionosphere.py so to only process
  ionosphere_smtp_alerter_metrics
- Added lists of smtp_alerter_metrics and non_smtp_alerter_metrics to analyzer.py
  so to only process smtp_alerter_metrics
- Restored the previous redis_img_tag method as some smtp alerts were
  coming without a Redis graph, not all but some and for some reason,
  I am pretty certain retrospectively that it was done that way from
  testing I just wanted to try and be cleaner.
- Added all_calc_features_sum, all_calc_features_count, sum_calc_values,
  common_features_count, tsfresh_version to SQL
- Added details of match anomalies for verification
- Added all_calc_features_sum, all_calc_features_count, sum_calc_values,
  common_features_count, tsfresh_version to ionosphere.py and SQL
- Added graphite_matched_images|length to features_prfile.html
- More to do in webapp context to pass verfication values
- Added graphite_matched_images context and matched.fp_id to
  skyline_functions.get_graphite_metric
- Added graphite_matched_images context and db operations to
  ionosphere_backend.py (geez that is a not of not DRY on the DB stuff)
- Added graphite_matched_images gmimages to webapp and features_profile.html and
  ported the training_data.html image order method to features_profile.html
- Done to the The White Stripes Live - Under Nova Scotian Lights Full
  https://www.youtube.com/watch?v=fpG8-P_BpcQ I was tried of Flowjob
  https://soundcloud.com/search/sounds?q=flowjob&filter.duration=epic which a
  lot of Ionosphere has been patterned to.
- Fixed typo in database.py
- Added ionosphere_matched update to ionosphere.py
- Un/fortunately there is no simple method by which to annotate
  these Graphite NOW graphs at the anomaly timestamp, if these were
  from Grafana, yes but we cannot add Grafana as a dep :)  It would
  be possible to add these using the dygraph js method ala now, then
  and Panorama, but that is BEYOND the scope of js I want to have to
  deal with.  I think we can leave this up to the operator's
  neocortex to do the processing.  Which may be a valid point as
  sticking a single red line vertical line in the graphs ala Etsy
  deployments https://codeascraft.com/2010/12/08/track-every-release/
  or how @andymckay does it https://blog.mozilla.org/webdev/2012/04/05/tracking-deployments-in-graphite/
  would arguably introduce a bias in this context.  The neocortex
  should be able to handle this timeshifting fairly simply with a
  little practice.
- Added human_date to the Graphite graphs NOW block for the above
- Exclude graph resolution if matches TARGET_HOURS - unique only
- Added ionosphere_matched_table_meta
- Standardised all comments in SQL
- Added the ionosphere_matched DB table
- After debugging calls on the readthedocs build, adding to
  readthedocs.requirements.txt should solve this
- After debugging calls on the readthedocs build, adding to requirements.txt
  should solve this
- Try use setup.cfg install_requires
- use os.path.isfile although I am not sure on the path
- readthedocs build is failing as they are Running Sphinx v1.3.5 and returns
  Sphinx version error:
  This project needs at least Sphinx v1.4.8 and therefore cannot be built with this version.
- Added returns to skyline_functions.get_graphite_metric and specific webapp
  Ionosphere URL parameters for the Graphite NOW graphs
- Removed unused get_graphite_metric from skyline/webapp/backend.py
- Added get_graphite_metric, the graphite_now png context and retrieving the
  graphite_now_images at TARGET_HOURS, 24h, 7d and 30d
- Removed the reference to #14 from webapp Ionosphere templates
- Added order and graphite_now_images block to training_data.html
- Added sorted_images for order and graphite_now_images to webapp
- Added Grumpy testing to the roadmap
- bug on graphite in image match - stats.statsd.graphiteStats.flush_time, but
  graphite could be in the namespace. So match on .redis.plot and removed the
  unknown image context as it should not really ever happen...?
- Enabled utilities as the files were added
- Added the full_duration parameter so that the appropriate graphs can be
  embedded for the user in the training data page in the webapp context
- Added utilities TODO
- Removed the buggy skyline_functions.load_metric_vars and replaced with the
  new_load_metric_vars(self, metric_vars_file) function
- Fixes #14
- Changed ionosphere_backend.py to the new function
- Removed the buggy skyline_functions.load_metric_vars and replaced with the
  new_load_metric_vars(self, metric_vars_file) function
- Fixes #14
- Clarified Mirage alert matching in docs
- Removed unused things
- Added 'app' and 'source' to string_keys
- Added anomalous_value to correct calc_value overwriting value
- Added an app alert key ionosphere.%s.alert app alert key
- Remove check file is an alert key exists
- Added TODO: ionosphere.analyzer.unique_metrics (at FULL_DURATION)
              ionosphere.mirage.unique_metrics (NOT at FULL_DURATION)
- Get ionosphere.features_calculation_time Graphite metric working
- Correct auth for requests should webapp be called
- Cache fp ids for 300 seconds?
- Send Ionosphere metrics to Graphite and Reset lists
- Minor log bug fix in features_profile.py
- Correct rate limitining Ionosphere on last_alert cache key
- Added SQLAlchemy engine dispose
- Differentiate between Mirage and Ionosphere context in logging not alerting
- Do not alert on Mirage metrics, surfaced again as Ionosphere was introduced
- Added new new_load_metric_vars to ionosphere to get rid of the skyline_functions
  load_metric_vars as imp is deprecated in py3 anyway and this should fix #24 as
  well.
- Added THE FIRST to docs
- Clarifed log message
- Handle refreshing mirage.unique_metrics and ionosphere.unique_metrics
- Refactoring some things in ionosphere.py
- THE FIRST prod match is at this commit
- Added last_checked and checked_count to features profile details to Ionosphere
  features profile page, feature profile details page.
- Added ionosphere last_checked checked_count columns to record the number of
  times a features profile is checked and when last checked
- Ionosphere update checked count and timestamp
- int timestamp in the alerters
- Set default as 0 on ionosphere matched_count and last_matched
- Set default as NULL on ionosphere matched_count and last_matched
- Added context in Analyzer
- Added in Analyzer added a self.all_anomalous_metrics to join any Ionosphere
  Redis alert keys with self.anomalous_metrics
- Refresh mirage.unique_metrics Redis set
- Refresh ionosphere.unique_metrics Redis set
- Pushing alerts back to the apps from Ionosphere
- Added update SQL
- PoCing pushing alerts back to the apps from Ionosphere
- Only match features profiles that have the same full_duration
- Added the full_duration context to the send_anomalous_metric to Ionosphere in
  Analyzer, Mirage, skyline_functions and the database which needs to be
  recorded to allow Mirage metrics to be profiled on Redis timeseries data at
  FULL_DURATION
- Added IONOSPHERE_FEATURES_PERCENT_SIMILAR to validate_settings
- Bringing Ionosphere ionosphere_enabled and ionosphere.unique_metrics online
- Some flake8 linting
- Enable ionosphere metrics in DB if features profile is created
- Added more ionosphere functionality, now checking DB features profiles
- Determine relevant common features
- Calculate percent difference in sums
- Added IONOSPHERE_FEATURES_PERCENT_SIMILAR to settings
- Use str in alerters Graphite URL for full_duration_in_hours
- Added the baselines from tsfresh, they diff the same as 0.3.1
- Update to tsfresh-0.4.0 to make use of the new
  ReasonableFeatureExtractionSettings that was introduced to exclude the
  computationally high cost of extracting features from very static timeseries
  that has little to no variation is the values, which results in features
  taking up to almost 600 seconds to calculate on a timeseries of length 10075
  exclude high_comp_cost features.
- Added the baselines from tsfresh, diff the same
- Use str not int for full_duration_in_hours context in alerts, etc
- Updated tsfresh to 0.3.1 not to 0.4.0 as tqdm may not be so great in the mix
  for now.
- Added new v0.3.1 baselines from tsfresh 0.4.0 that were committed at
  https://github.com/blue-yonder/tsfresh/commit/46eb72c60f35de225a962a4149e4a4e25dd02aa0
  They test fine.
- Update deps
- Send Graphite metrics from mirage and ionosphere
- Added stop process after terminate, if any still exist as any terminate called
  on extract_features calls terminate, but the pid remains, debugging
- Added __future__ division to all scopes using division (pep-0238)
- Changed to InnoDB from MyISAM as no files open issues and MyISAM clean
  up, there can be LOTS of file_per_table z_fp_ tables/files without
  the MyISAM issues.  z_fp_ and z_ts_ tables are mostly read and will be shuffled
  in the table cache as required.
- Added some additional logging in ionosphere on the slow hunt to determine why
  tsfresh extract_features is sometimes just hanging and needs to be killed
- Removed p.join from all p.terminate blocks as it hangs there
- Added missing app context to features_profile.py
- mirage send anomaly timestamp not last timestamp from SECOND_ORDER_RESOLUTION
  timeseries
- Always pass the f_calc time to webapp
- Aligned the alerters HTML format
- Added the ionosphere context to features_profile.py
- Changed to InnoDB from MyISAM as no files open issues and MyISAM clean
  up, there can be LOTS of file_per_table z_fp_ tables/files without
  the MyISAM issues.  z_fp_ tables are mostly read and will be shuffled
  in the table cache as required.
- Enabled feature calculation on all anomaly checks in ionosphere
- Moved features_profile.py to the skyline dir some can be used by the webapp
  and ionosphere
- Decouple ionosphere from the webapp
- In ionosphere calculate the features for every anomaly check file, remove to
  extract features calcuate and wait time from the user.
- Corrected assert in test.
- Added some notes
- Use ast.literal_eval instead of eval
- Added tsfresh feature names with ids in list as of tsfresh-0.3.0
- Deleted skyline/tsfresh/scripts as is now moved to skyline/tsfresh_features/scripts
  to stop tsfresh namespace pollution
- Moved the tsfresh test resources into tests/baseline to match tsfresh tests
  methodology.
- Added note to ionosphere.py about numpy.testing.assert_almost_equal
- Reordered TSFRESH_FEATURES based on the new method.
- Adding the meat to Ionosphere bones on the service side, incomplete.
- Updated the upgading doc
- Added development tsfresh docs on howto upgrade, etc
- Added the creation of tsfresh resources to building-documentation.rst
- Updated development/ionosphere.rst to reflect tsfresh is not slow
- Draft text for ionosphere.rst
- Added Ionosphere reference to panorama.rst
- Updated path change in tsfresh.rst
- Modifications for testing
- Added Ionosphere requirements for tsfresh and SQLAlchemy
- Added a tsfresh tests
- Added a tsfresh-0.3.0 baseline features upon which tsfresh is tested
- Added a tsfresh-0.1.2 baseline features upon which tsfresh is tested
- Added a baseline timeseries upon which tsfresh is tested
- Added IONOSPHERE_PROFILES_FOLDER test
- Added tsfresh feature names with ids in list as of tsfresh-0.3.0
- Some minor refactors in skyline_functions.py
- Added RepresentsInt
- Added the ionosphere tables
- Moved the Ionosphere settings all to the end in settings.py to make it easier
  to upgrade
- Added some additional Ionosphere settings
- Updated to tfresh-0.3.0
- Added the SQLAlchemy definitions
- Added app.errorhandler(500) to webapp.py traceback rendered nicely by Jinja2 as per
  https://gist.github.com/probonopd/8616a8ff05c8a75e4601
- Added the ionosphere app.route to webapp.py
- Minor addition to backend.py log and panorama_anomaly_id request arg
- Added Ionosphere to the html templates
- Added webapp training_data.html
- Added webapp traceback.html which provides the user with real traceback
- Added webapp features_profiles.html
- Added webapp ionospere_backend.py which works with training data and features
  profiles data
- Added webapp functions to create features profiles
- Moved skyline/tsfresh to skyline/tsfresh_features so that it does not pollute
  the tsfresh namespace
- Added generate_tsfresh_features script
- Added missing string in panorama.py in the logger.error context
- mirage.py refactored quite a bit of the conditional blocks workflow to cater
  for Ionosphere
- Added metric_timestamp to the trigger_alert metric alert tuple and added
  sent_to_ metrics (analyzer.py and mirage.py) - metric[2]: anomaly timestamp
- Added Ionosphere training data timeseries json, redis plot png and Ionosphere
  training data link to the Analyzer and Mirage alerters.
- Mirage alerter creates a Redis timeseries json too - tbd allow user to build
  features profile on either full Mirage timeseries or on the Redis
  FULL_DURATION timeseries.
- analyzer.py use skyline_functions.send_anomalous_metric_to (self function
  removed) and some new Ionosphere refactoring
- Modified bin scripts to not match the app name in the virtualenv path should
  the path happen to have the app name string in the path
- Corrected webapp bin service string match
- Bifurcate os.chmod mode for Python 2 and 3
- Fixes https://github.com/earthgecko/skyline/issues/27 - return False if stdDev
  is 0
- Also readded IONOSPHERE_CHECK_MAX_AGE from settings.py as it will be required
- Mirage changes include a changed to panorama style skyline_functions
  load_metric_vars and fail_check
- Handle Panorama stampede on restart after not running #26
  Added to settings and Panorama to allow to discard any checks older than
  PANORAMA_CHECK_MAX_AGE to prevent a stampede if desired, not ideal but solves
  the stampede problem for now - https://github.com/earthgecko/skyline/issues/26
- Added the original Skyline UI back as a then tab, for nostalgic and historical
  reasons.
- Bumped to version 1.0.8

## Ionosphere - v1.1.0-ionosphere-alpha

- Added alpha Ionosphere functionality using @blue-yonder / tsfresh feature
  extraction see docs/development/ionosphere.rst
- This also added a fairly generic script to extract_features from a &format=csv
  Graphite @graphite-project / graphite-web rendered csv for a single timeseries
  skyline/tsfresh/scripts/tsfresh_graphite_csv.py

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
