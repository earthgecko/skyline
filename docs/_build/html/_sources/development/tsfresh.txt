*********************
Development - tsfresh
*********************

tsfresh in the Skyline context
==============================

Skyline uses its own internal list of tsfresh feature names.  When a tsfresh
update is required, this internal list of tsfresh feature names may need
additions in Skyline as well in skyline/tsfresh_feature_names.py

How Skyline was over complicatedly upgraded from tsfresh-0.1.2 to tsfresh-0.3.0
-------------------------------------------------------------------------------

.. note: This is how it has worked before, but sometimes things change and it
  might not.  Remember this is Skyline, just installing and setting it up is a
  bit of a chore and could not be described as simple.  And considering this is
  a development relate page...  I blame this on the tests that are maybe trying to be
  clever, but did and still do seem like a good idea.  Luckily no users really
  have to worry about this seemingly over complicated progress that undoubtedly
  could be achieved in a much more elegant manner.  I do apologise.  Further
  the full extent of how baseline versions, etc, should be handled was not
  necessarily thought out fully at the time the tests written or how this good
  idea of testing that calculated features on as baseline do not change version
  to version (that would raise some worrying questions, if the values changed),
  could become bothersome when an upgrade was attempted.
  However, it is still a good idea that just requires some tender, loving care,
  and probably careful planning, less crypt code and it seems sorting helped.

- Locally upgrade your tsfresh version in your Skyline Python virtualenv
- Run tests and if they fail we need to ensure that any new feature names are
  updated in skyline/tsfresh_feature_names.py
- Calculate the features of the baseline data set and compare the last version
  baseline e.g. tests/tsfresh-0.1.2.stats.statsd.bad_lines_seen.20161110.csv.features.transposed.csv

.. code-block: bash

  ####
  # START USER CONFIG

  # APP_DIR - path to your Skyline dir, e.g. ~/github/earthgecko/skyline/develop/skyline
  APP_DIR="<YOUR_APP_DIR>"

  # LAST_TSFRESH_BASELINE - the last version baseline which has a baseline e.g.
  # tests/tsfresh-0.1.2.stats.statsd.bad_lines_seen.20161110.csv.features.transposed.csv
  # e.g LAST_TSFRESH_BASELINE="0.1.2"

  LAST_TSFRESH_BASELINE="<YOUR_LAST_TSFRESH_BASELINE_VERSION>"

  # The last ID in skyline/tsfresh_feature_names.py
  LAST_FEATURE_NAME_ID=206

  PYTHON_MAJOR_VERSION="2.7"
  PYTHON_VIRTUALENV_DIR="/opt/python_virtualenv"
  PROJECT="skyline-py2712"

  # END USER CONFIG
  ####

  cd "${PYTHON_VIRTUALENV_DIR}/projects/${PROJECT}"
  source bin/activate

  W_DIR="/tmp/skyline/stats.statsd.bad_lines_seen"
  mkdir -p "$W_DIR"

  TEST_DIR="/tmp/skyline/testing"

  if [ -d "$TEST_DIR" ]; then
    rm -rf "$TEST_DIR"
  fi
  mkdir -p "$TEST_DIR"

  rsync -az --delete --exclude .git/ "$APP_DIR/" $TEST_DIR/

  cp $APP_DIR/tests/stats.statsd.bad_lines_seen.20161110.csv $W_DIR/stats.statsd.bad_lines_seen.20161110.csv

  cd "${PYTHON_VIRTUALENV_DIR}/projects/${PROJECT}"
  "bin/python${PYTHON_MAJOR_VERSION}" $TEST_DIR/skyline/tsfresh_features/scripts/tsfresh_graphite_csv.py $W_DIR/stats.statsd.bad_lines_seen.20161110.csv UTC

  # Compare
  cat "$TEST_DIR/tests/tsfresh-${LAST_TSFRESH_BASELINE}.stats.statsd.bad_lines_seen.20161110.csv.features.transposed.csv" | sort > "$W_DIR/tsfresh-${LAST_TSFRESH_BASELINE}.stats.statsd.bad_lines_seen.20161110.csv.features.transposed.csv"
  cat $W_DIR/stats.statsd.bad_lines_seen.20161110.csv.features.transposed.csv | sort > $W_DIR/stats.statsd.bad_lines_seen.20161110.csv.features.transposed.sorted.csv

  diff "$W_DIR/tsfresh-${LAST_TSFRESH_BASELINE}.stats.statsd.bad_lines_seen.20161110.csv.features.transposed.csv" $W_DIR/stats.statsd.bad_lines_seen.20161110.csv.features.transposed.sorted.csv

  # ONLY new additions should be present, if any.
  # NO names or values should change, if they do address as appropriate

- If you have verified that only new feature names have been added, to generate
  the TSFRESH_FEATURES list

.. code-block: bash

  rsync -avz --delete --exclude .git/ "$APP_DIR/" $TEST_DIR/
  USE_PYTHON="${PYTHON_VIRTUALENV_DIR}/projects/${PROJECT}/bin/python${PYTHON_MAJOR_VERSION}"

  cd $TEST_DIR/skyline
  $USE_PYTHON tsfresh_features/generate_tsfresh_features.py

- Update skyline/tsfresh_feature_names.py with new feature names AND tsfresh
  versions
- Add the new baseline for the version of tsfresh

.. code-block: bash

  # NEW_TSFRESH_VERSION e.g NEW_TSFRESH_VERSION="0.3.0"
  NEW_TSFRESH_VERSION="<NEW_VERSION>"

  # Copy new baseline
  cp $W_DIR/stats.statsd.bad_lines_seen.20161110.csv.features.transposed.csv $APP_DIR/tests/tsfresh-${NEW_TSFRESH_VERSION}.stats.statsd.bad_lines_seen.20161110.csv.features.transposed.csv
  cat /tmp/data.json.features.transposed.csv > $APP_DIR/tests/tsfresh-${NEW_TSFRESH_VERSION}.data.json.features.transposed.csv

- Run tests

.. code-block: bash

  rsync -avz --delete --exclude .git/ "$APP_DIR/" $TEST_DIR/

  USE_PYTHON="${PYTHON_VIRTUALENV_DIR}/projects/${PROJECT}/bin/python${PYTHON_MAJOR_VERSION}"

  cd $TEST_DIR

  $USE_PYTHON -m pytest tests/tsfresh_features_test.py
  $USE_PYTHON -m pytest tests/algorithms_test.py
  $USE_PYTHON -m pytest tests/
