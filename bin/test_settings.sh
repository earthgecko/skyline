#!/bin/bash

# This is used to test the Skyline settings.py through the validate_settings.py
# function OUTSIDE the Skyline application runtimes.
# It prints out the test results and exits with 0 on a success and 1 on any
# failures.
CURRENT_DIR=$(dirname "${BASH_SOURCE[0]}")
BASEDIR=$(dirname "$CURRENT_DIR")

RETVAL=0

# Uses the existing /etc/skyline/skyline.conf
if [ -f /etc/skyline/skyline.conf ]; then
  # Test the config file has sensible variables
  bash -n /etc/skyline/skyline.conf
  if [ $? -eq 0 ]; then
    . /etc/skyline/skyline.conf
  else
    echo "error: There is a syntax error in /etc/skyline/skyline.conf, try bash -n /etc/skyline/skyline.conf"
    exit 1
  fi
fi
if [ ! -f /etc/skyline/skyline.conf ]; then
  echo "error: /etc/skyline/skyline.conf file not found"
  echo "error: test_settings.sh needs Python variables from /etc/skyline/skyline.conf, please see the installation page in the docs."
  exit 1
fi

USE_VIRTUALENV=0
if [ "$PYTHON_VIRTUALENV" == "true" ]; then
  if [ ! -f "$USE_PYTHON" ]; then
    echo "error: The python binary specified does not exists as specified as USE_PYTHON in /etc/skyline/skyline.conf"
    exit 1
  fi
# Test the python binary
  $USE_PYTHON --version > /dev/null 2>&1
  if [ $? -eq 0 ]; then
    USE_VIRTUALENV=1
  else
    echo "error: The python binary specified does not execute as expected as specified as USE_PYTHON in /etc/skyline/skyline.conf"
    exit 1
  fi
fi

echo "Running $BASEDIR/skyline/validate_settings_test.py"
if [ $USE_VIRTUALENV -eq 0 ]; then
  /usr/bin/env python "$BASEDIR/skyline/validate_settings_test.py"
else
  $USE_PYTHON "$BASEDIR/skyline/validate_settings_test.py"
fi
RETVAL=$?
echo "$BASEDIR/skyline/validate_settings_test.py exited with $RETVAL"
exit $RETVAL
