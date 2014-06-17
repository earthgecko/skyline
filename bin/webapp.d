#!/bin/bash
#
# This is used to start/stop webapp

BASEDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )/.."
RETVAL=0

# Determine the script to run depending on whether this script is in the source
# tree or installed
SCRIPT="${BASEDIR}/bin/skyline-webapp"
if [ -e "${BASEDIR}/setup.py" ]
then
    SCRIPT="${BASEDIR}/src/skyline/webapp/webapp.py"
fi

start () {
    rm -f $BASEDIR/src/webapp/*.pyc
    /usr/bin/env python $SCRIPT start
        RETVAL=$?
        if [[ $RETVAL -eq 0 ]]; then
            echo "started webapp"
        else
            echo "failed to start webapp"
        fi
        return $RETVAL
}

stop () {
    /usr/bin/env python $SCRIPT stop
        RETVAL=$?
        if [[ $RETVAL -eq 0 ]]; then
            echo "stopped webapp"
        else
            echo "failed to stop webapp"
        fi
        return $RETVAL
}

restart () {
    rm -f $BASEDIR/src/webapp/*.pyc
    /usr/bin/env python $SCRIPT restart
        RETVAL=$?
        if [[ $RETVAL -eq 0 ]]; then
            echo "restarted webapp"
        else
            echo "failed to restart webapp"
        fi
        return $RETVAL
}

run () {
    echo "running webapp"
    /usr/bin/env python $SCRIPT run
}

# See how we were called.
case "$1" in
  start)
    start
        ;;
  stop)
    stop
        ;;
  restart)
    restart
        ;;
  run)
    run
        ;;

  *)
        echo $"Usage: $0 {start|stop|run}"
        exit 2
        ;;
esac
