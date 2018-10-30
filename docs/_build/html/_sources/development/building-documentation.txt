======================
Building documentation
======================

Currently `Sphinx <http://www.sphinx-doc.org>`__ and sphinx-apidoc is being used
to documented the code and project, however to get working docs, a small
modification is required to the files outputted by sphinx-apidoc.

This is related to the package restructure undertaken to make the Skyline code
and layout more in line with a normal python package.  Although this seems to
have been achieved, the small hack to the sphinx-apidoc output suggests that
this is not 100% correct.  Further evidence of this is in terms of importing
from settings.py, the path needs to be appended in the code, which really should
not be required.  However, it is working and in the future this should be
figured and fixed.  Perhaps the below edit of the auto generated .rst files
could be achieved with a sphinx `conf.py` setting, if anyone knows please do
let us know :)

Part of the build
=================

The below documentation script not only builds the documentation, it also auto
generates some documentation and it also auto generates some Python code when
updates are required, such as the compilation of the tsfresh_features.py

For now...

Install docs-requirements.txt
=============================

In your Python virtualenv first pip install the required modules in
docs-requirements.txt (as per documented `Running in Python virtualenv
<running-in-python-virtualenv.html>`__)

.. code-block:: bash

    PYTHON_MAJOR_VERSION="2.7"
    PYTHON_VIRTUALENV_DIR="/opt/python_virtualenv"
    PROJECT="skyline-py2714"

    cd "${PYTHON_VIRTUALENV_DIR}/projects/${PROJECT}"
    source bin/activate

    bin/"pip${PYTHON_MAJOR_VERSION}" install -r /opt/skyline/github/skyline/docs-requirements.txt

    deactivate


Your Python interpretor
=======================

The docs/conf.py self interpolates the Python path if you are running in a
virtualenv in the documented manner.  If you are not you may need to change the
following to your python interpretor site-packages path by setting
python_site_packages_path in docs/conf.py, e.g.

.. code-block:: bash

  #    sys.path.insert(0, os.path.abspath('/opt/python_virtualenv/projects/skyline-py2714/lib/python2.7/site-packages'))
      sys.path.insert(0, os.path.abspath(python_site_packages_path))

.rst and .md wtf?
=================

The documentation is written in both .md and .rst format, because it can be
thanks to the awesome of Sphinx.  The original Skyline documentation was written
in .md for github.  The documentation is being ported over to .rst to allow for
the full functionality of Sphinx documentation.

Build
=====

.. code-block:: bash

  PYTHON_MAJOR_VERSION="2.7"
  PYTHON_VIRTUALENV_DIR="/opt/python_virtualenv"
  PROJECT="skyline-py2714"

  cd "${PYTHON_VIRTUALENV_DIR}/projects/${PROJECT}"
  source bin/activate

  function build_docs() {

    # Arguments:
    # APP_DIR - path to your Skyline dir, e.g.
    # build_docs  # e.g. ~/github/earthgecko/skyline/develop/skyline
    # pyflakes    # run pyflakes if passed

    if [ -n "$1" ]; then
      APPDIR=$1
    fi

    if [ -z "$APPDIR" ]; then
      echo "error: could not determine APPDIR - $APPDIR"
      return 1
    fi

    if [ ! -d "$APPDIR/docs" ]; then
      echo "error: directory not found - $APPDIR/docs"
      return 1
    fi

    # @added 20161119 - Branch #922: ionosphere
    #                   Task #1718: review.tsfresh
    # Build the pytz.rst page to generate the pytz timezone list for Skyline
    # Ionosphere and tsfresh, creates "$APPDIR/docs/development/pytz.rst"

    python${PYTHON_MAJOR_VERSION} "$APPDIR/skyline/tsfresh_features/scripts/make-pytz.all_timezones-rst.py"

    # Run tests
    ORIGINAL_DIR=$(pwd)
    cd "$APPDIR"
    python${PYTHON_MAJOR_VERSION} -m pytest tests/
    if [ $? -ne 0 ]; then
      echo "Tests failed not building documentation"
      return 1
    fi

    # @added 20170308 - Task #1966: Add pyflakes tests to build_docs
    #                   Feature #1960: ionosphere_layers
    if [ -n "$2" ]; then
      find "$APPDIR" -type f -name "*.py" | while read i_file
      do
        pyflakes "$i_file"
      done
    fi

    # @added 20170913 - Task #2160: Test skyline with bandit
    # For static analysis - https://github.com/openstack/bandit
    bandit -r "$APPDIR" -x "${APPDIR}/skyline/settings.py"

    cd "$APPDIR/docs"
    echo "Building Skyline documentation - in $APPDIR/docs"
    sphinx-apidoc --force -o "${APPDIR}/docs" "${APPDIR}/skyline" skyline

    # Inline edit all apidoc generated .rst files in docs/skyline.*rst
    for i in $(find "${APPDIR}/docs" -type f -name "skyline.*rst")
    do
      cat "$i" > "${i}.org"
      cat "${i}.org" | sed -e '/package/!s/automodule:: skyline\./automodule:: /g' > "$i"
      rm -f "${i}.org"
    done

    cd "$APPDIR/docs"
    make clean
    rm -rf _build/*
    make html
    for i in $(find "$APPDIR" -type f -name "*.pyc")
    do
      rm -f "$i"
    done
    for i in $(find "$APPDIR" -type d -name "__pycache__")
    do
      rm -rf "$i"
    done
    cd "$ORIGINAL_DIR"
  }

  # Usage: build_docs <app_dir>
  # e.g.
  # cd /opt/python_virtualenv/projects/skyline-ionosphere-py2714/
  # build_docs /home/gary/sandbox/of/github/earthgecko/skyline/ionosphere/skyline


Auto generating .rst files
==========================

This may be a little unconventional but it probably beats trying to do it via
Sphinx support custom extensions, without using generates or includes or Jinga
templating, which may or may not work with readthedocs.

The script skyline/tsfresh_features/scripts/make-pytz.all_timezones-rst.py introduces a
novel way to automatically generate the docs/development/pytz.rst during the
local build process to provide a list of all pytz timezones at the current
version.

This pattern could be reused fairly easier.

Building workflow diagrams with UML
===================================

This can be quite handy to make simple diagrams, if not finicky.  A good
resource is the PlantUML.com server is handy for making workflow diagrams,
without having to create and edit SVGs.

The docs/skyline.simplified.workflow.uml rendered by the PlantUML server:
`Simplified Skyline workflow with PlantUML server
<http://plantuml.com/plantuml/png/ZLFBJWCn3BpdAwozmmTKLQLo0IIGW0ekY4EwwrAhUQoSNLO5yU-upQEx2A6U4eyzCqwSTDGPXDLkRyWX1BAjeGrX0uFdtSRuGIbTTvvXsLXo53hMXsW-XvlUQWUBXLAryNq3NmhWrMB7L8Stq07INdqhvNo3K5spRhVKOQLKoi750Z8Aiyo0pXSqSv-meL3bw7w_UhmhqTOpVNedST4ItOHEPMyYg79IwexAqweZfDIhDiWTSZnE3hAhTjhiFv4hbNSmlxmiSiiCjhqneaeM0p9XW0rxcomAK_h8-iBLpbj9H2ZxhNtl6itxIkNTnEygW__v5UOPVfPOlsCrwJ5YmcZxi4qqtmS8g8ENkZBpL7XeS3JV-uZ-tM6PUEAUqM8hA6khnqqsVd12pQaHLGRxaD3YSLxrwThEgo_fN7zyhrBCM7jk4kTmSd8nbqyqu5gtRQNYUXhm68Y44N3wA-N2n7FwOMVnvkJr_lh8mazICtWX7E3v5Zv8mvhz8EFv0G00>`__

.. image:: http://www.plantuml.com/plantuml/png/PL5DRzim3BthLn3fELRI0aKNk3usBT01su9j6B0BRR6sO5ao93g1KUo_Zt7Y8DrEqllW8qMnPKUGlmNFK0KNukFD_VsdXwUdOjUEkJxonGVLcBlLxLtXhAUp33-fnBw79PIOR0LXJt5lwQ0KLXmY_0e3v3ay3nrwA0gbV304Yst4lh5CYvduHiQc2_eyxhw-Nj1XQh60TmNGOzLYBQGFXmLW0ZclwI_eGyGE9sq6ruP8PJoNdguYIWRaMGRJ4B01jXqbYfOI6EHZ-PmqsQRfn2PSXsu4GupMfNzjynl_3uGtsSovMgfEStoOeAKngJzAJUiJ_hG24Nx8Rk7x2-mjmh5AKfMAjcvOGWDJX5qOOaNGCFTfetLjkHdAh0gT9epSr5IrGQTsF7wYvaYaz-jidQsMfJnZQOCFgMyeaZQXIheSh3FMrqj17Nb65aVMSET--orvYiXae1odVjAkT-XvwDpKS0P_0G00

The UML source to the above image is:

.. code-block:: text

    @startuml
    title <font color=#6698FF>Sky</font><font color=#dd3023>line</font> Webapp - basic overview

    actor You << Human >>

    node "Graphite"

    node "Redis"

    node "MySQL"

    node "webapp" {
      package "now" as now
      package "Panorama" as Panorama
      package "rebrow" as rebrow
      now <.. Redis : timeseries data
      Panorama <.. MySQL : anomaly details
      Panorama <.. Graphite : timeseries data for anomaly
      rebrow <.. Redis : keys
    }

    You <.. webapp : View UI via browser

    right footer \nSource https://github.com/earthgecko/skyline/tree/v1.1.0-beta-ionosphere/docs/building-documentation.html\nGenerated by http://plantuml.com/plantuml
    @enduml
