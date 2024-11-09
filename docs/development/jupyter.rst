*******
Jupyter
*******

Notes on various aspects of using Jupyter.

Patterns
========

Using Skyline logging
---------------------

When developing with Skyline using Jupyter it is useful to be able to access
any of the logging that is output by any functions or imported functions.  You
can use the normal Skyline logger functions in Jupyter using the following
pattern, which sets the skyline_app to skyline_notebook and logs any output from
logger calls to /tmp/skyline_notebook.log:

.. code-block:: python

    import logging

    skyline_app = 'skyline_notebook'
    skyline_app_logger = '%sLog' % skyline_app
    logger = logging.getLogger(skyline_app_logger)
    logfile = '/tmp/%s.log' % skyline_app
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter("%(asctime)s :: %(process)s :: %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    handler = logging.handlers.TimedRotatingFileHandler(
        logfile,
        when="midnight",
        interval=1,
        backupCount=5)
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    logger.info('This will log to /tmp/skyline_notebook')

