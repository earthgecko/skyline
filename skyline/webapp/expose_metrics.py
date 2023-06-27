import logging
import traceback

from prometheus_client import exposition
import requests

import settings
from functions.prometheus.generate_prometheus_metrics import generate_prometheus_metrics

skyline_app = 'webapp'
skyline_app_logger = '%sLog' % skyline_app
logger = logging.getLogger(skyline_app_logger)


# @added 20220725 -
def expose_metrics():
    """
    Generate Prometheus metrics and expose them.

    :return: metrics
    :rtype: str

    """

    scrape_response = None
    try:
        generate_prometheus_metrics('webapp')
        logger.info('/metrics expose_metrics :: generate_prometheus_metrics called')
    except Exception as err:
        trace = traceback.format_exc()
        message = 'error :: expose_metrics :: generate_prometheus_metrics failed - %s' % str(err)
        logger.error(trace)
        logger.error('error :: %s' % message)
    try:
        scrape_response = exposition.generate_latest()
    except Exception as err:
        logger.error(traceback.format_exc())
        logger.error('error :: expose_metrics :: exposition.generate_latest failed - %s' % (
            err))
    if scrape_response:
        scrape_response = str(scrape_response.decode())
    return scrape_response
