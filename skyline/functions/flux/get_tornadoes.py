"""
get_tornadoes.py
"""
import logging
import os

import settings

FLUX_TORNADO_ENABLED = False
try:
    from settings import FLUX_TORNADO_ENABLED
except:
    FLUX_TORNADO_ENABLED = False
if FLUX_TORNADO_ENABLED:
    try:
        from settings import FLUX_TORNADO_URL
    except:
        FLUX_TORNADO_URL = None
    try:
        from settings import FLUX_SELF_API_KEY
    except:
        FLUX_SELF_API_KEY = None
    if not FLUX_TORNADO_URL and not FLUX_SELF_API_KEY:
        FLUX_TORNADO_ENABLED = False


# @added 20241127 - Feature #5198: flux - tornado
#                   Feature #5563: mirage_vortex - use tornado
def get_tornadoes(current_skyline_app):
    """
    Return a dict of what algorithms have a flux tornado implementation, include
    the tornado_url and key which each tornado algorithm.

    :param current_skyline_app: the app calling the function
    :type current_skyline_app: str
    :return: tornadoes
    :rtype: dict

    """
    tornadoes = {}
    if not FLUX_TORNADO_ENABLED:
        return tornadoes

    try:
        current_dir = os.path.dirname(__file__)
        tornadoes_dir = current_dir.replace('functions/flux', 'flux/tornadoes')
        for dir_path, folders, files in os.walk(tornadoes_dir):
            if files:
                for i in files:
                    algo_ = str(i).replace('tornado_', '')
                    algo = str(algo_).replace('.py', '')
                    # FLUX_TORNADO_URL = 'http://127.0.0.1:8000/tornado'
                    tornado_url = '%s?algorithm=%s' % (FLUX_TORNADO_URL, algo)
                    tornadoes[algo] = {
                        'tornado_url': tornado_url, 'key': FLUX_SELF_API_KEY
                    }
    except Exception as err:
        current_skyline_app_logger = current_skyline_app + 'Log'
        current_logger = logging.getLogger(current_skyline_app_logger)
        current_logger.error('error :: get_tornadoes :: failed to tornadoes, err: %s' % (
            err))

    return tornadoes
