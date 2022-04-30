"""
filesafe_metricnames.py
"""
import logging

from skyline_functions import filesafe_metricname

skyline_app = 'analyzer'
skyline_app_logger = '%sLog' % skyline_app
logger = logging.getLogger(skyline_app_logger)


# @added 20220421 - Task #3800: Handle feedback metrics in Mirage and waterfall alerts
def filesafe_names(self, unique_base_names):
    """
    Create a dict of filesafe names with base_names and the
    metrics_manager.filesafe_base_names Redis hash.

    :param current_skyline_app: the app calling the function
    :param unique_base_names: the unique_base_names list
    :type current_skyline_app: str
    :type unique_base_names: list
    :return: filesafe_base_names_dict
    :rtype: dict

    """
    new_filesafe_base_names_dict = {}
    errors = []

    known_filesafe_names = {}
    try:
        known_filesafe_names = self.redis_conn_decoded.hgetall('metrics_manager.filesafe_base_names')
    except Exception as err:
        logger.error('metrics_manager :: filesafe_base_names :: failed to hgetall metrics_manager.filesafe_base_names - %s' % str(err))
    known_filesafe_base_names = list(known_filesafe_names.values())
    logger.info('metrics_manager :: filesafe_base_names :: found %s entries to the metrics_manager.filesafe_base_names hash' % str(len(known_filesafe_names)))

    for i_base_name in unique_base_names:
        if i_base_name in known_filesafe_base_names:
            continue
        filesafe_base_name = str(i_base_name)
        try:
            filesafe_base_name = filesafe_metricname(i_base_name)
        except Exception as err:
            errors.append([i_base_name, err])
            continue
        if i_base_name != filesafe_base_name:
            new_filesafe_base_names_dict[filesafe_base_name] = i_base_name
    if errors:
        logger.error('error :: metrics_manager :: filesafe_base_names :: encountered errors with filesafe_metricname, last error reported - %s' % str(errors[-1][1]))

    if new_filesafe_base_names_dict:
        try:
            self.redis_conn_decoded.hset('metrics_manager.filesafe_base_names', mapping=new_filesafe_base_names_dict)
        except Exception as err:
            logger.error('metrics_manager :: filesafe_base_names :: failed to add entires to metrics_manager.filesafe_base_names Redis hash - %s' % str(err))
    logger.info('metrics_manager :: filesafe_base_names :: added %s entries to the metrics_manager.filesafe_base_names hash' % str(len(new_filesafe_base_names_dict)))

    # Manage removed metrics
    remove_entries = []
    for base_name in known_filesafe_base_names:
        if base_name not in unique_base_names:
            for filesafe_name in list(known_filesafe_names.keys()):
                if known_filesafe_names[filesafe_name] == base_name:
                    remove_entries.append(filesafe_name)
                    break
    if remove_entries:
        try:
            self.redis_conn_decoded.hdel('metrics_manager.filesafe_base_names', *set(remove_entries))
        except Exception as err:
            logger.error('metrics_manager :: filesafe_base_names :: failed to remove entires to metrics_manager.filesafe_base_names Redis hash - %s' % str(err))
    logger.info('metrics_manager :: filesafe_base_names :: removed %s entries to the metrics_manager.filesafe_base_names hash' % str(len(remove_entries)))

    return new_filesafe_base_names_dict
