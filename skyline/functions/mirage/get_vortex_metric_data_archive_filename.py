"""
get_vortex_metric_data_archive_filename.py
"""
from os import walk


# @added 20221207 - Feature #4734: mirage_vortex
#                   Feature #4732: flux vortex
def get_vortex_metric_data_archive_filename(training_dir):
    """
    Return the path and filename of the metric_data_archive.

    :param training_dir: the training_dir
    :return: metric_data_archive
    :rtype: str

    """
    metric_data_archive = None
    for fpath, folders, files in walk(training_dir):
        for i_file in files:
            if i_file.startswith('vortex.metric_data.'):
                if i_file.endswith('.json.gz'):
                    metric_data_archive = '%s/%s' % (fpath, i_file)
                    return metric_data_archive
    return metric_data_archive
