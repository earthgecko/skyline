"""
common_motifs_to_validate.py
"""
import os
import logging
import traceback
from sqlalchemy.sql import select

import settings
from database import get_engine, ionosphere_table_meta
from functions.metrics.get_base_name_from_metric_id import get_base_name_from_metric_id

skyline_app = 'webapp'
skyline_app_logger = '%sLog' % skyline_app
logger = logging.getLogger(skyline_app_logger)
skyline_app_logfile = '%s/%s.log' % (settings.LOG_PATH, skyline_app)
logfile = '%s/%s.log' % (settings.LOG_PATH, skyline_app)


def get_common_motifs_to_validate(
        from_timestamp, until_timestamp, state='unvalidated', namespace=None):
    function_str = 'get_common_motifs_to_validate'

    common_motifs_fps = {}

    logger.info('%s :: determining all %s common_motifs feature profiles for namespace %s' % (
        function_str, state, str(namespace)))

    try:
        engine, fail_msg, trace = get_engine(skyline_app)
    except Exception as err:
        trace = traceback.format_exc()
        logger.error('%s' % trace)
        fail_msg = 'error :: %s :: failed to get MySQL engine, err: %s' % (function_str, err)
        logger.error('%s' % fail_msg)
        # return None, fail_msg, trace
        raise  # to webapp to return in the UI

    try:
        ionosphere_table, fail_msg, trace = ionosphere_table_meta(skyline_app, engine)
    except Exception as err:
        trace = traceback.format_exc()
        logger.error('%s' % trace)
        fail_msg = 'error ::  %s :: failed to get ionosphere_table meta, err: %s' % (function_str, err)
        logger.error('%s' % fail_msg)
        try:
            engine.dispose()
        except Exception as dispose_err:
            logger.error(traceback.format_exc())
            logger.error('error :: %s :: calling engine.dispose(), err: %s' % (function_str, dispose_err))
        raise  # to webapp to return in the UI

    try:
        #connection = engine.connect()
        if state == 'validated':
            # @modified 20260225 - Task #5176: Migrate to sqlalchemy v2 API
            #                      Task #5628: Build v5.0.0 and test
            #stmt = select([ionosphere_table]).\
            stmt = select(ionosphere_table).\
                where(ionosphere_table.c.anomaly_timestamp >= from_timestamp).\
                where(ionosphere_table.c.anomaly_timestamp <= until_timestamp).\
                where(ionosphere_table.c.validated == 1).\
                where(ionosphere_table.c.label == 'LEARNT - common_motifs').\
                where(ionosphere_table.c.enabled == 1)
        elif state == 'invalid':
            # @modified 20260225 - Task #5176: Migrate to sqlalchemy v2 API
            #                      Task #5628: Build v5.0.0 and test
            #stmt = select([ionosphere_table]).\
            stmt = select(ionosphere_table).\
                where(ionosphere_table.c.anomaly_timestamp >= from_timestamp).\
                where(ionosphere_table.c.anomaly_timestamp <= until_timestamp).\
                where(ionosphere_table.c.label == 'LEARNT - common_motifs').\
                where(ionosphere_table.c.enabled == 0)
        else:
            # @modified 20260225 - Task #5176: Migrate to sqlalchemy v2 API
            #                      Task #5628: Build v5.0.0 and test
            #stmt = select([ionosphere_table]).\
            stmt = select(ionosphere_table).\
                where(ionosphere_table.c.anomaly_timestamp >= from_timestamp).\
                where(ionosphere_table.c.anomaly_timestamp <= until_timestamp).\
                where(ionosphere_table.c.validated == 0).\
                where(ionosphere_table.c.label == 'LEARNT - common_motifs').\
                where(ionosphere_table.c.enabled == 1)
        # @modified 20260227 - Task #5176: Migrate to sqlalchemy v2 API
        #                      Task #5628: Build v5.0.0 and test
        #results = connection.execute(stmt)
        with engine.connect() as connection:
            result = connection.execute(stmt)
            results = [dict(row._mapping) for row in result.fetchall()]
        for row in results:
            try:
                fp_id = int(row['id'])
                common_motifs_fps[fp_id] = dict(row)
                # Coerce for json
                for key, item in common_motifs_fps[fp_id].items():
                    if 'decimal.Decimal' in str(type(common_motifs_fps[fp_id][key])):
                        common_motifs_fps[fp_id][key] = float(row[key])
                    if 'datetime.datetime' in str(type(common_motifs_fps[fp_id][key])):
                        common_motifs_fps[fp_id][key] = str(row[key])
                
            except Exception as row_err:
                logger.error('error :: %s :: bad row data, row: %s, err: %s' % (
                    function_str, str(row), row_err))
        #connection.close()
    except Exception as err:
        trace = traceback.format_exc()
        logger.error('%s' % trace)
        logger.error('error :: %s :: select error, err: %s' % (
            function_str, err))
        raise

    try:
        engine.dispose()
    except Exception as dispose_err:
        logger.error(traceback.format_exc())
        logger.error('error :: %s :: calling engine.dispose(), err: %s' % (function_str, dispose_err))

    remove_non_namespace_fps = []
    for fp_id, item in common_motifs_fps.items():
        metric_id = item['metric_id']
        try:
            base_name = get_base_name_from_metric_id(skyline_app, metric_id)
        except Exception as err:
            logger.error('error :: %s :: get_base_name_from_metric_id failed to determine base_name for metric_id: %s, err: %s' % (
                function_str, str(metric_id), str(err)))
            base_name = 'unknown'
        if namespace and namespace not in base_name:
            remove_non_namespace_fps.append(fp_id)
            continue
        common_motifs_fps[fp_id]['metric'] = base_name
        use_base_name = str(base_name)
        labelled_metric_name = None
        if '{' in base_name and '}' in base_name and '_tenant_id="' in base_name:
            labelled_metric_name = 'labelled_metrics.%s' % str(metric_id)
        if labelled_metric_name:
            use_base_name = str(labelled_metric_name)
        common_motifs_fps[fp_id]['labelled_metric_name'] = labelled_metric_name
        timeseries_dir = use_base_name.replace('.', '/')
        metric_data_dir = '%s/%s/%s' % (
            settings.IONOSPHERE_PROFILES_FOLDER, timeseries_dir,
            str(item['anomaly_timestamp']))
        validation_link = '%s/ionosphere?fp_view=true&timestamp=%s&metric=%s&validate_fp=true&format=json' % (
            settings.SKYLINE_URL, str(item['anomaly_timestamp']), use_base_name)
        validation_uri = '?fp_view=true&timestamp=%s&metric=%s&validate_fp=true&format=json' % (
            str(item['anomaly_timestamp']), use_base_name)
        graphs_url = '%s/ionosphere_images?image=' % (settings.SKYLINE_URL)
        common_motifs_fps[fp_id]['graphs'] = {}
        for dir_path, folders, files in os.walk(metric_data_dir):
            try:
                if files:
                    for i in files:
                        path_and_file = '%s/%s' % (dir_path, i)
                        if 'motif_annihilation.' in i:
                            if i.endswith('motif_annihilation.png'):
                                image_url = '%s%s' % (graphs_url, path_and_file)
                                common_motifs_fps[fp_id]['graphs']['features_profile'] = image_url
                            if i.endswith('motif_annihilation.derived.png'):
                                image_url = '%s%s' % (graphs_url, path_and_file)
                                common_motifs_fps[fp_id]['graphs']['derived'] = image_url
                            if i.endswith('motif_annihilation.pw5_timeseries.png'):
                                image_url = '%s%s' % (graphs_url, path_and_file)
                                common_motifs_fps[fp_id]['graphs']['pw5_timeseries'] = image_url
                                common_motifs_fps[fp_id]['validation_link'] = validation_link
                                common_motifs_fps[fp_id]['validation_uri'] = validation_uri
                        if 'common_motifs.' in i:
                            if i.endswith('common_motifs.png'):
                                image_url = '%s%s' % (graphs_url, path_and_file)
                                common_motifs_fps[fp_id]['graphs']['features_profile'] = image_url
                            if i.endswith('common_motifs.derived.png'):
                                image_url = '%s%s' % (graphs_url, path_and_file)
                                common_motifs_fps[fp_id]['graphs']['derived'] = image_url
                            if i.endswith('common_motifs.pw5_timeseries.png'):
                                image_url = '%s%s' % (graphs_url, path_and_file)
                                common_motifs_fps[fp_id]['graphs']['pw5_timeseries'] = image_url
                                common_motifs_fps[fp_id]['validation_link'] = validation_link
                                common_motifs_fps[fp_id]['validation_uri'] = validation_uri
            except Exception as err:
                logger.error('error :: %s :: os.walk failed on %s, err: %s' % (
                    function_str, str(metric_data_dir), err))
    if remove_non_namespace_fps:
        logger.info('%s :: removing %s unvalidated common_motifs feature profiles that are not for namespace %s' % (
            function_str, str(len(remove_non_namespace_fps)), str(namespace)))
        for fp_id in remove_non_namespace_fps:
            del common_motifs_fps[fp_id]

    return common_motifs_fps
