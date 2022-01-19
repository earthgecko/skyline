from __future__ import division
import redis
import logging
import simplejson as json
import sys
import re
import csv
import traceback
from msgpack import Unpacker
from functools import wraps
from flask import (
    Flask, request, render_template, redirect, Response, abort, flash,
    send_file, jsonify)
from daemon import runner
from os.path import isdir
from os import path
import string
from os import remove as os_remove
from time import sleep

# @added 20201126 - Feature #3850: webapp - yhat_values API endoint
from timeit import default_timer as timer

# @added 20160703 - Feature #1464: Webapp Redis browser
import time
# @modified 20180918 - Feature #2602: Graphs in search_features_profiles
# from datetime import datetime, timedelta
from datetime import timedelta

import os
import base64

# @added 20220112 - Bug #4374: webapp - handle url encoded chars
# @modified 20220115 - Bug #4374: webapp - handle url encoded chars
# Roll back change - breaking existing metrics with colons
# import urllib.parse

# @added 20210415 - Feature #4014: Ionosphere - inference
from shutil import rmtree

# flask things for rebrow
# @modified 20180918 - Feature #2602: Graphs in search_features_profiles
# from flask import session, g, url_for, flash, Markup, json
from flask import url_for, Markup

# For secret_key
import uuid

# @added 20170122 - Feature #1872: Ionosphere - features profile page by id only
# Determine the features profile dir path for a fp_id
import datetime
# from pytz import timezone
import pytz

# @added 20180520 - Feature #2378: Add redis auth to Skyline and rebrow
# Added auth to rebrow as per https://github.com/marians/rebrow/pull/20 by
# elky84
from six.moves.urllib.parse import quote

# @modified 20180918 - Feature #2602: Graphs in search_features_profiles
# from features_profile import feature_name_id, calculate_features_profile
from features_profile import calculate_features_profile

from tsfresh_feature_names import TSFRESH_VERSION

# @modified 20180526 - Feature #2378: Add redis auth to Skyline and rebrow
# Use PyJWT instead of pycryptodome
# from Crypto.Cipher import AES
# import base64
# @added 20180526 - Feature #2378: Add redis auth to Skyline and rebrow
import jwt
# @added 20180527 - Feature #2378: Add redis auth to Skyline and rebrow
import hashlib
from sys import version_info
from ast import literal_eval

# @added 20180721 - Feature #2464: luminosity_remote_data
# Use a gzipped response as the response as raw preprocessed time series
# added cStringIO, gzip and functools to implement Gzip for particular views
# http://flask.pocoo.org/snippets/122/
from flask import after_this_request
# from cStringIO import StringIO as IO
import gzip
# @added 20180721 - Feature #2464: luminosity_remote_data
# Use a gzipped response as the response as raw preprocessed time series
# added cStringIO to implement Gzip for particular views
# http://flask.pocoo.org/snippets/122/
import io as IO

import functools

# @modified 20210421 - Task #4030: refactoring
# from logging.handlers import TimedRotatingFileHandler, MemoryHandler

import os.path
# @added 20190116 - Cross-Site Scripting Security Vulnerability #85
#                   Bug #2816: Cross-Site Scripting Security Vulnerability
from flask import escape as flask_escape

# @added 20191029 - Task #3302: Handle csv.reader in py3
#                   Branch #3262: py3
import codecs

# @added 20220112 - Bug #4374: webapp - handle url encoded chars
# @modified 20220115 - Bug #4374: webapp - handle url encoded chars
# Roll back change - breaking existing metrics with colons
# from werkzeug.urls import iri_to_uri

if True:
    sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
    sys.path.insert(0, os.path.dirname(__file__))
    import settings
    from validate_settings import validate_settings_variables
    import skyline_version
    from skyline_functions import (
        get_graphite_metric,
        # @added 20170604 - Feature #2034: analyse_derivatives
        in_list,
        # @added 20180804 - Feature #2488: Allow user to specifically set metric as a derivative metric in training_data
        set_metric_as_derivative,
        # @added 20190510 - Feature #2990: Add metrics id to relevant web pages
        # get_memcache_metric_object,
        # @added 20190920 - Feature #3230: users DB table
        #                   Ideas #2476: Label and relate anomalies
        #                   Feature #2516: Add label to features profile
        get_user_details,
        # @added 20191030 - Bug #3266: py3 Redis binary objects not strings
        #                   Branch #3262: py3
        # Added a single functions to deal with Redis connection and the
        # charset='utf-8', decode_responses=True arguments required in py3
        get_redis_conn, get_redis_conn_decoded,
        # @added 20200813 - Feature #3670: IONOSPHERE_CUSTOM_KEEP_TRAINING_TIMESERIES_FOR
        historical_data_dir_exists,
        # @added 20201202- Feature #3858: skyline_functions - correlate_or_relate_with
        correlate_or_relate_with,
        # @added 20210324 - Feature #3642: Anomaly type classification
        get_anomaly_type,
    )

    from backend import (
        panorama_request, get_list,
        # @added 20180720 - Feature #2464: luminosity_remote_data
        luminosity_remote_data,
        # @added 20200908 - Feature #3740: webapp - anomaly API endpoint
        panorama_anomaly_details,
        # @added 20201103 - Feature #3824: get_cluster_data
        get_cluster_data,
        # @added 20201125 - Feature #3850: webapp - yhat_values API endoint
        get_yhat_values,
        # @added 20210326 - Feature #3994: Panorama - mirage not anomalous
        get_mirage_not_anomalous_metrics,
        # @added 20210328 - Feature #3994: Panorama - mirage not anomalous
        plot_not_anomalous_metric,
        # @added 20210617 - Feature #4144: webapp - stale_metrics API endpoint
        #                   Feature #4076: CUSTOM_STALE_PERIOD
        #                   Branch #1444: thunder
        namespace_stale_metrics,
    )
    from ionosphere_backend import (
        ionosphere_data, ionosphere_metric_data,
        # @modified 20170114 - Feature #1854: Ionosphere learn
        # Decoupled create_features_profile from ionosphere_backend
        # ionosphere_get_metrics_dir, create_features_profile,
        ionosphere_get_metrics_dir,
        features_profile_details,
        # @added 20170118 - Feature #1862: Ionosphere features profiles search page
        ionosphere_search,
        # @added 20170305 - Feature #1960: ionosphere_layers
        create_ionosphere_layers, feature_profile_layers_detail,
        feature_profile_layer_alogrithms,
        # @added 20170308 - Feature #1960: ionosphere_layers
        # To present the operator with the existing layers and algorithms for the metric
        metric_layers_alogrithms,
        # @added 20170327 - Feature #2004: Ionosphere layers - edit_layers
        #                   Task #2002: Review and correct incorrectly defined layers
        edit_ionosphere_layers,
        # @added 20170402 - Feature #2000: Ionosphere - validated
        validate_fp,
        # @added 20170617 - Feature #2054: ionosphere.save.training_data
        save_training_data_dir,
        # added 20170908 - Feature #2056: ionosphere - disabled_features_profiles
        features_profile_family_tree, disable_features_profile_family_tree,
        # @added 20170916 - Feature #1996: Ionosphere - matches page
        get_fp_matches,
        # @added 20170917 - Feature #1996: Ionosphere - matches page
        get_matched_id_resources,
        # @added 20180812 - Feature #2430: Ionosphere validate learnt features profiles page
        get_features_profiles_to_validate,
        # @added 20180815 - Feature #2430: Ionosphere validate learnt features profiles page
        get_metrics_with_features_profiles_to_validate,
        # @added 20181205 - Bug #2746: webapp time out - Graphs in search_features_profiles
        #                   Feature #2602: Graphs in search_features_profiles
        ionosphere_show_graphs,
        # @added 20190502 - Branch #2646: slack
        webapp_update_slack_thread,
        # @added 20190601 - Feature #3084: Ionosphere - validated matches
        validate_ionosphere_match,
        # @added 20200226: Ideas #2476: Label and relate anomalies
        #                  Feature #2516: Add label to features profile
        label_anomalies,
        # @added 20201213 - Feature #3890: metrics_manager - sync_cluster_files
        expected_features_profiles_dirs,
        # @added 20210413 - Feature #4014: Ionosphere - inference
        #                   Branch #3590: inference
        get_matched_motifs,
        # @added 20210419 - Feature #4014: Ionosphere - inference
        get_matched_motif_id,
    )

    # @added 20210107 - Feature #3934: ionosphere_performance
    from ionosphere_performance import get_ionosphere_performance

    # @added 20210415 - Feature #4014: Ionosphere - inference
    from on_demand_motif_analysis import on_demand_motif_analysis

    # @added 20210419 - Feature #4014: Ionosphere - inference
    from motif_plots import plot_motif_match

    # from utilites import alerts_matcher
    # @added 20201212 - Feature #3880: webapp - utilities - match_metric
    from matched_or_regexed_in_list import matched_or_regexed_in_list

    # @added 20170114 - Feature #1854: Ionosphere learn
    # Decoupled the create_features_profile from ionosphere_backend and moved to
    # ionosphere_functions so it can be used by ionosphere/learn
    from ionosphere_functions import (
        create_features_profile, get_ionosphere_learn_details,
        # @added 20180414 - Branch #2270: luminosity
        get_correlations,
        # @added 20200113 - Feature #3390: luminosity related anomalies
        #                   Branch #2270: luminosity
        get_related)

    # @added 20200420 - Feature #3500: webapp - crucible_process_metrics
    #                   Feature #1448: Crucible web UI
    #                   Branch #868: crucible
    from crucible_backend import (
        submit_crucible_job, get_crucible_jobs, get_crucible_job,
        send_crucible_job_metric_to_panorama)

    # @added 20210317 - Feature #3978: luminosity - classify_metrics
    #                   Feature #3642: Anomaly type classification
    from luminosity_backend import get_classify_metrics

    # @added 20210604 - Branch #1444: thunder
    from functions.metrics.get_top_level_namespaces import get_top_level_namespaces

    # @added 20210617 - Feature #4144: webapp - stale_metrics API endpoint
    #                   Feature #4076: CUSTOM_STALE_PERIOD
    #                   Branch #1444: thunder
    from functions.redis.get_metric_timeseries import get_metric_timeseries
    from functions.timeseries.determine_data_sparsity import determine_data_sparsity

    # @added 20210710 - Bug #4168: webapp/ionosphere_backend.py - handle old features profile dir not been found
    from functions.database.queries.get_all_db_metric_names import get_all_db_metric_names

    # @added 20210720 - Feature #4188: metrics_manager.boundary_metrics
    from get_boundary_metrics import get_boundary_metrics

    # @added 20210727 - Feature #4206: webapp - saved_training_data page
    from get_saved_training_data import get_saved_training_data

    # @added 20210825 - Feature #4164: luminosity - cloudbursts
    from luminosity_cloudbursts import get_cloudbursts
    from luminosity_plot_cloudburst import get_cloudburst_plot

    # @added 20211001 - Feature #4264: luminosity - cross_correlation_relationships
    from functions.metrics.get_related_metrics import get_related_metrics
    from functions.metrics.get_metric_id_from_base_name import get_metric_id_from_base_name
    # from functions.database.queries.base_name_from_metric_id import base_name_from_metric_id
    from functions.metrics.get_base_name_from_metric_id import get_base_name_from_metric_id
    from functions.database.queries.get_metric_group_info import get_metric_group_info
    from functions.metrics.get_related_to_metric_groups import get_related_to_metric_groups

    # @added 20211102 - Branch #3068: SNAB
    #                   Bug #4308: matrixprofile - fN on big drops
    from functions.database.queries.get_algorithms import get_algorithms
    from functions.database.queries.get_algorithm_groups import get_algorithm_groups
    from get_snab_results import get_snab_results

    # @added 20211125 - Feature #4326: webapp - panorama_plot_anomalies
    from panorama_plot_anomalies import panorama_plot_anomalies

    # @added 20220114 - Feature #4376: webapp - update_external_settings
    from functions.settings.manage_external_settings import manage_external_settings

    # @added 20220117 - Feature #4324: flux - reload external_settings
    #                   Feature #4376: webapp - update_external_settings
    from functions.flux.reload_flux import reload_flux

# @added 20200516 - Feature #3538: webapp - upload_data endoint
file_uploads_enabled = False
try:
    flux_process_uploads = settings.FLUX_PROCESS_UPLOADS
except:
    flux_process_uploads = False
if flux_process_uploads:
    try:
        file_uploads_enabled = settings.WEBAPP_ACCEPT_DATA_UPLOADS
    except:
        file_uploads_enabled = False
if file_uploads_enabled:
    from werkzeug.utils import secure_filename
    from skyline_functions import mkdir_p, write_data_to_file
    try:
        DATA_UPLOADS_PATH = settings.DATA_UPLOADS_PATH
    except:
        DATA_UPLOADS_PATH = '/tmp/skyline/data_uploads'
    ALLOWED_EXTENSIONS = {'json', 'csv', 'xlsx', 'xls', 'zip', 'gz'}
    ALLOWED_FORMATS = {'csv', 'xlsx', 'xls'}
    # @modified 20200520 - Bug #3552: flux.uploaded_data_worker - tar.gz
    # tar.gz needs more work
    # ALLOWED_ARCHIVES = {'none', 'zip', 'gz', 'tar_gz'}
    ALLOWED_ARCHIVES = {'none', 'zip', 'gz'}
    try:
        flux_upload_keys = settings.FLUX_UPLOADS_KEYS
    except:
        flux_upload_keys = {}

# @added 20200929 - Task #3748: POC SNAB
#                   Branch #3068: SNAB
try:
    SNAB_ENABLED = settings.SNAB_ENABLED
except:
    SNAB_ENABLED = False
if SNAB_ENABLED:
    from snab_backend import update_snab_result
else:
    update_snab_result = None

# @added 20201127 - Feature #3820: HORIZON_SHARDS
# Added the HORIZON_SHARD variable to add to alerts for determining which
# Skyline instance sent the alert
try:
    HORIZON_SHARDS = settings.HORIZON_SHARDS.copy()
except:
    HORIZON_SHARDS = {}
this_host = str(os.uname()[1])
HORIZON_SHARD = 0
if HORIZON_SHARDS:
    HORIZON_SHARD = HORIZON_SHARDS[this_host]

# @added 20201210 - Feature #3824: get_cluster_data
try:
    AUTH_DEBUG = settings.AUTH_DEBUG
except:
    AUTH_DEBUG = False
try:
    PASS_AUTH_ON_REDIRECT = settings.PASS_AUTH_ON_REDIRECT
except:
    PASS_AUTH_ON_REDIRECT = False

# @added 20210324 - Feature #3642: Anomaly type classification
try:
    LUMINOSITY_CLASSIFY_ANOMALIES = settings.LUMINOSITY_CLASSIFY_ANOMALIES
except:
    LUMINOSITY_CLASSIFY_ANOMALIES = False

skyline_version = skyline_version.__absolute_version__

skyline_app = 'webapp'
skyline_app_logger = '%sLog' % skyline_app
logger = logging.getLogger(skyline_app_logger)
skyline_app_logfile = '%s/%s.log' % (settings.LOG_PATH, skyline_app)
skyline_app_loglock = '%s.lock' % skyline_app_logfile
skyline_app_logwait = '%s.wait' % skyline_app_logfile
logfile = '%s/%s.log' % (settings.LOG_PATH, skyline_app)

# werkzeug access log for Python errors
access_logger = logging.getLogger('werkzeug')

python_version = int(version_info[0])

# @modified 20180519 - Feature #2378: Add redis auth to Skyline and rebrow
# @modified 20191030 - Bug #3266: py3 Redis binary objects not strings
#                      Branch #3262: py3
# Use get_redis_conn and get_redis_conn_decoded
# if settings.REDIS_PASSWORD:
#    # @modified 20190130 - Bug #3266: py3 Redis binary objects not strings
#    #                      Branch #3262: py3
#    # REDIS_CONN = redis.StrictRedis(password=settings.REDIS_PASSWORD, unix_socket_path=settings.REDIS_SOCKET_PATH)
#    # REDIS_CONN = redis.StrictRedis(password=settings.REDIS_PASSWORD, unix_socket_path=settings.REDIS_SOCKET_PATH, charset='utf-8', decode_responses=True)
#    # @added 20191015 - Bug #3266: py3 Redis binary objects not strings
#    #                   Branch #3262: py3
#    # REDIS_CONN_UNDECODE = redis.StrictRedis(password=settings.REDIS_PASSWORD, unix_socket_path=settings.REDIS_SOCKET_PATH)
# else:
#    # REDIS_CONN = redis.StrictRedis(unix_socket_path=settings.REDIS_SOCKET_PATH)
#    # REDIS_CONN = redis.StrictRedis(unix_socket_path=settings.REDIS_SOCKET_PATH, charset='utf-8', decode_responses=True)
#    # @added 20191015 - Bug #3266: py3 Redis binary objects not strings
#    #                   Branch #3262: py3
#    # REDIS_CONN_UNDECODE = redis.StrictRedis(unix_socket_path=settings.REDIS_SOCKET_PATH)

# @added 20191030 - Bug #3266: py3 Redis binary objects not strings
#                   Branch #3262: py3
# Added a single functions to deal with Redis connection and the
# charset='utf-8', decode_responses=True arguments required in py3
REDIS_CONN_UNDECODE = get_redis_conn(skyline_app)
REDIS_CONN = get_redis_conn_decoded(skyline_app)

ENABLE_WEBAPP_DEBUG = False

app = Flask(__name__)

# @modified 20190502 - Branch #2646: slack
# Reduce logging, removed gunicorn
# gunicorn_error_logger = logging.getLogger('gunicorn.error')
# logger.handlers.extend(gunicorn_error_logger.handlers)
# logger.setLevel(logging.DEBUG)

# app.secret_key = str(uuid.uuid5(uuid.NAMESPACE_DNS, settings.GRAPHITE_HOST))
secret_key = str(uuid.uuid5(uuid.NAMESPACE_DNS, settings.GRAPHITE_HOST))
app.secret_key = secret_key

app.config['PROPAGATE_EXCEPTIONS'] = True

app.config.update(
    SESSION_COOKIE_NAME='skyline',
    SESSION_COOKIE_SECURE=True,
    SECRET_KEY=secret_key
)

if file_uploads_enabled:
    app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024
    app.config['DATA_UPLOADS_PATH'] = DATA_UPLOADS_PATH

graph_url_string = str(settings.GRAPH_URL)
PANORAMA_GRAPH_URL = re.sub('\/render.*', '', graph_url_string)

# @added 20160727 - Bug #1524: Panorama dygraph not aligning correctly
# Defaults for momentjs to work if the setttings.py was not updated
try:
    WEBAPP_USER_TIMEZONE = settings.WEBAPP_USER_TIMEZONE
except:
    WEBAPP_USER_TIMEZONE = True
try:
    WEBAPP_FIXED_TIMEZONE = settings.WEBAPP_FIXED_TIMEZONE
except:
    WEBAPP_FIXED_TIMEZONE = 'Etc/GMT+0'
try:
    WEBAPP_JAVASCRIPT_DEBUG = settings.WEBAPP_JAVASCRIPT_DEBUG
except:
    WEBAPP_JAVASCRIPT_DEBUG = False

# @added 20190520 - Branch #3002: docker
try:
    GRAPHITE_RENDER_URI = settings.GRAPHITE_RENDER_URI
except:
    GRAPHITE_RENDER_URI = 'render'

# @added 20200731 - Feature #3654: IONOSPHERE_GRAPHITE_NOW_GRAPHS_OVERRIDE
try:
    IONOSPHERE_GRAPHITE_NOW_GRAPHS_OVERRIDE = settings.IONOSPHERE_GRAPHITE_NOW_GRAPHS_OVERRIDE
except:
    IONOSPHERE_GRAPHITE_NOW_GRAPHS_OVERRIDE = False

# @added 20200813 - Feature #3670: IONOSPHERE_CUSTOM_KEEP_TRAINING_TIMESERIES_FOR
try:
    IONOSPHERE_HISTORICAL_DATA_FOLDER = settings.IONOSPHERE_HISTORICAL_DATA_FOLDER
except:
    IONOSPHERE_HISTORICAL_DATA_FOLDER = '/opt/skyline/ionosphere/historical_data'
try:
    IONOSPHERE_CUSTOM_KEEP_TRAINING_TIMESERIES_FOR = settings.IONOSPHERE_CUSTOM_KEEP_TRAINING_TIMESERIES_FOR
except:
    IONOSPHERE_CUSTOM_KEEP_TRAINING_TIMESERIES_FOR = []

# @added 20210112 - Feature #3934: ionosphere_performance
ionosphere_dir = path.dirname(settings.IONOSPHERE_DATA_FOLDER)
performance_dir = '%s/performance' % (ionosphere_dir)

# @added 20210209 - Feature #1448: Crucible web UI
try:
    crucible_jobs_dir = '%s/jobs' % path.dirname(settings.CRUCIBLE_DATA_FOLDER)
except:
    crucible_jobs_dir = '/opt/skyline/crucible/jobs'

# @added 20210316 - Feature #3978: luminosity - classify_metrics
#                   Feature #3642: Anomaly type classification
try:
    luminosity_data_folder = settings.LUMINOSITY_DATA_FOLDER
except:
    luminosity_data_folder = '/opt/skyline/luminosity'


@app.before_request
# def setup_logging():
#    if not app.debug:
#        stream_handler = logging.StreamHandler()
#        stream_handler.setLevel(logging.DEBUG)
#        app.logger.addHandler(stream_handler)
#        import logging
#        from logging.handlers import TimedRotatingFileHandler, MemoryHandler
#        file_handler = MemoryHandler(app_logfile, mode='a')
#        file_handler.setLevel(logging.DEBUG)
#        app.logger.addHandler(file_handler)
#
#        formatter = logging.Formatter("%(asctime)s :: %(process)s :: %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
#        handler = logging.handlers.TimedRotatingFileHandler(
#            app_logfile,
#            when="midnight",
#            interval=1,
#            backupCount=5)
#        handler.setLevel(logging.DEBUG)
#
#        memory_handler = logging.handlers.MemoryHandler(100,
#                                                        flushLevel=logging.DEBUG,
#                                                        target=handler)
#        handler.setFormatter(formatter)
#        app.logger.addHandler(memory_handler)
#        app.logger.addHandler(handler)
def limit_remote_addr():
    """
    This function is called to check if the requesting IP address is in the
    settings.WEBAPP_ALLOWED_IPS array, if not 403.
    """
    ip_allowed = False
    for web_allowed_ip in settings.WEBAPP_ALLOWED_IPS:
        if request.remote_addr == web_allowed_ip:
            ip_allowed = True

    if not settings.WEBAPP_IP_RESTRICTED:
        ip_allowed = True

    if not ip_allowed:
        abort(403)  # Forbidden


# @added 20200514 - Feature #3538: webapp - upload_data endoint
def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


def check_auth(username, password):
    """This function is called to check if a username /
    password combination is valid.
    """
    # @added 20200814 - Feature #3670: IONOSPHERE_CUSTOM_KEEP_TRAINING_TIMESERIES_FOR
    # Log full request
    logger.info('request :: %s' % (request.url))

    if settings.WEBAPP_AUTH_ENABLED:
        return username == settings.WEBAPP_AUTH_USER and password == settings.WEBAPP_AUTH_USER_PASSWORD
    else:
        return True


def authenticate():
    """Sends a 401 response that enables basic auth"""
    return Response(
        'Forbidden', 401,
        {'WWW-Authenticate': 'Basic realm="Login Required"'})


def requires_auth(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if settings.WEBAPP_AUTH_ENABLED:
            auth = request.authorization
            if not auth or not check_auth(auth.username, auth.password):
                return authenticate()
            return f(*args, **kwargs)
        else:
            return True
    return decorated


# @added 20180721 - Feature #2464: luminosity_remote_data
# Use a gzipped response as the response as raw preprocessed time series with
# an implementation of Gzip for particular views
# http://flask.pocoo.org/snippets/122/
def gzipped(f):
    @functools.wraps(f)
    def view_func(*args, **kwargs):
        @after_this_request
        def zipper(response):
            accept_encoding = request.headers.get('Accept-Encoding', '')

            if 'gzip' not in accept_encoding.lower():
                return response

            response.direct_passthrough = False

            if (response.status_code < 200 or response.status_code >= 300 or 'Content-Encoding' in response.headers):
                return response
            gzip_buffer = IO()
            gzip_file = gzip.GzipFile(mode='wb',
                                      fileobj=gzip_buffer)
            gzip_file.write(response.data)
            gzip_file.close()

            response.data = gzip_buffer.getvalue()
            response.headers['Content-Encoding'] = 'gzip'
            response.headers['Vary'] = 'Accept-Encoding'
            response.headers['Content-Length'] = len(response.data)

            return response

        return f(*args, **kwargs)

    return view_func


# @added 20220112 - Bug #4374: webapp - handle url encoded chars
def url_encode_metric_name(metric_name):
    """
    URL Encode a metric name
    """
    url_encoded_metric_name = False
    if ':' in metric_name:
        url_encoded_metric_name = True
    if '/' in metric_name:
        url_encoded_metric_name = True
    if url_encoded_metric_name:
        metric_name = urllib.parse.quote(metric_name, safe='')
        logger.info('url encoded metric_name to %s' % metric_name)
    return metric_name


@app.errorhandler(500)
def internal_error(message, traceback_format_exc):
    """
    Show traceback in the browser when running a flask app on a production
    server.
    By default, flask does not show any useful information when running on a
    production server.
    By adding this view, we output the Python traceback to the error 500 page
    and log.

    As per:
    Show flask traceback when running on production server
    https://gist.github.com/probonopd/8616a8ff05c8a75e4601 - Python traceback
    rendered nicely by Jinja2

    This can be tested by hitting SKYLINE_URL/a_500

    """
    fail_msg = str(message)
    trace = str(traceback_format_exc)
    logger.debug('debug :: returning 500 as there was an error, why else would 500 be returned...')
    logger.debug('debug :: sending the user that caused the error to happen, this useful information')
    # logger.debug('debug :: but they or I may have already emailed it to you, you should check your inbox')
    logger.debug('%s' % str(traceback_format_exc))
    logger.debug('debug :: which is accompanied with the message')
    logger.debug('debug :: %s' % str(message))
    logger.debug('debug :: request url :: %s' % str(request.url))
    logger.debug('debug :: request referrer :: %s' % str(request.referrer))
    # resp = '<pre>%s</pre><pre>%s</pre>' % (str(message), str(traceback_format_exc))
#    return(resp), 500
    server_name = settings.SERVER_METRICS_NAME
    return render_template(
        'traceback.html', version=skyline_version,
        message=fail_msg, traceback=trace, bad_machine=server_name), 500


@app.route("/")
@requires_auth
def index():

    start = time.time()
    if 'uh_oh' in request.args:
        try:
            return render_template(
                'uh_oh.html', version=skyline_version,
                message="Testing uh_oh"), 200
        except:
            error_string = traceback.format_exc()
            logger.error('error :: failed to render uh_oh.html: %s' % str(error_string))
            return 'Uh oh ... a Skyline 500 :(', 500

    try:
        return render_template(
            'now.html', version=skyline_version,
            duration=(time.time() - start)), 200
    except:
        error_string = traceback.format_exc()
        logger.error('error :: failed to render index.html: %s' % str(error_string))
        return 'Uh oh ... a Skyline 500 :(', 500


@app.route("/a_500")
@requires_auth
def a_500():

    if 'message' in request.args:
        message = request.args.get(str('message'), None)
        logger.debug('debug :: message - %s' % str(message))
        test_500_string = message
    else:
        logger.debug('debug :: testing /a_500 route and app.errorhandler(500) internal_error function')
        message = 'Testing app.errorhandler(500) internal_error function, if you are seeing this it works - OK'
        test_500_string = 'This is a test to generate a ValueError and a HTTP response of 500 and display the traceback'

    try:
        test_errorhandler_500_internal_error = int(test_500_string)
        logger.debug(
            'debug :: test_errorhandler_500_internal_error tests OK with %s' % (
                str(test_errorhandler_500_internal_error)))
    except:
        trace = traceback.format_exc()
        logger.debug('debug :: test OK')
        return internal_error(message, trace)

    error_msg = 'failed test of /a_500 route and app.errorhandler(500) internal_error function'
    logger.error('error :: %s' % error_msg)
    resp = json.dumps({'results': error_msg})
    return resp, 501


@app.route("/now")
@requires_auth
def now():
    start = time.time()
    try:
        return render_template(
            'now.html', version=skyline_version, duration=(time.time() - start)), 200
    except:
        error_string = traceback.format_exc()
        logger.error('error :: failed to render now.html: %s' % str(error_string))
        return 'Uh oh ... a Skyline 500 :(', 500


@app.route("/then")
@requires_auth
def then():
    try:
        return render_template('then.html'), 200
    except:
        error_string = traceback.format_exc()
        logger.error('error :: failed to render then.html: %s' % str(error_string))
        return 'Uh oh ... a Skyline 500 :(', 500


@app.route("/anomalies.json")
def anomalies():
    try:
        anomalies_json = path.abspath(path.join(path.dirname(__file__), '..', settings.ANOMALY_DUMP))
        with open(anomalies_json, 'r') as f:
            json_data = f.read()
    except:
        logger.error('error :: failed to get anomalies.json: ' + traceback.format_exc())
        return 'Uh oh ... a Skyline 500 :(', 500
    return json_data, 200


@app.route("/panorama.json")
def panorama_anomalies():
    try:
        anomalies_json = path.abspath(path.join(path.dirname(__file__), '..', settings.ANOMALY_DUMP))
        # @modified 20191014 - Task #3270: Deprecate string.replace for py3
        # panorama_json = string.replace(str(anomalies_json), 'anomalies.json', 'panorama.json')
        panorama_json = anomalies_json.replace('anomalies.json', 'panorama.json')
        logger.info('opening - %s' % panorama_json)
        with open(panorama_json, 'r') as f:
            json_data = f.read()
    except:
        logger.error('error :: failed to get panorama.json: ' + traceback.format_exc())
        return 'Uh oh ... a Skyline 500 :(', 500
    return json_data, 200


@app.route("/panorama_not_anomalous.json")
def panorama_not_anomalous():
    try:
        anomalies_json = path.abspath(path.join(path.dirname(__file__), '..', settings.ANOMALY_DUMP))
        panorama_not_anomalous_json = anomalies_json.replace('anomalies.json', 'panorama_not_anomalous.json')
        logger.info('opening - %s' % panorama_not_anomalous_json)
        with open(panorama_not_anomalous_json, 'r') as f:
            json_data = f.read()
    except:
        logger.error('error :: failed to get panorama_not_anomalous.json: ' + traceback.format_exc())
        return 'Uh oh ... a Skyline 500 :(', 500
    return json_data, 200


@app.route("/app_settings")
@requires_auth
def app_settings():

    try:
        app_settings = {'GRAPH_URL': settings.GRAPH_URL,
                        'OCULUS_HOST': settings.OCULUS_HOST,
                        'FULL_NAMESPACE': settings.FULL_NAMESPACE,
                        'SKYLINE_VERSION': skyline_version,
                        'PANORAMA_ENABLED': settings.PANORAMA_ENABLED,
                        'PANORAMA_DATABASE': settings.PANORAMA_DATABASE,
                        'PANORAMA_DBHOST': settings.PANORAMA_DBHOST,
                        'PANORAMA_DBPORT': settings.PANORAMA_DBPORT,
                        'PANORAMA_DBUSER': settings.PANORAMA_DBUSER,
                        'PANORAMA_DBUSERPASS': 'redacted',
                        'PANORAMA_GRAPH_URL': PANORAMA_GRAPH_URL,
                        'WEBAPP_USER_TIMEZONE': settings.WEBAPP_USER_TIMEZONE,
                        'WEBAPP_FIXED_TIMEZONE': settings.WEBAPP_FIXED_TIMEZONE,
                        'WEBAPP_JAVASCRIPT_DEBUG': settings.WEBAPP_JAVASCRIPT_DEBUG
                        }
    except Exception as e:
        error = "error: " + str(e)
        resp = json.dumps({'app_settings': error})
        return resp, 500

    resp = json.dumps(app_settings)
    return resp, 200


@app.route("/version")
@requires_auth
def version():

    try:
        version_settings = {'SKYLINE_VERSION': skyline_version}
        resp = json.dumps(version_settings)
        return resp, 200
    except:
        return "Not Found", 404


@app.route("/api", methods=['GET'])
# @modified 20180720 - Feature #2464: luminosity_remote_data
# Rnamed def from data to api for the purpose of trying to add some
# documentation relating to the API endpoints and their required parameters,
# the results or error and status codes they return and the context in which
# they are used.
# def data():
def api():

    start = time.time()

    # @added 20201110 - Feature #3824: get_cluster_data
    # Allow for the cluster_data argument to be added to certain api requests
    # to allow for data from all remote Skyline instances to be concatenated
    # into a single response
    # IMPORTANT: cluster_data MUST be the first argument that is evaluated as it
    #            is used and required by many of the following API methods
    cluster_data = False
    if 'cluster_data' in request.args:
        try:
            cluster_data_argument = request.args.get('cluster_data', 'false')
            if cluster_data_argument == 'true':
                cluster_data = True
                logger.info('api request with cluster_data=true')
            elif cluster_data_argument == 'false':
                cluster_data = False
                logger.info('api request with cluster_data=false')
            else:
                logger.error('error :: api request with invalid cluster_data argument - %s' % str(cluster_data_argument))
                return 'Bad Request', 400
        except:
            logger.error('error :: /api request with invalid cluster_data argument')
            return 'Bad Request', 400

    # IMPORTANT: cluster_data ^^ MUST be the first argument that is evaluated as it
    #            is used and required by many of the following API methods

    # @added 20211006 - Feature #4264: luminosity - cross_correlation_relationships
    related_to_metrics_request = False
    if 'related_to_metrics' in request.args:
        logger.info('/api?related_to_metrics')
        related_to_metrics_dict = {}
        start_related_to_metrics = timer()
        metric = None
        metric_id = 0
        try:
            metric = request.args.get('metric', 'false')
            if str(metric) == 'false':
                metric = None
        except:
            logger.error('error :: /api?related_to_metrics request with invalid metric')
            return 'Bad Request', 400
        try:
            metric_id = request.args.get('metric_id', 0)
            if str(metric_id) != '0':
                metric_id = int(metric_id)
        except:
            logger.error('error :: /api?related_to_metrics request with metric_id argument')
            return 'Bad Request', 400
        try:
            related_to_metrics_dict = get_related_to_metric_groups(skyline_app, metric, metric_id)
        except Exception as err:
            logger.error(traceback.format_exc())
            logger.error('error :: get_related_to_metric_groups failed - %s' % str(err))
        end_related_to_metrics = timer()
        related_to_metrics_time = (end_related_to_metrics - start_related_to_metrics)
        if not related_to_metrics_dict:
            data_dict = {"status": {"cluster_data": cluster_data, "request_time": related_to_metrics_time, "response": 404}, "data": related_to_metrics_dict}
            return jsonify(data_dict), 404
        data_dict = {"status": {"cluster_data": cluster_data, "request_time": related_to_metrics_time, "response": 200}, "data": related_to_metrics_dict}
        # The related_to_metrics dict is an ordered dict sorted by confidence
        # and avg_coefficient flask.jsonify does not preserve order so in this
        # case the response is created with json.dumps to maintain insertion
        # order in the dict.
        response = app.response_class(
            json.dumps(data_dict, sort_keys=False),
            mimetype=app.config['JSONIFY_MIMETYPE'])
        return response, 200

    # @added 20211001 - Feature #4264: luminosity - cross_correlation_relationships
    related_metrics_request = False
    if 'related_metrics' in request.args:
        related_metrics_request = True
        metric = None
        logger.info('/api?related_metrics')
        start_related_metrics = timer()
    if related_metrics_request:
        metric = None
        full_details = False
        metric_id = 0
        try:
            metric = request.args.get('metric', 'false')
            if str(metric) == 'false':
                metric = False
        except:
            logger.error('error :: /api?related_metrics request with invalid metric')
            return 'Bad Request', 400
        try:
            metric_id = request.args.get('metric_id', 0)
            if str(metric_id) != '0':
                metric_id = int(metric_id)
        except:
            logger.error('error :: /api?related_metrics request with metric_id argument')
            return 'Bad Request', 400
        if not metric and metric_id:
            try:
                # metric = base_name_from_metric_id(skyline_app, metric_id)
                metric = get_base_name_from_metric_id(skyline_app, metric_id)
            except Exception as err:
                logger.error('error :: base_name_from_metric_id failed to determine metric from metric_id: %s - %s' % (
                    str(metric_id), str(err)))
                metric_id = 0
        if not metric_id and metric:
            try:
                metric_id = get_metric_id_from_base_name(skyline_app, metric)
            except Exception as err:
                logger.error('error :: get_metric_id_from_base_name failed to determine metric_id from base_name: %s - %s' % (
                    str(metric), str(err)))
                metric_id = 0
        if not metric and not metric_id:
            logger.error('error :: /api?related_metrics request with invalid metric or metric_id argument')
            return 'Bad Request', 400

        if not metric_id:
            end_related_metrics = timer()
            related_metrics_time = (end_related_metrics - start_related_metrics)
            data_dict = {"status": {"cluster_data": cluster_data, "request_time": related_metrics_time, "response": 404}, "data": {"error": "no metric found"}}
            return jsonify(data_dict), 404
        if 'full_details' in request.args:
            try:
                full_details_argument = request.args.get('full_details', 'false')
                if str(full_details_argument) == 'true':
                    full_details = True
            except Exception as err:
                logger.error('error :: /api invalid full_details argument - %s' % str(err))
        related_metrics = {}
        try:
            related_metrics = get_related_metrics(skyline_app, cluster_data, full_details, metric, metric_id)
        except Exception as err:
            logger.error(traceback.format_exc())
            logger.error('error :: get_related_metrics failed - %s' % str(err))
        end_related_metrics = timer()
        related_metrics_time = (end_related_metrics - start_related_metrics)
        if not related_metrics:
            data_dict = {"status": {"cluster_data": cluster_data, "request_time": related_metrics_time, "response": 404}, "data": related_metrics}
            return jsonify(data_dict), 404
        data_dict = {"status": {"cluster_data": cluster_data, "request_time": related_metrics_time, "response": 200}, "data": related_metrics}
        # The related_metrics dict is an ordered dict sorted by avg_coefficient
        # flask.jsonify does not preserve order so in this case the response is
        # created with json.dumps to maintain insertion order in the dict.
        # return jsonify(data_dict), 200
        response = app.response_class(
            json.dumps(data_dict, sort_keys=False),
            mimetype=app.config['JSONIFY_MIMETYPE'])
        return response, 200

    # @added 20210619 - Feature #4148: analyzer.metrics_manager.resolutions
    #                   Bug #4146: check_data_sparsity - incorrect on low fidelity and inconsistent metrics
    #                   Feature #4144: webapp - stale_metrics API endpoint
    #                   Feature #4076: CUSTOM_STALE_PERIOD
    #                   Branch #1444: thunder
    if 'metrics_resolutions' in request.args:
        logger.info('/api?metrics_resolutions')
        start_metrics_resolution = timer()
        namespace = 'all'
        try:
            namespace = request.args.get('namespace', 'all')
        except KeyError:
            namespace = 'all'
        except Exception as e:
            trace = traceback.format_exc()
            fail_msg = 'error :: Webapp error with api?metrics_resolutions - %s' % e
            logger.error(fail_msg)
            return internal_error(fail_msg, trace)
        logger.info('/api?metrics_resolutions - namespace: %s' % namespace)
        remove_prefix = False
        try:
            remove_prefix_str = request.args.get('remove_prefix', 'false')
            if remove_prefix_str != 'false':
                remove_prefix = remove_prefix_str
        except KeyError:
            remove_prefix = False
        except Exception as e:
            trace = traceback.format_exc()
            fail_msg = 'error :: Webapp error with api?metrics_resolutions - %s' % e
            logger.error(fail_msg)
            return internal_error(fail_msg, trace)
        logger.info('/api?metrics_resolutions - remove_prefix: %s' % str(remove_prefix))

        less_than = False
        try:
            less_than_str = request.args.get('less_than', 'false')
            if less_than_str != 'false':
                less_than = int(less_than_str)
        except KeyError:
            less_than = False
        except Exception as e:
            trace = traceback.format_exc()
            fail_msg = 'error :: Webapp error with api?metrics_resolutions - %s' % e
            logger.error(fail_msg)
            return internal_error(fail_msg, trace)
        logger.info('/api?metrics_resolutions - less_than: %s' % str(less_than))
        greater_than = False
        try:
            greater_than_str = request.args.get('greater_than', 'false')
            if greater_than_str != 'false':
                greater_than = int(greater_than_str)
        except KeyError:
            greater_than = False
        except Exception as e:
            trace = traceback.format_exc()
            fail_msg = 'error :: Webapp error with api?metrics_resolutions - %s' % e
            logger.error(fail_msg)
            return internal_error(fail_msg, trace)
        logger.info('/api?metrics_resolutions - greater_than: %s' % str(greater_than))
        count_by_resolution = False
        try:
            count_by_resolution_str = request.args.get('count_by_resolution', 'false')
            if count_by_resolution_str == 'true':
                count_by_resolution = True
        except KeyError:
            greater_than = False
        except Exception as e:
            trace = traceback.format_exc()
            fail_msg = 'error :: Webapp error with api?metrics_resolutions - %s' % e
            logger.error(fail_msg)
            return internal_error(fail_msg, trace)
        logger.info('/api?metrics_resolutions - count_by_resolution: %s' % str(count_by_resolution))

        redis_hash_key = 'analyzer.metrics_manager.resolutions'
        resolutions_dict = {}
        try:
            resolutions_dict = REDIS_CONN.hgetall(redis_hash_key)
        except Exception as e:
            trace = traceback.format_exc()
            logger.error(trace)
            fail_msg = 'error :: /api?metrics_resolutions fail to query Redis hash key %s - %s' % (
                redis_hash_key, e)
            logger.error(fail_msg)
            return internal_error(fail_msg, trace)

        if settings.REMOTE_SKYLINE_INSTANCES and cluster_data:
            remote_resolutions_dicts = []
            redis_metric_data_uri = 'metrics_resolutions'
            try:
                remote_resolutions_dicts = get_cluster_data(redis_metric_data_uri, 'resolutions')
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: Webapp could not get resolution from the remote Skyline instances')
            if remote_resolutions_dicts:
                for remote_resolutions_dict in remote_resolutions_dicts:
                    logger.info('got remote_resolutions_dict of length %s from a remote Skyline instances' % str(len(remote_resolutions_dict)))
                    new_resolutions_dict = {**resolutions_dict, **remote_resolutions_dict}
                    resolutions_dict = new_resolutions_dict.copy()
                    del new_resolutions_dict
            else:
                logger.warning('warning :: failed to get remote_resolutions_dicts from the remote Skyline instances')
        logger.info('%s resolutions determined' % (
            str(len(resolutions_dict))))
        filtered_discarded = 0
        filtered_resolutions_dict = {}
        for metric in list(resolutions_dict.keys()):
            metric_data = resolutions_dict[metric]
            use_metric = metric.replace(settings.FULL_NAMESPACE, '')
            if greater_than:
                if int(float(metric_data)) <= greater_than:
                    filtered_discarded += 1
                    continue
            if less_than:
                if int(float(metric_data)) >= less_than:
                    filtered_discarded += 1
                    continue
            if namespace != 'all':
                if not use_metric.startswith(namespace):
                    filtered_discarded += 1
                    continue
            if remove_prefix:
                if not remove_prefix.endswith('.'):
                    remove_prefix = '%s.' % str(remove_prefix)
                use_metric = metric.replace(remove_prefix, '')
            filtered_resolutions_dict[use_metric] = float(metric_data)
        resolutions_dict = filtered_resolutions_dict
        if remove_prefix:
            logger.info('removed prefix %s from all metrics' % str(remove_prefix))
        if namespace != 'all':
            logger.info('filtered out %s metrics from the response that did not match namespace %s' % (
                str(filtered_discarded), namespace))
        resolutions_count = len(resolutions_dict)
        if count_by_resolution:
            resolutions_count_dict = {}
            resolutions = []
            metric_resolutions = []
            for metric in list(resolutions_dict.keys()):
                resolutions.append(resolutions_dict[metric])
                metric_resolutions.append([metric, resolutions_dict[metric]])
            unique_resolutions = list(set(resolutions))
            for resolution in unique_resolutions:
                int_resolution = int(resolution)
                resolutions_count_dict[int_resolution] = {}
                metrics_matching_resolution = [metric for metric, res in metric_resolutions if res == resolution]
                resolutions_count_dict[int_resolution]['metrics'] = metrics_matching_resolution
                resolutions_count_dict[int_resolution]['count'] = len(metrics_matching_resolution)
            resolutions_dict = resolutions_count_dict

        end_metrics_resolution = timer()
        metrics_resolution_time = (end_metrics_resolution - start_metrics_resolution)
        if resolutions_dict:
            data_dict = {"status": {"cluster_data": cluster_data, "request_time": metrics_resolution_time, "response": 200}, "data": {'resolutions': resolutions_dict, 'resolution_results_count': resolutions_count}}
            return jsonify(data_dict), 200
        else:
            data_dict = {"status": {"cluster_data": cluster_data, "request_time": metrics_resolution_time, "response": 404}, "data": {"resolutions": 'null', 'resolution_results_count': 0}}
            return jsonify(data_dict), 404

    # @added 20210618 - Bug #4146: check_data_sparsity - incorrect on low fidelity and inconsistent metrics
    #                   Feature #4144: webapp - stale_metrics API endpoint
    #                   Feature #4076: CUSTOM_STALE_PERIOD
    #                   Branch #1444: thunder
    if 'metrics_sparsity' in request.args:
        logger.info('/api?metrics_sparsity')
        start_metrics_sparsity = timer()
        namespace = 'all'
        try:
            namespace = request.args.get('namespace', 'all')
        except KeyError:
            namespace = 'all'
        except Exception as e:
            trace = traceback.format_exc()
            fail_msg = 'error :: Webapp error with api?metrics_sparsity - %s' % e
            logger.error(fail_msg)
            return internal_error(fail_msg, trace)
        logger.info('/api?metrics_sparsity - namespace: %s' % namespace)
        remove_prefix = False
        try:
            remove_prefix_str = request.args.get('remove_prefix', 'false')
            if remove_prefix_str != 'false':
                remove_prefix = remove_prefix_str
        except KeyError:
            remove_prefix = False
        except Exception as e:
            trace = traceback.format_exc()
            fail_msg = 'error :: Webapp error with api?metrics_sparsity - %s' % e
            logger.error(fail_msg)
            return internal_error(fail_msg, trace)
        logger.info('/api?metrics_sparsity - remove_prefix: %s' % str(remove_prefix))

        below = False
        try:
            below_str = request.args.get('below', 'false')
            if below_str != 'false':
                below = float(below_str)
        except KeyError:
            below = False
        except Exception as e:
            trace = traceback.format_exc()
            fail_msg = 'error :: Webapp error with api?metrics_sparsity - %s' % e
            logger.error(fail_msg)
            return internal_error(fail_msg, trace)
        logger.info('/api?metrics_sparsity - below: %s' % str(remove_prefix))

        redis_hash_key = 'analyzer.metrics_manager.hash_key.metrics_data_sparsity'
        sparsity_dict = {}
        try:
            sparsity_dict = REDIS_CONN.hgetall(redis_hash_key)
        except Exception as e:
            trace = traceback.format_exc()
            logger.error(trace)
            fail_msg = 'error :: /api?metrics_sparsity fail to query Redis hash key %s - %s' % (
                redis_hash_key, e)
            logger.error(fail_msg)
            return internal_error(fail_msg, trace)

        if settings.REMOTE_SKYLINE_INSTANCES and cluster_data:
            remote_sparsity_dicts = []
            redis_metric_data_uri = 'metrics_sparsity'
            try:
                remote_sparsity_dicts = get_cluster_data(redis_metric_data_uri, 'sparsity')
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: Webapp could not get timeseries from the remote Skyline instances')
            if remote_sparsity_dicts:
                for remote_sparsity_dict in remote_sparsity_dicts:
                    logger.info('got remote_sparsity_dict of length %s from a remote Skyline instances' % str(len(remote_sparsity_dict)))
                    new_sparsity_dict = {**sparsity_dict, **remote_sparsity_dict}
                    sparsity_dict = new_sparsity_dict.copy()
                    del new_sparsity_dict
            else:
                logger.warning('warning :: failed to get remote_sparsity_dicts from the remote Skyline instances')
        logger.info('%s sparsity determined' % (
            str(len(sparsity_dict))))
        filtered_discarded = 0
        filtered_sparsity_dict = {}
        for metric in list(sparsity_dict.keys()):
            metric_data = sparsity_dict[metric]
            use_metric = metric.replace(settings.FULL_NAMESPACE, '')
            if below:
                if float(metric_data) > below:
                    filtered_discarded += 1
                    continue
            if namespace != 'all':
                if not use_metric.startswith(namespace):
                    filtered_discarded += 1
                    continue
            if remove_prefix:
                if not remove_prefix.endswith('.'):
                    remove_prefix = '%s.' % str(remove_prefix)
                use_metric = metric.replace(remove_prefix, '')
            filtered_sparsity_dict[use_metric] = float(metric_data)
        sparsity_dict = filtered_sparsity_dict
        if remove_prefix:
            logger.info('removed prefix %s from all metrics' % str(remove_prefix))
        if namespace != 'all':
            logger.info('filtered out %s metrics from the response that did not match namespace %s' % (
                str(filtered_discarded), namespace))
        sparsity_count = len(sparsity_dict)

        end_metrics_sparsity = timer()
        metrics_sparsity_time = (end_metrics_sparsity - start_metrics_sparsity)
        if sparsity_dict:
            data_dict = {"status": {"cluster_data": cluster_data, "request_time": metrics_sparsity_time, "response": 200}, "data": {'sparsity': sparsity_dict, 'sparsity_results_count': sparsity_count}}
            return jsonify(data_dict), 200
        else:
            data_dict = {"status": {"cluster_data": cluster_data, "request_time": metrics_sparsity_time, "response": 404}, "data": {"sparsity": 'null', 'sparsity_results_count': 0}}
            return jsonify(data_dict), 404

    # @added 20210617 - Feature #4144: webapp - stale_metrics API endpoint
    #                   Feature #4076: CUSTOM_STALE_PERIOD
    #                   Branch #1444: thunder
    if 'metrics_timestamp_resolutions' in request.args:
        logger.info('/api?metrics_timestamp_resolutions')
        start_metrics_timestamp_resolutions = timer()
        namespace = 'all'
        try:
            namespace = request.args.get('namespace', 'all')
        except KeyError:
            namespace = 'all'
        except Exception as e:
            trace = traceback.format_exc()
            fail_msg = 'error :: Webapp error with api?metrics_timestamp_resolutions - %s' % e
            logger.error(fail_msg)
            return internal_error(fail_msg, trace)
        logger.info('/api?metrics_timestamp_resolutions - namespace: %s' % namespace)
        remove_prefix = False
        try:
            remove_prefix_str = request.args.get('remove_prefix', 'false')
            if remove_prefix_str != 'false':
                remove_prefix = remove_prefix_str
        except KeyError:
            remove_prefix = False
        except Exception as e:
            trace = traceback.format_exc()
            fail_msg = 'error :: Webapp error with api?metrics_timestamp_resolutions - %s' % e
            logger.error(fail_msg)
            return internal_error(fail_msg, trace)
        logger.info('/api?metrics_timestamp_resolutions - remove_prefix: %s' % str(remove_prefix))
        redis_hash_key = 'analyzer.metrics_manager.hash_key.metrics_timestamp_resolutions'
        timestamp_resolutions_dict = {}
        try:
            timestamp_resolutions_dict = REDIS_CONN.hgetall(redis_hash_key)
        except Exception as e:
            trace = traceback.format_exc()
            logger.error(trace)
            fail_msg = 'error :: /api?metrics_timestamp_resolutions fail to query Redis hash key %s - %s' % (
                redis_hash_key, e)
            logger.error(fail_msg)
            return internal_error(fail_msg, trace)

        if settings.REMOTE_SKYLINE_INSTANCES and cluster_data:
            remote_timestamp_resolutions_dict = {}
            redis_metric_data_uri = 'metrics_timestamp_resolutions'
            try:
                remote_timestamp_resolutions_dicts = get_cluster_data(redis_metric_data_uri, 'timestamp_resolutions')
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: Webapp could not get timeseries from the remote Skyline instances')
            if remote_timestamp_resolutions_dicts:
                for remote_timestamp_resolutions_dict in remote_timestamp_resolutions_dicts:
                    logger.info('got remote_timestamp_resolutions_dict of length %s from a remote Skyline instances' % str(len(remote_timestamp_resolutions_dict)))
                    new_timestamp_resolutions_dict = {**timestamp_resolutions_dict, **remote_timestamp_resolutions_dict}
                    timestamp_resolutions_dict = new_timestamp_resolutions_dict.copy()
                    del new_timestamp_resolutions_dict
            else:
                logger.warning('warning :: failed to get remote_timestamp_resolutions_dicts from the remote Skyline instances')
        logger.info('%s timestamp_resolutions determined' % (
            str(len(timestamp_resolutions_dict))))
        filtered_discarded = 0
        filtered_timestamp_resolutions_dict = {}
        for metric in list(timestamp_resolutions_dict.keys()):
            metric_data = timestamp_resolutions_dict[metric]
            use_metric = metric.replace(settings.FULL_NAMESPACE, '')
            if namespace != 'all':
                if not use_metric.startswith(namespace):
                    filtered_discarded += 1
                    continue
            if remove_prefix:
                if not remove_prefix.endswith('.'):
                    remove_prefix = '%s.' % str(remove_prefix)
                use_metric = metric.replace(remove_prefix, '')
            filtered_timestamp_resolutions_dict[use_metric] = metric_data
        timestamp_resolutions_dict = filtered_timestamp_resolutions_dict
        if remove_prefix:
            logger.info('removed prefix %s from all metrics' % str(remove_prefix))
        if namespace != 'all':
            logger.info('filtered out %s metrics from the response that did not match namespace %s' % (
                str(filtered_discarded), namespace))

        end_metrics_timestamp_resolutions = timer()
        metrics_timestamp_resolutions_time = (end_metrics_timestamp_resolutions - start_metrics_timestamp_resolutions)
        if timestamp_resolutions_dict:
            data_dict = {"status": {"cluster_data": cluster_data, "request_time": metrics_timestamp_resolutions_time, "response": 200}, "data": {'timestamp_resolutions': timestamp_resolutions_dict}}
            return jsonify(data_dict), 200
        else:
            data_dict = {"status": {"cluster_data": cluster_data, "request_time": metrics_timestamp_resolutions_time, "response": 404}, "data": {"timestamp_resolutions": 'null'}}
            return jsonify(data_dict), 404

    # @added 20210617 - Feature #4144: webapp - stale_metrics API endpoint
    #                   Feature #4076: CUSTOM_STALE_PERIOD
    #                   Branch #1444: thunder
    if 'determine_data_sparsity' in request.args:
        logger.info('/api?determine_data_sparsity')
        start_stale_metrics = timer()
        base_name = None
        try:
            base_name = request.args.get('metric')
            logger.info('/api?determine_data_sparsity with metric: %s' % base_name)
        except Exception as e:
            logger.error('error :: api determine_data_sparsity request no metric argument - %s' % e)
            return 'Bad Request', 400
        if not base_name:
            return 'Bad Request', 400
        timeseries = []
        try:
            timeseries = get_metric_timeseries(skyline_app, base_name)
        except Exception as e:
            trace = traceback.format_exc()
            fail_msg = 'error :: Webapp error with api?determine_data_sparsity - %s' % e
            logger.error(fail_msg)
            return internal_error(fail_msg, trace)

        if not timeseries:
            if settings.REMOTE_SKYLINE_INSTANCES and cluster_data:
                redis_metric_data_uri = 'metric=%s&format=json' % str(base_name)
                try:
                    timeseries = get_cluster_data(redis_metric_data_uri, 'timeseries')
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: Webapp could not get timeseries from the remote Skyline instances')
                if timeseries:
                    logger.info('got timeseries of length %s from the remote Skyline instances' % str(len(timeseries)))
                else:
                    logger.warning('warning :: failed to get timeseries from the remote Skyline instances')
        if not timeseries:
            data_dict = {"status": {"cluster_data": cluster_data, "response": 404}, "data": {"sparsity": 'null'}}
            return jsonify(data_dict), 404

        sparsity = None
        if timeseries:
            try:
                sparsity = determine_data_sparsity(skyline_app, timeseries, None, True)
            except Exception as e:
                trace = traceback.format_exc()
                fail_msg = 'error :: Webapp error with api?determine_data_sparsity - %s' % e
                logger.error(fail_msg)
                return internal_error(fail_msg, trace)

        if sparsity is not None:
            data_dict = {"status": {"cluster_data": cluster_data, "response": 200}, "data": {"sparsity": {base_name: sparsity}}}
            return jsonify(data_dict), 200
        else:
            data_dict = {"status": {"cluster_data": cluster_data, "response": 404}, "data": {"sparsity": {base_name: 'null'}}}
            return jsonify(data_dict), 404

    # @added 20210617 - Feature #4144: webapp - stale_metrics API endpoint
    #                   Feature #4076: CUSTOM_STALE_PERIOD
    #                   Branch #1444: thunder
    if 'stale_metrics' in request.args:
        logger.info('/api?stale_metrics')
        start_stale_metrics = timer()
        namespace = 'all'
        try:
            namespace = request.args.get('namespace', 'all')
        except KeyError:
            namespace = 'all'
        except Exception as e:
            trace = traceback.format_exc()
            fail_msg = 'error :: Webapp error with api?stale_metrics - %s' % e
            logger.error(fail_msg)
            return internal_error(fail_msg, trace)
        exclude_sparsely_populated = False
        try:
            exclude_sparsely_populated_str = request.args.get('exclude_sparsely_populated', 'false')
            if exclude_sparsely_populated_str == 'true':
                exclude_sparsely_populated = True
        except KeyError:
            exclude_sparsely_populated = False
        except Exception as e:
            trace = traceback.format_exc()
            fail_msg = 'error :: Webapp error with api?stale_metrics - %s' % e
            logger.error(fail_msg)
            return internal_error(fail_msg, trace)
        # Add a key that functions.thunder.stale_metrics uses to determine if
        # webapp requests should include sparsely_populated_metrics or not
        if exclude_sparsely_populated:
            try:
                REDIS_CONN.setex('webapp.stale_metrics.exclude_sparsely_populated', 2, str(exclude_sparsely_populated))
            except Exception as e:
                trace = traceback.format_exc()
                fail_msg = 'error :: Webapp error with api?stale_metrics - %s' % e
                logger.error(fail_msg)
                return internal_error(fail_msg, trace)

        namespaces_namespace_stale_metrics_dict = {}
        try:
            namespaces_namespace_stale_metrics_dict = namespace_stale_metrics(namespace, cluster_data, exclude_sparsely_populated)
        except Exception as e:
            trace = traceback.format_exc()
            fail_msg = 'error :: Webapp error with api?stale_metrics - %s' % e
            logger.error(fail_msg)
            return internal_error(fail_msg, trace)
        logger.info('%s namespaces checked for stale metrics discovered with thunder_stale_metrics' % (
            str(len(namespaces_namespace_stale_metrics_dict))))
        end_stale_metrics = timer()
        stale_metrics_time = (end_stale_metrics - start_stale_metrics)
        if namespaces_namespace_stale_metrics_dict:
            data_dict = {"status": {"cluster_data": cluster_data, "request_time": stale_metrics_time, "response": 200}, "data": namespaces_namespace_stale_metrics_dict}
            return jsonify(data_dict), 200
        else:
            data_dict = {"status": {"cluster_data": cluster_data, "request_time": stale_metrics_time, "response": 404}, "data": {"stale_metrics": 'null'}}
            return jsonify(data_dict), 404

    # @added 20210601 - Branch #1444: thunder
    if 'thunder_stale_metrics' in request.args:
        logger.info('/api?thunder_stale_metrics')
        start_thunder_stale_metrics = timer()
        try:
            timestamp = request.args.get('timestamp')
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: /api?thunder_stale_metrics request with no timestamp argument')
            timestamp = None
        if not timestamp:
            data_dict = {"status": {"error": "no timestamp passed"}}
            return jsonify(data_dict), 400
        redis_key = 'thunder.stale_metrics.alert.%s' % str(timestamp)
        stale_metrics_dict = {}
        try:
            stale_metrics_raw = REDIS_CONN.get(redis_key)
            if stale_metrics_raw:
                stale_metrics_dict = literal_eval(stale_metrics_raw)
        except Exception as e:
            logger.error(traceback.format_exc())
            logger.error('error :: /api?thunder_stale_metrics fail to query Redis key %s  %s' % (redis_key, e))
            stale_metrics_dict = {}
        end_thunder_stale_metrics = timer()
        thunder_stale_metrics_time = (end_thunder_stale_metrics - start_thunder_stale_metrics)
        if stale_metrics_dict:
            data_dict = {"status": {"request_time": thunder_stale_metrics_time, "response": 200}, "data": stale_metrics_dict}
            return jsonify(data_dict), 200
        else:
            data_dict = {"status": {"request_time": thunder_stale_metrics_time, "response": 404}, "data": {"metrics": 'null'}}
            return jsonify(data_dict), 404

    # @added 20210720 - Branch #1444: thunder
    if 'thunder_no_data_remove_namespace' in request.args:
        logger.info('/api?thunder_no_data_remove_namespace')
        start_thunder_no_data_remove_namespace = timer()
        try:
            namespace = request.args.get('namespace')
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: /api?thunder_no_data_remove_namespace request with no namespace argument')
            namespace = None
        if not namespace:
            data_dict = {"status": {"error": "no namespace passed"}}
            return jsonify(data_dict), 400
        redis_key = 'webapp.thunder.remove.namespace.metrics.last_timeseries_timestamp'
        key_set = False
        try:
            REDIS_CONN.set(redis_key, str(namespace))
            key_set = True
        except Exception as e:
            logger.error(traceback.format_exc())
            logger.error('error :: /api?thunder_no_data_remove_namespace fail to create Redis key %s  %s' % (redis_key, e))
        end_thunder_no_data_remove_namespace = timer()
        thunder_no_data_remove_namespace_time = (end_thunder_no_data_remove_namespace - start_thunder_no_data_remove_namespace)
        if key_set:
            data_dict = {"status": {"request_time": thunder_no_data_remove_namespace_time, "response": 200}, "data": {'namespace': namespace, 'remove_namespace_from_no_data_check': True}}
            return jsonify(data_dict), 200
        else:
            data_dict = {"status": {"request_time": thunder_no_data_remove_namespace_time, "response": 500}, "data": {'namespace': namespace, 'remove_namespace_from_no_data_check': False, 'reason': 'failed to add key'}}
            return jsonify(data_dict), 500

    # @added 20210720 - Feature #4188: metrics_manager.boundary_metrics
    if 'boundary_metrics' in request.args:
        logger.info('/api?boundary_metrics')
        start_boundary_metrics = timer()
        metric = None
        try:
            metric = request.args.get('metric')
        except KeyError:
            metric = None
        except Exception as e:
            logger.error(traceback.format_exc())
            logger.error('error :: /api?boundary_metrics request evaluating metric parameter - %s' % e)
        namespaces = None
        try:
            namespaces = request.args.get('namespaces')
        except KeyError:
            namespaces = None
        except Exception as e:
            logger.error(traceback.format_exc())
            namespaces = None
            logger.error('error :: /api?boundary_metrics request evaluating namespaces parameter - %s' % e)
        metrics = []
        if metric is not None:
            metrics = [str(metric)]
        if namespaces is not None:
            namespaces_str = namespaces
            namespaces = namespaces_str.split(',')
        else:
            namespaces = []
        logger.info('/api?boundary_metrics - get_boundary_metrics with metrics: %s, namespaces: %s' % (
            str(metrics), str(namespaces)))
        boundary_metrics = None
        try:
            boundary_metrics = get_boundary_metrics(skyline_app, metrics, namespaces, cluster_data, True)
        except Exception as e:
            logger.error(traceback.format_exc())
            logger.error('error :: /api?boundary_metrics get_boundary_metrics failed - %s' % e)
            end_boundary_metrics = timer()
            boundary_metrics_time = (end_boundary_metrics - start_boundary_metrics)
            data_dict = {"status": {"cluster_data": cluster_data, "request_time": boundary_metrics_time, "response": 500}, "data": None}
            return jsonify(data_dict), 500
        end_boundary_metrics = timer()
        boundary_metrics_time = (end_boundary_metrics - start_boundary_metrics)
        if boundary_metrics:
            data_dict = {"status": {"cluster_data": cluster_data, "request_time": boundary_metrics_time, "response": 200}, "data": {"boundary_metrics": boundary_metrics}}
            return jsonify(data_dict), 200
        else:
            data_dict = {"status": {"cluster_data": cluster_data, "request_time": boundary_metrics_time, "response": 404}, "data": {"boundary_metrics": None}}
            return jsonify(data_dict), 404

    # @added 20210211 - Feature - last_analyzed_timestamp
    # @added 20210211 - Feature #3968: webapp - last_analyzed_timestamp API endpoint
    if 'last_analyzed_timestamp' in request.args:
        logger.info('/api?last_analyzed_timestamp')
        start_last_analyzed_timestamp = timer()
        try:
            metric = request.args.get('metric')
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: /api?last_analyzed_timestamp request with invalid metric argument')
            metric = None
        if not metric:
            data_dict = {"status": {"error": "no metric passed"}}
            return jsonify(data_dict), 400
        redis_hash_key = 'analyzer.metrics.last_analyzed_timestamp'
        last_analyzed_timestamp = 0
        try:
            last_analyzed_timestamp = REDIS_CONN.hget(redis_hash_key, metric)
        except Exception as e:
            logger.error(traceback.format_exc())
            logger.error('error :: /api?last_analyzed_timestamp fail to query Redis hash key %s for %s - %s' % (redis_hash_key, metric, e))
            last_analyzed_timestamp = None
        if not last_analyzed_timestamp:
            redis_hash_key = 'analyzer.low_priority_metrics.last_analyzed_timestamp'
            redis_metric_name = '%s%s' % (settings.FULL_NAMESPACE, metric)
            # @added 20220112 - Bug #4374: webapp - handle url encoded chars
            # @modified 20220115 - Bug #4374: webapp - handle url encoded chars
            # Roll back change - breaking existing metrics with colons
            # redis_metric_name = url_encode_metric_name(redis_metric_name)
            try:
                last_analyzed_timestamp = REDIS_CONN.hget(redis_hash_key, redis_metric_name)
            except Exception as e:
                logger.error(traceback.format_exc())
                logger.error('error :: /api?last_analyzed_timestamp fail to query Redis hash key %s for %s - %s' % (redis_hash_key, redis_metric_name, e))
                last_analyzed_timestamp = None

        # @added 20210714 - Feature #3884: ANALYZER_CHECK_LAST_TIMESTAMP
        # Alway return the last timestamp for the metric for the Redis metric
        # data if the ANALYZER_CHECK_LAST_TIMESTAMP feature is not enabled
        if not last_analyzed_timestamp:
            metric_timeseries = None
            try:
                metric_timeseries = get_metric_timeseries(skyline_app, metric, log=True)
            except Exception as e:
                logger.error(traceback.format_exc())
                logger.error('error :: /api?last_analyzed_timestamp get_metric_timeseries failed for %s - %s' % (redis_metric_name, e))
                last_analyzed_timestamp = None
            if metric_timeseries:
                try:
                    last_analyzed_timestamp = int(metric_timeseries[-1][0])
                except Exception as e:
                    logger.error(traceback.format_exc())
                    logger.error('error :: /api?last_analyzed_timestamp failed to determine last timestamp for %s from metric_timeseries - %s' % (redis_metric_name, e))
                    last_analyzed_timestamp = None

        if last_analyzed_timestamp:
            end_last_analyzed_timestamp = timer()
            last_analyzed_timestamp_time = '%.6f' % (end_last_analyzed_timestamp - start_last_analyzed_timestamp)
            data_dict = {"status": {"cluster_data": cluster_data, "remote_data": False, "request_time": float(last_analyzed_timestamp_time), "response": 200}, "data": {"metric": metric, "last_analyzed_timestamp": int(last_analyzed_timestamp)}}
            return jsonify(data_dict), 200
        remote_last_analyzed_timestamp = None
        if settings.REMOTE_SKYLINE_INSTANCES and cluster_data:
            last_analyzed_timestamp_uri = 'last_analyzed_timestamp&metric=%s' % metric
            try:
                remote_last_analyzed_timestamp = get_cluster_data(last_analyzed_timestamp_uri, 'last_analyzed_timestamp')
            except Exception as e:
                logger.error('error :: /api?last_analyzed_timestamp fail to get last_analyzed_timestamp for %s from other cluster nodes - %s' % (metric, e))
        end_last_analyzed_timestamp = timer()
        last_analyzed_timestamp_time = '%.6f' % (end_last_analyzed_timestamp - start_last_analyzed_timestamp)
        if remote_last_analyzed_timestamp:
            status_code = 200
            data_dict = {"status": {"cluster_data": cluster_data, "remote_data": True, "request_time": float(last_analyzed_timestamp_time), "response": 200}, "data": {"metric": metric, "last_analyzed_timestamp": int(remote_last_analyzed_timestamp)}}
        else:
            status_code = 400
            data_dict = {"status": {"cluster_data": cluster_data, "remote_data": False, "request_time": float(last_analyzed_timestamp_time), "reason": "no last analyzed timestamp found for the named metric, does not exist", "response": 400}, "data": {"metric": metric, "last_analyzed_timestamp": None}}
        return jsonify(data_dict), status_code

    # @added 20201213 - Feature #3890: metrics_manager - sync_cluster_files
    if 'expected_features_profiles_dir' in request.args:
        logger.info('/api?expected_features_profiles_dir')
        start_features_profiles_dir = timer()
        features_profiles_dirs = {}
        try:
            features_profiles_dirs = expected_features_profiles_dirs()
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: expected_features_profiles_dirs failed returning 500')
            return 'Internal Server Error', 500
        end_features_profiles_dir = timer()
        features_profiles_dir_time = '%.6f' % (end_features_profiles_dir - start_features_profiles_dir)
        logger.info('report %s expected features_profiles_dirs took, %s seconds' % (
            str(len(features_profiles_dirs)), str(features_profiles_dir_time)))
        if features_profiles_dirs:
            data_dict = {"status": {"request_time": features_profiles_dir_time, "response": 200}, "data": {"features_profile_dirs": features_profiles_dirs}}
            return jsonify(data_dict), 200
        else:
            data_dict = {"status": {"request_time": features_profiles_dir_time, "response": 404}, "data": {"features_profile_dirs": 'null'}}
            return jsonify(data_dict), 404

    # @added 20201125 - Feature #3850: webapp - yhat_values API endoint
    # api?yhat_value=true&metric=metric&from=<from>&until=<until>&include_value=true&include_mean=true&include_yhat_real_lower=true
    if 'yhat_values' in request.args:
        logger.info('/api?yhat_values request')
        # @added 20201126 - Feature #3850: webapp - yhat_values API endoint
        start_yhat = timer()
        try:
            metric = request.args.get('metric', 0)
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: /api?yhat_values request with invalid metric argument')
            metric = None
        if not metric:
            data_dict = {"status": {"error": "no metric passed"}}
            return jsonify(data_dict), 400
        try:
            from_timestamp = int(request.args.get('from', '0'))
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: /api?yhat_values request with invalid from argument')
            from_timestamp = None
        if not from_timestamp:
            data_dict = {"status": {"error": "invalid from argument passed"}}
            return jsonify(data_dict), 400
        try:
            until_timestamp = int(request.args.get('until', '0'))
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: /api?yhat_values request with invalid until argument')
            until_timestamp = None
        if not until_timestamp:
            data_dict = {"status": {"error": "invalid until argument passed"}}
            return jsonify(data_dict), 400
        include_value = False
        try:
            include_value_str = request.args.get('include_value', 'false')
            if include_value_str == 'true':
                include_value = True
        except:
            pass
        include_mean = False
        try:
            include_mean_str = request.args.get('include_mean', 'false')
            if include_mean_str == 'true':
                include_mean = True
        except:
            pass
        include_yhat_real_lower = False
        try:
            include_yhat_real_lower_str = request.args.get('include_yhat_real_lower', 'false')
            if include_yhat_real_lower_str == 'true':
                include_yhat_real_lower = True
        except:
            pass

        # @modified 20210126 - Task #3958: Handle secondary algorithms in yhat_values
        # Added anomalous periods and remove prefix
        include_anomalous_periods = False
        try:
            include_anomalous_periods_str = request.args.get('include_anomalous_periods', 'false')
            if include_anomalous_periods_str == 'true':
                include_anomalous_periods = True
        except:
            pass
        remove_prefix = False
        try:
            remove_prefix_str = request.args.get('remove_prefix', 'false')
            if remove_prefix_str != 'false':
                remove_prefix = True
        except:
            pass

        yhat_dict = {}

        # @added 20201126 - Feature #3850: webapp - yhat_values API endoint
        # Cache request yhat_dict
        yhat_dict_cache_key = 'webapp.%s.%s.%s.%s.%s.%s' % (
            metric, str(from_timestamp), str(until_timestamp),
            str(include_value), str(include_mean),
            str(include_yhat_real_lower))

        yhat_dict_str = None
        try:
            yhat_dict_str = REDIS_CONN.get(yhat_dict_cache_key)
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: Webapp could not get the Redis key - %s' % yhat_dict_cache_key)
        use_cache_data = False
        if yhat_dict_str:
            logger.info('got yhat_values from Redis key - %s' % yhat_dict_cache_key)
            try:
                yhat_dict = literal_eval(yhat_dict_str)
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: Webapp failed to literal_eval yhat_dict_str')
        if yhat_dict:
            use_cache_data = True

        # @modified 20210126 - Task #3958: Handle secondary algorithms in yhat_values
        # Added anomalous_periods_dict
        anomalous_periods_dict = {}
        if include_anomalous_periods:
            anomalous_periods_dict_cache_key = 'webapp.%s.%s.%s.%s.%s.%s.anomalous_periods' % (
                metric, str(from_timestamp), str(until_timestamp),
                str(include_value), str(include_mean),
                str(include_yhat_real_lower))
            anomalous_periods_dict_str = None
            try:
                anomalous_periods_dict_str = REDIS_CONN.get(anomalous_periods_dict_cache_key)
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: Webapp could not get the Redis key - %s' % anomalous_periods_dict_cache_key)
            if anomalous_periods_dict_str:
                logger.info('got anomalous_periods_dict from Redis key - %s' % anomalous_periods_dict_cache_key)
                try:
                    anomalous_periods_dict = literal_eval(anomalous_periods_dict_str)
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: Webapp failed to literal_eval anomalous_periods_dict_str')
            if not anomalous_periods_dict:
                use_cache_data = False
                yhat_dict = {}

        if not yhat_dict:
            try:
                logger.info('running get_yhat_values(%s, %s, %s, %s, %s, %s)' % (
                    metric, str(from_timestamp), str(until_timestamp),
                    str(include_value), str(include_mean),
                    str(include_yhat_real_lower)))
                # @modified 20210126 - Task #3958: Handle secondary algorithms in yhat_values
                # Added anomalous_periods_dict and include_anomalous_periods
                yhat_dict, anomalous_periods_dict = get_yhat_values(metric, from_timestamp, until_timestamp, include_value, include_mean, include_yhat_real_lower, include_anomalous_periods)
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: error in get_yhat_values(%s, %s, %s, %s, %s, %s)' % (
                    metric, str(from_timestamp), str(until_timestamp),
                    str(include_value), str(include_mean),
                    str(include_yhat_real_lower)))
                logger.error('error :: returning 500')
                return 'Internal Server Error', 500
        else:
            logger.info('yhat_values retrieved from Redis')

        # @added 20201126 - Feature #3850: webapp - yhat_values API endoint
        # Added yhat_time
        end_yhat = timer()
        yhat_time = '%.6f' % (end_yhat - start_yhat)
        logger.info('yhat_values calculations took %s seconds' % str(yhat_time))
        if yhat_dict is None:
            logger.error('error :: returning 500 yhat_dict is %s' % str(yhat_dict))
            data_dict = {"status": {"request_time": yhat_time, "response": 500}, "data": {}}
            return 'Internal Server Error', 500

        # @added 20210126 - Task #3958: Handle secondary algorithms in yhat_values
        # Allow for the removal of a prefix from the metric name
        if remove_prefix:
            try:
                if remove_prefix_str.endswith('.'):
                    remove_prefix = '%s' % remove_prefix_str
                else:
                    remove_prefix = '%s.' % remove_prefix_str
                metric = metric.replace(remove_prefix, '')
            except Exception as e:
                logger.error('error :: failed to remove prefix %s from %s - %s' % (str(remove_prefix_str), metric, e))

        if yhat_dict:
            if include_anomalous_periods:
                data_dict = {"status": {"request_time": yhat_time, "response": 200, "cached": use_cache_data}, "data": {"metric": metric, "yhat_values": yhat_dict, "anomalous_periods": anomalous_periods_dict}}
            else:
                data_dict = {"status": {"request_time": yhat_time, "response": 200, "cached": use_cache_data}, "data": {"metric": metric, "yhat_values": yhat_dict}}
            return jsonify(data_dict), 200
        else:
            # @modified 20210112 - Feature #3850: webapp - yhat_values API endoint
            # Return a 204 with null rather than a 404
            # data_dict = {"status": {"request_time": yhat_time, "response": 404}, "data": {"metric": metric, "yhat_values": 'null'}}
            # return jsonify(data_dict), 404
            # @modified 20210112 - Feature #3850: webapp - yhat_values API endoint
            # Return a 200 with null rather than 204 as 204 can have No content
            # data_dict = {"status": {"request_time": yhat_time, "response": 204, "cached": use_cache_data, "no_data": "true"}, "data": {"metric": metric, "yhat_values": 'null'}}
            # return jsonify(data_dict), 204
            if include_anomalous_periods:
                data_dict = {"status": {"request_time": yhat_time, "response": 200, "cached": use_cache_data, "no_data": "true"}, "data": {"metric": metric, "yhat_values": None, "anomalous_periods": None}}
            else:
                data_dict = {"status": {"request_time": yhat_time, "response": 200, "cached": use_cache_data, "no_data": "true"}, "data": {"metric": metric, "yhat_values": None}}
            return jsonify(data_dict), 200

    # @added 20201103 - Feature #3770: webapp - analyzer_last_status API endoint
    if 'analyzer_last_status' in request.args:
        logger.info('/api?analyzer_last_status request')
        analyzer_last_status = None
        try:
            analyzer_last_status = int(float(str(REDIS_CONN.get('analyzer'))))
        # @added 20210511 - Feature #3770: webapp - analyzer_last_status API endoint
        # Handle a None result as a warning not an error
        except ValueError as e:
            logger.warning('warning :: Webapp could not get the analyzer key from Redis - %s' % e)
        except Exception as e:
            logger.error(traceback.format_exc())
            logger.error('error :: Webapp could not get the analyzer key from Redis - %s' % e)
        logger.info('/api?analyzer_last_status responding with %s' % str(analyzer_last_status))
        data_dict = {"status": {}, "data": {"timestamp": analyzer_last_status}}
        if not analyzer_last_status:
            # Return with status code 410 Gone
            logger.info('/api?analyzer_last_status responding with status code 410 GONE')
            return jsonify(data_dict), 410
        return jsonify(data_dict), 200

    # @added 20201007 - Feature #3770: webapp - batch_processing metrics API endoint
    if 'batch_processing_metrics' in request.args:
        logger.info('/api?batch_processing_metrics request')
        batch_processing_metrics = []
        try:
            batch_processing_metrics = list(REDIS_CONN.smembers('aet.analyzer.batch_processing_metrics'))
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: Webapp could not get the aet.analyzer.batch_processing_metrics list from Redis')
            return 'Internal Server Error', 500

        # @added 20201103 - Feature #3824: get_cluster_data
        if settings.REMOTE_SKYLINE_INSTANCES and cluster_data:
            remote_batch_processing_metrics = None
            try:
                remote_batch_processing_metrics = get_cluster_data('batch_processing_metrics', 'metrics')
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: Webapp could not get batch_processing_metrics from the remote Skyline instances')
            if remote_batch_processing_metrics:
                logger.info('got %s remote metrics from the remote Skyline instances' % str(len(remote_batch_processing_metrics)))
                batch_processing_metrics_list = batch_processing_metrics + remote_batch_processing_metrics
                batch_processing_metrics = list(set(batch_processing_metrics_list))

        logger.info('/api?batch_processing_metrics responding with %s metrics' % str(len(batch_processing_metrics)))
        data_dict = {"status": {"cluster_data": cluster_data}, "data": {"metrics": batch_processing_metrics}}
        return jsonify(data_dict), 200

    # @added 20200929 - Task #3748: POC SNAB
    #                   Branch #3068: SNAB
    if 'snab' in request.args:
        logger.info('/api?snab request')
        if not SNAB_ENABLED:
            logger.info('/api?snab not handled and snab is not enabled')
            return 'Bad Request', 400
        snab_id = None
        if 'snab_id' in request.args:
            try:
                snab_id = int(request.args.get('snab_id', None))
                logger.info('/api?snab request with snab_id - %s' % str(snab_id))
            except:
                logger.error('error :: /api?snab request with invalid snab_id - %s' % str(request.args.get('anomaly_id', None)))
                snab_id = False
        if not snab_id:
            logger.error('error :: /api?snab no valid snab_id parameter sent')
            return 'Bad Request', 400
        anomaly_id = None
        if 'anomaly_id' in request.args:
            try:
                anomaly_id = int(request.args.get('anomaly_id', None))
                logger.info('/api?snab request with anomaly_id - %s' % str(anomaly_id))
            except:
                logger.error('error :: /api?snab request with invalid anomaly_id - %s' % str(request.args.get('anomaly_id', None)))
                anomaly_id = False
        if not anomaly_id:
            logger.error('error :: /api?snab no anomaly_id parameter sent')
            return 'Bad Request', 400
        logger.info('/api?snab request with anomaly_id - %s' % (
            str(anomaly_id)))
        result = None
        snab_results = ['tP', 'fP', 'tN', 'fN', 'unsure', 'NULL']
        if 'result' in request.args:
            snab_result = request.args.get('result', None)
            if snab_result not in snab_results:
                logger.error('error :: /api?snab invalid result value sent - %s' % str(snab_result))
                return 'Bad Request', 400
            else:
                result = snab_result
        if not result:
            logger.error('error :: /api?snab no result value was sent')
            return 'Bad Request', 400
        base_name = None
        anomaly_timestamp = None
        snab_result_updated = None
        snab_dict = {}
        try:
            # @modified 20201004 - Task #3748: POC SNAB
            #                      Branch #3068: SNAB
            # Allow results to be changed
            # snab_result_updated, base_name, anomaly_timestamp = update_snab_result(snab_id, anomaly_id, result)
            snab_result_updated, base_name, anomaly_timestamp, existing_result = update_snab_result(snab_id, anomaly_id, result)
            snab_dict = {
                'snab_id': snab_id,
                'anomaly_id': anomaly_id,
                'metric': base_name,
                'anomaly_timestamp': anomaly_timestamp,
                'result': result,
                'updated': snab_result_updated,
            }
        except:
            trace = traceback.format_exc()
            fail_msg = 'error :: Webapp error with update_snab_result'
            logger.error(fail_msg)
            return internal_error(fail_msg, trace)

        if base_name and anomaly_timestamp:
            # Using the snab_id instead of base_name, a bit of hack but works
            slack_updated = webapp_update_slack_thread(snab_id, int(anomaly_timestamp), result, 'snab_result')
            logger.info('slack_updated for snab evaluation - %s, with result %s' % (
                str(slack_updated), str(result)))

        # @added 20201004 - Task #3748: POC SNAB
        #                   Branch #3068: SNAB
        # Allow results to be changed
        if existing_result:
            slack_notified_of_change = webapp_update_slack_thread(snab_id, int(anomaly_timestamp), [existing_result, result], 'snab_result_changed')
            logger.info('slack_updated for snab evaluation - %s, with result %s' % (
                str(slack_notified_of_change), str(result)))

        data_dict = {"status": {}, "data": {"snab_updated": snab_dict}}
        if snab_result_updated and snab_dict:
            logger.info('/api?snab request returning 200 with json data - %s' % (
                str(data_dict)))
            return jsonify(data_dict), 200
        else:
            logger.info('/api?snab request returning 500 with json data - %s' % (
                str(data_dict)))
            return jsonify(data_dict), 500

    # @added 20200908 - Feature #3740: webapp - anomaly API endpoint
    if 'anomaly' in request.args:
        logger.info('/api?anomaly')
        anomaly_id = None
        if 'id' in request.args:
            try:
                anomaly_id = int(request.args.get('id', 0))
            except:
                logger.error('error :: /api?anomaly id parameter sent is not an int')
                anomaly_id = False
        if not anomaly_id:
            logger.error('error :: /api?anomaly no id parameter sent')
            return 'Bad Request', 400
        logger.info('/api?anomaly request with id - %s' % (
            str(anomaly_id)))

        start = time.time()
        anomaly_data = []
        try:
            # anomaly_data = [int(anomaly_id), str(metric), anomalous_datapoint, anomaly_timestamp, full_duration, created_timestamp, anomaly_end_timestamp]
            anomaly_data = panorama_anomaly_details(anomaly_id)
        except:
            trace = traceback.format_exc()
            fail_msg = 'error :: Webapp error with panorama_anomaly_details'
            logger.error(fail_msg)
            return internal_error(fail_msg, trace)

        anomaliesDict = {}
        matchesDict = {}
        if anomaly_data:
            metric = anomaly_data[1]
            anomaly_timestamp = int(anomaly_data[3])
            requested_timestamp = anomaly_timestamp
            try:
                anomaly_end_timestamp = int(anomaly_data[6])
            except:
                anomaly_end_timestamp = None
            related = []
            try:
                related, labelled_anomalies, fail_msg, trace = get_related(skyline_app, anomaly_id, anomaly_timestamp)
            except:
                trace = traceback.format_exc()
                fail_msg = 'error :: Webapp error with get_related'
                logger.error(fail_msg)
                return internal_error(fail_msg, trace)
            for related_anomaly_id, related_metric_id, related_metric_name, related_anomaly_timestamp, related_full_duration in related:
                # @added 20201202- Feature #3858: skyline_functions - correlate_or_relate_with
                # Filter only related anomalies that are in correlations group
                correlate_or_relate = correlate_or_relate_with(skyline_app, metric, related_metric_name)
                if not correlate_or_relate:
                    continue
                metric_dict = {
                    'metric': related_metric_name,
                    'timestamp': int(related_anomaly_timestamp),
                }
                anomaliesDict[related_anomaly_id] = metric_dict
            related_matches = []
            related_metric = 'get_related_matches'
            related_metric_like = 'all'
            related_fp_id = None
            related_layer_id = None
            minus_two_minutes = int(requested_timestamp) - 120
            plus_two_minutes = int(requested_timestamp) + 120
            related_limited_by = None
            related_ordered_by = 'DESC'
            try:
                related_matches, fail_msg, trace = get_fp_matches(related_metric, related_metric_like, related_fp_id, related_layer_id, minus_two_minutes, plus_two_minutes, related_limited_by, related_ordered_by)
            except:
                trace = traceback.format_exc()
                fail_msg = 'error :: Webapp error with get_fp_matches for related_matches'
                logger.error(fail_msg)
                return internal_error(fail_msg, trace)
            if len(related_matches) == 1:
                # @modified 20200908 - Feature #3740: webapp - anomaly API endpoint
                # Added match_anomaly_timestamp
                # @modified 20210413 - Feature #4014: Ionosphere - inference
                #                      Branch #3590: inference
                # Added related_motifs_matched_id
                for related_human_date, related_match_id, related_matched_by, related_fp_id, related_layer_id, related_metric, related_uri_to_matched_page, related_validated, match_anomaly_timestamp, related_motifs_matched_id in related_matches:
                    if related_matched_by == 'no matches were found':
                        related_matches = []
            if related_matches:
                logger.info('%s possible related matches found' % (str(len(related_matches))))
                # @modified 20200908 - Feature #3740: webapp - anomaly API endpoint
                # Added match_anomaly_timestamp
                # @modified 20210413 - Feature #4014: Ionosphere - inference
                #                      Branch #3590: inference
                # Added related_motifs_matched_id
                for related_human_date, related_match_id, related_matched_by, related_fp_id, related_layer_id, related_metric, related_uri_to_matched_page, related_validated, match_anomaly_timestamp, related_motifs_matched_id in related_matches:
                    # @added 20201202- Feature #3858: skyline_functions - correlate_or_relate_with
                    # Filter only related matches that are in correlations group
                    correlate_or_relate = correlate_or_relate_with(skyline_app, metric, related_metric)
                    if not correlate_or_relate:
                        continue

                    metric_dict = {
                        'metric': related_metric,
                        'timestamp': match_anomaly_timestamp,
                        'matched by': related_matched_by,
                        'fp id': related_fp_id,
                        'layer id': related_layer_id,
                        # @modified 20210413 - Feature #4014: Ionosphere - inference
                        #                      Branch #3590: inference
                        # Added motifs_matched_id
                        'motif id': related_motifs_matched_id,
                    }
                    matchesDict[related_match_id] = metric_dict

        try:
            correlations, fail_msg, trace = get_correlations(skyline_app, anomaly_id)
        except:
            trace = traceback.format_exc()
            fail_msg = 'error :: Webapp error with get_correlations'
            logger.error(fail_msg)
            return internal_error(fail_msg, trace)
        correlationsDict = {}
        try:
            for correlation in correlations:
                correlated_metric = correlation[0]

                # @added 20201202- Feature #3858: skyline_functions - correlate_or_relate_with
                # Filter only correlations that are in correlations group
                correlate_or_relate = correlate_or_relate_with(skyline_app, metric, correlated_metric)
                if not correlate_or_relate:
                    continue

                metric_dict = {
                    'coefficient': float(correlation[1]),
                    'shifted': float(correlation[2]),
                    'shifted_coefficient': float(correlation[3])
                }
                correlationsDict[correlated_metric] = metric_dict
        except:
            trace = traceback.format_exc()
            fail_msg = 'error :: Webapp error with get_correlations'
            logger.error(fail_msg)
            return internal_error(fail_msg, trace)

        # @added 20210324 - Feature #3642: Anomaly type classification
        anomaly_types = []
        if LUMINOSITY_CLASSIFY_ANOMALIES:
            try:
                anomaly_types, anomaly_type_ids = get_anomaly_type(skyline_app, anomaly_id)
            except Exception as e:
                fail_msg = 'error :: Webapp error with get_anomaly_type - %s' % str(e)
                logger.error(fail_msg)

        anomaly_dict = {
            # @modified 20201201 - Feature #3740: webapp - anomaly API endpoint
            # Due to the anomaly ids being used as json keys in the possible
            # related anomalies and matches the anomaly id is always typed as a
            # str
            # 'id': anomaly_id,
            'id': str(anomaly_id),
            'metric': anomaly_data[1],
            'timestamp': anomaly_data[3],
            'value': anomaly_data[2],
            'end_timestamp': anomaly_end_timestamp,
            'cross correlations': correlationsDict,
            # @modified 20201201 - Feature #3740: webapp - anomaly API endpoint
            # Corrected the key names
            # 'possible related anomaly ids': anomaliesDict,
            # 'possible related match ids': matchesDict,
            'possible related anomalies': anomaliesDict,
            'possible related matches': matchesDict,
            # @added 20210324 - Feature #3642: Anomaly type classification
            'anomaly_types': anomaly_types,
        }
        data_dict = {"status": {"response": 200, "request_time": (time.time() - start)}, "data": {"anomaly": anomaly_dict}}
        logger.info('/api?anomaly request returning data - %s' % (
            str(data_dict)))
        return jsonify(data_dict), 200

    # @added 20200611 - Feature #3578: Test alerts
    if 'test_alert' in request.args:
        logger.info('/api?test_alert request')
        if 'skyline_app' in request.args:
            test_skyline_app = request.args.get('skyline_app', None)
        if not test_skyline_app:
            logger.error('error :: /api?test_alert request no skyline_app parameter sent')
            return 'Bad Request', 400
        if 'metric' in request.args:
            metric = request.args.get('metric', None)
        if not metric:
            logger.error('error :: /api?test_alert request no metric parameter sent')
            return 'Bad Request', 400

        logger.info('/api?test_alert request with skyline_app - %s and metric - %s' % (
            test_skyline_app, metric))
        test_alert_redis_key = '%s.test_alerts' % test_skyline_app
        try:
            REDIS_CONN.sadd(test_alert_redis_key, metric)
        except Exception as e:
            logger.error(traceback.format_exc())
            logger.error('error :: could not add %s to %s Redis set - %s' % (
                test_skyline_app, metric, e))
            return 'Internal Server Error', 500
        test_alert_dict = {
            'skyline_app': test_skyline_app,
            'metric': metric
        }
        data_dict = {"status": {}, "data": {"test_alert": test_alert_dict}}
        logger.info('/api?test_alert request returning data - %s' % (
            str(data_dict)))
        return jsonify(data_dict), 200

    # @added 20200517 - Feature #3538: webapp - upload_data endpoint
    #                   Feature #3550: flux.uploaded_data_worker
    if 'upload_status' in request.args:
        logger.info('/api?upload_status request')
        if not file_uploads_enabled:
            return 'Not Found', 404
        if 'upload_id_key' in request.args:
            upload_id_key = request.args.get('upload_id_key', None)
        if not upload_id_key:
            logger.error('error :: /api?upload_status request')
            resp = json.dumps(
                {'error': 'no upload_id_key was passed'})
            return resp, 400
        logger.info('/api?upload_status request for upload_id_key - %s' % upload_id_key)
        upload_status_redis_key = 'flux.upload_status.%s' % upload_id_key
        try:
            upload_status = REDIS_CONN.get(upload_status_redis_key)
            if not upload_status:
                resp = json.dumps(
                    {'not found': 'there is not status for ' + str(upload_id_key)})
                return flask_escape(resp), 404
        except Exception as e:
            logger.error(traceback.format_exc())
            logger.error('error :: could not get the %s from Redis - %s' % (upload_status_redis_key, e))
            logger.error('error :: /api?upload_status request')
            return 'Internal Server Error', 500
        upload_status_dict = {}
        if upload_status:
            try:
                upload_status_dict = literal_eval(upload_status)
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: could not literal_eval the %s data from Redis' % upload_status_redis_key)
                return 'Internal Server Error', 500
        data_dict = {"status": {}, "data": {"upload": upload_status_dict}}
        logger.info('/api?upload_status request for upload_id_key - %s, returning data' % (
            upload_id_key))
        return jsonify(data_dict), 200

    # @added 20200410 - Feature #3474: webapp api - training_data
    #                   Feature #3472: ionosphere.training_data Redis set
    if 'training_data' in request.args:
        metric_filter = None
        timestamp_filter = None
        if 'metric' in request.args:
            metric_filter = request.args.get('metric', None)
        if 'timestamp' in request.args:
            timestamp_filter = request.args.get('timestamp', 0)
        training_data = []
        training_data_raw = list(REDIS_CONN.smembers('ionosphere.training_data'))
        for training_data_str in training_data_raw:
            try:
                training_data_item = literal_eval(training_data_str)
                training_metric = str(training_data_item[0])
                training_timestamp = int(training_data_item[1])
                # @added 20200425 - Feature #3508: ionosphere.untrainable_metrics
                # Added resolution_seconds
                try:
                    training_resolution_seconds = int(training_data_item[2])
                except:
                    training_resolution_seconds = None
                if not training_resolution_seconds:
                    for alert in settings.ALERTS:
                        if training_resolution_seconds:
                            break
                        alert_match_pattern = alert[0]
                        metric_pattern = training_metric
                        pattern_match = False
                        try:
                            alert_match_pattern = re.compile(alert_match_pattern)
                            pattern_match = alert_match_pattern.match(metric_pattern)
                            if pattern_match:
                                pattern_match = True
                        except:
                            pattern_match = False
                        if not pattern_match:
                            if alert[0] in training_metric:
                                pattern_match = True
                        if not pattern_match:
                            continue
                        try:
                            training_resolution_seconds = int(alert[3] * 3600)
                        except:
                            training_resolution_seconds = None
                if not training_resolution_seconds:
                    training_resolution_seconds = settings.FULL_DURATION

                add_to_response = True
                if metric_filter:
                    if metric_filter != training_metric:
                        add_to_response = False

                    # @added 20200417 - Feature #3474: webapp api - training_data
                    # Allow to match namespaces too
                    if not add_to_response:
                        if metric_filter in training_metric:
                            add_to_response = True
                    if not add_to_response:
                        metric_filter_namespace_elements = metric_filter.split('.')
                        training_metric_namespace_elements = training_metric.split('.')
                        elements_matched = set(metric_filter_namespace_elements) & set(training_metric_namespace_elements)
                        if len(elements_matched) == len(metric_filter_namespace_elements):
                            add_to_response = True

                if timestamp_filter:
                    if int(timestamp_filter) != training_timestamp:
                        add_to_response = False

                # @added 20200425 - Feature #3508: ionosphere.untrainable_metrics
                # Remove ionosphere.untrainable_metrics from the training data
                # when entries exist at the same resolution
                ionosphere_untrainable_metrics = []
                ionosphere_untrainable_metrics_redis_set = 'ionosphere.untrainable_metrics'
                try:
                    ionosphere_untrainable_metrics = list(REDIS_CONN.smembers(ionosphere_untrainable_metrics_redis_set))
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: could not get the ionosphere.untrainable_metrics set from Redis')
                    ionosphere_untrainable_metrics = None
                if add_to_response and ionosphere_untrainable_metrics:
                    for ionosphere_untrainable_metric_str in ionosphere_untrainable_metrics:
                        try:
                            ionosphere_untrainable_metric = literal_eval(ionosphere_untrainable_metric_str)
                            ium_metric_name = ionosphere_untrainable_metric[0]
                            if ium_metric_name == training_metric:
                                ium_resolution = int(ionosphere_untrainable_metric[5])
                                if ium_resolution == training_resolution_seconds:
                                    add_to_response = False
                                    logger.info('removed training data from response as identified as untrainable as has negative value/s - %s' % str(ionosphere_untrainable_metric))
                                    break
                        except:
                            logger.error(traceback.format_exc())
                            logger.error('error :: failed to determine if metric is in ionosphere.untrainable_metrics Redis set')

                if add_to_response:
                    training_data.append([training_metric, training_timestamp])
            except:
                logger.error(traceback.format_exc())
                logger.error(
                    'error :: failed to iterate literal_eval of training_data_raw')
                return 'Internal Server Error', 500

        # @added 20201110 - Feature #3824: get_cluster_data
        if settings.REMOTE_SKYLINE_INSTANCES and cluster_data:
            remote_training_data = None
            training_data_uri = 'training_data'
            if metric_filter:
                training_data_uri = 'training_data&metric=%s' % metric_filter
            if timestamp_filter:
                training_data_uri = '%s&timestamp=%s' % (training_data_uri, timestamp_filter)
            try:
                remote_training_data = get_cluster_data(training_data_uri, 'metrics')
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: Webapp could not get remote_training_data from the remote Skyline instances')
            if remote_training_data:
                logger.info('got %s remote metrics training data instances from the remote Skyline instances' % str(len(remote_training_data)))
                remote_training_data_list = training_data + remote_training_data
                # @modified 20201126 - Feature #3824: get_cluster_data
                # set cannot be used here as each training_data item includes a
                # list which is unhashable
                # training_data = list(set(remote_training_data_list))
                training_data = remote_training_data_list

        data_dict = {"status": {"cluster_data": cluster_data}, "data": {"metrics": training_data}}
        return jsonify(data_dict), 200

    # @added 20200129 - Feature #3422: webapp api - alerting_metrics and non_alerting_metrics
    if 'non_alerting_metrics' in request.args:
        non_alerting_metrics = []
        try:
            non_alerting_metrics = list(REDIS_CONN.smembers('aet.analyzer.non_smtp_alerter_metrics'))
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: Webapp could not get the aet.analyzer.smtp_alerter_metrics list from Redis')
            return 'Internal Server Error', 500

        # @added 20201103 - Feature #3824: get_cluster_data
        if settings.REMOTE_SKYLINE_INSTANCES and cluster_data:
            remote_non_alerting_metrics = None
            try:
                remote_non_alerting_metrics = get_cluster_data('non_alerting_metrics', 'metrics')
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: Webapp could not get non_alerting_metrics from the remote Skyline instances')
            if remote_non_alerting_metrics:
                logger.info('got %s remote non_alerting_metrics from the remote Skyline instances' % str(len(remote_non_alerting_metrics)))
                non_alerting_metrics_list = non_alerting_metrics + remote_non_alerting_metrics
                non_alerting_metrics = list(set(non_alerting_metrics_list))

        logger.info('/api?non_alerting_metrics responding with %s metrics' % str(len(non_alerting_metrics)))
        data_dict = {"status": {"cluster_data": cluster_data}, "data": {"metrics": non_alerting_metrics}}
        return jsonify(data_dict), 200

    if 'alerting_metrics' in request.args:
        alerting_metrics = []
        try:
            alerting_metrics = list(REDIS_CONN.smembers('aet.analyzer.smtp_alerter_metrics'))
        except Exception as e:
            logger.error(traceback.format_exc())
            logger.error('error :: Webapp could not get the aet.analyzer.smtp_alerter_metrics list from Redis - %s' % e)
            return 'Internal Server Error', 500

        # @added 20201103 - Feature #3824: get_cluster_data
        if settings.REMOTE_SKYLINE_INSTANCES and cluster_data:
            remote_alerting_metrics = None
            try:
                remote_alerting_metrics = get_cluster_data('alerting_metrics', 'metrics')
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: Webapp could not get alerting_metrics from the remote Skyline instances')
            if remote_alerting_metrics:
                logger.info('got %s remote alerting_metrics from the remote Skyline instances' % str(len(remote_alerting_metrics)))
                alerting_metrics_list = alerting_metrics + remote_alerting_metrics
                alerting_metrics = list(set(alerting_metrics_list))

        logger.info('/api?non_alerting_metrics responding with %s metrics' % str(len(alerting_metrics)))
        data_dict = {"status": {"cluster_data": cluster_data}, "data": {"metrics": alerting_metrics}}
        return jsonify(data_dict), 200

    # @added 20200117 - Feature #3400: Identify air gaps in the metric data
    if 'airgapped_metrics' in request.args:
        airgapped_metrics = []
        try:
            airgapped_metrics = list(REDIS_CONN.smembers('analyzer.airgapped_metrics'))
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: Webapp could not get the analyzer.airgapped_metrics list from Redis')
            return 'Internal Server Error', 500
        logger.info('airgapped_metrics responding with %s metrics' % str(len(airgapped_metrics)))
        data_dict = {"status": {}, "data": {"metrics": airgapped_metrics}}
        return jsonify(data_dict), 200
    # @added 20200205 - Feature #3400: Identify air gaps in the metric data
    # /api?airgap_filled&metric=<metric>&resoluion=<resolution>&start=<start-unix-timestamp>&end=<end-unix-timestamp>&attempt<attempts>
    if 'airgap_filled' in request.args:
        metric = None
        resolution = None
        start = None
        end = None
        attempts = None
        for i in request.args:
            key = str(i)
            value = request.args.get(key, None)
            logger.info('api?airgap_filled - request argument - %s=%s' % (key, str(value)))
        if 'metric' in request.args:
            metric = request.args.get('metric', None)
        if not metric:
            resp = json.dumps(
                {'results': 'Error: no metric parameter was passed to /api?airgap_filled'})
            return resp, 400
        if 'resolution' in request.args:
            resolution = request.args.get('resolution', None)
        if not resolution:
            resp = json.dumps(
                {'results': 'Error: no resolution parameter was passed to /api?airgap_filled'})
            return resp, 400
        if 'start' in request.args:
            start = request.args.get('start', None)
        if not start:
            resp = json.dumps(
                {'results': 'Error: no start parameter was passed to /api?airgap_filled'})
            return resp, 400
        if 'end' in request.args:
            end = request.args.get('end', None)
        if not end:
            resp = json.dumps(
                {'results': 'Error: no end parameter was passed to /api?airgap_filled'})
            return resp, 400
        if 'attempts' in request.args:
            attempts = request.args.get('attempts', None)
        if not attempts:
            resp = json.dumps(
                {'results': 'Error: no attempts parameter was passed to /api?airgap_filled'})
            return resp, 400
        # Check airgap is in the set
        airgapped_metrics = []
        try:
            airgapped_metrics = REDIS_CONN.smembers('analyzer.airgapped_metrics')
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: Webapp could not get the analyzer.airgapped_metrics list from Redis')
            return 'Internal Server Error', 500
        airgap_present = False
        for airgap_str in airgapped_metrics:
            try:
                airgap = literal_eval(airgap_str)
                if str(metric) != (airgap[0]):
                    continue
                if int(resolution) != int(airgap[1]):
                    continue
                if int(start) != int(airgap[2]):
                    continue
                if int(end) != int(airgap[3]):
                    continue
                if int(attempts) != int(airgap[4]):
                    continue
                airgap_present = airgap
                break
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: failed to iterate literal_eval of airgapped_metrics')
                continue
        if not airgap_present:
            logger.info('api?airgap_filled responding with 404 no matching airgap found in analyzer.airgapped_metrics Redis set')
            return 'Not found', 404
        logger.info('removing airgapped metric item - %s' % str(airgap))
        try:
            REDIS_CONN_UNDECODE.srem('analyzer.airgapped_metrics', str(airgap))
        except Exception as e:
            logger.error(traceback.format_exc())
            logger.error('error :: could not remove item from analyzer.airgapped_metrics Redis set - %s' % str(e))
            resp = json.dumps(
                {'error': 'Error: could not remove item from analyzer.airgapped_metrics Redis set'})
            return resp, 500
        # @added 20200501 - Feature #3400: Identify air gaps in the metric data
        # Handle airgaps filled so that once they have been submitted as filled
        # Analyzer will not identify them as airgapped again
        try:
            REDIS_CONN.sadd('analyzer.airgapped_metrics.filled', str(airgap))
        except Exception as e:
            logger.error(traceback.format_exc())
            logger.error('error :: could not add item from analyzer.airgapped_metrics.filled Redis set - %s' % str(e))

        data_dict = {"status": {}, "data": {"removed_airgap": airgap}}
        return jsonify(data_dict), 200

    # @added 20191231 - Feature #3368: webapp api - ionosphere_learn_work
    if 'ionosphere_learn_work' in request.args:
        ionosphere_learn_work = []
        try:
            ionosphere_learn_work = list(REDIS_CONN.smembers('ionosphere.learn.work'))
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: Webapp could not get the ionosphere.learn.work list from Redis')
            return 'Internal Server Error', 500

        # @added 20200109 - Feature #3380: Create echo features profile when a Mirage features profile is created
        # Add pending echo features profiles to the to response
        try:
            ionosphere_echo_work = list(REDIS_CONN.smembers('ionosphere.echo.work'))
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: Webapp could not get the ionosphere.echo.work list from Redis')
        echo_work_no_full_duration = []
        if ionosphere_echo_work:
            for work in ionosphere_echo_work:
                work_list = literal_eval(work)
                work_no_fd = [work_list[0], work_list[1], work_list[2], work_list[3], work_list[4], work_list[5]]
                echo_work_no_full_duration.append(work_no_fd)
                # @added 20201110 - Feature #3368: webapp api - ionosphere_learn_work
                # Just add as it is literal_evaled below or not
                ionosphere_learn_work.append(work)
        # if echo_work_no_full_duration:
        #     new_ionosphere_learn_work = ionosphere_learn_work + echo_work_no_full_duration
        #     ionosphere_learn_work = new_ionosphere_learn_work

        # @added 20201202 - Feature #3824: get_cluster_data
        # Added some debug logging
        logger.debug('debug :: ionosphere_learn_work - %s' % str(ionosphere_learn_work))

        # @added 20201112 - Feature #3824: get_cluster_data
        if settings.REMOTE_SKYLINE_INSTANCES and cluster_data:
            remote_ionosphere_learn_work = None
            try:
                remote_ionosphere_learn_work = get_cluster_data('ionosphere_learn_work', 'metrics')
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: Webapp could not get ionosphere_learn_work from the remote Skyline instances')
            if remote_ionosphere_learn_work:
                logger.info('got %s remote ionosphere_learn_work from the remote Skyline instances' % str(len(remote_ionosphere_learn_work)))
                ionosphere_learn_work_list = ionosphere_learn_work + remote_ionosphere_learn_work
                # @modified 20201202 - Feature #3824: get_cluster_data
                # Cannot use set on unhashable list and debug logging
                # ionosphere_learn_work = list(set(ionosphere_learn_work_list))
                ionosphere_learn_work = list(ionosphere_learn_work_list)
                logger.debug('debug :: remote_ionosphere_learn_work - ionosphere_learn_work - %s' % str(ionosphere_learn_work))

        # @added 20201104 - Feature #3368: webapp api - ionosphere_learn_work
        # Convert strings to json
        convert_to_json = False
        if 'format' in request.args:
            json_passed = request.args.get('format', None)
            if json_passed == 'json':
                convert_to_json = True
        if convert_to_json:
            # @modified 20201208 - Feature #3368: webapp api - ionosphere_learn_work
            # Return an array of work
            # metrics_json = {}
            work_list = []
            for str_item in ionosphere_learn_work:
                try:
                    item = literal_eval(str_item)
                    if item[4] is None:
                        parent_id = 0
                    else:
                        parent_id = item[4]
                    if item[5] is None:
                        generation = 0
                    else:
                        generation = item[5]
                    # @modified 20201208 - Feature #3368: webapp api - ionosphere_learn_work
                    # Return an array of work
                    # metrics_json[item[3]] = {
                    #    'deadline_type': item[0],
                    #    'job_type': item[1],
                    #    'timestamp': item[2],
                    #    'parent_id': parent_id,
                    #    'generation': generation
                    work_dict = {
                        'metric': item[3],
                        'deadline_type': item[0],
                        'job_type': item[1],
                        'timestamp': item[2],
                        'parent_id': parent_id,
                        'generation': generation
                    }
                    work_list.append(work_dict)
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: Webapp could not build the ionosphere_learn_work dict to jsonify - %s' % str(str_item))
            # @modified 20201208 - Feature #3368: webapp api - ionosphere_learn_work
            # Return an array of work
            # data_dict = {"status": {"cluster_data": cluster_data}, "data": {"metrics": metrics_json}}
            data_dict = {"status": {"cluster_data": cluster_data}, "data": {"work": work_list}}

            # @added 20201202 - Feature #3824: get_cluster_data
            # Added some debug logging
            logger.debug('debug :: data_dict - %s' % str(data_dict))

            return jsonify(data_dict), 200

        data_dict = {"status": {"cluster_data": cluster_data}, "data": {"metrics": ionosphere_learn_work}}
        return jsonify(data_dict), 200

    # @added 20191203 - Feature #3350: webapp api - mirage_metrics and ionosphere_metrics
    if 'mirage_metrics' in request.args:
        try:
            mirage_metrics = list(REDIS_CONN.smembers('mirage.unique_metrics'))
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: Webapp could not get the mirage.unique_metrics list from Redis')
            return 'Internal Server Error', 500

        # @added 20201112 - Feature #3824: get_cluster_data
        if settings.REMOTE_SKYLINE_INSTANCES and cluster_data:
            remote_mirage_metrics = None
            try:
                remote_mirage_metrics = get_cluster_data('mirage_metrics', 'metrics')
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: Webapp could not get mirage_metrics from the remote Skyline instances')
            if remote_mirage_metrics:
                logger.info('got %s remote mirage_metrics from the remote Skyline instances' % str(len(remote_mirage_metrics)))
                mirage_metrics_list = mirage_metrics + remote_mirage_metrics
                mirage_metrics = list(set(mirage_metrics_list))

        data_dict = {"status": {"cluster_data": cluster_data}, "data": {"metrics": mirage_metrics}}
        return jsonify(data_dict), 200

    # @added 20191203 - Feature #3350: webapp api - mirage_metrics and ionosphere_metrics
    if 'ionosphere_metrics' in request.args:
        try:
            ionosphere_metrics = list(REDIS_CONN.smembers('ionosphere.unique_metrics'))
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: Webapp could not get the ionosphere.unique_metrics list from Redis')
            return 'Internal Server Error', 500

        # @added 20201112 - Feature #3824: get_cluster_data
        if settings.REMOTE_SKYLINE_INSTANCES and cluster_data:
            remote_ionosphere_metrics = None
            try:
                remote_ionosphere_metrics = get_cluster_data('ionosphere_metrics', 'metrics')
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: Webapp could not get ionosphere_metrics from the remote Skyline instances')
            if remote_ionosphere_metrics:
                logger.info('got %s remote ionosphere_metrics from the remote Skyline instances' % str(len(remote_ionosphere_metrics)))
                ionosphere_metrics_list = ionosphere_metrics + remote_ionosphere_metrics
                ionosphere_metrics = list(set(ionosphere_metrics_list))

        data_dict = {
            "status": {"cluster_data": cluster_data},
            "data": {
                "metrics": ionosphere_metrics
            }
        }
        return jsonify(data_dict), 200

    # @added 20191126 - Feature #3336: webapp api - derivative_metrics
    if 'derivative_metrics' in request.args:
        try:
            # @modified 20211012 - Feature #4280: aet.metrics_manager.derivative_metrics Redis hash
            # derivative_metrics = list(REDIS_CONN.smembers('derivative_metrics'))
            derivative_metrics = list(REDIS_CONN.smembers('aet.metrics_manager.derivative_metrics'))
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: Webapp could not get the derivative_metrics list from Redis')
            return 'Internal Server Error', 500

        # @added 20201112 - Feature #3824: get_cluster_data
        if settings.REMOTE_SKYLINE_INSTANCES and cluster_data:
            remote_derivative_metrics = None
            try:
                # @modified 20211012 - Feature #4280: aet.metrics_manager.derivative_metrics Redis hash
                # remote_derivative_metrics = get_cluster_data('derivative_metrics', 'metrics')
                remote_derivative_metrics = get_cluster_data('aet.metrics_manager.derivative_metrics', 'metrics')
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: Webapp could not get aet.metrics_manager.derivative_metrics from the remote Skyline instances')
            if remote_derivative_metrics:
                logger.info('got %s remote aet.metrics_manager.derivative_metrics from the remote Skyline instances' % str(len(remote_derivative_metrics)))
                derivative_metrics_list = derivative_metrics + remote_derivative_metrics
                derivative_metrics = list(set(derivative_metrics_list))

        data_dict = {
            "status": {"cluster_data": cluster_data},
            "data": {
                "metrics": derivative_metrics
            }
        }
        return jsonify(data_dict), 200

    # @added 20191008 - Feature #3252: webapp api - unique_metrics
    if 'unique_metrics' in request.args:
        try:
            unique_metrics = list(REDIS_CONN.smembers(settings.FULL_NAMESPACE + 'unique_metrics'))
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: Webapp could not get the unique_metrics list from Redis')
            return 'Internal Server Error', 500

        # @added 20201112 - Feature #3824: get_cluster_data
        if settings.REMOTE_SKYLINE_INSTANCES and cluster_data:
            remote_unique_metrics = None
            try:
                remote_unique_metrics = get_cluster_data('unique_metrics', 'metrics')
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: Webapp could not get unique_metrics from the remote Skyline instances')
            if remote_unique_metrics:
                logger.info('got %s remote unique_metrics from the remote Skyline instances' % str(len(remote_unique_metrics)))
                unique_metrics_list = unique_metrics + remote_unique_metrics
                unique_metrics = list(set(unique_metrics_list))

        # @added 20210416 - Feature #3252: webapp api - unique_metrics
        # Allow to filter by namespace and remove_full_namespace_prefix
        total_unique_metrics = len(unique_metrics)
        filtered = False
        filter_namespace = None
        if 'filter_namespace' in request.args:
            filtered_unique_metrics = []
            filter_namespace_str = request.args.get('filter_namespace', '')
            logger.info('/api?unique_metrics filter_namespace passed filtering: %s' % filter_namespace_str)
            filter_namespace = [filter_namespace_str]
            for metric_name in unique_metrics:
                pattern_match, matched_by = matched_or_regexed_in_list(skyline_app, metric_name, filter_namespace, False)
                if pattern_match:
                    filtered_unique_metrics.append(metric_name)
            logger.info('/api?unique_metrics filtered_namespace found %s metrics from the %s unique_metrics' % (
                str(len(filtered_unique_metrics)), str(total_unique_metrics)))
            unique_metrics = filtered_unique_metrics
            filtered = True
        remove_full_namespace_prefix = False
        if 'remove_full_namespace_prefix' in request.args:
            filtered_unique_metrics = []
            remove_full_namespace_prefix_str = request.args.get('remove_full_namespace_prefix', 'false')
            if remove_full_namespace_prefix_str == 'true':
                remove_full_namespace_prefix = True
                logger.info('/api?unique_metrics remove_full_namespace_prefix passed stripping: %s' % settings.FULL_NAMESPACE)
            if remove_full_namespace_prefix:
                basename_unique_metrics = []
                for metric_name in unique_metrics:
                    if metric_name.startswith(settings.FULL_NAMESPACE):
                        base_name = metric_name.replace(settings.FULL_NAMESPACE, '', 1)
                    else:
                        base_name = metric_name
                    basename_unique_metrics.append(base_name)
            logger.info('/api?unique_metrics stripped: %s' % settings.FULL_NAMESPACE)
            unique_metrics = basename_unique_metrics
        duration = (time.time() - start)

        data_dict = {
            "status": {
                "cluster_data": cluster_data,
                "filtered": filtered,
                "filter_namespace": filter_namespace,
                "remove_full_namespace_prefix": remove_full_namespace_prefix,
                "response": 200,
                "request_time": duration
            },
            "data": {
                "metrics": unique_metrics
            }
        }
        return jsonify(data_dict), 200

    # @added 20180929
    if 'get_json' in request.args:
        source = None
        metric = None
        timestamp = None
        full_duration_data = False
        if 'source' in request.args:
            valid_source = False
            source = request.args.get('source', None)
            if source == 'features_profile' or source == 'training_data':
                valid_source = True
            if not valid_source:
                resp = json.dumps(
                    {'results': 'Error: an invalid source parameter was passed to /api?get_json valid sources are features_profile or training_data'})
                return resp, 400
        else:
            resp = json.dumps(
                {'results': 'Error: the required parameter source was not passed to /api?get_json - valid sources are features_profile or training_data'})
            return resp, 400
        if 'metric' in request.args:
            metric = request.args.get('metric', None)
        if not metric:
            resp = json.dumps(
                {'results': 'Error: no metric parameter was passed to /api?get_json'})
            return resp, 400
        if 'timestamp' in request.args:
            timestamp = request.args.get('timestamp', None)
        if not timestamp:
            resp = json.dumps(
                {'results': 'Error: no timestamp parameter was passed to /api?get_json'})
            return resp, 400
        if metric and timestamp:
            tuple_json_file = '%s.json' % metric
            if 'full_duration_data' in request.args:
                full_duration_data = request.args.get('full_duration_data', None)
                if full_duration_data == 'true':
                    full_duration_in_hours = int(settings.FULL_DURATION / 60 / 60)
                    tuple_json_file = '%s.mirage.redis.%sh.json' % (metric, str(full_duration_in_hours))
            metric_timeseries_dir = metric.replace('.', '/')
            if source == 'features_profile':
                source_file = '%s/%s/%s/%s' % (
                    settings.IONOSPHERE_PROFILES_FOLDER, metric_timeseries_dir,
                    str(timestamp), tuple_json_file)
            if source == 'training_data':
                source_file = '%s/%s/%s/%s' % (
                    settings.IONOSPHERE_DATA_FOLDER, str(timestamp),
                    metric_timeseries_dir, tuple_json_file)
            logger.info('converting tuple data for %s %s at %s to json' % (source, metric, timestamp))
            datapoints = None
            if not os.path.isfile(source_file):
                logger.error('error :: file not found - %s' % source_file)
                resp = json.dumps(
                    {'results': '404 data file not found'})
                return resp, 404
            with open(source_file) as f:
                for line in f:
                    datapoints = str(line).replace('(', '[').replace(')', ']')
            data_dict = {'metric': metric}
            datapoints = literal_eval(datapoints)
            data_dict['datapoints'] = datapoints
            return jsonify(data_dict), 200

    # @added 20180720 - Feature #2464: luminosity_remote_data
    # Added luminosity_remote_data endpoint, requires two request parameter:
    if 'luminosity_remote_data' in request.args:
        logger.info('/api?luminosity_remote_data')
        anomaly_timestamp = None
        if 'anomaly_timestamp' in request.args:
            anomaly_timestamp_str = request.args.get(str('anomaly_timestamp'), 0)
            try:
                anomaly_timestamp = int(anomaly_timestamp_str)
            except:
                anomaly_timestamp = None
        else:
            logger.info('/api?luminosity_remote_data - no anomaly_timestamp parameter was passed to /api?luminosity_remote_data')
            resp = json.dumps(
                {'results': 'Error: no anomaly_timestamp parameter was passed to /api?luminosity_remote_data'})
            return resp, 400

        # @added 20201203 - Feature #3860: luminosity - handle low frequency data
        # Add the metric resolution
        resolution = 60
        if 'resolution' in request.args:
            resolution_str = request.args.get('resolution', 0)
            try:
                resolution = int(resolution_str)
            except:
                resolution = 60

        luminosity_data = []
        if anomaly_timestamp:
            # @modified 20201117 - Feature #3824: get_cluster_data
            #                      Feature #2464: luminosity_remote_data
            #                      Bug #3266: py3 Redis binary objects not strings
            #                      Branch #3262: py3
            # Wrapped in try
            try:
                # @modified 20201203 - Feature #3860: luminosity - handle low frequency data
                # Added resolution
                # luminosity_data, success, message = luminosity_remote_data(anomaly_timestamp)
                luminosity_data, success, message = luminosity_remote_data(anomaly_timestamp, resolution)

                if luminosity_data:
                    # @modified 20201123 - Feature #3824: get_cluster_data
                    #                      Feature #2464: luminosity_remote_data
                    # Change from json.dumps to jsonify
                    # resp = json.dumps(
                    #    {'results': luminosity_data})
                    # return resp, 200
                    data_dict = {'results': luminosity_data}
                    return jsonify(data_dict), 200
                else:
                    resp = json.dumps(
                        {'results': 'No data found'})
                    return resp, 404
            except Exception as e:
                error = "Error: " + str(e)
                logger.error('error :: luminosity_remote_data - %s' % str(e))
                resp = json.dumps({'results': error})
                return resp, 500
        # @added 20201117 - Feature #3824: get_cluster_data
        #                   Feature #2464: luminosity_remote_data
        # Log 400 if no timestamp
        else:
            logger.info('/api?luminosity_remote_data - no anomaly_timestamp parameter was passed to /api?luminosity_remote_data, returning 400')
            resp = json.dumps(
                {'results': 'Error: no anomaly_timestamp parameter was passed to /api?luminosity_remote_data'})
            return resp, 400

    if 'metric' in request.args:
        metric = request.args.get(str('metric'), None)
        try:
            # @modified 20200225 - Bug #3266: py3 Redis binary objects not strings
            #                      Branch #3262: py3
            # raw_series = REDIS_CONN.get(metric)
            raw_series = REDIS_CONN_UNDECODE.get(metric)
            if not raw_series:
                metric_name = '%s%s' % (settings.FULL_NAMESPACE, metric)
                # @added 20220112 - Bug #4374: webapp - handle url encoded chars
                # @modified 20220115 - Bug #4374: webapp - handle url encoded chars
                # Roll back change - breaking existing metrics with colons
                # metric_name = url_encode_metric_name(metric_name)

                raw_series = REDIS_CONN_UNDECODE.get(metric_name)
            if not raw_series:
                resp = json.dumps(
                    {'results': 'Error: No metric by that name - try /api?metric=' + settings.FULL_NAMESPACE + 'metric_namespace'})
                return resp, 404
            else:
                unpacker = Unpacker(use_list=False)
                unpacker.feed(raw_series)
                # @modified 20201117 - Feature #3824: get_cluster_data
                #                      Feature #2464: luminosity_remote_data
                #                      Bug #3266: py3 Redis binary objects not strings
                #                      Branch #3262: py3
                # Replace redefinition of item from line 1338
                # timeseries = [item[:2] for item in unpacker]
                timeseries = [ts_item[:2] for ts_item in unpacker]
                # @added 20210416
                remove_full_namespace_prefix = False
                if 'remove_full_namespace_prefix' in request.args:
                    remove_full_namespace_prefix_str = request.args.get('remove_full_namespace_prefix', 'false')
                    if remove_full_namespace_prefix_str == 'true':
                        remove_full_namespace_prefix = True
                        if metric.startswith(settings.FULL_NAMESPACE):
                            metric = metric.replace(settings.FULL_NAMESPACE, '', 1)
                if 'format' in request.args:
                    format = request.args.get(str('format'), 'pjson')
                    if format == 'json':
                        duration = (time.time() - start)
                        data_dict = {
                            "status": {
                                "cluster_data": cluster_data,
                                "format": format,
                                "response": 200,
                                "request_time": duration,
                                "remove_full_namespace_prefix": remove_full_namespace_prefix
                            },
                            "data": {
                                "metric": metric,
                                "timeseries": timeseries
                            }
                        }
                        return jsonify(data_dict), 200
                resp = json.dumps({'results': timeseries})
                return resp, 200
        except Exception as e:
            error = "Error: " + str(e)
            resp = json.dumps({'results': error})
            return resp, 500

    if 'graphite_metric' in request.args:
        logger.info('processing graphite_metric api request')
        for i in request.args:
            key = str(i)
            value = request.args.get(key, None)
            logger.info('request argument - %s=%s' % (key, str(value)))

        valid_request = True
        missing_arguments = []

        metric = request.args.get('graphite_metric', None)
        from_timestamp = request.args.get('from_timestamp', None)
        until_timestamp = request.args.get('until_timestamp', None)

        if not metric:
            valid_request = False
            missing_arguments.append('graphite_metric')
            logger.error('graphite_metric argument not found')
        else:
            logger.info('graphite_metric - %s' % metric)

        if not from_timestamp:
            valid_request = False
            missing_arguments.append('from_timestamp')
            logger.error('from_timestamp argument not found')
        else:
            logger.info('from_timestamp - %s' % str(from_timestamp))

        if not until_timestamp:
            valid_request = False
            missing_arguments.append('until_timestamp')
        else:
            logger.info('until_timestamp - %s' % str(until_timestamp))

        if not valid_request:
            error = 'Error: not all arguments where passed, missing %s' % str(missing_arguments)
            resp = json.dumps({'results': error})
            return resp, 404
        else:
            logger.info('requesting data from graphite for %s from %s to %s' % (
                str(metric), str(from_timestamp), str(until_timestamp)))

        try:
            timeseries = get_graphite_metric(
                skyline_app, metric, from_timestamp, until_timestamp, 'json',
                'object')
        except:
            error = 'error :: %s' % str(traceback.print_exc())
            resp = json.dumps({'results': error})
            return resp, 500

        resp = json.dumps({'results': timeseries})
        cleaned_resp = False
        try:
            # @modified 20191014 - Task #3270: Deprecate string.replace for py3
            # format_resp_1 = string.replace(str(resp), '"[[', '[[')
            # cleaned_resp = string.replace(str(format_resp_1), ']]"', ']]')
            resp_str = str(resp)
            format_resp_1 = resp_str.replace('"[[', '[[')
            cleaned_resp = format_resp_1.replace(']]"', ']]')

        except:
            logger.error('error :: failed string replace resp: ' + traceback.format_exc())

        if cleaned_resp:
            return cleaned_resp, 200
        else:
            resp = json.dumps(
                {'results': 'Error: failed to generate timeseries'})
            return resp, 404

    # @modified 20200128 - Feature #3424: webapp - api documentation
    # resp = json.dumps(
    #     {'results': 'Error: No argument passed - try /api?metric= or /api?graphite_metric='})
    # return resp, 404
    try:
        start = time.time()
        return render_template(
            'api.html', version=skyline_version, duration=(time.time() - start)), 200
    except:
        return 'Uh oh ... a Skyline 500 :(', 500


# @added 20200116: Feature #3396: http_alerter
@app.route("/mock_api", methods=['GET', 'POST'])
def mock_api():
    if 'alert_reciever' in request.args:
        if request.method == 'POST':
            try:
                # post_data = request.form.to_dict()
                post_data = request.get_json()
                logger.info('mock_api :: /alert_reciever recieved %s' % str(post_data))
                metric = str(post_data['data']['alert']['metric'])
                # timestamp = int(request.form['timestamp'])
                # value = float(request.form['value'])
                timestamp = int(post_data['data']['alert']['timestamp'])
                value = float(post_data['data']['alert']['value'])
                # full_duration = int(request.form['full_duration'])
                # expiry = int(request.form['expiry'])
                full_duration = int(post_data['data']['alert']['full_duration'])
                expiry = int(post_data['data']['alert']['expiry'])
                source = str(post_data['data']['alert']['source'])
                token = str(post_data['data']['alert']['token'])
                metric_alert_dict = {
                    "metric": metric,
                    "timestamp": timestamp,
                    "value": value,
                    "full_duration": full_duration,
                    "expiry": expiry,
                    "source": source,
                    "token": token
                }
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: Webapp could not get alert details from the alert_reciever POST request - %s' % str(request.form))
                resp = json.dumps(
                    {'error': 'malformed or missing data in the POST request'})
                return resp, 400
            testing_http_alerter = False
            if testing_http_alerter:
                resp = json.dumps(
                    {'status': 'testing with a 400'})
                return resp, 400
            data_dict = {"status": {}, "data": {"alert": metric_alert_dict}}
            redis_set = 'mock_api.alert_reciever.alerts'
            try:
                REDIS_CONN_UNDECODE.sadd(redis_set, str(data_dict))
                # @added 20200528 - Feature #3560: External alert config
                # Expire this set
                REDIS_CONN_UNDECODE.expire(redis_set, 3600)
                logger.info('added alert to %s' % redis_set)
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: mock_api :: could not add data_dict to Redis set - %s' % redis_set)
            logger.info('mock_api :: responding with 200 and %s' % str(data_dict))
            return jsonify(data_dict), 200

    # @added 20200528 - Feature #3560: External alert config
    if 'alert_config' in request.args:
        token = None
        if request.method == 'GET':
            # return 'Method Not Allowed', 405
            try:
                token = request.args.get('token', None)
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: Webapp could not get token from the alert_config request, returning 400')
        if request.method == 'POST':
            try:
                post_data = request.get_json()
                logger.info('mock_api :: /alert_config recieved %s' % str(post_data))
                token = str(post_data['data']['token'])
            except:
                logger.error(traceback.format_exc())
                # @modified 20210421 - Task #4030: refactoring
                # semgrep - python-logger-credential-disclosure
                # logger.error('error :: Webapp could not get token from the alert_config POST request, returning 400 - %s' % str(request.form))
                logger.error('error :: Webapp could not get token from the alert_config POST request, returning 400')
        if not token:
            error_msg = 'no token found'
            logger.error('error :: %s, returning 400' % error_msg)
            data_dict = {"status": {"error"}, "data": {"error": error_msg}}
            return jsonify(data_dict), 400
        if token != settings.FLUX_SELF_API_KEY:
            error_msg = 'no token found'
            # @modified 20210421 - Task #4030: refactoring
            # semgrep - python-logger-credential-disclosure
            # logger.error('error :: the token %s does not match settings.FLUX_SELF_API_KEY, returning 401' % str(token))
            logger.error('error :: the token does not match settings.FLUX_SELF_API_KEY, returning 401')
            return 'Unauthorized', 401
        count = 1
        alerts_dict = {}
        for alert in settings.ALERTS:
            try:
                namespace = alert[0]
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: mock_api :: alert_config :: could not determine namespace from alert_tuple - %s' % str(alert))
                continue
            try:
                alerter = alert[1]
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: mock_api :: alert_config :: could not determine alerter from alert_tuple - %s' % str(alert))
                continue
            try:
                expiration = alert[2]
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: mock_api :: alert_config :: could not determine expiration from alert_tuple - %s' % str(alert))
                continue
            try:
                second_order_resolution_hours = int(alert[3])
                second_order_resolution = int(second_order_resolution_hours * 3600)
            except:
                second_order_resolution = settings.FULL_DURATION
            namespace_prefix_1 = namespace.replace('^', '')
            namespace_prefix_2 = namespace_prefix_1.replace('\\', '')
            namespace_prefix = namespace_prefix_2.split('.')[0]
            learn_days = 30
            count_id = str(count)
            alerts_dict[count_id] = {
                'alerter': str(alerter),
                'expiration': str(expiration),
                'namespace': str(namespace),
                'namespace_prefix': str(namespace_prefix),
                'second_order_resolution': str(second_order_resolution),
                'learn_days': str(learn_days)
            }
            count += 1
        data_dict = {"status": {}, "data": alerts_dict}
        return jsonify(data_dict), 200

    # @added 20200528 - Feature #3560: External alert config
    if 'test_alert_config' in request.args:
        alerts_dict = {}
        alerts_dict['test_external_alert_config'] = {
            'alerter': 'http_alerter-mock_api',
            'expiration': '60',
            'namespace': 'analyzer.runtime',
            'namespace_prefix': 'skyline',
            'second_order_resolution': '604800',
            'learn_days': '30'
        }
        data_dict = {"status": {}, "data": alerts_dict}
        return jsonify(data_dict), 200

    # @added 20210601 - Feature #4000: EXTERNAL_SETTINGS
    if 'test_external_settings' in request.args:
        settings_dict = {
            'id': 'test_external_settings',
            'namespace': 'skyline-test-external-settings',
            'retention_1_resolution_seconds': 60,
            'retention_1_period_seconds': 604800,
            'retention_2_resolution_seconds': 600,
            'retention_2_period_seconds': 63072000,
            'full_duration': 86400,
            'second_order_resolution_seconds': 604800,
            'learn_full_duration_seconds': 2592000,
            'flux_token': None,
            "thunder_alert_endpoint": 'http://127.0.0.1:1500/mock_api?alert_reciever',
            'thunder_alert_token': None,
            'alert_on_no_data': {
                'enabled': True,
                'stale_period': 300,
                'expiry': 1800,
            },
            'alert_on_stale_metrics': {
                'enabled': True,
                'stale_period': 900,
                'expiry': 0,
            },
        }
        namespaces = [settings_dict]
        data_dict = {"status": {}, "data": {"namespaces": namespaces}}
        return jsonify(data_dict), 200


# @added 20180721 - Feature #2464: luminosity_remote_data
# Add a specific route for the luminosity_remote_data endpoint so that the
# response can be gzipped as even the preprocessed data can run into megabyte
# reponses.
@app.route("/luminosity_remote_data", methods=['GET'])
@gzipped
def luminosity_remote_data_endpoint():
    # The luminosity_remote_data_endpoint, requires onerequest parameter:
    if 'anomaly_timestamp' in request.args:
        anomaly_timestamp_str = request.args.get(str('anomaly_timestamp'), None)
        try:
            anomaly_timestamp = int(anomaly_timestamp_str)
        except:
            anomaly_timestamp = None
    else:
        resp = json.dumps(
            {'results': 'Error: no anomaly_timestamp parameter was passed to /luminosity_remote_data'})
        return resp, 400
    luminosity_data = []
    if anomaly_timestamp:
        luminosity_data, success, message = luminosity_remote_data(anomaly_timestamp)
        if luminosity_data:
            resp = json.dumps(
                {'results': luminosity_data})
            logger.info('returning gzipped response')
            return resp, 200
        else:
            resp = json.dumps(
                {'results': 'No data found'})
            return resp, 404


@app.route("/docs")
@requires_auth
def docs():
    start = time.time()
    try:
        return render_template(
            'docs.html', version=skyline_version, duration=(time.time() - start)), 200
    except:
        return 'Uh oh ... a Skyline 500 :(', 500


@app.route("/panorama", methods=['GET'])
@requires_auth
def panorama():
    if not settings.PANORAMA_ENABLED:
        try:
            return render_template(
                'uh_oh.html', version=skyline_version,
                message="Panorama is not enabled, please see the Panorama section in the docs and settings.py"), 200
        except:
            return 'Uh oh ... a Skyline 500 :(', 500

    start = time.time()

    # @added 20211125 - Feature #4326: webapp - panorama_plot_anomalies
    if 'plot_metric_anomalies' in request.args:
        metric = None
        anomalies_dict = {}
        plot_file = None
        if 'metric' in request.args:
            metric = request.args.get('metric', 'None')
            if metric == 'None':
                metric = None
        from_timestamp = None
        if 'from_timestamp' in request.args:
            from_timestamp = request.args.get('from_timestamp', 'all')
            if ':' in from_timestamp:
                new_from_timestamp = time.mktime(datetime.datetime.strptime(from_timestamp, '%Y-%m-%d %H:%M').timetuple())
                from_timestamp = int(new_from_timestamp)
            if from_timestamp == 'all':
                from_timestamp = None
            else:
                try:
                    from_timestamp = int(from_timestamp)
                except:
                    data_dict = {"status": {"response": 400, "request_time": (time.time() - start)}, "data": {"error": "invalid from_timestamp"}}
                    return jsonify(data_dict), 400
        until_timestamp = None
        if 'until_timestamp' in request.args:
            until_timestamp = request.args.get('until_timestamp', 'all')
            if ':' in until_timestamp:
                new_until_timestamp = time.mktime(datetime.datetime.strptime(until_timestamp, '%Y-%m-%d %H:%M').timetuple())
                until_timestamp = int(new_until_timestamp)
            if until_timestamp == 'all':
                until_timestamp = None
            else:
                try:
                    until_timestamp = int(until_timestamp)
                except:
                    data_dict = {"status": {"response": 400, "request_time": (time.time() - start)}, "data": {"error": "invalid until_timestamp"}}
                    return jsonify(data_dict), 400
        if metric:
            try:
                anomalies_dict, plot_file = panorama_plot_anomalies(metric, from_timestamp, until_timestamp)
            except:
                trace = traceback.format_exc()
                message = 'Uh oh ... a Skyline 500 using panorama_plot_anomalies'
                return internal_error(message, trace)
        return render_template(
            'panorama.html', plot_metric_anomalies=True, metric=metric,
            anomalies_dict=anomalies_dict, plot_file=plot_file,
            version=skyline_version,
            duration=(time.time() - start), print_debug=False), 200

    # @added 20210326 - Feature #3994: Panorama - mirage not anomalous
    if 'not_anomalous' in request.args:
        not_anomalous_dict = {}
        anomalies_dict = {}
        try:
            not_anomalous_dict, anomalies_dict = get_mirage_not_anomalous_metrics()
        except:
            trace = traceback.format_exc()
            message = 'Uh oh ... a Skyline 500 using get_mirage_not_anomalous_metrics'
            return internal_error(message, trace)
        format = None
        if 'format' in request.args:
            format = request.args.get('format', 'normal')
        anomalies = False
        if 'anomalies' in request.args:
            anomalies_str = request.args.get('anomalies', 'false')
            if anomalies_str == 'true':
                anomalies = True
        if format == 'json':
            if not not_anomalous_dict and not anomalies_dict:
                data_dict = {"status": {"response": 204, "request_time": (time.time() - start), "message": "no data"}, "data": {"not anomalous": {}, "anomalies": {}}}
            else:
                if anomalies:
                    data_dict = {"status": {"response": 200, "request_time": (time.time() - start)}, "data": {"not anomalous": not_anomalous_dict, "anomalies": anomalies_dict}}
                else:
                    data_dict = {"status": {"response": 200, "request_time": (time.time() - start)}, "data": {"not anomalous": not_anomalous_dict}}
            logger.info('mirage_not_anomalous returned json with %s metrics' % str(len(not_anomalous_dict)))
            return jsonify(data_dict), 200

        not_anomalous_metrics_dict = {}
        if not_anomalous_dict:
            for i_metric in list(not_anomalous_dict.keys()):
                not_anomalous_metrics_dict[i_metric] = {}
                not_anomalous_metrics_dict[i_metric]['count'] = len(not_anomalous_dict[i_metric]['timestamps'])
                not_anomalous_metrics_dict[i_metric]['from'] = not_anomalous_dict[i_metric]['from']
                not_anomalous_metrics_dict[i_metric]['until'] = not_anomalous_dict[i_metric]['until']
        anomalies_metrics_dict = {}
        if anomalies_dict:
            for i_metric in list(anomalies_dict.keys()):
                anomalies_metrics_dict[i_metric] = {}
                anomalies_metrics_dict[i_metric]['count'] = len(anomalies_dict[i_metric]['timestamps'])
                anomalies_metrics_dict[i_metric]['from'] = anomalies_dict[i_metric]['from']
                anomalies_metrics_dict[i_metric]['until'] = anomalies_dict[i_metric]['until']

        return render_template(
            'panorama.html', not_anomalous=True,
            not_anomalous_dict=not_anomalous_dict,
            not_anomalous_metrics_dict=not_anomalous_metrics_dict,
            anomalies_dict=anomalies_dict,
            anomalies_metrics_dict=anomalies_metrics_dict,
            version=skyline_version,
            duration=(time.time() - start), print_debug=False), 200

    # @added 20210328 - Feature #3994: Panorama - mirage not anomalous
    if 'not_anomalous_metric' in request.args:
        base_name = None
        anomalies = False
        if 'metric' in request.args:
            base_name = request.args.get('metric', None)
            if base_name == 'all':
                base_name = None
        if not base_name:
            data_dict = {"status": {"response": 400, "request_time": (time.time() - start)}, "error": "no metric parameter passed", "data": {}}
            return jsonify(data_dict), 400
        from_timestamp = None
        if 'from_timestamp' in request.args:
            from_timestamp = request.args.get('from_timestamp', 'all')
        if not from_timestamp:
            data_dict = {"status": {"response": 400, "request_time": (time.time() - start)}, "error": "no from_timestamp parameter passed", "data": {}}
            return jsonify(data_dict), 400
        until_timestamp = None
        if 'until_timestamp' in request.args:
            until_timestamp = request.args.get('until_timestamp', 'all')
        if not until_timestamp:
            data_dict = {"status": {"response": 400, "request_time": (time.time() - start)}, "error": "no until_timestamp parameter passed", "data": {}}
            return jsonify(data_dict), 400
        if 'anomalies' in request.args:
            anomalies_arg = request.args.get('anomalies', 'false')
            if anomalies_arg == 'true':
                anomalies = True

        def get_cache_dict(plot_type, base_name, from_timestamp, until_timestamp):
            data_dict_key = 'panorama.%s_dict.%s.%s.%s' % (
                plot_type, str(from_timestamp), str(until_timestamp), base_name)
            metric_data_dict = {}
            data_dict = {}
            try:
                raw_data_dict = REDIS_CONN.get(data_dict_key)
                if raw_data_dict:
                    metric_data_dict = literal_eval(raw_data_dict)
                if metric_data_dict:
                    logger.info('key found: %s' % data_dict_key)
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: Webapp could not get the Redis key - %s' % data_dict_key)
            if not metric_data_dict:
                logger.info('key not found: %s' % data_dict_key)
                try:
                    data_dict_all_key = 'panorama.%s_dict.%s.%s' % (
                        plot_type, str(from_timestamp), str(until_timestamp))
                    raw_data_dict_all = REDIS_CONN.get(data_dict_all_key)
                    data_dict_all = {}
                    if raw_data_dict_all:
                        data_dict_all = literal_eval(raw_data_dict_all)
                    if data_dict_all:
                        metric_data_dict = data_dict_all[base_name]
                    if data_dict:
                        logger.info('key found: %s' % data_dict_all_key)
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: Webapp could not get the Redis key - %s' % data_dict_all_key)
            if not metric_data_dict:
                logger.info('key not found: %s' % data_dict_all_key)
                recent_data_dict_all_key = 'panorama.%s_dict.recent' % plot_type
                try:
                    raw_data_dict_all = REDIS_CONN.get(recent_data_dict_all_key)
                    data_dict_all = {}
                    if raw_data_dict_all:
                        data_dict_all = literal_eval(raw_data_dict_all)
                    if data_dict_all:
                        metric_data_dict = data_dict_all[base_name]
                    if data_dict:
                        logger.info('key found: %s' % data_dict_all_key)
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: Webapp could not get the Redis key - %s' % recent_data_dict_all_key)
            if metric_data_dict:
                data_dict[base_name] = metric_data_dict
            return data_dict

        not_anomalous_dict = {}
        try:
            not_anomalous_dict = get_cache_dict('not_anomalous', base_name, from_timestamp, until_timestamp)
        except:
            trace = traceback.format_exc()
            message = 'Uh oh ... a Skyline 500 using get_mirage_not_anomalous_metrics'
            return internal_error(message, trace)
        anomalies_dict = {}
        if anomalies:
            try:
                anomalies_dict = get_cache_dict('anomalies', base_name, from_timestamp, until_timestamp)
            except:
                trace = traceback.format_exc()
                message = 'Uh oh ... a Skyline 500 using get_mirage_not_anomalous_metrics'
                return internal_error(message, trace)

        if not not_anomalous_dict:
            logger.info('not_anomalous_dict data not found in Redis')
            logger.info('calling get_mirage_not_anomalous_metrics(%s, %s, %s, %s)' % (
                str(base_name), str(from_timestamp), str(until_timestamp),
                str(anomalies)))
            try:
                not_anomalous_dict, anomalies_dict = get_mirage_not_anomalous_metrics(base_name, from_timestamp, until_timestamp, anomalies)
            except:
                trace = traceback.format_exc()
                message = 'Uh oh ... a Skyline 500 using get_mirage_not_anomalous_metrics'
                return internal_error(message, trace)
        format = None
        if 'format' in request.args:
            format = request.args.get('format', 'normal')
        if format == 'json':
            if anomalies:
                data_dict = {"status": {"response": 200, "request_time": (time.time() - start)}, "data": {"not anomalous": not_anomalous_dict, "anomalies": anomalies_dict}}
            else:
                data_dict = {"status": {"response": 200, "request_time": (time.time() - start)}, "data": {"not anomalous": not_anomalous_dict}}
            try:
                not_anomalous_count = len(not_anomalous_dict[base_name]['timestamps'])
            except:
                not_anomalous_count = 0
            if anomalies:
                try:
                    anomalies_count = len(anomalies_dict[base_name]['timestamps'])
                except:
                    anomalies_count = 0
            if anomalies:
                logger.info('mirage_not_anomalous returned json with %s not_anomalous events and %s anomalies for %s' % (
                    str(not_anomalous_count), str(anomalies_count), base_name))
            else:
                logger.info('mirage_not_anomalous returned json with %s not_anomalous events for %s' % (
                    str(not_anomalous_count), base_name))
            return jsonify(data_dict), 200

        try:
            not_anomalous_plot = plot_not_anomalous_metric(not_anomalous_dict, anomalies_dict, 'not_anomalous')
        except:
            trace = traceback.format_exc()
            message = 'Uh oh ... a Skyline 500 using get_mirage_not_anomalous_metrics'
            return internal_error(message, trace)

        anomalies_plot = False
        if anomalies:
            try:
                anomalies_plot = plot_not_anomalous_metric(not_anomalous_dict, anomalies_dict, 'anomalies')
            except:
                trace = traceback.format_exc()
                message = 'Uh oh ... a Skyline 500 using get_mirage_not_anomalous_metrics'
                return internal_error(message, trace)

        return render_template(
            'panorama.html', not_anomalous_metric=True, metric=base_name,
            not_anomalous_dict=not_anomalous_dict,
            from_timestamp=from_timestamp, until_timestamp=until_timestamp,
            not_anomalous_plot=not_anomalous_plot,
            anomalies_plot=anomalies_plot,
            version=skyline_version,
            duration=(time.time() - start), print_debug=False), 200

    try:
        apps = get_list('app')
    except:
        logger.error('error :: %s' % traceback.print_exc())
        apps = ['None']
    try:
        sources = get_list('source')
    except:
        logger.error('error :: %s' % traceback.print_exc())
        sources = ['None']
    try:
        algorithms = get_list('algorithm')
    except:
        logger.error('error :: %s' % traceback.print_exc())
        algorithms = ['None']
    try:
        hosts = get_list('host')
    except:
        logger.error('error :: %s' % traceback.print_exc())
        hosts = ['None']

    request_args_present = False
    try:
        request_args_len = len(request.args)
        request_args_present = True
    except:
        request_args_len = 0

    # @added 20200226: Ideas #2476: Label and relate anomalies
    #                  Feature #2516: Add label to features profile
    label_anomalies_request = False
    if 'label_anomalies' in request.args:
        label_anomalies_request = request.args.get(str('label_anomalies'), None)
        if label_anomalies_request == 'true':
            label_anomalies_request = True
    if label_anomalies_request:
        start_timestamp = 0
        end_timestamp = 0
        metrics_list = []
        namespaces_list = []
        label = None
        if 'start_timestamp' in request.args:
            start_timestamp = request.args.get(str('start_timestamp'), None)
        if 'end_timestamp' in request.args:
            end_timestamp = request.args.get(str('end_timestamp'), None)
        if 'metrics' in request.args:
            metrics = request.args.get(str('metrics'), None)
        if 'namespaces' in request.args:
            namespaces = request.args.get(str('namespaces'), None)
        if 'label' in request.args:
            label = request.args.get(str('label'), None)
        do_label_anomalies = False
        if start_timestamp and end_timestamp and label:
            if metrics:
                do_label_anomalies = True
                metrics_list = metrics.split(',')
                logger.info('label_anomalies metrics_list - %s' % str(metrics_list))
            if namespaces:
                do_label_anomalies = True
                namespaces_list = namespaces.split(',')
                logger.info('label_anomalies namespaces_list - %s' % str(namespaces_list))
        labelled = False
        anomalies_labelled = 0
        if do_label_anomalies:
            labelled, anomalies_labelled = label_anomalies(start_timestamp, end_timestamp, metrics_list, namespaces_list, label)
        return render_template(
            'panorama.html', label_anomalies=True,
            anomalies_labelled=anomalies_labelled,
            start_timestamp=int(start_timestamp), end_timestamp=int(end_timestamp),
            metrics=metrics_list, namespaces=namespaces_list, label=label,
            version=skyline_version,
            duration=(time.time() - start), print_debug=False), 200

    # @added 20160803 - Sanitize request.args
    REQUEST_ARGS = ['from_date',
                    'from_time',
                    'from_timestamp',
                    'until_date',
                    'until_time',
                    'until_timestamp',
                    'count_by_metric',
                    'metric',
                    'metric_like',
                    'app',
                    'source',
                    'host',
                    'algorithm',
                    'limit',
                    'order',
                    # @added 20161127 - Branch #922: ionosphere
                    'panorama_anomaly_id',
                    ]

    # @added 20190919 - Feature #3230: users DB table
    #                   Ideas #2476: Label and relate anomalies
    #                   Feature #2516: Add label to features profile
    user_id = None
    if settings.WEBAPP_AUTH_ENABLED:
        auth = request.authorization
        user = auth.username
    else:
        user = 'Skyline'
        user_id = 1
    if not user_id:
        success, user_id = get_user_details(skyline_app, 'id', 'username', str(user))
        if not success:
            logger.error('error : /panorama could not get_user_details(%s)' % str(user))
            return 'Internal Server Error - ref: i - could not determine user_id', 500
        else:
            try:
                user_id = int(user_id)
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: /panorama get_user_details(%s, %s) did not return an int' % (
                    str(user), str(user_id)))
                return 'Internal Server Error - ref: p - user_id not int', 500

    get_anomaly_id = False
    if request_args_present:
        for i in request.args:
            key = str(i)
            if key not in REQUEST_ARGS:
                logger.error('error :: invalid request argument - %s=%s' % (key, str(i)))
                # @modified 20190524 - Branch #3002: docker
                # Return data
                # return 'Bad Request', 400
                error_string = 'error :: invalid request argument - %s=%s' % (key, str(i))
                logger.error(error_string)
                resp = json.dumps(
                    {'400 Bad Request': error_string})
                return flask_escape(resp), 400

            value = request.args.get(key, None)
            logger.info('request argument - %s=%s' % (key, str(value)))

            # @added 20161127 - Branch #922: ionosphere
            if key == 'panorama_anomaly_id':
                if str(value) == 'true':
                    get_anomaly_id = True

            if key == 'metric' and value != 'all':
                if value != '':
                    try:
                        unique_metrics = list(REDIS_CONN.smembers(settings.FULL_NAMESPACE + 'unique_metrics'))
                    except:
                        logger.error('error :: Webapp could not get the unique_metrics list from Redis')
                        logger.info(traceback.format_exc())
                        return 'Internal Server Error', 500
                    metric_name = settings.FULL_NAMESPACE + value
                    # @added 20220112 - Bug #4374: webapp - handle url encoded chars
                    # @modified 20220115 - Bug #4374: webapp - handle url encoded chars
                    # Roll back change - breaking existing metrics with colons
                    # metric_name = url_encode_metric_name(metric_name)

                    # @added 20180423 - Feature #2034: analyse_derivatives
                    #                   Branch #2270: luminosity
                    other_unique_metrics = []
                    # @added 20190105 - Bug #2792: webapp 500 error on no metric
                    # This needs to be set before the below conditional check
                    # otherwise webapp return a 500 server error instead of a
                    # 404 if the metric does not exist
                    metric_found_in_other_redis = False

                    if metric_name not in unique_metrics and settings.OTHER_SKYLINE_REDIS_INSTANCES:
                        metric_found_in_other_redis = False

                        # @modified 20180519 - Feature #2378: Add redis auth to Skyline and rebrow
                        # for redis_ip, redis_port in settings.OTHER_SKYLINE_REDIS_INSTANCES:
                        for redis_ip, redis_port, redis_password in settings.OTHER_SKYLINE_REDIS_INSTANCES:
                            if not metric_found_in_other_redis:
                                try:
                                    if redis_password:
                                        other_redis_conn = redis.StrictRedis(host=str(redis_ip), port=int(redis_port), password=str(redis_password))
                                    else:
                                        other_redis_conn = redis.StrictRedis(host=str(redis_ip), port=int(redis_port))
                                    other_unique_metrics = list(other_redis_conn.smembers(settings.FULL_NAMESPACE + 'unique_metrics'))
                                except:
                                    logger.error(traceback.format_exc())
                                    logger.error('error :: failed to connect to Redis at %s on port %s' % (str(redis_ip), str(redis_port)))
                                if metric_name in other_unique_metrics:
                                    metric_found_in_other_redis = True
                                    logger.info('%s found in derivative_metrics in Redis at %s on port %s' % (metric_name, str(redis_ip), str(redis_port)))

                    # @added 20201127 - Feature #3824: get_cluster_data
                    #                   Feature #3820: HORIZON_SHARDS
                    # Change from the deprecated OTHER_SKYLINE_REDIS_INSTANCES
                    # to use the REMOTE_SKYLINE_INSTANCES and get_cluster_data
                    if settings.REMOTE_SKYLINE_INSTANCES:
                        remote_unique_metrics = None
                        try:
                            remote_unique_metrics = get_cluster_data('unique_metrics', 'metrics')
                        except:
                            logger.error(traceback.format_exc())
                            logger.error('error :: Webapp could not get unique_metrics from the remote Skyline instances')
                        if remote_unique_metrics:
                            logger.info('got %s remote unique_metrics from the remote Skyline instances' % str(len(remote_unique_metrics)))
                            unique_metrics_list = unique_metrics + remote_unique_metrics
                            unique_metrics = list(set(unique_metrics_list))

                    if metric_name not in unique_metrics and not metric_found_in_other_redis:
                        # @added 20200715 - Task #3648: webapp - fp_search - do not use Redis metric check
                        # Do not use the Redis metric check result in a webapp
                        # fp_search request as this allows retried metric fps to
                        # accessed
                        if request.args.get(str('fp_search'), None):
                            logger.info('check request.args -  %s not in Redis, but fp_search request so continuing' % metric_name)
                        else:
                            error_string = 'error :: no metric - %s - exists in Redis' % metric_name
                            logger.error(error_string)
                            resp = json.dumps(
                                {'404 Not Found': error_string})
                            # @modified 20190116 - Cross-Site Scripting Security Vulnerability #85
                            #                      Bug #2816: Cross-Site Scripting Security Vulnerability
                            # return resp, 404
                            return flask_escape(resp), 404

            if key == 'count_by_metric':
                count_by_metric_invalid = True
                if value == 'false':
                    count_by_metric_invalid = False
                if value == 'true':
                    count_by_metric_invalid = False
                if count_by_metric_invalid:
                    error_string = 'error :: invalid %s value passed %s' % (key, str(value))
                    logger.error(error_string)
                    # @modified 20190524 - Branch #3002: docker
                    # Return data
                    # return 'Bad Request', 400
                    resp = json.dumps(
                        {'400 Bad Request': error_string})
                    return flask_escape(resp), 400

            if key == 'metric_like':
                if value == 'all':
                    metric_namespace_pattern = value.replace('all', '')

                metric_namespace_pattern = value.replace('%', '')
                if metric_namespace_pattern != '' and value != 'all':
                    try:
                        unique_metrics = list(REDIS_CONN.smembers(settings.FULL_NAMESPACE + 'unique_metrics'))
                    except:
                        logger.error('error :: Webapp could not get the unique_metrics list from Redis')
                        logger.info(traceback.format_exc())
                        return 'Internal Server Error', 500

                    matching = [s for s in unique_metrics if metric_namespace_pattern in s]
                    if len(matching) == 0:
                        error_string = 'error :: no metric like - %s - exists in Redis' % metric_namespace_pattern
                        logger.error(error_string)
                        resp = json.dumps(
                            {'results': error_string})
                        # @modified 20190116 - Cross-Site Scripting Security Vulnerability #85
                        #                      Bug #2816: Cross-Site Scripting Security Vulnerability
                        # return resp, 404
                        return flask_escape(resp), 404

            if key == 'from_timestamp' or key == 'until_timestamp':
                timestamp_format_invalid = True
                if value == 'all':
                    timestamp_format_invalid = False
                # unix timestamp
                if value.isdigit():
                    timestamp_format_invalid = False
                # %Y%m%d %H:%M timestamp
                if timestamp_format_invalid:
                    value_strip_colon = value.replace(':', '')
                    new_value = value_strip_colon.replace(' ', '')
                    if new_value.isdigit():
                        timestamp_format_invalid = False
                if timestamp_format_invalid:
                    error_string = 'error :: invalid %s value passed %s' % (key, value)
                    logger.error('error :: invalid %s value passed %s' % (key, value))
                    # @modified 20190524 - Branch #3002: docker
                    # Return data
                    # return 'Bad Request', 400
                    resp = json.dumps(
                        {'400 Bad Request': error_string})
                    return flask_escape(resp), 400

            if key == 'app':
                if value != 'all':
                    if value not in apps:
                        error_string = 'error :: no %s - %s' % (key, value)
                        logger.error(error_string)
                        resp = json.dumps(
                            {'results': error_string})
                        # @modified 20190116 - Cross-Site Scripting Security Vulnerability #85
                        #                      Bug #2816: Cross-Site Scripting Security Vulnerability
                        # return resp, 404
                        return flask_escape(resp), 404

            if key == 'source':
                if value != 'all':
                    if value not in sources:
                        error_string = 'error :: no %s - %s' % (key, value)
                        logger.error(error_string)
                        resp = json.dumps(
                            {'results': error_string})
                        # @modified 20190116 - Cross-Site Scripting Security Vulnerability #85
                        #                      Bug #2816: Cross-Site Scripting Security Vulnerability
                        # return resp, 404
                        return flask_escape(resp), 404

            if key == 'algorithm':
                if value != 'all':
                    if value not in algorithms:
                        error_string = 'error :: no %s - %s' % (key, value)
                        logger.error(error_string)
                        resp = json.dumps(
                            {'results': error_string})
                        # @modified 20190116 - Cross-Site Scripting Security Vulnerability #85
                        #                      Bug #2816: Cross-Site Scripting Security Vulnerability
                        # return resp, 404
                        return flask_escape(resp), 404

            if key == 'host':
                if value != 'all':
                    if value not in hosts:
                        error_string = 'error :: no %s - %s' % (key, value)
                        logger.error(error_string)
                        resp = json.dumps(
                            {'results': error_string})
                        # @modified 20190116 - Cross-Site Scripting Security Vulnerability #85
                        #                      Bug #2816: Cross-Site Scripting Security Vulnerability
                        # return resp, 404
                        return flask_escape(resp), 404

            if key == 'limit':
                limit_invalid = True
                limit_is_not_numeric = True
                if value.isdigit():
                    limit_is_not_numeric = False

                if limit_is_not_numeric:
                    error_string = 'error :: %s must be a numeric value - requested %s' % (key, value)
                    logger.error(error_string)
                    resp = json.dumps(
                        {'results': error_string})
                    # @modified 20190116 - Cross-Site Scripting Security Vulnerability #85
                    #                      Bug #2816: Cross-Site Scripting Security Vulnerability
                    # return resp, 400
                    return flask_escape(resp), 400

                new_value = int(value)
                try:
                    valid_value = new_value + 1
                except:
                    valid_value = None
                if valid_value and new_value < 101:
                    limit_invalid = False
                if limit_invalid:
                    error_string = 'error :: %s must be < 100 - requested %s' % (key, value)
                    logger.error(error_string)
                    resp = json.dumps(
                        {'results': error_string})
                    # @modified 20190116 - Cross-Site Scripting Security Vulnerability #85
                    #                      Bug #2816: Cross-Site Scripting Security Vulnerability
                    # return resp, 400
                    return flask_escape(resp), 400

            if key == 'order':
                order_invalid = True
                if value == 'DESC':
                    order_invalid = False
                if value == 'ASC':
                    order_invalid = False
                if order_invalid:
                    error_string = 'error :: %s must be DESC or ASC' % (key)
                    logger.error(error_string)
                    resp = json.dumps(
                        {'results': error_string})
                    # @modified 20190116 - Cross-Site Scripting Security Vulnerability #85
                    #                      Bug #2816: Cross-Site Scripting Security Vulnerability
                    # return resp, 400
                    return flask_escape(resp), 400

    # @added 20161127 - Branch #922: ionosphere
    if get_anomaly_id:
        try:
            query, panorama_data = panorama_request()
            logger.info('debug :: panorama_data - %s' % str(panorama_data))
        except:
            logger.error('error :: failed to get panorama: ' + traceback.format_exc())
            return 'Uh oh ... a Skyline 500 :(', 500

        try:
            duration = (time.time() - start)
            logger.info('debug :: duration - %s' % str(duration))
            resp = str(panorama_data)
            return resp, 200
        except:
            logger.error('error :: failed to render panorama.html: ' + traceback.format_exc())
            return 'Uh oh ... a Skyline 500 :(', 500

    if request_args_len == 0:
        try:
            panorama_data = panorama_request()
            # logger.info('panorama_data - %s' % str(panorama_data))
            return render_template(
                'panorama.html', anomalies=panorama_data, app_list=apps,
                source_list=sources, algorithm_list=algorithms,
                host_list=hosts, results='Latest anomalies',
                version=skyline_version, duration=(time.time() - start)), 200
        except:
            logger.error('error :: failed to get panorama: ' + traceback.format_exc())
            return 'Uh oh ... a Skyline 500 :(', 500
    else:
        count_request = 'false'
        if 'count_by_metric' in request.args:
            count_by_metric = request.args.get('count_by_metric', None)
            if count_by_metric == 'true':
                count_request = 'true'
        try:
            query, panorama_data = panorama_request()
            try:
                if settings.ENABLE_DEBUG or settings.ENABLE_WEBAPP_DEBUG:
                    logger.info('panorama_data - %s' % str(panorama_data))
                    logger.info('debug :: query - %s' % str(query))
                    logger.info('debug :: panorama_data - %s' % str(panorama_data))
                    logger.info('debug :: skyline_version - %s' % str(skyline_version))
            except:
                logger.error('error :: ENABLE_DEBUG or ENABLE_WEBAPP_DEBUG are not set in settings.py')
        except:
            logger.error('error :: failed to get panorama_request: ' + traceback.format_exc())
            return 'Uh oh ... a Skyline 500 :(', 500

        try:
            results_string = 'Found anomalies for %s' % str(query)

            duration = (time.time() - start)
            logger.info('debug :: duration - %s' % str(duration))
            return render_template(
                'panorama.html', anomalies=panorama_data, app_list=apps,
                source_list=sources, algorithm_list=algorithms,
                host_list=hosts, results=results_string, count_request=count_request,
                version=skyline_version, duration=(time.time() - start)), 200
        except:
            logger.error('error :: failed to render panorama.html: ' + traceback.format_exc())
            return 'Uh oh ... a Skyline 500 :(', 500


# Feature #1448: Crucible web UI - @earthgecko
# Branch #868: crucible - @earthgecko
# This may actually need Django, perhaps this is starting to move outside the
# realms of Flask..
@app.route("/crucible", methods=['GET'])
@requires_auth
def crucible():

    crucible_web_ui_implemented = False
    if crucible_web_ui_implemented:
        try:
            return render_template(
                'uh_oh.html', version=skyline_version,
                message="Sorry the Crucible web UI is not completed yet"), 200
        except:
            return render_template(
                'uh_oh.html', version=skyline_version,
                message="Sorry the Crucible web UI is not completed yet"), 200
    # @added 20200420 - Feature #1448: Crucible web UI
    #                   Branch #868: crucible
    else:
        start = time.time()
    try:
        request_args_len = len(request.args)
    except:
        request_args_len = 0
    from_timestamp = 0
    until_timestamp = 0
    metrics_list = []
    namespaces_list = []
    source = 'graphite'
    alert_interval = int(settings.PANORAMA_EXPIRY_TIME)
    crucible_job_id = None
    metrics_submitted_to_process = []
    process_metrics = None
    process_metrics_request = None
    user_id = None
    user = None
    job_submitted = None
    crucible_jobs_list = []
    crucible_jobs = []
    crucible_job_request = False
    crucible_job_dir = None
    crucible_job_metric = None
    crucible_job_details = []
    completed_job = False
    has_anomalies = False
    skyline_anomalies = []
    skyline_consensus_anomalies = []
    image_files = []
    image_file_names = []
    graph_image_file = None
    send_anomalies_panorama = False
    send_to_panorama_request = False
    sent_to_panorama = None
    panorama_done = False
    panorama_done_timestamp = None
    panorama_done_user_id = None
    skyline_consensus_anomalies_present = 0
    # @added 20200421 - Feature #3500: webapp - crucible_process_metrics
    #                   Feature #1448: Crucible web UI
    add_to_panorama = False
    add_to_panorama_request = False
    # @added 20200422 - Feature #3500: webapp - crucible_process_metrics
    #                   Feature #1448: Crucible web UI
    pad_timeseries = None
    # @added 20200428 - Feature #3500: webapp - crucible_process_metrics
    #                   Feature #1448: Crucible web UI
    # Paginate crucible_jobs page as when there are 1000s of jobs this page
    # fails to load
    pagination_start = 0
    pagination_end = 100
    offset = 100
    total_crucible_jobs = 0
    pagination_start_request = False
    offset_request = False
    # @added 20200607 - Feature #3630: webapp - crucible_process_training_data
    training_data_json = None

    # @added 20200817 - Feature #3682: SNAB - webapp - crucible_process - run_algorithms
    # Allow the user to pass algorithms to run -->
    run_algorithms = settings.ALGORITHMS

    if request_args_len:
        if 'process_metrics' in request.args:
            process_metrics_request = request.args.get(str('process_metrics'), None)
            if process_metrics_request == 'true':
                process_metrics_request = True
                process_metrics = True
        if 'crucible_job' in request.args:
            crucible_job_request = request.args.get(str('crucible_job'), None)
            if crucible_job_request == 'true':
                crucible_job_request = True
                process_metrics_request = False
                process_metrics = False
        if 'send_anomalies_panorama' in request.args:
            send_to_panorama_request = request.args.get(str('send_anomalies_panorama'), None)
            if send_to_panorama_request == 'true':
                send_anomalies_panorama = True
                send_to_panorama_request = True
                process_metrics_request = False
                process_metrics = False
                crucible_job_request = False
                process_metrics_request = False
                process_metrics = False

        # @added 20200428 - Feature #3500: webapp - crucible_process_metrics
        #                   Feature #1448: Crucible web UI
        # Added paginate crucible_jobs page
        if 'pagination_start' in request.args:
            pagination_start_request = request.args.get(str('pagination_start'), '0')
            if pagination_start_request:
                try:
                    pagination_start = int(pagination_start_request)
                    pagination_start_request = True
                except:
                    pagination_start = 0
        if 'offset' in request.args:
            offset_request = request.args.get(str('offset'), '100')
            if offset_request:
                try:
                    offset = int(offset)
                    offset_request = True
                except:
                    offset = 100

    if crucible_job_request or send_to_panorama_request:
        if 'crucible_job_id' in request.args:
            crucible_job_id = request.args.get(str('crucible_job_id'), None)
        if 'metric' in request.args:
            crucible_job_metric = request.args.get(str('metric'), None)
        if crucible_job_id and crucible_job_metric:
            crucible_path = os.path.dirname(settings.CRUCIBLE_DATA_FOLDER)
            crucible_job_dir = '%s/jobs/%s/%s' % (crucible_path, crucible_job_id, crucible_job_metric)

    if crucible_job_request:
        logger.info('crucible_job request for crucible_job_id %s and metric %s' % (
            str(crucible_job_id), str(crucible_job_metric)))
        crucible_job_details, completed_job, has_anomalies, skyline_anomalies, skyline_consensus_anomalies, panorama_done, panorama_done_timestamp, panorama_done_user_id, image_files, image_file_names, graph_image_file, fail_msg, trace = get_crucible_job(crucible_job_id, crucible_job_metric)

    if process_metrics or send_anomalies_panorama:
        if settings.WEBAPP_AUTH_ENABLED:
            auth = request.authorization
            user = auth.username
        else:
            user = 'Skyline'
            user_id = 1
        # Allow the user_id to be passed as a request argument
        if 'user_id' in request.args:
            user_id_str = request.args.get(str('user_id'), None)
            try:
                user_id = int(user_id_str)
            except:
                logger.error('error :: the /crucible user_id argument is not an int - %s' % str(user_id_str))
                return '400 Bad Request - invalid user_id argument', 400
        if not user_id:
            success, user_id = get_user_details(skyline_app, 'id', 'username', str(user))
            if not success:
                logger.error('error :: /crucible could not get_user_details(%s)' % str(user))
                return 'Internal Server Error - ref: i - could not determine user_id', 500
            else:
                try:
                    user_id = int(user_id)
                    logger.info('/crucible get_user_details() with %s returned user id %s' % (
                        str(user), str(user_id)))
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: /crucible get_user_details() with %s did not return an int' % (
                        str(user)))
                    return 'Internal Server Error - ref: i - user_id not int', 500

    if process_metrics_request:
        logger.info('process_metrics request')
        from_timestamp = 0
        until_timestamp = 0
        metrics_list = []
        namespaces_list = []
        source = 'graphite'
        alert_interval = str(int(settings.PANORAMA_EXPIRY_TIME))
        if 'from_timestamp' in request.args:
            try:
                # If int is used it does not work
                # from_timestamp = request.args.get(int('from_timestamp'), 0)
                str_from_timestamp = request.args.get(str('from_timestamp'), 0)
                if str_from_timestamp:
                    from_timestamp = int(str_from_timestamp)
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: from_timestamp argument is not an int')
                return 'Bad Request - from_timestamp argument is not an int', 400
        if 'until_timestamp' in request.args:
            try:
                str_until_timestamp = request.args.get(str('until_timestamp'), 0)
                if str_until_timestamp:
                    until_timestamp = int(str_until_timestamp)
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: until_timestamp argument is not an int')
                return 'Bad Request - until_timestamp argument is not an int', 400
        if 'metrics' in request.args:
            metrics = request.args.get(str('metrics'), None)
        if 'namespaces' in request.args:
            namespaces = request.args.get(str('namespaces'), None)
        if 'source' in request.args:
            source = request.args.get(str('source'), None)
        if 'alert_interval' in request.args:
            try:
                str_alert_interval = request.args.get(str('alert_interval'), 0)
                if str_alert_interval:
                    alert_interval = int(str_alert_interval)
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: alert_interval argument is not an int')
                return 'Bad Request - alert_interval argument is not an int', 400
        # @added 20200421 - Feature #3500: webapp - crucible_process_metrics
        #                   Feature #1448: Crucible web UI
        # Added add_to_panorama
        if 'add_to_panorama' in request.args:
            add_to_panorama_request = request.args.get(str('add_to_panorama'), None)
            if add_to_panorama_request == 'true':
                add_to_panorama = True
            else:
                add_to_panorama = False
        # @added 20200422 - Feature #3500: webapp - crucible_process_metrics
        #                   Feature #1448: Crucible web UI
        if 'pad_timeseries' in request.args:
            str_pad_timeseries = request.args.get(str('pad_timeseries'), 'auto')
            if str_pad_timeseries:
                pad_timeseries = str_pad_timeseries

        # @added 20200607 - Feature #3630: webapp - crucible_process_training_data
        if 'training_data_json' in request.args:

            training_data_json = request.args.get(str('training_data_json'), None)
        # @added 20200817 - Feature #3682: SNAB - webapp - crucible_process - run_algorithms
        # Allow the user to pass run_algorithms to run
        if 'run_algorithms' in request.args:
            known_algorithms = ['detect_drop_off_cliff']
            for s_algorithm in settings.ALGORITHMS:
                known_algorithms.append(s_algorithm)
            run_algorithms_str = request.args.get(str('run_algorithms'), settings.ALGORITHMS)
            run_algorithms = []
            if run_algorithms_str == 'true':
                run_algorithms = settings.ALGORITHMS
            if not run_algorithms:
                run_algorithms_list = run_algorithms_str.split(',')
                logger.info('crucible_process_metrics - run_algorithms passed, generated run_algorithms_list - %s' % str(run_algorithms_list))
                if run_algorithms_list:
                    run_algorithms = []
                    for r_algorithm in run_algorithms_list:
                        if r_algorithm in known_algorithms:
                            run_algorithms.append(r_algorithm)
                        else:
                            logger.error('error :: crucible_process_metrics - run_algorithms passed, with invalid algorithm - %s, excluding' % str(r_algorithm))

        do_process_metrics = False
        if from_timestamp and until_timestamp:
            if metrics:
                do_process_metrics = True
                metrics_list = metrics.split(',')
                logger.info('crucible_process_metrics - metrics_list - %s' % str(metrics_list))
            if namespaces:
                do_process_metrics = True
                namespaces_list = namespaces.split(',')
                logger.info('crucible_process_metrics namespaces_list - %s' % str(namespaces_list))

        # @added 20200607 - Feature #3630: webapp - crucible_process_training_data
        if source == 'training_data' and training_data_json:
            do_process_metrics = True

        crucible_job_id = None
        metrics_submitted_to_process = []
        if do_process_metrics:
            try:
                # @modified 20200421 - Feature #3500: webapp - crucible_process_metrics
                #                      Feature #1448: Crucible web UI
                # Added add_to_panorama
                # crucible_job_id, metrics_submitted_to_process, message, trace = submit_crucible_job(from_timestamp, until_timestamp, metrics_list, namespaces_list, source, alert_interval, user_id, user)
                # @modified 20200422 - Feature #3500: webapp - crucible_process_metrics
                #                      Feature #1448: Crucible web UI
                # Added pad_timeseries
                # @added 20200607 - Feature #3630: webapp - crucible_process_training_data
                # Added training_data_json
                # @modified 20200817 - Feature #3682: SNAB - webapp - crucible_process - run_algorithms
                # Allow the user to pass run_algorithms to run
                crucible_job_id, metrics_submitted_to_process, message, trace = submit_crucible_job(from_timestamp, until_timestamp, metrics_list, namespaces_list, source, alert_interval, user_id, user, add_to_panorama, pad_timeseries, training_data_json, run_algorithms)
                logger.info('crucible_process_metrics - submit_crucible_job returned (%s, %s, %s, %s)' % (
                    str(crucible_job_id), str(metrics_submitted_to_process),
                    str(message), str(trace)))
            except:
                trace = traceback.format_exc()
                message = 'Uh oh ... a Skyline 500 using submit_crucible_job(%s, %s, %s, %s, %s, %s, %s, %s)' % (
                    str(from_timestamp), str(until_timestamp), str(metrics_list),
                    str(namespaces_list), str(source), str(alert_interval),
                    str(user_id), str(user))
                return internal_error(message, trace)
            if not crucible_job_id:
                return internal_error(message, trace)
            else:
                job_submitted = True

    if send_to_panorama_request:
        logger.info('send_anomalies_panorama request')
        logger.info('send_anomalies_panorama request for crucible_job_id %s and metric %s' % (
            str(crucible_job_id), str(crucible_job_metric)))
        crucible_job_details, completed_job, has_anomalies, skyline_anomalies, skyline_consensus_anomalies, panorama_done, panorama_done_timestamp, panorama_done_user_id, image_files, image_file_names, graph_image_file, fail_msg, trace = get_crucible_job(crucible_job_id, crucible_job_metric)
        try:
            sent_to_panorama, message, trace = send_crucible_job_metric_to_panorama(crucible_job_id, crucible_job_metric, user_id, user, skyline_consensus_anomalies)
            logger.info('send_anomalies_panorama request sent %s anomalies to Panorama' % str(sent_to_panorama))
            crucible_job_request = True
        except:
            trace = traceback.format_exc()
            message = 'Uh oh ... a Skyline 500 using get_crucible_jobs'
            return internal_error(message, trace)

    if not process_metrics_request and not crucible_job_request and not send_to_panorama_request:
        logger.info('crucible_jobs_list request')
        try:
            crucible_jobs_list, message, trace = get_crucible_jobs()
            logger.info('crucible_jobs_list determined')
        except:
            trace = traceback.format_exc()
            message = 'Uh oh ... a Skyline 500 using get_crucible_jobs'
            return internal_error(message, trace)
        if crucible_jobs_list:

            # @added 20200428 - Feature #3500: webapp - crucible_process_metrics
            #                   Feature #1448: Crucible web UI
            # Added pagination to crucible_jobs page
            sorted_crucible_jobs_list = sorted(crucible_jobs_list, key=lambda x: x[0])
            # sorted_crucible_jobs = sorted(crucible_jobs, key=lambda x: x[8])
            sorted_crucible_jobs_list.reverse()
            total_crucible_jobs = len(crucible_jobs_list)
            logger.info('crucible_jobs_list has a total of %s jobs' % (
                str(total_crucible_jobs)))
            paginated_crucible_jobs_list = []
            pagination_end = pagination_start + offset
            for job in sorted_crucible_jobs_list[pagination_start:pagination_end]:
                paginated_crucible_jobs_list.append(job)
            logger.info('crucible_jobs_list paginated from %s to %s' % (
                str(pagination_start), str(pagination_end)))
            del sorted_crucible_jobs_list

            # @modified 20200428 - Feature #3500: webapp - crucible_process_metrics
            #                      Feature #1448: Crucible web UI
            # Added pagination to crucible_jobs page
            # for metric_name, root, crucible_job_id, human_date, completed_job, has_anomalies, panorama_done, skyline_consensus_anomalies_present in crucible_jobs_list:
            job_list_item = 1
            for metric_name, root, crucible_job_id, human_date, completed_job, has_anomalies, panorama_done, skyline_consensus_anomalies_present in paginated_crucible_jobs_list:
                crucible_jobs.append([human_date, crucible_job_id, completed_job, has_anomalies, root, metric_name, panorama_done, skyline_consensus_anomalies_present, job_list_item])
                job_list_item += 1
            del paginated_crucible_jobs_list

            if crucible_jobs:
                sorted_crucible_jobs = sorted(crucible_jobs, key=lambda x: x[0])
                # sorted_crucible_jobs = sorted(crucible_jobs, key=lambda x: x[8])
                crucible_jobs = sorted_crucible_jobs
                crucible_jobs.reverse()
                del sorted_crucible_jobs
            del crucible_jobs_list

    return render_template(
        'crucible.html', process_metrics=process_metrics,
        crucible_job_id=crucible_job_id,
        from_timestamp=int(from_timestamp), until_timestamp=int(until_timestamp),
        metrics=metrics_list, namespaces=namespaces_list, source=source,
        alert_interval=alert_interval, job_submitted=job_submitted,
        metrics_submitted_to_process=metrics_submitted_to_process,
        crucible_jobs=crucible_jobs, crucible_job=crucible_job_request,
        crucible_job_dir=crucible_job_dir,
        crucible_job_metric=crucible_job_metric,
        crucible_job_details=crucible_job_details, completed_job=completed_job,
        has_anomalies=has_anomalies, skyline_anomalies=skyline_anomalies,
        skyline_consensus_anomalies=skyline_consensus_anomalies,
        skyline_consensus_anomalies_present=skyline_consensus_anomalies_present,
        sent_to_panorama=sent_to_panorama, panorama_done=panorama_done,
        panorama_done_timestamp=panorama_done_timestamp,
        panorama_done_user_id=panorama_done_user_id,
        image_files=image_files, image_file_names=image_file_names,
        graph_image_file=graph_image_file,
        crucible_enabled=settings.ENABLE_CRUCIBLE,
        # @added 20200428 - Feature #3500: webapp - crucible_process_metrics
        #                   Feature #1448: Crucible web UI
        # Added pagination to crucible_jobs page
        pagination_start=pagination_start, offset=offset,
        pagination_end=pagination_end,
        total_crucible_jobs=total_crucible_jobs,
        version=skyline_version,
        duration=(time.time() - start), print_debug=False), 200


# @added 20161123 - Branch #922: ionosphere
@app.route('/ionosphere', methods=['GET'])
@requires_auth
def ionosphere():
    if not settings.IONOSPHERE_ENABLED:
        try:
            return render_template(
                'uh_oh.html', version=skyline_version,
                message="Ionosphere is not enabled, please see the Ionosphere section in the docs and settings.py"), 200
        except:
            return 'Uh oh ... a Skyline 500 :(', 500

    start = time.time()

    # @added 20190919 - Feature #3230: users DB table
    #                   Ideas #2476: Label and relate anomalies
    #                   Feature #2516: Add label to features profile
    user_id = None
    if settings.WEBAPP_AUTH_ENABLED:
        auth = request.authorization
        user = auth.username
    else:
        user = 'Skyline'
        user_id = 1

    # @added 20201210 - Feature #3824: get_cluster_data
    if AUTH_DEBUG and settings.WEBAPP_AUTH_ENABLED:
        try:
            logger.debug('debug :: auth debug - username - %s' % str(auth.username))
        except:
            logger.error(traceback.format_exc())
            logger.error('error ::auth debug - username')
        try:
            logger.debug('debug :: auth debug - password - %s' % str(auth.password))
        except:
            logger.error(traceback.format_exc())
            logger.error('error ::auth debug - password')

    # @added 20191211 - Feature #3230: users DB table
    #                   Ideas #2476: Label and relate anomalies
    #                   Feature #2516: Add label to features profile
    # Allow the user_id to be passed as a request argument
    if 'user_id' in request.args:
        user_id_str = request.args.get(str('user_id'), None)
        try:
            user_id = int(user_id_str)
        except:
            logger.error('error :: the /ionosphere user_id argument is not an int - %s' % str(user_id_str))
            return '400 Bad Request - invalid user_id argument', 400

    if not user_id:
        success, user_id = get_user_details(skyline_app, 'id', 'username', str(user))
        if not success:
            logger.error('error :: /ionosphere could not get_user_details(%s)' % str(user))
            return 'Internal Server Error - ref: i - could not determine user_id', 500
        else:
            try:
                user_id = int(user_id)
                logger.info('/ionosphere get_user_details() with %s returned user id %s' % (
                    str(user), str(user_id)))
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: /ionosphere get_user_details() with %s did not return an int' % (
                    str(user)))
                return 'Internal Server Error - ref: i - user_id not int', 500

    logger.info('request.url: %s' % str(request.url))
    # logger.debug('request.args: %s' % str(request.args))

    request_args_present = False
    try:
        request_args_len = len(request.args)
        request_args_present = True
    except:
        request_args_len = 0

    if request_args_len:
        for i in request.args:
            key = str(i)
            value = request.args.get(key, None)
            logger.info('request argument - %s=%s' % (key, str(value)))

    # @added 20180419 - Feature #1996: Ionosphere - matches page
    #                   Branch #2270: luminosity
    # Change the default search parameters to return all matches for the
    # past 24 hours
    matched_request_timestamp = int(time.time())
    default_matched_from_timestamp = matched_request_timestamp - 86400
    matched_from_datetime = time.strftime('%Y%m%d %H:%M', time.localtime(default_matched_from_timestamp))

    # @added 20190328 - Feature #2484: FULL_DURATION feature profiles
    # Added ionosphere_echo
    echo_hdate = False

    metric_id = False

    # @added 20210417 - Feature #4014: Ionosphere - inference
    #                   Branch #3590: inference
    batch_size = None
    top_matches = None
    max_distance = None
    saved_training_data = None

    # @added 20210727 - Feature #4206: webapp - saved_training_data page
    ionosphere_saved_training_data = None

    # @added 20210415 - Feature #4014: Ionosphere - inference
    #                   Branch #3590: inference
    motif_analysis_images = None
    motif_analysis = None

    # @added 20210419 - Feature #4014: Ionosphere - inference
    inference_match_motif_dict = None
    inference_match_motif_image = None

    # @added 20210422 - Feature #4014: Ionosphere - inference
    # Allow user to specify the range_padding
    motif_range_padding = None

    # @added 20210425 - Feature #4014: Ionosphere - inference
    # Allow user to specify the difference between the areas under the
    # curve
    motif_max_area_percent_diff = None

    # This allows to plot best first, just makes an ordered list of the motif_id
    # in the dict ordered by dist, just to add some order to a dict :)
    motif_analysis_motif_id_by_distance = []
    motif_analysis_images = []
    if 'motif_analysis' in request.args:
        MOTIF_ANALYSIS_ARGS = [
            'motif_analysis', 'metric', 'timestamp', 'requested_timestamp',
            'similarity', 'purge',
            # @added 20210417 - Feature #4014: Ionosphere - inference
            # Allow the user to define the batch_size per similarity search
            'batch_size', 'top_matches', 'max_distance',
            # @added 20210418 - Feature #4014: Ionosphere - inference
            # Allow for the similarity search on saved_training_data
            'saved_training_data',
            # @added 20210422 - Feature #4014: Ionosphere - inference
            # Allow user to specify the range_padding
            'range_padding',
            # @added 20210425 - Feature #4014: Ionosphere - inference
            # Allow user to specify the difference between the areas under the
            # curve
            'max_area_percent_diff',
        ]
        motif_purge = False
        for i in request.args:
            key = str(i)
            if key not in MOTIF_ANALYSIS_ARGS:
                error_string = 'error :: invalid request argument - %s' % (key)
                logger.error('%s - not in MOTIF_ANALYSIS_ARGS' % (error_string))
                resp = json.dumps(
                    {'400 Bad Request': error_string})
                return flask_escape(resp), 400
            value = request.args.get(key, 0)
            logger.info('request argument - %s=%s' % (key, str(value)))
            if key == 'metric':
                metric = str(value)
            if key == 'timestamp':
                timestamp = str(value)
            if key == 'requested_timestamp':
                requested_timestamp = str(value)
            if key == 'similarity':
                similarity = str(value)
            if key == 'purge':
                if str(value) == 'true':
                    motif_purge = True
            # @added 20210417 - Feature #4014: Ionosphere - inference
            # Allow the user to define the batch_size per similarity search
            if key == 'batch_size':
                batch_size = int(value)
            if key == 'top_matches':
                top_matches = int(value)
            if key == 'max_distance':
                max_distance = float(value)
            # @added 20210418 - Feature #4014: Ionosphere - inference
            # Allow for the similarity search on saved_training_data
            if key == 'saved_training_data':
                if str(value) == 'true':
                    saved_training_data = True
            # @added 20210422 - Feature #4014: Ionosphere - inference
            # Allow user to specify the range_padding
            if key == 'range_padding':
                motif_range_padding = int(value)
            # @added 20210425 - Feature #4014: Ionosphere - inference
            # Allow user to specify the difference between the areas under the
            # curve
            if key == 'max_area_percent_diff':
                max_area_percent_diff = float(value)

        if not timestamp and not metric and not similarity and not batch_size and not top_matches and not max_distance and not motif_range_padding and not max_area_percent_diff:
            error_string = 'error :: invalid request'
            logger.error(error_string)
            resp = json.dumps(
                {'400 Bad Request': error_string})
            return flask_escape(resp), 400
        # Use previous if they exist
        motif_metricname_dir = metric.replace('.', '/')

        # @modified 20210419 - Feature #4014: Ionosphere - inference
        # Create a unique dir for each batch_size, top_matches and max_distance
        # motif_metric_dir = '%s/%s/%s/motifs' % (
        #     settings.IONOSPHERE_DATA_FOLDER, str(timestamp), motif_metricname_dir)
        motif_metric_dir = '%s/%s/%s/motifs/batch_size.%s/top_matches.%s/max_distance.%s/range_padding.%s/max_area_percent_diff.%s' % (
            settings.IONOSPHERE_DATA_FOLDER, str(timestamp),
            motif_metricname_dir, str(batch_size), str(top_matches),
            str(max_distance), str(motif_range_padding),
            str(max_area_percent_diff))

        # @added 20210418 - Feature #4014: Ionosphere - inference
        # Allow for the similarity search on saved_training_data
        if saved_training_data:
            try:
                # saved_motif_metric_dir = '%s_saved/%s/%s/motifs' % (
                #    settings.IONOSPHERE_DATA_FOLDER, str(timestamp), motif_metricname_dir)
                saved_data_dir = '%s_saved' % settings.IONOSPHERE_DATA_FOLDER
                saved_motif_metric_dir = motif_metric_dir.replace(settings.IONOSPHERE_DATA_FOLDER, saved_data_dir)
                if os.path.exists(saved_motif_metric_dir):
                    motif_metric_dir = saved_motif_metric_dir
                    logger.info('motif_analysis - using saved training_data dir - %s' % (saved_motif_metric_dir))
            except Exception as e:
                error_string = 'error :: saved_training_data dir not found - %s/%s - %s' % (
                    str(timestamp), motif_metricname_dir, e)
                logger.error(error_string)
                resp = json.dumps(
                    {'400 Bad Request': error_string})
                return flask_escape(resp), 400

        # @modified 20210419 - Feature #4014: Ionosphere - inference
        # Create a unique dir for each batch_size, top_matches and max_distance
        # metric_motif_analysis_file = '%s/motif.analysis.%s.%s.%s.dict' % (
        #     motif_metric_dir, similarity, str(batch_size), str(max_distance))
        metric_motif_analysis_file = '%s/motif.analysis.similarity_%s.batch_size_%s.top_matches_%s.max_distance_%s.range_padding_%s.max_area_percent_diff_%s.dict' % (
            motif_metric_dir, similarity, str(batch_size), str(top_matches),
            str(max_distance), str(motif_range_padding),
            str(max_area_percent_diff))

        if motif_purge:
            if os.path.exists(motif_metric_dir):
                try:
                    rmtree(motif_metric_dir)
                    logger.info('motif_analysis - purge true - removed - %s' % (motif_metric_dir))
                except Exception as e:
                    logger.error(traceback.format_exc())
                    logger.error('error :: motif_analysis - purge true - failed to rmtree %s - %s' % (motif_metric_dir, e))
        if os.path.isfile(metric_motif_analysis_file):
            try:
                with open(metric_motif_analysis_file, 'r') as f:
                    motif_analysis_str = f.read()
                motif_analysis = literal_eval(motif_analysis_str)
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: failed to evaluate motif_analysis from %s' % metric_motif_analysis_file)
                motif_analysis = None
        else:
            logger.info('no previous motif_analysis file found %s, running analysis' % metric_motif_analysis_file)

        if not motif_analysis:
            # @modified 20210417 - Feature #4014: Ionosphere - inference
            # Allow the user to define the batch_size per similarity search
            # @modified 20210425 - Feature #4014: Ionospher - inference
            # Added max_area_percent_diff
            motif_analysis, fail_msg, trace = on_demand_motif_analysis(
                metric, timestamp, similarity, batch_size, top_matches,
                max_distance, motif_range_padding, max_area_percent_diff)
        else:
            logger.info('previous motif_analysis loaded from %s' % metric_motif_analysis_file)
        if motif_analysis:
            motif_ids_found = list(motif_analysis[metric]['motifs'].keys())
            motif_id_and_distance = []
            for motif_id_found in motif_ids_found:
                motif_id_and_distance.append([motif_id_found, motif_analysis[metric]['motifs'][motif_id_found]['distance']])
                try:
                    if motif_analysis[metric]['motifs'][motif_id_found]['image']:
                        motif_analysis_images.append(motif_analysis[metric]['motifs'][motif_id_found]['image'])
                except Exception as e:
                    logger.error('error :: failed to append motif to motif_id_and_distance for motif_id_found: %s - %s' % (str(motif_id_found), e))
            ordered_motif_id_by_distance = sorted(motif_id_and_distance, key=lambda x: x[1])
            all_motif_analysis_motif_id_by_distance = [item[0] for item in ordered_motif_id_by_distance]
            # Now only add the one which have images
            for motif_id_found in all_motif_analysis_motif_id_by_distance:
                try:
                    has_image = motif_analysis[metric]['motifs'][motif_id_found]['image']
                    if has_image in motif_analysis_images:
                        motif_analysis_motif_id_by_distance.append(motif_id_found)
                except Exception as e:
                    # logger.error(traceback.format_exc())
                    logger.error('error :: failed to evaluate if has image for motif_id_found: %s - %s' % (str(motif_id_found), e))
        # logger.debug('debug :: motif_analysis_motif_id_by_distance: %s' % str(motif_analysis_motif_id_by_distance))
        # logger.debug('debug :: motif_analysis: %s' % str(motif_analysis))

    # @added 20210413 - Feature #4014: Ionosphere - inference
    #                   Branch #3590: inference
    motif_matches_req = False
    if 'motif_matches' in request.args:
        motif_matches_req = True
        MOTIF_MATCHES_ARGS = [
            'motif_matches', 'metric', 'metric_like', 'from_timestamp',
            'until_timestamp', 'validated_equals', 'limit', 'format',
            'primary_match',
            # @added 20210415 - Feature #4014: Ionosphere - inference
            #                   Branch #3590: inference
            'sort_by', 'order_by',
        ]
        metric = None
        metric_like = None
        from_timestamp = 'all'
        until_timestamp = 'all'
        limit = 100
        format = 'normal'

        # Validate a metric or metric_like parameter with all known metrics in
        # the DB not just Redis.  These parameters are validated as they are
        # passed to a database query in get_matched_motifs
        known_db_metrics = []
        try:
            known_db_metrics = list(REDIS_CONN.smembers('aet.analyzer.metrics_manager.db.metric_names'))
        except Exception as e:
            logger.error('error :: failed to known_db_metrics from Redis set aet.analyzer.metrics_manager.db.metric_names - %s' % (e))

        for i in request.args:
            key = str(i)
            if key not in MOTIF_MATCHES_ARGS:
                error_string = 'error :: invalid request argument - %s' % (key)
                logger.error(error_string)
                resp = json.dumps(
                    {'400 Bad Request': error_string})
                return flask_escape(resp), 400
            value = request.args.get(key, None)
            logger.info('request argument - %s=%s' % (key, str(value)))
            if key == 'metric':
                metric_str = str(value)
                if metric_str == 'all':
                    metric = 'all'
                else:
                    if metric_str in known_db_metrics:
                        metric = metric_str

            if key == 'metric_like':
                metric_like_str = str(value)
                if metric_like_str == 'all':
                    metric_like = 'all'
                else:
                    metric_like_namespace_str = metric_like_str.replace('%', '')
                    for i_metric in known_db_metrics:
                        if metric_like_namespace_str in i_metric:
                            metric_like = metric_like_str

            if key == 'from_timestamp':
                from_timestamp = str(value)
            if key == 'until_timestamp':
                until_timestamp = str(value)
            if key == 'validated_equals':
                validated_equals = str(value)
            if key == 'format':
                format = str(value)
            # @added 20210415 - Feature #4014: Ionosphere - inference
            #                   Branch #3590: inference
            # Allow to sort_by, to enable sorting by distance as the larger the distance
            # the less similar
            sort_by = None
            valid_sort_by_columns = [
                'id', 'metric_id', 'fp_id', 'metric_timestamp', 'distance',
                'size',
            ]
            if 'sort_by' in request.args:
                sort_by = request.args.get('sort_by', 'none')
                if sort_by == 'none':
                    sort_by = None
                if sort_by not in valid_sort_by_columns:
                    error_string = 'error :: invalid request argument - %s: %s' % (key, str(value))
                    logger.error(error_string)
                    resp = json.dumps(
                        {'400 Bad Request': error_string})
                    return flask_escape(resp), 400
            if 'order_by' in request.args:
                valid_order_by = ['DESC', 'ASC']
                order_by = request.args.get('order_by', 'DESC')
                if order_by not in valid_order_by:
                    error_string = 'error :: invalid request argument - %s: %s' % (key, str(value))
                    logger.error(error_string)
                    resp = json.dumps(
                        {'400 Bad Request': error_string})
                    return flask_escape(resp), 400

        matched_motifs, fail_msg, trace = get_matched_motifs(metric, metric_like, from_timestamp, until_timestamp, sort_by)
        if matched_motifs is False:
            return internal_error(fail_msg, trace)

        data_dict = {"status": {"response": 200, "request_time": (time.time() - start)}, "data": {"metric": metric, "metric_like": metric_like, "matched_motifs": matched_motifs}}
        if not matched_motifs:
            data_dict = {"status": {"response": 204, "request_time": (time.time() - start)}, "data": {"metric": metric, "metric_like": metric_like, "matched_motifs": matched_motifs, "success": False, "reason": "no data for query"}}
        if format == 'json':
            logger.info('ionosphere motif_matches returned json with %s matched_motifs elements listed' % str(len(matched_motifs)))
            return jsonify(data_dict), 200
        return render_template(
            'ionosphere.html', motif_matches=motif_matches_req,
            matched_motifs=matched_motifs, for_metric=metric,
            matched_from_datetime=matched_from_datetime,
            version=skyline_version, user=user,
            duration=(time.time() - start), print_debug=False), 200

    # @added 20180812 - Feature #2430: Ionosphere validate learnt features profiles page
    features_profiles_to_validate = []
    fp_validate_req = False
    if 'fp_validate' in request.args:
        fp_validate_req = request.args.get(str('fp_validate'), None)
        if fp_validate_req == 'true':
            fp_validate_req = True
    if fp_validate_req:
        metric_found = False
        # @modified 20190503 - Branch #2646: slack - linting
        # timestamp = False
        base_name = False
        # @added 20181013 - Feature #2430: Ionosphere validate learnt features profiles page
        # Added the validate_all context and function
        validate_all = False
        all_validated = False
        metric_id = False
        validated_count = 0

        for i in request.args:
            key = str(i)
            value = request.args.get(key, None)
            logger.info('request argument - %s=%s' % (key, str(value)))

            # @added 20181013 - Feature #2430: Ionosphere validate learnt features profiles page
            # Added the validate_all context and function
            if key == 'validate_all':
                if str(value) == 'true':
                    validate_all = True
            if key == 'all_validated':
                if str(value) == 'true':
                    all_validated = True
                    # Ensure that validate_all is set to False so another call
                    # is not made to the validate_all function
                    validate_all = False
            if key == 'metric_id':
                try:
                    if isinstance(int(value), int):
                        metric_id = int(value)
                except:
                    logger.error('error :: the metric_id request parameter was passed but is not an int - %s' % str(value))
            if key == 'validated_count':
                try:
                    if isinstance(int(value), int):
                        validated_count = int(value)
                except:
                    logger.error('error :: the validated_count request parameter was passed but is not an int - %s' % str(value))

            if key == 'order':
                order = str(value)
                if order == 'DESC':
                    ordered_by = 'DESC'
                if order == 'ASC':
                    ordered_by = 'ASC'
            if key == 'limit':
                limit = str(value)
                try:
                    test_limit = int(limit) + 0
                    limited_by = test_limit
                except:
                    logger.error('error :: limit is not an integer - %s' % str(limit))
                    limited_by = '30'
            if key == 'metric':
                base_name = str(value)
                if base_name == 'all':
                    metric_found = True
                    metric_name = 'all'
                    limited_by = 0
                    ordered_by = 'DESC'

                # @added 20210710 - Bug #4168: webapp/ionosphere_backend.py - handle old features profile dir not been found
                # Check all known metrics not just those in Redis
                if not metric_found:
                    metric_names = []
                    try:
                        metric_names = get_all_db_metric_names(skyline_app)
                    except Exception as e:
                        logger.error(traceback.format_exc())
                        logger.error('error :: failed to get_all_db_metric_names - %s' % (
                            e))
                    if base_name in metric_names:
                        metric_found = True
                        metric_name = base_name

                if not metric_found:
                    metric_name = settings.FULL_NAMESPACE + base_name
                    # @added 20220112 - Bug #4374: webapp - handle url encoded chars
                    # @modified 20220115 - Bug #4374: webapp - handle url encoded chars
                    # Roll back change - breaking existing metrics with colons
                    # metric_name = url_encode_metric_name(metric_name)

                    try:
                        unique_metrics = list(REDIS_CONN.smembers(settings.FULL_NAMESPACE + 'unique_metrics'))
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: Webapp could not get the unique_metrics list from Redis')
                        return 'Internal Server Error', 500
                    if metric_name in unique_metrics:
                        metric_found = True
                    # @added 20180423 - Feature #2034: analyse_derivatives
                    #                   Branch #2270: luminosity
                    other_unique_metrics = []
                    if metric_name not in unique_metrics and settings.OTHER_SKYLINE_REDIS_INSTANCES:
                        metric_found_in_other_redis = False
                        # @modified 20180519 - Feature #2378: Add redis auth to Skyline and rebrow
                        # for redis_ip, redis_port in settings.OTHER_SKYLINE_REDIS_INSTANCES:
                        for redis_ip, redis_port, redis_password in settings.OTHER_SKYLINE_REDIS_INSTANCES:
                            if not metric_found_in_other_redis:
                                try:
                                    if redis_password:
                                        other_redis_conn = redis.StrictRedis(host=str(redis_ip), port=int(redis_port), password=str(redis_password))
                                    else:
                                        other_redis_conn = redis.StrictRedis(host=str(redis_ip), port=int(redis_port))
                                    other_unique_metrics = list(other_redis_conn.smembers(settings.FULL_NAMESPACE + 'unique_metrics'))
                                except:
                                    logger.error(traceback.format_exc())
                                    logger.error('error :: failed to connect to Redis at %s on port %s' % (str(redis_ip), str(redis_port)))
                                if metric_name in other_unique_metrics:
                                    metric_found_in_other_redis = True
                                    metric_found = True
                                    logger.info('%s found in derivative_metrics in Redis at %s on port %s' % (metric_name, str(redis_ip), str(redis_port)))
        if metric_found:

            # @added 20181013 - Feature #2430: Ionosphere validate learnt features profiles page
            # Added the validate_all context and function, first do the
            # validate_all if it has been passed
            if validate_all and metric_id:
                features_profiles_to_validate_count = 0
                try:
                    features_profiles_to_validate, fail_msg, trace = get_features_profiles_to_validate(base_name)
                    features_profiles_to_validate_count = len(features_profiles_to_validate)
                    logger.info('%s features profiles found that need validating for %s with metric_id %s' % (
                        str(features_profiles_to_validate_count), base_name, str(metric_id)))
                except:
                    trace = traceback.format_exc()
                    message = 'Uh oh ... a Skyline 500 using get_features_profiles_to_validate(%s) with metric_id %s' % (str(base_name), str(metric_id))
                    return internal_error(message, trace)
                if features_profiles_to_validate_count > 0:
                    try:
                        # @modified 20190919 - Feature #3230: users DB table
                        #                      Ideas #2476: Label and relate anomalies
                        #                      Feature #2516: Add label to features profile
                        # Added user_id
                        # all_validated, fail_msg, traceback_format_exc = validate_fp(int(metric_id), 'metric_id')
                        all_validated, fail_msg, traceback_format_exc = validate_fp(int(metric_id), 'metric_id', user_id)
                        logger.info('validated all the enabled, unvalidated features profiles for metric_id - %s' % str(metric_id))
                        if all_validated:
                            validated_count = features_profiles_to_validate_count
                    except:
                        trace = traceback.format_exc()
                        message = 'Uh oh ... a Skyline 500 using get_features_profiles_to_validate(%s) with metric_id %s' % (str(base_name), str(metric_id))
                        return internal_error(message, trace)

            features_profiles_to_validate = []
            if metric_name != 'all':
                try:
                    features_profiles_to_validate, fail_msg, trace = get_features_profiles_to_validate(base_name)
                    # features_profiles_to_validate
                    # [ fp_id, metric_id, metric, full_duration, anomaly_timestamp,
                    #   fp_parent_id, parent_full_duration, parent_anomaly_timestamp,
                    #   fp_date, fp_graph_uri, parent_fp_date, parent_fp_graph_uri,
                    #   parent_prent_fp_id, fp_learn_graph_uri, parent_fp_learn_graph_uri,
                    #   minimum_full_duration, maximum_full_duration]
                    logger.info('%s features profiles found that need validating for %s' % (
                        str(len(features_profiles_to_validate)), base_name))
                except:
                    trace = traceback.format_exc()
                    message = 'Uh oh ... a Skyline 500 using get_features_profiles_to_validate(%s)' % str(base_name)
                    return internal_error(message, trace)
            try:
                default_learn_full_duration = int(settings.IONOSPHERE_LEARN_DEFAULT_FULL_DURATION_DAYS) * 24 * 60 * 60
            except:
                default_learn_full_duration = 30 * 24 * 60 * 60
            metrics_with_features_profiles_to_validate = []
            if not features_profiles_to_validate:
                # metrics_with_features_profiles_to_validate
                # [[metric_id, metric, fps_to_validate_count]]
                metrics_with_features_profiles_to_validate, fail_msg, trace = get_metrics_with_features_profiles_to_validate()

                # @added 20190501 - Feature #2430: Ionosphere validate learnt features profiles page
                # Only add to features_profiles_to_validate if the metric is active
                # in Redis
                if not settings.OTHER_SKYLINE_REDIS_INSTANCES:
                    if metrics_with_features_profiles_to_validate:
                        active_metrics_with_features_profiles_to_validate = []
                        try:
                            unique_metrics = list(REDIS_CONN.smembers(settings.FULL_NAMESPACE + 'unique_metrics'))
                        except:
                            logger.error('error :: Webapp could not get the unique_metrics list from Redis')
                            logger.info(traceback.format_exc())
                            return 'Internal Server Error', 500
                        for i_metric_id, i_metric, fps_to_validate_count in metrics_with_features_profiles_to_validate:
                            i_metric_name = '%s%s' % (settings.FULL_NAMESPACE, str(i_metric))
                            if i_metric_name not in unique_metrics:
                                continue
                            active_metrics_with_features_profiles_to_validate.append([i_metric_id, i_metric, fps_to_validate_count])
                        metrics_with_features_profiles_to_validate = active_metrics_with_features_profiles_to_validate

                logger.info('no features_profiles_to_validate was passed so determined metrics_with_features_profiles_to_validate')

            # @added 20190503 - Branch #2646: slack
            if validated_count > 0:
                slack_updated = webapp_update_slack_thread(base_name, 0, validated_count, 'validated')
                logger.info('slack_updated for validated features profiles %s' % str(slack_updated))

            return render_template(
                'ionosphere.html', fp_validate=fp_validate_req,
                features_profiles_to_validate=features_profiles_to_validate,
                metrics_with_features_profiles_to_validate=metrics_with_features_profiles_to_validate,
                for_metric=base_name, order=ordered_by, limit=limited_by,
                default_learn_full_duration=default_learn_full_duration,
                matched_from_datetime=matched_from_datetime,
                validate_all=validate_all, all_validated=all_validated,
                validated_count=validated_count,
                version=skyline_version,
                # @added 20190919 - Feature #3230: users DB table
                #                   Feature #2516: Add label to features profile
                user=user,
                duration=(time.time() - start), print_debug=False), 200

###
    # @added 20210727 - Feature #4206: webapp - saved_training_data page
    saved_training_data_dict = {}
    if 'ionosphere_saved_training' in request.args:
        logger.info('/ionosphere?ionosphere_saved_training')
        REQUEST_ARGS = [
            'ionosphere_saved_training', 'metric', 'namespaces',
            'label_includes',
        ]
        metrics = []
        namespaces = []
        from_timestamp = 0
        until_timestamp = 0

        for i in request.args:
            key = str(i)
            if key not in REQUEST_ARGS:
                error_string = 'error :: ionosphere_saved_training request invalid request argument - %s' % (key)
                logger.error(error_string)
                resp = json.dumps(
                    {'400 Bad Request': error_string})
                return flask_escape(resp), 400

        metric = None
        try:
            metric = request.args.get('metric')
            if metric == 'all' or metric == '':
                metric = None
        except KeyError:
            metric = None
        except Exception as e:
            logger.error(traceback.format_exc())
            logger.error('error :: /ionosphere?ionosphere_saved_training request evaluating metric parameter - %s' % e)
        namespaces = None
        try:
            namespaces = request.args.get('namespaces')
            if namespaces == 'all' or namespaces == '':
                namespaces = None
        except KeyError:
            namespaces = None
        except Exception as e:
            logger.error(traceback.format_exc())
            namespaces = None
            logger.error('error :: /ionosphere?ionosphere_saved_training request evaluating namespaces parameter - %s' % e)
        try:
            label_includes = request.args.get('label_includes')
            if label_includes == '':
                label_includes = None
        except KeyError:
            label_includes = None
        except Exception as e:
            logger.error(traceback.format_exc())
            label_includes = None
            logger.error('error :: /ionosphere?ionosphere_saved_training request evaluating namespaces parameter - %s' % e)

        if metric is not None:
            metrics = [str(metric)]
        if namespaces is not None:
            namespaces_str = namespaces
            namespaces = namespaces_str.split(',')
        else:
            namespaces = []
        logger.info('/ionosphere?ionosphere_saved_training with metrics: %s, namespaces: %s' % (
            str(metrics), str(namespaces)))
        try:
            saved_training_data_dict = get_saved_training_data(skyline_app, 'ionosphere', metrics, namespaces, label_includes, False)
        except Exception as e:
            trace = traceback.format_exc()
            fail_msg = 'error :: get_saved_training_data failed - %s' % e
            logger.error(trace)
            logger.error(fail_msg)
            return internal_error(fail_msg, trace)
        saved_training_data_metrics = []
        if saved_training_data_dict:
            for metric in list(saved_training_data_dict.keys()):
                for timestamp in list(saved_training_data_dict[metric].keys()):
                    saved_training_data_metrics.append([metric, timestamp])
            saved_training_data_metrics = sorted(saved_training_data_metrics, key=lambda x: x[1], reverse=True)

        return render_template(
            'ionosphere.html', saved_training_data_page=True,
            saved_training_data_dict=saved_training_data_dict,
            saved_training_data_metrics=saved_training_data_metrics,
            version=skyline_version, duration=(time.time() - start),
            print_debug=False), 200

###
    # @added 20210107 - Feature #3934: ionosphere_performance
    performance_request = False
    if 'performance' in request.args:
        performance_req = request.args.get('performance', 'false')
        if performance_req == 'true':
            performance_request = True
    if performance_request:
        REQUEST_ARGS = [
            'performance', 'metric', 'metric_like', 'from_timestamp',
            'until_timestamp', 'format', 'anomalies', 'new_fps', 'total_fps',
            'fps_matched_count', 'layers_matched_count', 'sum_matches',
            'title', 'period', 'height', 'width', 'fp_type', 'remove_prefix',
            # @added 20210202 - Feature #3934: ionosphere_performance
            # Handle user timezone
            'tz',
        ]
        metric = None
        metric_like = None
        from_timestamp = 0
        until_timestamp = 0
        format = 'normal'

        # @added 20210128 - Feature #3934: ionosphere_performance
        # Improve performance and pass arguments to get_ionosphere_performance
        # for cache key
        anomalies = True
        new_fps = True
        fps_matched_count = False
        layers_matched_count = False
        sum_matches = True
        title = None
        period = 'weekly'
        height = '8'
        width = '4'
        fp_type = 'all'
        remove_prefix = False

        # @added 20210202 - Feature #3934: ionosphere_performance
        # Handle user timezone
        timezone_str = 'UTC'
        pytz_timezones = []
        for pytz_timezone in pytz.all_timezones:
            pytz_timezones.append(pytz_timezone)

        performance_data_request = False
        for i in request.args:
            key = str(i)
            if key not in REQUEST_ARGS:
                error_string = 'error :: invalid request argument - %s' % (key)
                logger.error(error_string)
                resp = json.dumps(
                    {'400 Bad Request': error_string})
                return flask_escape(resp), 400
            value = request.args.get(key, '0')
            if key == 'metric':
                if str(value) == 'all':
                    metric = str(value)
                    performance_data_request = True
                if str(value) != 'all':
                    metric = str(value)
                    performance_data_request = True
            if key == 'from_timestamp' or key == 'until_timestamp':
                timestamp_format_invalid = True
                if value == 'all':
                    timestamp_format_invalid = False
                # unix timestamp
                if value.isdigit():
                    timestamp_format_invalid = False
                # %Y%m%d %H:%M timestamp
                if timestamp_format_invalid:
                    value_strip_colon = value.replace(':', '')
                    new_value = value_strip_colon.replace(' ', '')
                    if new_value.isdigit():
                        timestamp_format_invalid = False
                if timestamp_format_invalid:
                    error_string = 'error :: invalid %s value passed %s' % (key, value)
                    logger.error('error :: invalid %s value passed %s' % (key, value))
                    error_string = 'error :: invalid request argument - %s' % (key)
                    logger.error(error_string)
                    resp = json.dumps(
                        {'400 Bad Request': error_string})
                    return flask_escape(resp), 400
                if key == 'from_timestamp':
                    from_timestamp = value
                if key == 'until_timestamp':
                    until_timestamp = value
            if key == 'metric_like':
                logger.info('metric_like %s was passed' % value)
                metric_like = str(value)
                if metric_like != 'all':
                    performance_data_request = True
            if key == 'format':
                if value == 'json':
                    format = 'json'
                    logger.info('format %s was passed' % value)
            if key == 'new_fps':
                if value == 'false':
                    new_fps = False
            if key == 'total_fps':
                if value == 'true':
                    fps_matched_count = True

        try:
            remove_prefix_str = request.args.get('remove_prefix', 'false')
            if remove_prefix_str != 'false':
                remove_prefix = True
        except:
            pass

        # @added 20210202 - Feature #3934: ionosphere_performance
        # Handle user timezone
        try:
            timezone_str = request.args.get('tz', 'UTC')
        except:
            pass

        ionosphere_performance_data = {}
        if performance_data_request:
            try:
                ionosphere_performance_data = get_ionosphere_performance(
                    metric, metric_like, from_timestamp, until_timestamp, format,
                    # @added 20210128 - Feature #3934: ionosphere_performance
                    # Improve performance and pass arguments to get_ionosphere_performance
                    # for cache key
                    anomalies, new_fps, fps_matched_count, layers_matched_count,
                    sum_matches, title, period, height, width, fp_type,
                    # @added 20210202 - Feature #3934: ionosphere_performance
                    # Handle user timezone
                    timezone_str)
            except:
                trace = traceback.format_exc()
                fail_msg = 'error :: Webapp error with get_ionosphere_performance'
                logger.error(trace)
                logger.error(fail_msg)
                return internal_error(fail_msg, trace)
            if not ionosphere_performance_data:
                trace = ionosphere_performance_data
                fail_msg = 'error :: Webapp no ionosphere_performance_data'
                return internal_error(fail_msg, trace)

        performance_json_dict = {}

        performance_success = False
        if ionosphere_performance_data:
            performance_success = ionosphere_performance_data['success']
        if performance_success:
            logger.info('ionosphere ionosphere_performance_data has length - %s performance elements listed' % str(len(ionosphere_performance_data)))
            performance_list = ionosphere_performance_data['performance']
            columns = []
            for item in performance_list:
                for element in item:
                    columns.append(element)
                break
            dates = []
            for item in performance_list:
                dates.append(item['date'])
            for date_str in dates:
                performance_json_dict[date_str] = {}
                for item in performance_list:
                    if item['date'] == date_str:
                        for column in columns:
                            if column == 'date':
                                continue
                            performance_json_dict[date_str][column] = item[column]
                            # @added 20210127 - Feature #3934: ionosphere_performance
                            # Convert nan to null or 0 for valid json
                            if str(item[column]) == 'nan':
                                if column == 'fps_total_count':
                                    performance_json_dict[date_str][column] = 0
                                else:
                                    performance_json_dict[date_str][column] = None
        # Allow for the removal of a prefix from the metric name
        if remove_prefix:
            try:
                if remove_prefix_str.endswith('.'):
                    remove_prefix = '%s' % remove_prefix_str
                else:
                    remove_prefix = '%s.' % remove_prefix_str
                metric = metric.replace(remove_prefix, '')
            except Exception as e:
                logger.error('error :: failed to remove prefix %s from %s - %s' % (str(remove_prefix_str), metric, e))

        data_dict = {"status": {"response": 200, "request_time": (time.time() - start)}, "data": {"metric": metric, "metric_like": metric_like, "performance": performance_json_dict}}
        if not performance_success:
            data_dict = {"status": {"response": 204, "request_time": (time.time() - start)}, "data": {"metric": metric, "metric_like": metric_like, "performance": performance_json_dict, "success": False, "reason": "no data for query"}}
        if format == 'json':
            logger.info('ionosphere performance returned json with %s performance elements listed' % str(len(performance_json_dict)))
            return jsonify(data_dict), 200

        return render_template(
            'ionosphere.html', performance=True,
            performance_results=ionosphere_performance_data,
            performance_json_dict=data_dict,
            for_metric=metric, metric_like=metric_like,
            from_timestamp=from_timestamp, until_timestamp=until_timestamp,
            matched_from_datetime=matched_from_datetime,
            # @added 20210202 - Feature #3934: ionosphere_performance
            # Handle user timezone
            pytz_timezones=pytz_timezones,
            version=skyline_version, user=user,
            duration=(time.time() - start), print_debug=False), 200

    # @added 20170220 - Feature #1862: Ionosphere features profiles search page
    # Ionosphere features profiles by generations
    fp_search_req = None
    # @added 20170916 - Feature #1996: Ionosphere - matches page
    # Handle both fp_search and fp_matches
    fp_search_or_matches_req = False

    if 'fp_search' in request.args:
        fp_search_req = request.args.get(str('fp_search'), None)
        if fp_search_req == 'true':
            fp_search_req = True
            fp_search_or_matches_req = True
        else:
            fp_search_req = False

    # @added 20170916 - Feature #1996: Ionosphere - matches page
    fp_matches_req = None
    if 'fp_matches' in request.args:
        fp_matches_req = request.args.get(str('fp_matches'), None)
        if fp_matches_req == 'true':
            fp_matches_req = True
            fp_search_or_matches_req = True
            from_timestamp = None
            until_timestamp = None
        else:
            fp_matches_req = False
    # @modified 20170916 - Feature #1996: Ionosphere - matches page
    # Handle both fp_search and fp_matches
    # if fp_search_req and request_args_len > 1:
    if fp_search_or_matches_req and request_args_len > 1:
        REQUEST_ARGS = ['fp_search',
                        'metric',
                        'metric_like',
                        'from_timestamp',
                        'until_timestamp',
                        'generation_greater_than',
                        # @added 20170315 - Feature #1960: ionosphere_layers
                        'layers_id_greater_than',
                        # @added 20170402 - Feature #2000: Ionosphere - validated
                        'validated_equals',
                        # @added 20170518 - Feature #1996: Ionosphere - matches page - matched_greater_than
                        'matched_greater_than',
                        'full_duration',
                        'enabled',
                        'tsfresh_version',
                        'generation',
                        'count_by_metric',
                        'count_by_matched',
                        'count_by_generation',
                        'count_by_checked',
                        'limit',
                        'order',
                        # @added 20170916 - Feature #1996: Ionosphere - matches page
                        'fp_matches',
                        # @added 20170917 - Feature #1996: Ionosphere - matches page
                        'fp_id', 'layer_id',
                        # @added 20180804 - Feature #2488: Allow user to specifically set metric as a derivative metric in training_data
                        'load_derivative_graphs',
                        # @added 20180917 - Feature #2602: Graphs in search_features_profiles
                        'show_graphs',
                        # @added 20191212 - Feature #3230: users DB table
                        #                   Feature #2516: Add label to features profile
                        # Allow the user_id to be passed as a request argument
                        'user_id',
                        ]

        count_by_metric = None
        ordered_by = None
        limited_by = None
        get_metric_profiles = False
        not_metric_wildcard = True

        # @added 20200710 - Feature #1996: Ionosphere - matches page
        # Moved from within the loop below
        matching = False
        metric_like = False

        for i in request.args:
            key = str(i)
            if key not in REQUEST_ARGS:
                # @modified 20190524 - Branch #3002: docker
                # Return data
                # logger.error('error :: invalid request argument - %s' % (key))
                # return 'Bad Request', 400
                error_string = 'error :: invalid request argument - %s' % (key)
                logger.error(error_string)
                resp = json.dumps(
                    {'400 Bad Request': error_string})
                return flask_escape(resp), 400

            value = request.args.get(key, None)
            logger.info('request argument - %s=%s' % (key, str(value)))

            if key == 'order':
                order = str(value)
                if order == 'DESC':
                    ordered_by = 'DESC'
                if order == 'ASC':
                    ordered_by = 'ASC'
            if key == 'limit':
                limit = str(value)
                try:
                    test_limit = int(limit) + 0
                    limited_by = test_limit
                except:
                    logger.error('error :: limit is not an integer - %s' % str(limit))
                    limited_by = '30'

            if key == 'from_timestamp' or key == 'until_timestamp':
                timestamp_format_invalid = True
                if value == 'all':
                    timestamp_format_invalid = False
                # unix timestamp
                if value.isdigit():
                    timestamp_format_invalid = False
                # %Y%m%d %H:%M timestamp
                if timestamp_format_invalid:
                    value_strip_colon = value.replace(':', '')
                    new_value = value_strip_colon.replace(' ', '')
                    if new_value.isdigit():
                        timestamp_format_invalid = False
                if timestamp_format_invalid:
                    error_string = 'error :: invalid %s value passed %s' % (key, value)
                    logger.error('error :: invalid %s value passed %s' % (key, value))
                    # @modified 20190524 - Branch #3002: docker
                    # Return data
                    # return 'Bad Request', 400
                    error_string = 'error :: invalid request argument - %s' % (key)
                    logger.error(error_string)
                    resp = json.dumps(
                        {'400 Bad Request': error_string})
                    return flask_escape(resp), 400

                # @added 20190524 - Bug #3050: Ionosphere - Skyline and Graphite feedback
                #                   Branch #3002: docker
                # Added the missing definition of these 2 variables in the
                # fp_matches context
                if key == 'from_timestamp':
                    from_timestamp = value
                if key == 'until_timestamp':
                    until_timestamp = value

            if key == 'count_by_metric':
                count_by_metric = request.args.get(str('count_by_metric'), None)
                if count_by_metric == 'true':
                    count_by_metric = True
                else:
                    count_by_metric = False

            if key == 'metric':
                if str(value) == 'all' or str(value) == '*':
                    not_metric_wildcard = False
                    get_metric_profiles = True
                    metric = str(value)

            if key == 'metric' and not_metric_wildcard:
                try:
                    unique_metrics = list(REDIS_CONN.smembers(settings.FULL_NAMESPACE + 'unique_metrics'))
                except:
                    logger.error('error :: Webapp could not get the unique_metrics list from Redis')
                    logger.info(traceback.format_exc())
                    return 'Internal Server Error', 500
                metric_name = settings.FULL_NAMESPACE + str(value)
                # @added 20220112 - Bug #4374: webapp - handle url encoded chars
                # @modified 20220115 - Bug #4374: webapp - handle url encoded chars
                # Roll back change - breaking existing metrics with colons
                # metric_name = url_encode_metric_name(metric_name)

                # @added 20180423 - Feature #2034: analyse_derivatives
                #                   Branch #2270: luminosity
                metric_found_in_other_redis = False
                other_unique_metrics = []
                if metric_name not in unique_metrics and settings.OTHER_SKYLINE_REDIS_INSTANCES:
                    # @modified 20180519 - Feature #2378: Add redis auth to Skyline and rebrow
                    # for redis_ip, redis_port in settings.OTHER_SKYLINE_REDIS_INSTANCES:
                    for redis_ip, redis_port, redis_password in settings.OTHER_SKYLINE_REDIS_INSTANCES:
                        if not metric_found_in_other_redis:
                            try:
                                if redis_password:
                                    other_redis_conn = redis.StrictRedis(host=str(redis_ip), port=int(redis_port), password=str(redis_password))
                                else:
                                    other_redis_conn = redis.StrictRedis(host=str(redis_ip), port=int(redis_port))
                                other_unique_metrics = list(other_redis_conn.smembers(settings.FULL_NAMESPACE + 'unique_metrics'))
                            except:
                                logger.error(traceback.format_exc())
                                logger.error('error :: failed to connect to Redis at %s on port %s' % (str(redis_ip), str(redis_port)))
                            if metric_name in other_unique_metrics:
                                metric_found_in_other_redis = True
                                logger.info('%s found in derivative_metrics in Redis at %s on port %s' % (metric_name, str(redis_ip), str(redis_port)))

                # @added 20201127 - Feature #3824: get_cluster_data
                #                   Feature #3820: HORIZON_SHARDS
                # Change from the deprecated OTHER_SKYLINE_REDIS_INSTANCES
                # to use the REMOTE_SKYLINE_INSTANCES and get_cluster_data
                if settings.REMOTE_SKYLINE_INSTANCES:
                    remote_unique_metrics = None
                    try:
                        remote_unique_metrics = get_cluster_data('unique_metrics', 'metrics')
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: Webapp could not get unique_metrics from the remote Skyline instances')
                    if remote_unique_metrics:
                        logger.info('got %s remote unique_metrics from the remote Skyline instances' % str(len(remote_unique_metrics)))
                        unique_metrics_list = unique_metrics + remote_unique_metrics
                        unique_metrics = list(set(unique_metrics_list))

                if metric_name not in unique_metrics and not metric_found_in_other_redis:
                    # @added 20200715 - Task #3648: webapp - fp_search - do not use Redis metric check
                    # Do not use the Redis metric check result in a webapp
                    # fp_search request as this allows retried metric fps to
                    # accessed
                    if fp_search_req:
                        logger.info('fp_search_or_matches_req - %s not in Redis, but fp_search request so continuing' % metric_name)
                        get_metric_profiles = True
                        metric = str(value)
                    else:
                        error_string = 'error :: no metric - %s - exists in Redis' % metric_name
                        logger.error(error_string)
                        resp = json.dumps(
                            {'results': error_string})
                        # @modified 20190116 - Cross-Site Scripting Security Vulnerability #85
                        #                      Bug #2816: Cross-Site Scripting Security Vulnerability
                        # return resp, 404
                        return flask_escape(resp), 404
                else:
                    get_metric_profiles = True
                    metric = str(value)

            # @added 20170917 - Feature #1996: Ionosphere - matches page
            # @modified 20170917 - Feature #1996: Ionosphere - matches page
            # Move outside the loop
            # matching = False
            # metric_like = False
            if key == 'metric_like':
                if value == 'all':
                    metric_namespace_pattern = value.replace('all', '')

                metric_namespace_pattern = value.replace('%', '')
                if metric_namespace_pattern != '' and value != 'all':
                    try:
                        unique_metrics = list(REDIS_CONN.smembers(settings.FULL_NAMESPACE + 'unique_metrics'))
                    except:
                        trace = traceback.format_exc()
                        fail_msg = 'error :: Webapp could not get the unique_metrics list from Redis'
                        logger.error(fail_msg)
                        logger.info(traceback.format_exc())
                        return internal_error(fail_msg, trace)

                    matching = [s for s in unique_metrics if metric_namespace_pattern in s]
                    if len(matching) == 0:
                        error_string = 'error :: no metric like - %s - exists in Redis' % metric_namespace_pattern
                        logger.error(error_string)
                        resp = json.dumps(
                            {'results': error_string})
                        # @modified 20190116 - Cross-Site Scripting Security Vulnerability #85
                        #                      Bug #2816: Cross-Site Scripting Security Vulnerability
                        # return resp, 404
                        return flask_escape(resp), 404
                    else:
                        # @added 20200710 - Feature #1996: Ionosphere - matches page
                        logger.info('metric_like %s was passed, %s metrics found matching' % (value, str(len(matching))))
                        metric_like = str(value)
                if matching:
                    metric_like = str(value)

    if fp_search_req and request_args_len > 1:
        if count_by_metric:
            # @modified 20180717 - Task #2446: Optimize Ionosphere
            # Added missing search_success variable
            features_profiles, fps_count, mc, cc, gc, full_duration_list, enabled_list, tsfresh_version_list, generation_list, search_success, fail_msg, trace = ionosphere_search(False, True)
            logger.info('fp_search_req returning response')
            return render_template(
                'ionosphere.html', fp_search=fp_search_req,
                fp_search_results=fp_search_req,
                features_profiles_count=fps_count, order=ordered_by,
                limit=limited_by, matched_count=mc, checked_count=cc,
                generation_count=gc, matched_from_datetime=matched_from_datetime,
                version=skyline_version,
                duration=(time.time() - start), print_debug=False), 200

        # @added 20180917 - Feature #2602: Graphs in search_features_profiles
        features_profiles_with_images = []
        show_graphs = False

        if get_metric_profiles:
            search_success = False
            try:
                fps, fps_count, mc, cc, gc, full_duration_list, enabled_list, tsfresh_version_list, generation_list, search_success, fail_msg, trace = ionosphere_search(False, True)
            except:
                trace = traceback.format_exc()
                fail_msg = 'error :: Webapp error with search_ionosphere'
                logger.error(fail_msg)
                return internal_error(fail_msg, trace)
            if not search_success:
                return internal_error(fail_msg, trace)

            # @added 20180917 - Feature #2602: Graphs in search_features_profiles
            if search_success and fps:
                show_graphs = request.args.get(str('show_graphs'), False)
                if show_graphs == 'true':
                    show_graphs = True
            if search_success and fps and show_graphs:
                # @modified 20190503 - Branch #2646: slack - linting
                # query_context = 'features_profiles'

                for fp_elements in fps:
                    # Get images
                    try:
                        fp_id = fp_elements[0]
                        base_name = fp_elements[2]
                        requested_timestamp = fp_elements[4]

                        # @modified 20181205 - Bug #2746: webapp time out - Graphs in search_features_profiles
                        #                      Feature #2602: Graphs in search_features_profiles
                        # This function was causing the webapp to time out due
                        # to fetching all the matched Graphite graphs
                        # mpaths, images, hdate, m_vars, ts_json, data_to_process, p_id, gimages, gmimages, times_matched, glm_images, l_id_matched, ts_fd, i_ts_json, anomalous_timeseries, f_id_matched, fp_details_list = ionosphere_metric_data(requested_timestamp, base_name, query_context, fp_id)
                        images, gimages = ionosphere_show_graphs(requested_timestamp, base_name, fp_id)

                        new_fp = []
                        for fp_element in fp_elements:
                            new_fp.append(fp_element)

                        # @added 20180918 - Feature #2602: Graphs in search_features_profiles
                        # The images are required to be sorted here in terms of
                        # only passing the Redis image (if present) and the
                        # full duration graph, as it is a bit too much
                        # achieve in the Jinja template.
                        full_duration_float = fp_elements[3]
                        full_duration = int(full_duration_float)
                        full_duration_in_hours = full_duration / 60 / 60
                        full_duration_in_hours_image_string = '.%sh.png' % str(int(full_duration_in_hours))

                        # @modified 20190503 - Branch #2646: slack - linting
                        # show_graph_images = []

                        redis_image = 'No Redis data graph'
                        full_duration_image = 'No full duration graph'
                        # @modified 20180918 - Feature #2602: Graphs in search_features_profiles
                        # Append individual redis_image and full_duration_image
                        # list elements instead of just added the images or
                        #  gimages list
                        # if images:
                        #     new_fp.append(images)
                        # else:
                        #     new_fp.append(gimages)
                        if images:
                            for image in images:
                                if '.redis.plot' in image:
                                    redis_image = image
                                if full_duration_in_hours_image_string in image:
                                    full_duration_image = image
                        else:
                            for image in gimages:
                                if '.redis.plot' in image:
                                    redis_image = image
                                if full_duration_in_hours_image_string in image:
                                    full_duration_image = image
                        if full_duration_image == 'No full duration graph':
                            for image in gimages:
                                if full_duration_in_hours_image_string in image:
                                    full_duration_image = image
                        new_fp.append(full_duration_image)
                        new_fp.append(redis_image)

                        features_profiles_with_images.append(new_fp)
                    except:
                        message = 'Uh oh ... a Skyline 500 :('
                        trace = traceback.format_exc()
                        return internal_error(message, trace)

            if not features_profiles_with_images:
                if fps:
                    for fp_elements in fps:
                        try:
                            new_fp = []
                            for fp_element in fp_elements:
                                new_fp.append(fp_element)
                            new_fp.append(None)
                            features_profiles_with_images.append(new_fp)
                        except:
                            message = 'Uh oh ... a Skyline 500 :('
                            trace = traceback.format_exc()
                            return internal_error(message, trace)

            # @modified 20170912 - Feature #2056: ionosphere - disabled_features_profiles
            # Added enabled_list to display DISABLED in search_features_profiles
            # page results.
            return render_template(
                'ionosphere.html', fp_search=fp_search_req,
                fp_search_results=fp_search_req, features_profiles=fps,
                for_metric=metric, order=ordered_by, limit=limited_by,
                matched_count=mc, checked_count=cc, generation_count=gc,
                enabled_list=enabled_list,
                matched_from_datetime=matched_from_datetime,
                # @added 20180917 - Feature #2602: Graphs in search_features_profiles
                features_profiles_with_images=features_profiles_with_images,
                show_graphs=show_graphs,
                version=skyline_version, duration=(time.time() - start),
                print_debug=False), 200

    # @added 20170916 - Feature #1996: Ionosphere - matches page
    if fp_matches_req:
        # @added 20170917 - Feature #1996: Ionosphere - matches page
        # Added by fp_id or layer_id as well
        fp_id = None
        layer_id = None
        # @added 20190619 - Feature #3084: Ionosphere - validated matches
        validated_equals = None
        for i in request.args:
            key = str(i)
            value = request.args.get(key, None)
            if key == 'fp_id':
                logger.info('request key %s set to %s' % (key, str(value)))
                try:
                    # @modified 20190524 - Branch #3002: docker
                    # test_fp_id = int(value) + 0
                    # if test_fp_id > 0:
                    #     fp_id = str(test_fp_id)
                    test_fp_id = int(value) + 1
                    if test_fp_id > -1:
                        fp_id = str(value)
                    else:
                        # @modified 20190116 - Cross-Site Scripting Security Vulnerability #85
                        #                      Bug #2816: Cross-Site Scripting Security Vulnerability
                        # Test that the fp_id is an int first
                        # fp_id = None
                        logger.error('error :: invalid request argument - fp_id is not an int')
                        # @modified 20190524 - Branch #3002: docker
                        # Return data
                        # return 'Bad Request', 400
                        error_string = 'error :: invalid request argument - fp_id is not an int'
                        logger.error(error_string)
                        resp = json.dumps(
                            {'400 Bad Request': error_string})
                        return flask_escape(resp), 200

                    logger.info('fp_id now set to %s' % (str(fp_id)))
                except:
                    error_string = 'error :: the fp_id argument was passed but not as an int - %s' % str(value)
                    logger.error(error_string)
                    resp = json.dumps(
                        {'400 Bad Request': error_string})
                    # @modified 20190116 - Cross-Site Scripting Security Vulnerability #85
                    #                      Bug #2816: Cross-Site Scripting Security Vulnerability
                    # return resp, 404
                    return flask_escape(resp), 404
            if key == 'layer_id':
                logger.info('request key %s set to %s' % (key, str(value)))
                try:
                    test_layer_id = int(value) + 0
                    if test_layer_id > 0:
                        layer_id = str(test_layer_id)
                    else:
                        layer_id = None
                    logger.info('layer_id now set to %s' % (str(layer_id)))
                except:
                    error_string = 'error :: the layer_id argument was passed but not as an int - %s' % str(value)
                    logger.error(error_string)
                    resp = json.dumps(
                        {'400 Bad Request': error_string})
                    # @modified 20190116 - Cross-Site Scripting Security Vulnerability #85
                    #                      Bug #2816: Cross-Site Scripting Security Vulnerability
                    # return resp, 404
                    return flask_escape(resp), 404

            # @added 20190619 - Feature #3084: Ionosphere - validated matches
            if key == 'validated_equals':
                validated_equals = str(value)

        logger.info('get_fp_matches with arguments :: %s, %s, %s, %s, %s, %s, %s, %s' % (
            str(metric), str(metric_like), str(fp_id), str(layer_id),
            str(from_timestamp), str(until_timestamp), str(limited_by),
            str(ordered_by)))

        matches, fail_msg, trace = get_fp_matches(metric, metric_like, fp_id, layer_id, from_timestamp, until_timestamp, limited_by, ordered_by)
        if not matches:
            return internal_error(fail_msg, trace)

        # @added 20190619 - Feature #3084: Ionosphere - validated matches
        if validated_equals:
            filter_matches = True
            if validated_equals == 'any':
                filter_matches = False
            if validated_equals == 'true':
                filter_match_validation = 1
            if validated_equals == 'false':
                filter_match_validation = 0
            if validated_equals == 'invalid':
                filter_match_validation = 2
            if filter_matches:
                logger.info('matches filtered by validated = %s' % (
                    str(filter_match_validation)))

        return render_template(
            'ionosphere.html', fp_matches=fp_matches_req, for_metric=metric,
            fp_matches_results=matches, order=ordered_by, limit=limited_by,
            matched_from_datetime=matched_from_datetime,
            version=skyline_version, duration=(time.time() - start),
            print_debug=False), 200

    # @modified 20170118 - Feature #1862: Ionosphere features profiles search page
    # Added fp_search parameter
    # @modified 20170122 - Feature #1872: Ionosphere - features profile page by id only
    # Added fp_id parameter
    # @modified 20170305 - Feature #1960: ionosphere_layers
    # Added layers arguments d_condition to fp_layer
    # @modified 20160315 - Feature #1972: ionosphere_layers - use D layer boundary for upper limit
    # Added d_boundary_times
    # @modified 20170327 - Feature #2004: Ionosphere layers - edit_layers
    # Added layers_id and edit_fp_layers
    # @added 20170402 - Feature #2000: Ionosphere - validated
    IONOSPHERE_REQUEST_ARGS = [
        'timestamp', 'metric', 'metric_td', 'a_dated_list', 'timestamp_td',
        'requested_timestamp', 'fp_view', 'calc_features', 'add_fp',
        'features_profiles', 'fp_search', 'learn', 'fp_id', 'd_condition',
        'd_boundary_limit', 'd_boundary_times', 'e_condition', 'e_boundary_limit',
        'e_boundary_times', 'es_layer', 'es_day', 'f1_layer', 'f1_from_time',
        'f1_layer', 'f2_until_time', 'fp_layer', 'fp_layer_label',
        'add_fp_layer', 'layers_id', 'edit_fp_layers', 'validate_fp',
        'validated_equals',
        # @added 20170616 - Feature #2048: D1 ionosphere layer
        'd1_condition', 'd1_boundary_limit', 'd1_boundary_times',
        # @added 20170617 - Feature #2054: ionosphere.save.training_data
        'save_training_data', 'saved_td_label', 'saved_training_data',
        # added 20170908 - Feature #2056: ionosphere - disabled_features_profiles
        'disable_fp',
        # @added 20170917 - Feature #1996: Ionosphere - matches page
        'matched_fp_id', 'matched_layer_id',
        # @added 20180804 - Feature #2488: Allow user to specifically set metric as a derivative metric in training_data
        'load_derivative_graphs',
        # @added 20190601 - Feature #3084: Ionosphere - validated matches
        'match_validation',
        # @added 20190922 - Feature #2516: Add label to features profile
        'label',
        # @added 20191210 - Feature #3348: fp creation json response
        'format',
        # @added 20191212 - Feature #3230: users DB table
        #                   Feature #2516: Add label to features profile
        # Allow the user_id to be passed as a request argument
        'user_id',
        # @added 20210413 - Feature #4014: Ionosphere - inference
        #                   Branch #3590: inference
        'matched_motif_id',
        # @added 20210415 - Feature #4014: Ionosphere - inference
        'motif_analysis', 'similarity', 'purge',
        # @added 20210416 - Feature #4014: Ionosphere - inference
        'ionosphere_matched_id',
        # @added 20210417 - Feature #4014: Ionosphere - inference
        # Allow the user to define the batch_size per similarity search
        'batch_size', 'top_matches', 'max_distance',
        # @added 20210422 - Feature #4014: Ionosphere - inference
        # Allow the user to specify the range_padding
        'range_padding',
        # @added 20210425 - Feature #4014: Ionosphere - inference
        # Allow user to specify the difference between the areas under the
        # curve
        'max_area_percent_diff',
    ]

    # @modified 20190503 - Branch #2646: slack - linting
    # determine_metric = False

    dated_list = False
    td_requested_timestamp = False

    # @modified 20190503 - Branch #2646: slack - linting
    # feature_profile_view = False

    calculate_features = False
    create_feature_profile = False
    fp_view = False
    fp_profiles = []
    # @added 20170118 - Feature #1862: Ionosphere features profiles search page
    fp_search = False
    # @added 20170120 -  Feature #1854: Ionosphere learn - generations
    # Added fp_learn and fp_fd_days parameters to allow the user to not learn at
    # use_full_duration_days
    fp_learn = False
    fp_fd_days = settings.IONOSPHERE_LEARN_DEFAULT_FULL_DURATION_DAYS

    # @added 20170327 - Feature #2004: Ionosphere layers - edit_layers
    #                   Task #2002: Review and correct incorrectly defined layers
    # Added the argument edit_fp_layers
    edit_fp_layers = False
    layers_id = None
    # @added 20170617 - Feature #2054: ionosphere.save.training_data
    save_training_data = False
    saved_training_data = False
    saved_td_label = False

    # added 20170908 - Feature #2056: ionosphere - disabled_features_profiles
    disable_fp = False

    # @added 20170917 - Feature #1996: Ionosphere - matches page
    matched_fp_id = False
    matched_layer_id = False

    # @added 20210413 - Feature #4014: Ionosphere - inference
    #                   Branch #3590: inference
    matched_motif_id = False
    # @added 20210416 - Feature #4014: Ionosphere - inference
    ionosphere_matched_id = False

    # @added 20190601 - Feature #3084: Ionosphere - validated matches
    match_validated = 0

    # @added 20190922 - Feature #2516: Add label to features profile
    fp_label = None

    # @added 20191210 - Feature #3348: fp creation json response
    response_format = None

    # @added 20210422 - Feature #4014: Ionosphere - inference
    # Allow the user to specify the range_padding
    motif_range_padding = None

    try:
        if request_args_present:
            timestamp_arg = False
            metric_arg = False
            metric_td_arg = False
            timestamp_td_arg = False
            # @added 20180804 - Feature #2488: Allow user to specifically set metric as a derivative metric in training_data
            set_derivative_metric = False

            if 'fp_view' in request.args:
                fp_view = request.args.get(str('fp_view'), None)
                base_name = request.args.get(str('metric'), None)

                # @added 20170917 - Feature #1996: Ionosphere - matches page
                if 'matched_fp_id' in request.args:
                    matched_fp_id = request.args.get(str('matched_fp_id'), None)
                if 'matched_layer_id' in request.args:
                    matched_layer_id = request.args.get(str('matched_layer_id'), None)
                # @added 20190601 - Feature #3084: Ionosphere - validated matches
                if 'match_validation' in request.args:
                    match_validated_str = request.args.get(str('match_validation'), None)
                    if match_validated_str:
                        try:
                            match_validated = int(match_validated_str)
                        except:
                            error_string = 'error :: invalid request argument - match_validation is not an int - %s' % str(match_validated_str)
                            logger.error(error_string)
                            resp = json.dumps(
                                {'results': error_string})
                            return flask_escape(resp), 400

                # @added 20210413 - Feature #4014: Ionosphere - inference
                #                   Branch #3590: inference
                if 'matched_motif_id' in request.args:
                    matched_motif_id = request.args.get(str('matched_motif_id'), 0)
                    logger.info('matched_motif_id: %s' % str(matched_motif_id))
                    # @added 20210422 - Feature #4014: Ionosphere - inference
                    # Allow the user to specify the range_padding
                    if 'range_padding' in request.args:
                        motif_range_padding = request.args.get('range_padding', 10)
                        logger.info('range_padding: %s' % str(motif_range_padding))
                    # @added 20210425 - Feature #4014: Ionosphere - inference
                    # Allow user to specify the difference between the areas under the
                    # curve
                    if 'max_area_percent_diff' in request.args:
                        motif_max_area_percent_diff = request.args.get('max_area_percent_diff', 10)
                        logger.info('max_area_percent_diff: %s' % str(motif_max_area_percent_diff))

                # @added 20210416 - Feature #4014: Ionosphere - inference
                if 'ionosphere_matched_id' in request.args:
                    ionosphere_matched_id = request.args.get(str('ionosphere_matched_id'), 0)
                    logger.info('matched_motif_id: %s' % str(matched_motif_id))

                # @added 20170122 - Feature #1872: Ionosphere - features profile page by id only
                # Determine the features profile dir path for a fp_id
                if 'fp_id' in request.args:
                    # @added 20190116 - Cross-Site Scripting Security Vulnerability #85
                    #                      Bug #2816: Cross-Site Scripting Security Vulnerability
                    # Test that the fp_id is an int first
                    try:
                        test_fp_id = request.args.get(str('fp_id'))
                        test_fp_id_valid = int(test_fp_id) + 1
                        logger.info('test_fp_id_valid tests OK with %s' % str(test_fp_id_valid))
                    except:
                        logger.error('error :: invalid request argument - fp_id is not an int')
                        # @modified 20190524 - Branch #3002: docker
                        # Return data
                        # return 'Bad Request', 400
                        error_string = 'error :: the fp_id argument was passed but not as an int - %s' % str(value)
                        logger.error(error_string)
                        resp = json.dumps(
                            {'results': error_string})
                        return flask_escape(resp), 400

                    fp_id = request.args.get(str('fp_id'), 0)

                    # @modified 20190503 - Branch #2646: slack - linting
                    # metric_timestamp = 0

                    try:
                        fp_details, fp_details_successful, fail_msg, traceback_format_exc, fp_details_object = features_profile_details(fp_id)
                        anomaly_timestamp = int(fp_details_object['anomaly_timestamp'])
                        created_timestamp = fp_details_object['created_timestamp']
                    except:
                        trace = traceback.format_exc()
                        message = 'failed to get features profile details for id %s' % str(fp_id)
                        return internal_error(message, trace)
                    if not fp_details_successful:
                        trace = traceback.format_exc()
                        fail_msg = 'error :: features_profile_details failed'
                        return internal_error(fail_msg, trace)

                    use_timestamp = 0
                    metric_timeseries_dir = base_name.replace('.', '/')
                    # @modified 20170126 - Feature #1872: Ionosphere - features profile page by id only
                    # The the incorrect logic, first it should be checked if
                    # there is a use_full_duration parent timestamp
                    dt = str(created_timestamp)
                    naive = datetime.datetime.strptime(dt, '%Y-%m-%d %H:%M:%S')
                    pytz_tz = settings.SERVER_PYTZ_TIMEZONE
                    local = pytz.timezone(pytz_tz)
                    local_dt = local.localize(naive, is_dst=None)
                    utc_dt = local_dt.astimezone(pytz.utc)
                    unix_created_timestamp = utc_dt.strftime('%s')
                    features_profiles_data_dir = '%s/%s/%s' % (
                        settings.IONOSPHERE_PROFILES_FOLDER, metric_timeseries_dir,
                        str(unix_created_timestamp))
                    if os.path.exists(features_profiles_data_dir):
                        use_timestamp = int(unix_created_timestamp)
                    else:
                        logger.error('no timestamp feature profiles data dir found for feature profile id %s at %s' % (str(fp_id), str(features_profiles_data_dir)))

                    # @added 20170915 - Bug #2162: ionosphere - mismatching timestamp metadata
                    #                   Feature #1872: Ionosphere - features profile page by id only
                    # Iterate back a few seconds as the features profile dir and
                    # file resources may have a slight offset timestamp from the
                    # created_timestamp which is based on MySQL CURRENT_TIMESTAMP
                    if use_timestamp == 0:
                        check_back_to_timestamp = int(unix_created_timestamp) - 10
                        check_timestamp = int(unix_created_timestamp) - 1
                        while check_timestamp > check_back_to_timestamp:
                            features_profiles_data_dir = '%s/%s/%s' % (
                                settings.IONOSPHERE_PROFILES_FOLDER, metric_timeseries_dir,
                                str(check_timestamp))
                            if os.path.exists(features_profiles_data_dir):
                                use_timestamp = int(check_timestamp)
                                check_timestamp = check_back_to_timestamp - 1
                            else:
                                check_timestamp -= 1

                    features_profiles_data_dir = '%s/%s/%s' % (
                        settings.IONOSPHERE_PROFILES_FOLDER, metric_timeseries_dir,
                        str(anomaly_timestamp))
                    # @modified 20170126 - Feature #1872: Ionosphere - features profile page by id only
                    # This was the incorrect logic, first it should be checked if
                    # there is a use_full_duration parent timestamp
                    if use_timestamp == 0:
                        if os.path.exists(features_profiles_data_dir):
                            use_timestamp = int(anomaly_timestamp)
                        else:
                            logger.error('no timestamp feature profiles data dir found for feature profile id %s at %s' % (str(fp_id), str(features_profiles_data_dir)))

                    if use_timestamp == 0:
                        logger.error('no timestamp feature profiles data dir found for feature profile id - %s' % str(fp_id))

                        # @added 20180420 - Branch #2270: luminosity
                        # Use settings.ALTERNATIVE_SKYLINE_URLS if they are
                        # declared
                        try:
                            use_alternative_urls = settings.ALTERNATIVE_SKYLINE_URLS
                        except:
                            use_alternative_urls = False

                        if use_alternative_urls:
                            alternative_urls = []
                            for alt_url in use_alternative_urls:
                                alt_redirect_url = '%s/ionosphere?fp_view=true&fp_id=%s&metric=%s' % (str(alt_url), str(fp_id), str(base_name))
                                alternative_urls.append(alt_redirect_url)
                            message = 'no timestamp feature profiles data dir found on this Skyline instance try at the alternative URLS listed below:'
                            logger.info('passing alternative_urls - %s' % str(alternative_urls))
                            try:
                                return render_template(
                                    'ionosphere.html', display_message=message,
                                    alternative_urls=alternative_urls,
                                    fp_view=True,
                                    version=skyline_version, duration=(time.time() - start),
                                    print_debug=True), 200
                            except:
                                message = 'Uh oh ... a Skyline 500 :('
                                trace = traceback.format_exc()
                                return internal_error(message, trace)

                        resp = json.dumps(
                            {'results': 'Error: no timestamp feature profiles data dir found for feature profile id - ' + str(fp_id) + ' - go on... nothing here.'})
                        # @modified 20190116 - Cross-Site Scripting Security Vulnerability #85
                        #                      Bug #2816: Cross-Site Scripting Security Vulnerability
                        # return resp, 400
                        return flask_escape(resp), 400

                    redirect_url = '%s/ionosphere?fp_view=true&timestamp=%s&metric=%s' % (settings.SKYLINE_URL, str(use_timestamp), base_name)
                    logger.info('base redirect_url - %s' % redirect_url)

                    # @added 20180815 - Feature #2430: Ionosphere validate learnt features profiles page
                    validate_fp_req = False
                    if 'validate_fp' in request.args:
                        validate_fp_req = request.args.get(str('validate_fp'), None)
                        if validate_fp_req == 'true':
                            validate_fp_req = True
                    if validate_fp_req:
                        redirect_url = '%s/ionosphere?fp_view=true&timestamp=%s&metric=%s&validate_fp=true' % (
                            settings.SKYLINE_URL, str(use_timestamp), base_name)

                    # @added 20180816 - Feature #2430: Ionosphere validate learnt features profiles page
                    disable_fp_req = False
                    if 'disable_fp' in request.args:
                        disable_fp_id = request.args.get(str('disable_fp'), None)
                        if isinstance(int(disable_fp_id), int):
                            disable_fp_req = True
                    if disable_fp_req:
                        redirect_url = '%s/ionosphere?fp_view=true&timestamp=%s&metric=%s&disable_fp=%s' % (
                            settings.SKYLINE_URL, str(use_timestamp), base_name,
                            str(disable_fp_id))

                    # @added 20170917 - Feature #1996: Ionosphere - matches page
                    if matched_fp_id:
                        if matched_fp_id != 'False':
                            redirect_url = '%s/ionosphere?fp_view=true&timestamp=%s&metric=%s&matched_fp_id=%s' % (
                                settings.SKYLINE_URL, str(use_timestamp), base_name,
                                str(matched_fp_id))
                    if matched_layer_id:
                        if matched_layer_id != 'False':
                            redirect_url = '%s/ionosphere?fp_view=true&timestamp=%s&metric=%s&matched_layer_id=%s' % (
                                settings.SKYLINE_URL, str(use_timestamp), base_name,
                                str(matched_layer_id))

                    # @added 20210413 - Feature #4014: Ionosphere - inference
                    #                   Branch #3590: inference
                    if matched_motif_id:
                        redirect_url = '%s/ionosphere?fp_view=true&timestamp=%s&metric=%s&matched_motif_id=%s' % (
                            settings.SKYLINE_URL, str(use_timestamp), base_name,
                            str(matched_motif_id))
                        logger.info('redirecting fp_id matched_motif_id request')
                    # @added 20210416 - Feature #4014: Ionosphere - inference
                    if ionosphere_matched_id:
                        ionosphere_matched_id_redirect_url = '%s&ionosphere_matched_id=%s' % (
                            redirect_url, str(ionosphere_matched_id))
                        redirect_url = ionosphere_matched_id_redirect_url

                    # @added 20190601 - Feature #3084: Ionosphere - validated matches
                    # @modified 20210413 Feature #4014: Ionosphere - inference
                    #                    Branch #3590: inference
                    # Added matched_motif_id
                    if matched_fp_id or matched_layer_id or matched_motif_id:
                        if 'match_validation' in request.args:
                            if match_validated > 0:
                                validate_matched_redirect_url = '%s&match_validation=%s' % (
                                    redirect_url, str(match_validated))
                                redirect_url = validate_matched_redirect_url

                    # @modified 20170327 - Feature #2004: Ionosphere layers - edit_layers
                    #                      Task #2002: Review and correct incorrectly defined layers
                    # Build the query string from the previous parameters
                    if 'edit_fp_layers' in request.args:
                        redirect_url = '%s/ionosphere?timestamp=%s' % (settings.SKYLINE_URL, str(use_timestamp))
                        for i in request.args:
                            key = str(i)
                            if key == 'timestamp':
                                continue
                            value = request.args.get(key, None)
                            new_redirect_url = '%s&%s=%s' % (
                                redirect_url, str(key), str(value))
                            redirect_url = new_redirect_url
                    if 'edit_fp_layers' in request.args:
                        logger.info('not returning redirect as edit_fp_layers request')
                    else:
                        logger.info('returned redirect on original request - %s' % str(redirect_url))
                        return redirect(redirect_url, code=302)

            for i in request.args:
                key = str(i)
                if key not in IONOSPHERE_REQUEST_ARGS:
                    logger.error('error :: invalid request argument - %s' % (key))
                    # @modified 20190524 - Branch #3002: docker
                    # Return data
                    # return 'Bad Request', 400
                    error_string = 'error :: invalid request argument - %s' % (key)
                    logger.error(error_string)
                    resp = json.dumps(
                        {'400 Bad Request': error_string})
                    return flask_escape(resp), 400

                value = request.args.get(key, None)
                logger.info('request argument - %s=%s' % (key, str(value)))

                if key == 'calc_features':
                    if str(value) == 'true':
                        calculate_features = True

                if key == 'add_fp':
                    if str(value) == 'true':
                        create_feature_profile = True
                        # @added 20191210 - Feature #3348: fp creation json response
                        if 'format' in request.args:
                            response_format = request.args.get(str('format'), None)
                            if response_format != 'json':
                                response_format = None

                # @added 20170317 - Feature #1960: ionosphere_layers - allow for floats
                if key == 'd_boundary_limit':
                    try:
                        d_boundary_limit = float(value)
                    except ValueError:
                        logger.error('error :: invalid request argument - %s is not numeric - %s' % (key, str(value)))
                        return 'Bad Request\n\ninvalid request argument - %s is not numeric - %s' % (key, str(value)), 400
                    logger.info('request argument OK - %s=%s' % (key, str(d_boundary_limit)))
                if key == 'e_boundary_limit':
                    try:
                        e_boundary_limit = float(value)
                    except ValueError:
                        logger.error('error :: invalid request argument - %s is not numeric - %s' % (key, str(value)))
                        return 'Bad Request\n\ninvalid request argument - %s is not numeric - %s' % (key, str(value)), 400
                    logger.info('request argument OK - %s=%s' % (key, str(e_boundary_limit)))

                if key == 'fp_view':
                    if str(value) == 'true':
                        fp_view = True

                # @added 20170118 - Feature #1862: Ionosphere features profiles search page
                # Added fp_search parameter
                if key == 'fp_search':
                    if str(value) == 'true':
                        fp_search = True

                # @added 20170120 -  Feature #1854: Ionosphere learn - generations
                # Added fp_learn parameter
                if key == 'learn':
                    if str(value) == 'true':
                        fp_learn = True
                    # @added 20170305 - Feature #1960: ionosphere_layers
                    # Being passed through as a boolean from the Create features
                    # profile arguments and I cannot be arsed to track it down
                    if str(value) == 'True':
                        fp_learn = True

                if key == 'features_profiles':
                    fp_profiles = str(value)
                    # @added 20190503 - Branch #2646: slack - linting
                    logger.info('fp_profiles is %s' % fp_profiles)

                # @added 20170327 - Feature #2004: Ionosphere layers - edit_layers
                #                   Task #2002: Review and correct incorrectly defined layers
                # Added layers and edit_fp_layers
                if key == 'layers_id':
                    test_layer_id = str(value)
                    try:
                        layers_id = int(test_layer_id)
                    except:
                        logger.info('bad request argument - %s=%s not numeric' % (str(key), str(value)))
                        resp = json.dumps(
                            {'results': 'Error: not a numeric id for the layers_id argument - %s' + str(value) + ' - please pass a proper id'})
                if key == 'edit_fp_layers':
                    if str(value) == 'true':
                        edit_fp_layers = True
                    if str(value) == 'True':
                        edit_fp_layers = True
                    if edit_fp_layers:
                        logger.info('edit_fp_layers is set to %s' % (str(edit_fp_layers)))

                if key == 'a_dated_list':
                    if str(value) == 'true':
                        dated_list = True

                if key == 'requested_timestamp':
                    valid_rt_timestamp = False
                    if str(value) == 'False':
                        valid_rt_timestamp = True
                    if not valid_rt_timestamp:
                        if not len(str(value)) == 10:
                            logger.info('bad request argument - %s=%s not an epoch timestamp' % (str(key), str(value)))
                            resp = json.dumps(
                                {'results': 'Error: not an epoch timestamp for ' + str(key) + ' - ' + str(value) + ' - please pass a proper epoch timestamp'})
                        else:
                            try:
                                timestamp_numeric = int(value) + 1
                                valid_rt_timestamp = True
                            except:
                                valid_timestamp = False
                                logger.info('bad request argument - %s=%s not numeric' % (str(key), str(value)))
                                resp = json.dumps(
                                    {'results': 'Error: not a numeric epoch timestamp for ' + str(key) + ' - ' + str(value) + ' - please pass a proper epoch timestamp'})

                    if not valid_rt_timestamp:
                        # @modified 20190116 - Cross-Site Scripting Security Vulnerability #85
                        #                      Bug #2816: Cross-Site Scripting Security Vulnerability
                        # return resp, 400
                        return flask_escape(resp), 400

                # @added 20170617 - Feature #2054: ionosphere.save.training_data
                if key == 'save_training_data':
                    if str(value) == 'true':
                        save_training_data = True
                if key == 'saved_td_label':
                    saved_td_label = str(value)
                if key == 'saved_training_data':
                    if str(value) == 'true':
                        saved_training_data = True
                check_for_purged = False
                if not saved_training_data:
                    if not fp_view:
                        check_for_purged = True

                if key == 'label':
                    label_arg = request.args.get('label')
                    label = label_arg[:255]
                    logger.info('label - %s ' % (str(value)))

                if key == 'timestamp' or key == 'timestamp_td':
                    valid_timestamp = True
                    if not len(str(value)) == 10:
                        valid_timestamp = False
                        logger.info('bad request argument - %s=%s not an epoch timestamp' % (str(key), str(value)))
                        resp = json.dumps(
                            {'results': 'Error: not an epoch timestamp for ' + str(key) + ' - ' + str(value) + ' - please pass a proper epoch timestamp'})
                    if valid_timestamp:
                        try:
                            timestamp_numeric = int(value) + 1
                            # @added 20190503 - Branch #2646: slack - linting
                            logger.info('timestamp_numeric tests OK with %s' % str(timestamp_numeric))
                        except:
                            valid_timestamp = False
                            logger.info('bad request argument - %s=%s not numeric' % (str(key), str(value)))
                            resp = json.dumps(
                                {'results': 'Error: not a numeric epoch timestamp for ' + str(key) + ' - ' + str(value) + ' - please pass a proper epoch timestamp'})

                    if not valid_timestamp:
                        # @modified 20190116 - Cross-Site Scripting Security Vulnerability #85
                        #                      Bug #2816: Cross-Site Scripting Security Vulnerability
                        # return resp, 400
                        return flask_escape(resp), 400

                    # if not fp_view:
                    if check_for_purged:
                        ionosphere_data_dir = '%s/%s' % (settings.IONOSPHERE_DATA_FOLDER, str(value))
                        if not isdir(ionosphere_data_dir):
                            valid_timestamp = False
                            now = time.time()
                            purged_timestamp = int(now) - int(settings.IONOSPHERE_KEEP_TRAINING_TIMESERIES_FOR)
                            if int(value) < purged_timestamp:
                                # @added 20200813 - Feature #3670: IONOSPHERE_CUSTOM_KEEP_TRAINING_TIMESERIES_FOR
                                historical_training_data_exists = False
                                if IONOSPHERE_CUSTOM_KEEP_TRAINING_TIMESERIES_FOR:
                                    check_for_historical_training_data = False
                                    base_name = request.args.get(str('metric'), None)
                                    # @added 20200814 - Feature #3670: IONOSPHERE_CUSTOM_KEEP_TRAINING_TIMESERIES_FOR
                                    # Handle when a request is submitted to list
                                    # the training data for a timestamp, not for
                                    # a metric, e.g. ionosphere?timestamp_td=1597061559&requested_timestamp=1597061559
                                    if not base_name:
                                        if 'timestamp_td' in request.args:
                                            if 'requested_timestamp' in request.args:
                                                check_for_historical_training_data = True

                                    for historical_metric_namespace in IONOSPHERE_CUSTOM_KEEP_TRAINING_TIMESERIES_FOR:
                                        # @added 20200814 - Feature #3670: IONOSPHERE_CUSTOM_KEEP_TRAINING_TIMESERIES_FOR
                                        # Handle when a request is submitted to list
                                        # the training data for a timestamp, so if
                                        # base_name is not set skip this check
                                        if not base_name:
                                            break

                                        if historical_metric_namespace in base_name:
                                            check_for_historical_training_data = True
                                            break
                                    if check_for_historical_training_data:
                                        logger.info('checking for historical training data for %s - %s' % (base_name, ionosphere_data_dir))
                                        try:
                                            historical_data, ionosphere_data_dir = historical_data_dir_exists('webapp', ionosphere_data_dir)
                                            if historical_data:
                                                logger.info('using historical training data - %s' % ionosphere_data_dir)
                                                historical_training_data_exists = True
                                                valid_timestamp = True
                                            else:
                                                logger.info('no historical training data found for %s' % ionosphere_data_dir)
                                        except:
                                            trace = traceback.format_exc()
                                            logger.error(trace)
                                            fail_msg = 'error :: ionosphere_metric_data :: failed to determine whether this is historical training data'
                                            logger.error('%s' % fail_msg)
                                if not historical_training_data_exists:
                                    logger.info('%s=%s timestamp it to old to have training data' % (key, str(value)))
                                    resp = json.dumps(
                                        {'results': 'Error: timestamp too old no training data exists, training data has been purged'})
                            else:
                                if settings.REMOTE_SKYLINE_INSTANCES:
                                    logger.info('%s=%s no local training data dir found - %s will check other Skyline remote hosts' % (key, str(value), ionosphere_data_dir))
                                else:
                                    logger.error('error :: %s=%s no timestamp training data dir found - %s' % (key, str(value), ionosphere_data_dir))

                                # @added 20201127 - Feature #3824: get_cluster_data
                                #                   Feature #3820: HORIZON_SHARDS
                                # Determine which remote Skyline host the metric
                                # assigned to and return the client a redirect
                                # to the remote Skyline instance that will have
                                # the ionosphere training data for the metric
                                if settings.REMOTE_SKYLINE_INSTANCES:
                                    base_name = request.args.get('metric', 0)
                                    redis_metric_name = '%s%s' % (settings.FULL_NAMESPACE, str(base_name))
                                    # @added 20220112 - Bug #4374: webapp - handle url encoded chars
                                    # @modified 20220115 - Bug #4374: webapp - handle url encoded chars
                                    # Roll back change - breaking existing metrics with colons
                                    # redis_metric_name = url_encode_metric_name(redis_metric_name)

                                    metric_assigned_to = None
                                    logger.info('checking which remote Skyline instance is assigned - %s' % str(base_name))
                                    for item in settings.REMOTE_SKYLINE_INSTANCES:
                                        try:
                                            remote_unique_metrics = None
                                            try:
                                                remote_unique_metrics = get_cluster_data('unique_metrics', 'metrics', only_host=str(item[0]))
                                            except:
                                                logger.error(traceback.format_exc())
                                                logger.error('error :: Webapp could not get unique_metrics from the remote Skyline instance - %s' % str(item[0]))
                                            if remote_unique_metrics:
                                                logger.info('got %s unique_metrics from the remote Skyline instance - %s' % (
                                                    str(len(remote_unique_metrics)), str(item[0])))
                                                if redis_metric_name in remote_unique_metrics:
                                                    metric_assigned_to = str(item[0])
                                                    logger.info('found %s on %s' % (str(base_name), str(item[0])))
                                                    break
                                        except:
                                            logger.error(traceback.format_exc())
                                            logger.error('error :: checking which remote Skyline instance is assigned metric to check training_data')

                                    if not metric_assigned_to:
                                        logger.error('metric %s not found on any Skyline host' % str(base_name))
                                        resp = json.dumps(
                                            {'results': 'Error: no training data dir exists only any Skyline host - ' + ionosphere_data_dir + ' - go on... nothing here.'})
                                    else:
                                        # @modified 20201210 - Feature #3824: get_cluster_data
                                        #                      Feature #3820: HORIZON_SHARDS
                                        # alt_redirect_url = '%s/ionosphere?timestamp=%s&metric=%s' % (str(metric_assigned_to), str(value), str(base_name))
                                        url_protocol = 'http'
                                        if 'https' in metric_assigned_to:
                                            url_protocol = 'https'
                                        # @added 20201210 - Feature #3824: get_cluster_data
                                        # Handle authentication details passed
                                        # in the URL, badly as there is no other
                                        # way to handle it, other than badly,
                                        # not recommended, hence not adding to
                                        # settings, but sometimes necessary.
                                        if PASS_AUTH_ON_REDIRECT:
                                            url_auth = '%s:\/\/%s:%s' % (url_protocol, str(auth.username), str(auth.password))
                                        else:
                                            url_auth = '%s:\/\/' % (url_protocol)
                                        replace_string = '%s:\/\/' % url_protocol
                                        url_with_auth = metric_assigned_to.replace(replace_string, url_auth)
                                        # alt_redirect_url = '%s/ionosphere?' % (str(metric_assigned_to))
                                        alt_redirect_url = '%s/ionosphere?' % (str(url_with_auth))
                                        for key in request.args:
                                            value = request.args.get(key)
                                            new_alt_redirect_url = '%s&%s=%s' % (alt_redirect_url, str(key), str(value))
                                            alt_redirect_url = new_alt_redirect_url
                                        logger.info('redirecting client to - %s' % alt_redirect_url)
                                        return redirect(alt_redirect_url)

                                # @added 20180713 - Branch #2270: luminosity
                                # Use settings.ALTERNATIVE_SKYLINE_URLS if they are
                                # declared
                                try:
                                    use_alternative_urls = settings.ALTERNATIVE_SKYLINE_URLS
                                except:
                                    use_alternative_urls = False
                                if use_alternative_urls:
                                    base_name = request.args.get(str('metric'), None)
                                    alternative_urls = []
                                    for alt_url in use_alternative_urls:
                                        alt_redirect_url = '%s/ionosphere?timestamp=%s&metric=%s' % (str(alt_url), str(value), str(base_name))
                                        if len(use_alternative_urls) == 1:
                                            return redirect(alt_redirect_url)
                                        alternative_urls.append(alt_redirect_url)
                                    message = 'no training data dir exists on this Skyline instance try at the alternative URLS listed below:'
                                    logger.info('passing alternative_urls - %s' % str(alternative_urls))
                                    try:
                                        return render_template(
                                            'ionosphere.html', display_message=message,
                                            alternative_urls=alternative_urls,
                                            fp_view=True,
                                            version=skyline_version, duration=(time.time() - start),
                                            print_debug=True), 200
                                    except:
                                        message = 'Uh oh ... a Skyline 500 :('
                                        trace = traceback.format_exc()
                                        return internal_error(message, trace)
                                else:
                                    resp = json.dumps(
                                        {'results': 'Error: no training data dir exists - ' + ionosphere_data_dir + ' - go on... nothing here.'})

                    if not valid_timestamp:
                        # @modified 20190116 - Cross-Site Scripting Security Vulnerability #85
                        #                      Bug #2816: Cross-Site Scripting Security Vulnerability
                        # return resp, 404
                        return flask_escape(resp), 404

                if key == 'timestamp':
                    requested_timestamp = str(value)
                    timestamp_arg = True

                if key == 'timestamp_td':
                    timestamp_td_arg = True
                    requested_timestamp_td = str(value)
                    # determine_metric = True

                if key == 'requested_timestamp':
                    td_requested_timestamp = str(value)

                if key == 'metric' or key == 'metric_td':
                    try:
                        unique_metrics = list(REDIS_CONN.smembers(settings.FULL_NAMESPACE + 'unique_metrics'))
                    except:
                        logger.error('error :: Webapp could not get the unique_metrics list from Redis')
                        logger.info(traceback.format_exc())
                        return 'Internal Server Error', 500
                    metric_name = settings.FULL_NAMESPACE + str(value)
                    # @added 20220112 - Bug #4374: webapp - handle url encoded chars
                    # @modified 20220115 - Bug #4374: webapp - handle url encoded chars
                    # Roll back change - breaking existing metrics with colons
                    # metric_name = url_encode_metric_name(metric_name)

                    metric_found = False
                    if metric_name not in unique_metrics and settings.OTHER_SKYLINE_REDIS_INSTANCES:
                        # @modified 20180519 - Feature #2378: Add redis auth to Skyline and rebrow
                        # for redis_ip, redis_port in settings.OTHER_SKYLINE_REDIS_INSTANCES:
                        for redis_ip, redis_port, redis_password in settings.OTHER_SKYLINE_REDIS_INSTANCES:
                            other_unique_metrics = []
                            if not metric_found:
                                try:
                                    if redis_password:
                                        OTHER_REDIS_CONN = redis.StrictRedis(host=str(redis_ip), port=int(redis_port), password=str(redis_password))
                                    else:
                                        OTHER_REDIS_CONN = redis.StrictRedis(host=str(redis_ip), port=int(redis_port))
                                    other_unique_metrics = list(OTHER_REDIS_CONN.smembers(settings.FULL_NAMESPACE + 'unique_metrics'))
                                    logger.info('metric found in Redis at %s on port %s' % (str(redis_ip), str(redis_port)))
                                except:
                                    logger.error(traceback.format_exc())
                                    logger.error('error :: failed to connect to Redis at %s on port %s' % (str(redis_ip), str(redis_port)))
                            if metric_name in other_unique_metrics:
                                metric_found = True

                    # @added 20201127 - Feature #3824: get_cluster_data
                    #                   Feature #3820: HORIZON_SHARDS
                    # Change from the deprecated OTHER_SKYLINE_REDIS_INSTANCES
                    # to use the REMOTE_SKYLINE_INSTANCES and get_cluster_data
                    if settings.REMOTE_SKYLINE_INSTANCES:
                        remote_unique_metrics = None
                        try:
                            remote_unique_metrics = get_cluster_data('unique_metrics', 'metrics')
                        except:
                            logger.error(traceback.format_exc())
                            logger.error('error :: Webapp could not get unique_metrics from the remote Skyline instances')
                        if remote_unique_metrics:
                            logger.info('got %s remote unique_metrics from the remote Skyline instances' % str(len(remote_unique_metrics)))
                            unique_metrics_list = unique_metrics + remote_unique_metrics
                            unique_metrics = list(set(unique_metrics_list))

                    # if metric_name not in unique_metrics:
                    if metric_name not in unique_metrics and not metric_found:
                        # @added 20170917 - Bug #2158: webapp - redis metric check - existing but sparsely represented metrics
                        # If this is an fp_view=true, it means that either the
                        # metric is sparsely represented or no longer exists,
                        # but an fp exists so continue and do not 404
                        if fp_view:
                            logger.info('%s not in Redis, but fp passed so continuing' % metric_name)
                        # @added 20200715 - Task #3648: webapp - fp_search - do not use Redis metric check
                        # Do not use the Redis metric check result in a webapp
                        # fp_search request as this allows retried metric fps to
                        # accessed
                        elif fp_search_req:
                            logger.info('IONOSPHERE_REQUEST_ARGS check - %s not in Redis, but fp_search request so continuing' % metric_name)
                        # @added 20200808
                        elif saved_training_data:
                            logger.info('IONOSPHERE_REQUEST_ARGS check - %s not in Redis, but saved_training_data request so continuing' % metric_name)
                        else:
                            error_string = 'error :: no metric - %s - exists in Redis' % metric_name
                            logger.error(error_string)
                            resp = json.dumps(
                                {'results': error_string})
                            # @modified 20190116 - Cross-Site Scripting Security Vulnerability #85
                            #                      Bug #2816: Cross-Site Scripting Security Vulnerability
                            # return resp, 404
                            return flask_escape(resp), 404

                if key == 'metric':
                    metric_arg = True

                if key == 'metric_td':
                    metric_td_arg = True

                if metric_arg or metric_td_arg:
                    if key == 'metric' or key == 'metric_td':
                        base_name = str(value)
                        # @added 20220112 - Bug #4374: webapp - handle url encoded chars
                        # @modified 20220115 - Bug #4374: webapp - handle url encoded chars
                        # Roll back change - breaking existing metrics with colons
                        # base_name = url_encode_metric_name(base_name)

                # @added 20180804 - Feature #2488: Allow user to specifically set metric as a derivative metric in training_data
                if timestamp_arg and metric_arg:
                    if key == 'load_derivative_graphs':
                        if str(value) == 'true':
                            set_derivative_metric = set_metric_as_derivative(skyline_app, base_name)
                            # @added 20180918 - Feature #2488: Allow user to specifically set metric as a derivative metric in training_data
                            # Remove any graphite_now png files that are present
                            # so that the webapp recreates the pngs as
                            # nonNegativeDerivative graphs.
                            # TODO - handle caching
                            try:
                                timeseries_dir = base_name.replace('.', '/')
                                ionosphere_data_dir = '%s/%s/%s' % (
                                    settings.IONOSPHERE_DATA_FOLDER,
                                    requested_timestamp, timeseries_dir)
                                pattern = 'graphite_now'
                                for f in os.listdir(ionosphere_data_dir):
                                    if re.search(pattern, f):
                                        remove_graphite_now_file = os.path.join(ionosphere_data_dir, f)
                                        os.remove(remove_graphite_now_file)
                                        logger.info('removed graphite_now image at user request - %s' % remove_graphite_now_file)
                            except:
                                logger.error('failed to remove graphite_now images')
                if set_derivative_metric:
                    return_url = '%s/ionosphere?timestamp=%s&metric=%s' % (str(settings.SKYLINE_URL), str(requested_timestamp), str(base_name))
                    return redirect(return_url)

                if timestamp_arg and metric_arg:
                    timeseries_dir = base_name.replace('.', '/')

                    if not fp_view:
                        ionosphere_data_dir = '%s/%s/%s' % (
                            settings.IONOSPHERE_DATA_FOLDER,
                            requested_timestamp, timeseries_dir)

                        # @added 20170617 - Feature #2054: ionosphere.save.training_data
                        if saved_training_data:
                            ionosphere_data_dir = '%s_saved/%s/%s' % (
                                settings.IONOSPHERE_DATA_FOLDER,
                                requested_timestamp, timeseries_dir)

                        # @added 20200813 - Feature #3670: IONOSPHERE_CUSTOM_KEEP_TRAINING_TIMESERIES_FOR
                        historical_training_data_exists = False
                        # @added 20200814 - Feature #3670: IONOSPHERE_CUSTOM_KEEP_TRAINING_TIMESERIES_FOR
                        training_data_dir_found = False

                        if not isdir(ionosphere_data_dir):

                            # @added 20200813 - Feature #3670: IONOSPHERE_CUSTOM_KEEP_TRAINING_TIMESERIES_FOR
                            historical_training_data_exists = False
                            if IONOSPHERE_CUSTOM_KEEP_TRAINING_TIMESERIES_FOR:
                                logger.info('checking for historical training data for %s' % ionosphere_data_dir)
                                try:
                                    historical_data, ionosphere_data_dir = historical_data_dir_exists('webapp', ionosphere_data_dir)
                                    if historical_data:
                                        logger.info('using historical training data - %s' % ionosphere_data_dir)
                                        historical_training_data_exists = True
                                        valid_timestamp = True
                                        # @added 20200814 - Feature #3670: IONOSPHERE_CUSTOM_KEEP_TRAINING_TIMESERIES_FOR
                                        training_data_dir_found = True
                                    else:
                                        logger.info('no historical training data found for %s' % ionosphere_data_dir)
                                except:
                                    trace = traceback.format_exc()
                                    logger.error(trace)
                                    fail_msg = 'error :: ionosphere_metric_data :: failed to determine whether this is historical training data'
                                    logger.error('%s' % fail_msg)
                        else:
                            # @added 20200814 - Feature #3670: IONOSPHERE_CUSTOM_KEEP_TRAINING_TIMESERIES_FOR
                            training_data_dir_found = True

                        # @added 20201127 - Feature #3824: get_cluster_data
                        #                   Feature #3820: HORIZON_SHARDS
                        # Determine which remote Skyline host the metric
                        # assigned to
                        if settings.REMOTE_SKYLINE_INSTANCES and not training_data_dir_found:
                            base_name = request.args.get('metric', 0)
                            redis_metric_name = '%s%s' % (settings.FULL_NAMESPACE, str(base_name))
                            # @added 20220112 - Bug #4374: webapp - handle url encoded chars
                            # @modified 20220115 - Bug #4374: webapp - handle url encoded chars
                            # Roll back change - breaking existing metrics with colons
                            # redis_metric_name = url_encode_metric_name(redis_metric_name)

                            metric_assigned_to = None
                            logger.info('checking which remote Skyline instance is assigned - %s' % str(base_name))
                            for item in settings.REMOTE_SKYLINE_INSTANCES:
                                try:
                                    remote_unique_metrics = None
                                    try:
                                        remote_unique_metrics = get_cluster_data('unique_metrics', 'metrics', only_host=str(item[0]))
                                    except:
                                        logger.error(traceback.format_exc())
                                        logger.error('error :: Webapp could not get unique_metrics from the remote Skyline instance - %s' % str(item[0]))
                                    if remote_unique_metrics:
                                        logger.info('got %s unique_metrics from the remote Skyline instance - %s' % (
                                            str(len(remote_unique_metrics)), str(item[0])))
                                        if redis_metric_name in remote_unique_metrics:
                                            metric_assigned_to = str(item[0])
                                            logger.info('found %s on %s' % (str(base_name), str(item[0])))
                                            break
                                except:
                                    logger.error(traceback.format_exc())
                                    logger.error('error :: checking which remote Skyline instance is assigned metric to check training_data')

                            if not metric_assigned_to:
                                logger.error('metric %s not found on any Skyline host' % str(base_name))
                            else:
                                # @modified 20201210 - Feature #3824: get_cluster_data
                                #                      Feature #3820: HORIZON_SHARDS
                                # alt_redirect_url = '%s/ionosphere?timestamp=%s&metric=%s' % (str(metric_assigned_to), str(value), str(base_name))
                                url_protocol = 'http'
                                if 'https' in metric_assigned_to:
                                    url_protocol = 'https'
                                if PASS_AUTH_ON_REDIRECT:
                                    url_auth = '%s://%s:%s@' % (url_protocol, str(auth.username), str(auth.password))
                                else:
                                    url_auth = '%s:\/\/' % (url_protocol)
                                replace_string = '%s://' % url_protocol
                                url_with_auth = metric_assigned_to.replace(replace_string, url_auth)
                                # alt_redirect_url = '%s/ionosphere?' % (str(metric_assigned_to))
                                alt_redirect_url = '%s/ionosphere?' % (str(url_with_auth))
                                for key in request.args:
                                    value = request.args.get(key)
                                    new_alt_redirect_url = '%s&%s=%s' % (alt_redirect_url, str(key), str(value))
                                    alt_redirect_url = new_alt_redirect_url
                                logger.info('redirecting client to - %s' % alt_redirect_url)
                                return redirect(alt_redirect_url)

                        # @added 20200814 - Feature #3670: IONOSPHERE_CUSTOM_KEEP_TRAINING_TIMESERIES_FOR
                        # if not historical_training_data_exists:
                        if not training_data_dir_found:
                            logger.info(
                                '%s=%s no timestamp metric training data dir found - %s' %
                                (key, str(value), ionosphere_data_dir))
                            resp = json.dumps(
                                {'results': 'Error: no training data dir exists - ' + ionosphere_data_dir + ' - go on... nothing here.'})
                            # @modified 20190116 - Cross-Site Scripting Security Vulnerability #85
                            #                      Bug #2816: Cross-Site Scripting Security Vulnerability
                            # return resp, 404
                            return flask_escape(resp), 404
                    else:
                        ionosphere_profiles_dir = '%s/%s/%s' % (
                            settings.IONOSPHERE_PROFILES_FOLDER, timeseries_dir,
                            requested_timestamp)

                        if not isdir(ionosphere_profiles_dir):
                            logger.info(
                                '%s=%s no timestamp metric features profile dir found - %s' %
                                (key, str(value), ionosphere_profiles_dir))

                            # @added 20180715 - Branch #2270: luminosity
                            # Use settings.ALTERNATIVE_SKYLINE_URLS if they are
                            # declared and redirect to alternative URL/s if no
                            # features profile directory exists on the Skyline
                            # instance.
                            try:
                                use_alternative_urls = settings.ALTERNATIVE_SKYLINE_URLS
                            except:
                                use_alternative_urls = False
                            if use_alternative_urls:
                                base_name = request.args.get(str('metric'), None)
                                alternative_urls = []
                                for alt_url in use_alternative_urls:
                                    alt_redirect_url_base = '%s/ionosphere' % str(alt_url)
                                    request_url = str(request.url)
                                    request_endpoint = '%s/ionosphere' % str(settings.SKYLINE_URL)
                                    alt_redirect_url = request_url.replace(request_endpoint, alt_redirect_url_base, 1)
                                    if len(use_alternative_urls) == 1:
                                        logger.info('redirecting to %s' % str(alt_redirect_url))
                                        return redirect(alt_redirect_url)
                                    alternative_urls.append(alt_redirect_url)
                                message = 'no features profile dir exists on this Skyline instance try at the alternative URLS listed below:'
                                logger.info('passing alternative_urls - %s' % str(alternative_urls))
                                try:
                                    return render_template(
                                        'ionosphere.html', display_message=message,
                                        alternative_urls=alternative_urls,
                                        fp_view=True,
                                        version=skyline_version, duration=(time.time() - start),
                                        print_debug=True), 200
                                except:
                                    message = 'Uh oh ... a Skyline 500 :('
                                    trace = traceback.format_exc()
                                    return internal_error(message, trace)
                            else:
                                resp = json.dumps(
                                    {'results': 'Error: no features profile dir exists - ' + ionosphere_profiles_dir + ' - go on... nothing here.'})
                                # @modified 20190116 - Cross-Site Scripting Security Vulnerability #85
                                #                      Bug #2816: Cross-Site Scripting Security Vulnerability
                                # return resp, 404
                                return flask_escape(resp), 404

        logger.info('arguments validated - OK')

    except:
        message = 'Uh oh ... a Skyline 500 :('
        trace = traceback.format_exc()
        return internal_error(message, trace)

    debug_on = False

    if fp_view:
        context = 'features_profiles'
    else:
        context = 'training_data'

    # @added 20170617 - Feature #2054: ionosphere.save.training_data
    if saved_training_data:
        context = 'saved_training_data'

    fp_view_on = fp_view

    do_first = False
    if fp_view:
        do_first = True
        args = [
            'timestamp', 'metric', 'metric_td', 'timestamp_td',
            'requested_timestamp', 'features_profiles']
        for i_arg in args:
            if i_arg in request.args:
                do_first = False

    if request_args_len == 0 or dated_list:
        do_first = True

    if do_first:
        listed_by = 'metric'
        if dated_list:
            listed_by = 'date'
        try:
            mpaths, unique_m, unique_ts, hdates = ionosphere_data(False, 'all', context)
            return render_template(
                'ionosphere.html', unique_metrics=unique_m, list_by=listed_by,
                unique_timestamps=unique_ts, human_dates=hdates,
                metric_td_dirs=zip(unique_ts, hdates), td_files=mpaths,
                requested_timestamp=td_requested_timestamp, fp_view=fp_view_on,
                matched_from_datetime=matched_from_datetime,
                version=skyline_version, duration=(time.time() - start),
                print_debug=debug_on), 200
        except:
            message = 'Uh oh ... a Skyline 500 :('
            trace = traceback.format_exc()
            return internal_error(message, trace)

    # @added 20170118 - Feature #1862: Ionosphere features profiles search page
    # Added fp_search parameter
    fp_search_param = None
    if fp_search:
        logger.debug('debug :: fp_search was True')
        fp_search_param = True
        listed_by = 'search'
        # get_options = [
        #     'full_duration', 'enabled', 'tsfresh_version', 'generation']
        fd_list = None
        try:
            # @modified 20170221 - Feature #1862: Ionosphere features profiles search page
            # fd_list, en_list, tsfresh_list, gen_list, fail_msg, trace = ionosphere_search_defaults(get_options)
            # @modified 20200715 - Task #3648: webapp - fp_search - do not use Redis metric check
            #                      Task #2446: Optimize Ionosphere
            # Added missing search_success variable
            # features_profiles, fp_count, mc, cc, gc, fd_list, en_list, tsfresh_list, gen_list, fail_msg, trace = ionosphere_search(True, False)
            features_profiles, fp_count, mc, cc, gc, fd_list, en_list, tsfresh_list, gen_list, search_success, fail_msg, trace = ionosphere_search(True, False)
            logger.debug('debug :: fd_list - %s' % str(fd_list))
        except:
            message = 'Uh oh ... a Skyline 500 :('
            trace = traceback.format_exc()
            return internal_error(message, trace)

        if fd_list:
            try:
                return render_template(
                    'ionosphere.html', list_by=listed_by, fp_search=fp_search_param,
                    full_duration_list=fd_list, enabled_list=en_list,
                    tsfresh_version_list=tsfresh_list, generation_list=gen_list,
                    matched_from_datetime=matched_from_datetime,
                    version=skyline_version, duration=(time.time() - start),
                    print_debug=debug_on), 200
            except:
                message = 'Uh oh ... a Skyline 500 :('
                trace = traceback.format_exc()
                return internal_error(message, trace)

    if metric_td_arg:
        listed_by = 'metric_td_dirs'
        try:
            mpaths, unique_m, unique_ts, hdates = ionosphere_data(False, base_name, context)
            return render_template(
                'ionosphere.html', metric_td_dirs=zip(unique_ts, hdates),
                list_by=listed_by, for_metric=base_name, td_files=mpaths,
                requested_timestamp=td_requested_timestamp, fp_view=fp_view_on,
                matched_from_datetime=matched_from_datetime,
                version=skyline_version, duration=(time.time() - start),
                print_debug=debug_on), 200
        except:
            message = 'Uh oh ... a Skyline 500 :('
            trace = traceback.format_exc()
            return internal_error(message, trace)

    if timestamp_td_arg:
        # Note to self.  Can we carry the referring timestamp through and when
        # a metric is selected the time is the only one wrapped in <code> .e.g red?
        listed_by = 'timestamp_td_dirs'
        try:
            mpaths, unique_m, unique_ts, hdates = ionosphere_get_metrics_dir(requested_timestamp_td, context)
            return render_template(
                'ionosphere.html', unique_metrics=unique_m, list_by=listed_by,
                unique_timestamps=unique_ts, human_dates=hdates, td_files=mpaths,
                metric_td_dirs=zip(unique_ts, hdates),
                requested_timestamp=td_requested_timestamp, fp_view=fp_view_on,
                matched_from_datetime=matched_from_datetime,
                version=skyline_version, duration=(time.time() - start),
                print_debug=debug_on), 200
        except:
            message = 'Uh oh ... a Skyline 500 :('
            trace = traceback.format_exc()
            return internal_error(message, trace)

    if timestamp_arg and metric_arg:
        try:
            # @modified 20170104 - Feature #1842: Ionosphere - Graphite now graphs
            # Added the full_duration_in_hours and changed graph color from blue to orange
            # full_duration_in_hours
            # GRAPH_URL = GRAPHITE_PROTOCOL + '://' + GRAPHITE_HOST + ':' + GRAPHITE_PORT + '/render/?width=1400&from=-' + TARGET_HOURS + 'hour&target='
            # A regex is required to change the TARGET_HOURS, no? extend do not modify?
            # Not certain will review after Dude morning excersion

            # @modified 20200417 - Task #3294: py3 - handle system parameter in Graphite cactiStyle
            # graph_url = '%scactiStyle(%s)%s&colorList=blue' % (
            graph_url = '%scactiStyle(%s,%%27si%%27)%s&colorList=blue' % (
                settings.GRAPH_URL, base_name, settings.GRAPHITE_GRAPH_SETTINGS)
        except:
            graph_url = False

        # @added 20170604 - Feature #2034: analyse_derivatives
        # Added nonNegativeDerivative to strictly
        # increasing monotonically metrics in graph_url
        known_derivative_metric = False
        try:
            # @modified 20211012 - Feature #4280: aet.metrics_manager.derivative_metrics Redis hash
            # derivative_metrics = list(REDIS_CONN.smembers('derivative_metrics'))
            derivative_metrics = list(REDIS_CONN.smembers('aet.metrics_manager.derivative_metrics'))
        except:
            derivative_metrics = []
        redis_metric_name = '%s%s' % (settings.FULL_NAMESPACE, str(base_name))
        # @added 20220112 - Bug #4374: webapp - handle url encoded chars
        # @modified 20220115 - Bug #4374: webapp - handle url encoded chars
        # Roll back change - breaking existing metrics with colons
        # redis_metric_name = url_encode_metric_name(redis_metric_name)

        if redis_metric_name in derivative_metrics:
            known_derivative_metric = True
        if known_derivative_metric:
            try:
                non_derivative_metrics = list(REDIS_CONN.smembers('non_derivative_metrics'))
            except:
                non_derivative_metrics = []
            skip_derivative = in_list(redis_metric_name, non_derivative_metrics)
            if skip_derivative:
                known_derivative_metric = False
        if known_derivative_metric:
            try:
                # @modified 20200417 - Task #3294: py3 - handle system parameter in Graphite cactiStyle
                # graph_url = '%scactiStyle(nonNegativeDerivative(%s))%s&colorList=blue' % (
                graph_url = '%scactiStyle(nonNegativeDerivative(%s),%%27si%%27)%s&colorList=blue' % (
                    settings.GRAPH_URL, base_name, settings.GRAPHITE_GRAPH_SETTINGS)
            except:
                graph_url = False

        # @added 20170327 - Feature #2004: Ionosphere layers - edit_layers
        #                   Task #2002: Review and correct incorrectly defined layers
        layers_updated = False
        if 'edit_fp_layers' in request.args:
            edit_fp_layers_arg = request.args.get('edit_fp_layers', False)
            if edit_fp_layers_arg == 'true':
                edit_fp_layers = True
                logger.info('editing layers id - %s' % str(layers_id))
            else:
                logger.info('not editing layers id - %s' % str(layers_id))

        if edit_fp_layers:
            logger.info('editing layers id - %s' % str(layers_id))
            try:
                layers_updated, fail_msg, traceback_format_exc = edit_ionosphere_layers(layers_id)
                logger.info('updated layers id - %s' % str(layers_id))
            except:
                trace = traceback.format_exc()
                message = 'failed to update layer calling edit_ionosphere_layers'
                return internal_error(message, trace)
            if not layers_updated:
                trace = 'none'
                message = 'failed to update layer'
                return internal_error(message, trace)
        else:
            logger.info('not editing layers')

        features = None
        f_calc = 'none'
        fp_exists = False
        fp_id = None

        if calculate_features or create_feature_profile or fp_view:
            try:
                fp_csv, successful, fp_exists, fp_id, fail_msg, traceback_format_exc, f_calc = calculate_features_profile(skyline_app, requested_timestamp, base_name, context)
            except:
                trace = traceback.format_exc()
                message = 'failed to calculate features'
                return internal_error(message, trace)

            if not successful:
                return internal_error(fail_msg, traceback_format_exc)

            if os.path.isfile(str(fp_csv)):
                features = []
                try:
                    with open(fp_csv, 'rb') as fr:
                        # @modified 20191029 - Task #3302: Handle csv.reader in py3
                        #                      Branch #3262: py3
                        # reader = csv.reader(fr, delimiter=',')
                        if python_version == 2:
                            reader = csv.reader(fr, delimiter=',')
                        else:
                            reader = csv.reader(codecs.iterdecode(fr, 'utf-8'), delimiter=',')
                        for i, line in enumerate(reader):
                            features.append([str(line[0]), str(line[1])])
                except:
                    trace = traceback.format_exc()
                    message = 'failed to read csv features from fp_csv - %s' % str(fp_csv)
                    return internal_error(message, trace)

        generation_zero = False
        if create_feature_profile or fp_view:
            if create_feature_profile:
                # Submit to Ionosphere to run tsfresh on
                create_feature_profile = True
            if not fp_id:
                # @modified 20170114 -  Feature #1854: Ionosphere learn - generations
                # Added parent_id and generation as all features profiles that
                # are created via the UI will be generation 0
                parent_id = 0
                generation = 0
                ionosphere_job = 'learn_fp_human'
                # @added 20190503 - Branch #2646: slack
                # Added slack_ionosphere_job
                slack_ionosphere_job = ionosphere_job

                # @added 20200216 - Feature #3450: Handle multiple requests to create a features profile
                # Ensure that one features profile can only be created if
                # multiple requests are received to create a features profile
                fp_pending = None
                try:
                    cache_key = 'fp_pending.%s.%s.%s' % (
                        str(skyline_app), str(requested_timestamp), str(base_name))
                    fp_pending = REDIS_CONN.get(cache_key)
                except:
                    trace = traceback.format_exc()
                    message = 'failed to determine if a features profile is pending'
                    return internal_error(message, trace)
                if create_feature_profile and fp_pending:
                    logger.info('fp pending for - %s on %s' % (
                        str(requested_timestamp), str(base_name)))
                    if response_format == 'json':
                        data_dict = {"status": {"created": "pending"}, "data": {"fp_id": 0}}
                        return jsonify(data_dict), 200
                    resp = json.dumps(
                        {'results': 'Notice: a features profile is already being created for ' + str(requested_timestamp) + ' - ' + str(base_name) + ' - please click the back button and refresh the page'})
                    return flask_escape(resp), 200

                try:
                    # @modified 20170120 -  Feature #1854: Ionosphere learn - generations
                    # Added fp_learn parameter to allow the user to not learn the
                    # use_full_duration_days
                    # fp_id, fp_in_successful, fp_exists, fail_msg, traceback_format_exc = create_features_profile(skyline_app, requested_timestamp, base_name, context, ionosphere_job, parent_id, generation, fp_learn)
                    # @modified 20190503 - Branch #2646: slack
                    # Added slack_ionosphere_job
                    # fp_id, fp_in_successful, fp_exists, fail_msg, traceback_format_exc = create_features_profile(skyline_app, requested_timestamp, base_name, context, ionosphere_job, parent_id, generation, fp_learn)
                    # @modified 20190919 - Feature #3230: users DB table
                    #                      Ideas #2476: Label and relate anomalies
                    #                      Feature #2516: Add label to features profile
                    # Added user_id and label
                    # fp_id, fp_in_successful, fp_exists, fail_msg, traceback_format_exc = create_features_profile(skyline_app, requested_timestamp, base_name, context, ionosphere_job, parent_id, generation, fp_learn, slack_ionosphere_job)
                    fp_id, fp_in_successful, fp_exists, fail_msg, traceback_format_exc = create_features_profile(skyline_app, requested_timestamp, base_name, context, ionosphere_job, parent_id, generation, fp_learn, slack_ionosphere_job, user_id, label)
                    if create_feature_profile:
                        generation_zero = True
                except:
                    # @modified 20161209 -  - Branch #922: ionosphere
                    #                        Task #1658: Patterning Skyline Ionosphere
                    # Use raise and traceback.format_exc() to carry
                    # ionosphere_backend.py through to the rendered page for the user, e.g
                    # me.
                    # trace = traceback_format_exc
                    trace = traceback.format_exc()
                    message = 'failed to create features profile'
                    return internal_error(message, trace)
                if not fp_in_successful:
                    trace = traceback.format_exc()
                    fail_msg = 'error :: create_features_profile failed'
                    return internal_error(fail_msg, 'no traceback available')

        fp_details = None
        fp_details_object = None

        # @added 20170305  - Feature #1960: ionosphere_layers
        l_id = None
        l_details = None
        l_details_object = False
        la_details = None

        # @added 20170402 - Feature #2000: Ionosphere - validated
        validated_fp_success = False

        # added 20170908 - Feature #2056: ionosphere - disabled_features_profiles
        family_tree_fp_ids = None
        disabled_fp_success = None

        if fp_view:
            # @added 20170402 - Feature #2000: Ionosphere - validated
            validate = False
            if 'validate_fp' in request.args:
                validate_arg = request.args.get('validate_fp', False)
                if validate_arg == 'true':
                    validate = True
                    logger.info('validate - %s' % str(validate))
            if validate:
                logger.info('validating - fp_ip %s' % str(fp_id))
                try:
                    # @modified 20181013 - Feature #2430: Ionosphere validate learnt features profiles page
                    # Added the extended validate_fp parameter of id_column_name
                    # validated_fp_success, fail_msg, traceback_format_exc = validate_fp(fp_id)
                    # @modified 20190919 - Feature #3230: users DB table
                    #                      Ideas #2476: Label and relate anomalies
                    #                      Feature #2516: Add label to features profile
                    # Added user_id
                    # validated_fp_success, fail_msg, traceback_format_exc = validate_fp(fp_id, 'id')
                    validated_fp_success, fail_msg, traceback_format_exc = validate_fp(fp_id, 'id', user_id)
                    logger.info('validated fp_id - %s' % str(fp_id))
                except:
                    trace = traceback.format_exc()
                    message = 'failed to validate features profile'
                    return internal_error(message, trace)

            # added 20170908 - Feature #2056: ionosphere - disabled_features_profiles
            family_tree_fp_ids, fail_msg, traceback_format_exc = features_profile_family_tree(fp_id)
            if 'disable_fp' in request.args:
                value = request.args.get(str('disable_fp'), None)
                if int(value) > 1:
                    disable_fp = int(value)
                    logger.info('disable_fp is set to %s' % str(disable_fp))
            if disable_fp:
                logger.info('disabling fp ids - %s' % str(family_tree_fp_ids))
                disabled_fp_success, fail_msg, traceback_format_exc = disable_features_profile_family_tree(family_tree_fp_ids)

            try:
                # @modified 20170114 -  Feature #1854: Ionosphere learn - generations
                # Return the fp_details_object so that webapp can pass the parent_id and
                # generation to the templates
                # fp_details, fp_details_successful, fail_msg, traceback_format_exc = features_profile_details(fp_id)
                fp_details, fp_details_successful, fail_msg, traceback_format_exc, fp_details_object = features_profile_details(fp_id)
            except:
                # trace = traceback_format_exc
                trace = traceback.format_exc()
                message = 'failed to get features profile details'
                return internal_error(message, trace)
            if not fp_details_successful:
                trace = traceback.format_exc()
                fail_msg = 'error :: features_profile_details failed'
                return internal_error(fail_msg, trace)

            # @added 20170305  - Feature #1960: ionosphere_layers
            fp_layers_id = None
            try:
                fp_layers_id = int(fp_details_object['layers_id'])
            except:
                fp_layers_id = 0

            # @added 20190922 - Feature #2516: Add label to features profile
            try:
                fp_label = fp_details_object['label']
            except:
                fp_label = None

            # @modified 20190503 - Branch #2646: slack - linting
            # layer_details = None

            layer_details_success = False
            if fp_layers_id:
                try:
                    l_details, layer_details_success, fail_msg, traceback_format_exc, l_details_object = feature_profile_layers_detail(fp_layers_id)
                except:
                    trace = traceback.format_exc()
                    message = 'failed to get features profile layers details for id %s' % str(fp_layers_id)
                    return internal_error(message, trace)
                try:
                    la_details, layer_algorithms_success, fail_msg, traceback_format_exc, la_details_object = feature_profile_layer_alogrithms(fp_layers_id)
                    l_id = fp_layers_id
                except:
                    trace = traceback.format_exc()
                    message = 'failed to get features profile layer algorithm details for id %s' % str(fp_layers_id)
                    return internal_error(message, trace)

        valid_learning_duration = None

        # @added 20170308  - Feature #1960: ionosphere_layers - glm_images to m_app_context
        glm_images = None
        l_id_matched = None
        m_app_context = 'Analyzer'
        # @added 20170309  - Feature #1960: ionosphere_layers - i_ts_json
        i_ts_json = None
        sample_ts_json = None
        sample_i_ts_json = None

        # @added 20170331 - Task #1988: Review - Ionosphere layers - always show layers
        #                   Feature #1960: ionosphere_layers
        anomalous_timeseries = None
        f_id_matched = None
        fp_details_list = None
        f_id_created = None
        fp_generation_created = None

        try:
            # @modified 20170106 - Feature #1842: Ionosphere - Graphite now graphs
            # Added graphite_now_images gimages
            # @modified 20170107 - Feature #1852: Ionosphere - features_profile matched graphite graphs
            # Added graphite_matched_images gmimages
            # @modified 20170308 - Feature #1960: ionosphere_layers
            # Show the latest matched layers graphs as well added glm_images - graphite_layers_matched_images
            # @modified 20170309 - Feature #1960: ionosphere_layers
            # Also return the Analyzer FULL_DURATION timeseries if available in a Mirage
            # based features profile added i_ts_json
            # @added 20170331 - Task #1988: Review - Ionosphere layers - always show layers
            #                   Feature #1960: ionosphere_layers
            # Return the anomalous_timeseries as an array to sample and fp_id_matched
            # @added 20170401 - Task #1988: Review - Ionosphere layers - added fp_id_created
            # @added 20190328 - Feature #2484: FULL_DURATION feature profiles
            # Added fp_anomaly_timestamp ionosphere_echo features profiles
            mpaths, images, hdate, m_vars, ts_json, data_to_process, p_id, gimages, gmimages, times_matched, glm_images, l_id_matched, ts_fd, i_ts_json, anomalous_timeseries, f_id_matched, fp_details_list, fp_anomaly_timestamp = ionosphere_metric_data(requested_timestamp, base_name, context, fp_id)

            # @added 20200711 - Feature #3634: webapp - ionosphere - report number of data points
            ts_json_length = 0
            anomalous_timeseries_length = 0

            # @added 20170309  - Feature #1960: ionosphere_layers - i_ts_json
            # Show the last 30
            if ts_json:
                try:
                    sample_ts_json = ts_json[-30:]
                    # @added 20200711 - Feature #3634: webapp - ionosphere - report number of data points
                    ts_json_length = len(ts_json)
                except:
                    trace = traceback.format_exc()
                    message = 'Failed to sample ts_json'
                    return internal_error(message, trace)

            # @modified 20170331 - Task #1988: Review - Ionosphere layers - always show layers
            #                      Feature #1960: ionosphere_layers
            # Return the anomalous_timeseries as an array to sample
            # if i_ts_json:
            #     sample_i_ts_json = i_ts_json[-30:]
            if anomalous_timeseries:
                sample_i_ts_json = anomalous_timeseries[-30:]
                # @added 20200711 - Feature #3634: webapp - ionosphere - report number of data points
                anomalous_timeseries_length = len(anomalous_timeseries)

            if fp_details_list:
                f_id_created = fp_details_list[0]
                # @modified 20170729 - Feature #1854: Ionosphere learn - generations
                # Make backwards compatible with older features profiles
                # fp_generation_created = fp_details_list[8]
                try:
                    fp_generation_created = fp_details_list[8]
                except:
                    fp_generation_created = 0

            # @added 20170120 -  Feature #1854: Ionosphere learn - generations
            # Added fp_learn parameter to allow the user to not learn the
            # use_full_duration_days so added fp_fd_days
            use_full_duration, valid_learning_duration, fp_fd_days, max_generations, max_percent_diff_from_origin = get_ionosphere_learn_details(skyline_app, base_name)

            # @added 20170104 - Feature #1842: Ionosphere - Graphite now graphs
            # Added the full_duration parameter so that the appropriate graphs can be
            # embedded for the user in the training data page
            full_duration = settings.FULL_DURATION
            full_duration_in_hours = int(full_duration / 3600)
            second_order_resolution_hours = False
            try:
                key = 'full_duration'
                value_list = [var_array[1] for var_array in m_vars if var_array[0] == key]
                m_full_duration = int(value_list[0])
                m_full_duration_in_hours = int(m_full_duration / 3600)
                if m_full_duration != full_duration:
                    second_order_resolution_hours = m_full_duration_in_hours
                    # @added 20170305  - Feature #1960: ionosphere_layers - m_app_context
                    m_app_context = 'Mirage'
            except:
                m_full_duration = False
                m_full_duration_in_hours = False
                message = 'Uh oh ... a Skyline 500 :( :: m_vars - %s' % str(m_vars)
                trace = traceback.format_exc()
                return internal_error(message, trace)

            # @added 20190330 - Feature #2484: FULL_DURATION feature profiles
            # For Ionosphere echo and adding red borders on the matched graphs
            if m_full_duration_in_hours:
                m_fd_in_hours_img_str = '%sh.png' % str(m_full_duration_in_hours)
            else:
                m_fd_in_hours_img_str = False

            # @added 20170105 - Feature #1842: Ionosphere - Graphite now graphs
            # We want to sort the images so that the Graphite image is always
            # displayed first in he training_data.html page AND we want Graphite
            # now graphs at TARGET_HOURS, 24h, 7d, 30d to inform the operator
            # about the metric
            sorted_images = sorted(images)

            # @modified 20170105 - Feature #1842: Ionosphere - Graphite now graphs
            # Added matched_count and only displaying one graph for each 10
            # minute period if there are mulitple matches in a 10 minute period
            # @modified 20170114 -  Feature #1854: Ionosphere learn - generations
            # Added parent_id and generation
            par_id = 0
            gen = 0
            # @added 20170402 - Feature #2000: Ionosphere - validated
            fp_validated = 0

            # added 20170908 - Feature #2056: ionosphere - disabled_features_profiles
            fp_enabled = False

            # @added 20190328 - Feature #2484: FULL_DURATION feature profiles
            # Added ionosphere_echo
            echo_fp_value = 0

            # @added 20190619 - Feature #2990: Add metrics id to relevant web pages
            metric_id = False

            # Determine the parent_id and generation as they were added to the
            # fp_details_object
            # if fp_details:
            if fp_details_object:
                try:
                    par_id = int(fp_details_object['parent_id'])
                    gen = int(fp_details_object['generation'])
                    # @added 20170402 - Feature #2000: Ionosphere - validated
                    fp_validated = int(fp_details_object['validated'])
                    # added 20170908 - Feature #2056: ionosphere - disabled_features_profiles
                    if int(fp_details_object['enabled']) == 1:
                        fp_enabled = True
                    # @added 20190328 - Feature #2484: FULL_DURATION feature profiles
                    # Added ionosphere_echo
                    try:
                        echo_fp_value = int(fp_details_object['echo_fp'])
                    except:
                        trace = traceback.format_exc()
                        fail_msg = 'error :: Webapp failed to determine the echo_hdate from the fp_anomaly_timestamp'
                        logger.error(fail_msg)
                        pass

                    # @added 20190619 - Feature #2990: Add metrics id to relevant web pages
                    # Determine the metric_id from the fp_details_object
                    if not metric_id:
                        metric_id = int(fp_details_object['metric_id'])

                except:
                    trace = traceback.format_exc()
                    message = 'Uh oh ... a Skyline 500 :( :: failed to determine parent or generation values from the fp_details_object'
                    return internal_error(message, trace)

                # @added 20190328 - Feature #2484: FULL_DURATION feature profiles
                # Added ionosphere_echo
                echo_hdate = hdate
                if echo_fp_value == 1:
                    try:
                        # echo_hdate = datetime.datetime.utcfromtimestamp(fp_anomaly_timestamp).strftime('%Y-%m-%d %H:%M:%S')
                        echo_hdate = time.strftime('%Y-%m-%d %H:%M:%S %Z (%A)', time.localtime(int(fp_anomaly_timestamp)))
                    except:
                        trace = traceback.format_exc()
                        fail_msg = 'error :: Webapp failed to determine the echo_hdate from the fp_anomaly_timestamp'
                        logger.error(fail_msg)
                        return internal_error(fail_msg, trace)

            # @added 20170114 -  Feature #1854: Ionosphere learn - generations
            # The fp_id will be in the fp_details_object, but if this is a
            # generation zero features profile we what set
            if generation_zero:
                par_id = 0
                gen = 0

            # @added 20170122 - Feature #1876: Ionosphere - training_data learn countdown
            # Add a countdown until Ionosphere will learn
            countdown_to = False
            if requested_timestamp and valid_learning_duration:
                try:
                    request_time = int(time.time())
                    vaild_learning_timestamp = int(requested_timestamp) + int(valid_learning_duration)
                    if request_time < vaild_learning_timestamp:
                        countdown_to = time.strftime(
                            '%Y-%m-%d %H:%M:%S', time.localtime(vaild_learning_timestamp))
                except:
                    trace = traceback.format_exc()
                    message = 'Uh oh ... a Skyline 500 :( :: failed to determine parent or generation values from the fp_details_object'
                    return internal_error(message, trace)

            iono_metric = False
            if base_name:
                try:
                    ionosphere_metrics = list(REDIS_CONN.smembers('ionosphere.unique_metrics'))
                except:
                    logger.warning('warning :: Webapp could not get the ionosphere.unique_metrics list from Redis, this could be because there are none')
                metric_name = settings.FULL_NAMESPACE + str(base_name)
                # @added 20220112 - Bug #4374: webapp - handle url encoded chars
                # @modified 20220115 - Bug #4374: webapp - handle url encoded chars
                # Roll back change - breaking existing metrics with colons
                # metric_name = url_encode_metric_name(metric_name)

                if metric_name in ionosphere_metrics:
                    iono_metric = True

            # @added 20170303 - Feature #1960: ionosphere_layers
            vconds = ['<', '>', '==', '!=', '<=', '>=']

            # @modified 20190503 - Branch #2646: slack - linting
            # condition_list = ['<', '>', '==', '!=', '<=', '>=', 'in', 'not in']

            crit_types = ['value', 'time', 'day', 'from_time', 'until_time']

            fp_layers = None
            if 'fp_layer' in request.args:
                fp_layer_arg = request.args.get(str('fp_layer'), None)
                if str(fp_layer_arg) == 'true':
                    fp_layers = True
            add_fp_layers = None
            if 'add_fp_layer' in request.args:
                add_fp_layer_arg = request.args.get(str('add_fp_layer'), None)
                if str(add_fp_layer_arg) == 'true':
                    add_fp_layers = True
            new_l_algos = None
            new_l_algos_ids = None
            if add_fp_layers:
                if 'learn' in request.args:
                    value = request.args.get(str('learn'))
                    if str(value) == 'true':
                        fp_learn = True
                    if str(value) == 'True':
                        fp_learn = True
                if 'fp_id' in request.args:
                    fp_id = request.args.get(str('fp_id'))
                l_id, layer_successful, new_l_algos, new_l_algos_ids, fail_msg, trace = create_ionosphere_layers(base_name, fp_id, requested_timestamp)
                if not layer_successful:
                    return internal_error(fail_msg, trace)
                else:
                    webapp_update_slack_thread(base_name, requested_timestamp, fp_id, 'layers_created')

            # @added 20170308 - Feature #1960: ionosphere_layers
            # To present the operator with the existing layers and algorithms for the metric
            # The metric layers algoritms are required to present the user with
            # if the add_fp=true argument is passed, which if so results in the
            # local variable of create_feature_profile being set and in the
            # fp_view
            metric_layers_details = None
            metric_layers_algorithm_details = None
            # @modified 20170331 - Task #1988: Review - Ionosphere layers - always show layers
            # Set to True so they are always displayed
            # get_metric_existing_layers = False
            get_metric_existing_layers = True
            metric_lc = 0
            metric_lmc = None
            if create_feature_profile:
                get_metric_existing_layers = True
            if fp_view and fp_details:
                get_metric_existing_layers = True
            if 'add_fp' in request.args:
                get_layers_add_fp = request.args.get(str('add_fp'), None)
                if get_layers_add_fp == 'true':
                    get_metric_existing_layers = True
            if get_metric_existing_layers:
                metric_layers_details, metric_layers_algorithm_details, metric_lc, metric_lmc, mlad_successful, fail_msg, trace = metric_layers_alogrithms(base_name)
                if not mlad_successful:
                    return internal_error(fail_msg, trace)

            # @added 20170616 - Feature #2048: D1 ionosphere layer
            fp_layer_algorithms = []
            if metric_layers_algorithm_details:
                for i_layer_algorithm in metric_layers_algorithm_details:
                    try:
                        if int(i_layer_algorithm[1]) == int(l_id):
                            fp_layer_algorithms.append(i_layer_algorithm)
                    except:
                        logger.warning('warning :: Webapp could not determine layer_algorithm in metric_layers_algorithm_details')
            fp_current_layer = []
            if metric_layers_details:
                for i_layer in metric_layers_details:
                    try:
                        if int(i_layer[0]) == int(l_id):
                            fp_current_layer.append(i_layer)
                    except:
                        logger.warning('warning :: Webapp could not determine layer in metric_layers_details')

            # @added 20170617 - Feature #2054: ionosphere.save.training_data
            training_data_saved = False
            saved_td_details = False
            if save_training_data:
                logger.info('saving training data')
                try:
                    request_time = int(time.time())
                    saved_hdate = time.strftime('%Y-%m-%d %H:%M:%S %Z (%A)', time.localtime(request_time))
                    training_data_saved, saved_td_details, fail_msg, trace = save_training_data_dir(requested_timestamp, base_name, saved_td_label, saved_hdate)
                    logger.info('saved training data')
                except:
                    logger.error('error :: Webapp could not save_training_data_dir')
                    return internal_error(fail_msg, trace)
            saved_td_requested = False
            if saved_training_data:
                saved_td_requested = True
                try:
                    training_data_saved, saved_td_details, fail_msg, trace = save_training_data_dir(requested_timestamp, base_name, None, None)
                    logger.info('got saved training data details')
                except:
                    logger.error('error :: Webapp could not get saved training_data details')
                    return internal_error(fail_msg, trace)

            # @added 20170917 - Feature #1996: Ionosphere - matches page
            matched_id_resources = None
            matched_graph_image_file = None

            # @added 20210415 - Feature #4014: Ionosphere - inference
            #                   Branch #3590: inference
            # Noow that the matched_details_object is being used set the default
            matched_details_object = None

            if matched_fp_id:
                if matched_fp_id != 'False':
                    matched_id_resources, successful, fail_msg, trace, matched_details_object, matched_graph_image_file = get_matched_id_resources(int(matched_fp_id), 'features_profile', base_name, requested_timestamp)
            if matched_layer_id:
                if matched_layer_id != 'False':
                    matched_id_resources, successful, fail_msg, trace, matched_details_object, matched_graph_image_file = get_matched_id_resources(int(matched_layer_id), 'layers', base_name, requested_timestamp)

            # @added 20210413 - Feature #4014: Ionosphere - inference
            #                   Branch #3590: inference
            if matched_motif_id:
                matched_id_resources, successful, fail_msg, trace, matched_details_object, matched_graph_image_file = get_matched_id_resources(int(matched_motif_id), 'motif', base_name, requested_timestamp)

            # @added 20180620 - Feature #2404: Ionosphere - fluid approximation
            # Added minmax scaling
            minmax = 0
            if matched_fp_id:
                minmax = int(matched_details_object['minmax'])
                logger.info('the fp match has minmax set to %s' % str(minmax))

            # @added 20190601 - Feature #3084: Ionosphere - validated matches
            # Update the DB that the match has been validated or invalidated
            validated_match_successful = None
            match_validated_db_value = None
            # @modified 20210413 - Feature #4014: Ionosphere - inference
            # Added matched_motif_id
            if matched_fp_id or matched_layer_id or matched_motif_id:
                match_validated_db_value = matched_details_object['validated']
                logger.info('the match_validated_db_value is set to %s' % str(match_validated_db_value))
                logger.info('the match_validated is set to %s' % str(match_validated))
                # Only update if the value in the DB is different from the value
                # in the argument, due to there being a difficulty in the
                # removal of the match_validation argument due to the
                # redirect_url function being applied earlier ^^ in the process.
                # Unfortunately without the redirect_url function being applied
                # this match_validation and match_validated would not work as
                # the matched context the redirect_url function is used.  Hence
                # more F.
                if int(match_validated_db_value) != int(match_validated):
                    if 'match_validation' in request.args:
                        if match_validated > 0:
                            logger.info('validating match')
                            if matched_fp_id:
                                match_id = matched_fp_id
                                validate_context = 'ionosphere_matched'
                            if matched_layer_id:
                                match_id = matched_layer_id
                                validate_context = 'ionosphere_layers_matched'
                            # @added 20210414 - Feature #4014: Ionosphere - inference
                            #                   Branch #3590: inference
                            if matched_motif_id:
                                match_id = matched_motif_id
                                validate_context = 'matched_motifs'
                            try:
                                # @modified 20190920 -
                                # Added user_id
                                # validated_match_successful = validate_ionosphere_match(match_id, validate_context, match_validated)
                                validated_match_successful = validate_ionosphere_match(match_id, validate_context, match_validated, user_id)
                                # @added 20190921 - Feature #3234: Ionosphere - related matches vaildation
                                # TODO - here related matches will also be validated
                                logger.info('validated match')
                                match_validated_db_value = match_validated
                            except:
                                trace = traceback.format_exc()
                                fail_msg = 'error :: Webapp error with search_ionosphere'
                                logger.error(fail_msg)
                                return internal_error(fail_msg, trace)

            # @added 20180921 - Feature #2558: Ionosphere - fluid approximation - approximately_close on layers
            approx_close = 0
            if matched_layer_id:
                approx_close = int(matched_details_object['approx_close'])
                logger.info('the layers match has approx_close set to %s' % str(approx_close))

            # @added 20180414 - Branch #2270: luminosity
            # Add correlations to features_profile and training_data pages if a
            # panorama_anomaly_id is present
            correlations = False
            correlations_with_graph_links = []
            if p_id:
                try:
                    correlations, fail_msg, trace = get_correlations(skyline_app, p_id)
                except:
                    trace = traceback.format_exc()
                    fail_msg = 'error :: Webapp error with get_correlations'
                    logger.error(fail_msg)
                    return internal_error(fail_msg, trace)
                if correlations:
                    # @added 20180723 - Feature #2470: Correlations Graphite graph links
                    #                   Branch #2270: luminosity
                    # Added Graphite graph links to Correlations block
                    for metric_name, coefficient, shifted, shifted_coefficient in correlations:
                        from_timestamp = int(requested_timestamp) - m_full_duration
                        graphite_from = datetime.datetime.fromtimestamp(int(from_timestamp)).strftime('%H:%M_%Y%m%d')
                        graphite_until = datetime.datetime.fromtimestamp(int(requested_timestamp)).strftime('%H:%M_%Y%m%d')
                        unencoded_graph_title = '%s\ncorrelated with anomaly id %s' % (
                            metric_name, str(p_id))
                        graph_title_string = quote(unencoded_graph_title, safe='')
                        graph_title = '&title=%s' % graph_title_string
                        if settings.GRAPHITE_PORT != '':
                            # @modified 20190520 - Branch #3002: docker
                            # correlation_graphite_link = '%s://%s:%s/render/?from=%s&until=%s&target=cactiStyle(%s)%s%s&colorList=blue' % (settings.GRAPHITE_PROTOCOL, settings.GRAPHITE_HOST, settings.GRAPHITE_PORT, str(graphite_from), str(graphite_until), metric_name, settings.GRAPHITE_GRAPH_SETTINGS, graph_title)
                            # @modified 20200417 - Task #3294: py3 - handle system parameter in Graphite cactiStyle
                            # correlation_graphite_link = '%s://%s:%s/%s/?from=%s&until=%s&target=cactiStyle(%s)%s%s&colorList=blue' % (
                            correlation_graphite_link = '%s://%s:%s/%s/?from=%s&until=%s&target=cactiStyle(%s,%%27si%%27)%s%s&colorList=blue' % (
                                settings.GRAPHITE_PROTOCOL, settings.GRAPHITE_HOST,
                                settings.GRAPHITE_PORT, GRAPHITE_RENDER_URI,
                                str(graphite_from), str(graphite_until), metric_name,
                                settings.GRAPHITE_GRAPH_SETTINGS, graph_title)
                        else:
                            # @modified 20190520 - Branch #3002: docker
                            # correlation_graphite_link = '%s://%s/render/?from=%s&until=%starget=cactiStyle(%s)%s%s&colorList=blue' % (settings.GRAPHITE_PROTOCOL, settings.GRAPHITE_HOST, str(graphite_from), str(graphite_until), metric_name, settings.GRAPHITE_GRAPH_SETTINGS, graph_title)
                            # @modified 20200417 - Task #3294: py3 - handle system parameter in Graphite cactiStyle
                            # correlation_graphite_link = '%s://%s/%s/?from=%s&until=%starget=cactiStyle(%s)%s%s&colorList=blue' % (
                            correlation_graphite_link = '%s://%s/%s/?from=%s&until=%starget=cactiStyle(%s,%%27si%%27)%s%s&colorList=blue' % (
                                settings.GRAPHITE_PROTOCOL, settings.GRAPHITE_HOST,
                                GRAPHITE_RENDER_URI, str(graphite_from),
                                str(graphite_until), metric_name,
                                settings.GRAPHITE_GRAPH_SETTINGS, graph_title)
                        correlations_with_graph_links.append([metric_name, coefficient, shifted, shifted_coefficient, str(correlation_graphite_link)])

            # @added 20200808 - Feature #3568: Ionosphere - report anomalies in training period
            labelled_anomalies = None

            # @added 20200113 - Feature #3390: luminosity related anomalies
            #                   Branch #2270: luminosity
            related = False
            related_with_graph_links = []
            related_matches = []
            if p_id:
                try:
                    # @modified 20200808 - Feature #3568: Ionosphere - report anomalies in training period
                    # related, fail_msg, trace = get_related(skyline_app, p_id, requested_timestamp)
                    related, labelled_anomalies, fail_msg, trace = get_related(skyline_app, p_id, requested_timestamp)
                except:
                    trace = traceback.format_exc()
                    fail_msg = 'error :: Webapp error with get_related'
                    logger.error(fail_msg)
                    return internal_error(fail_msg, trace)
                if related:
                    # Added Graphite graph links to Related block
                    for related_anomaly_id, related_metric_id, related_metric_name, related_anomaly_timestamp, related_full_duration in related:
                        related_from_timestamp = int(related_anomaly_timestamp) - related_full_duration
                        related_graphite_from = datetime.datetime.fromtimestamp(int(related_from_timestamp)).strftime('%H:%M_%Y%m%d')
                        related_graphite_until = datetime.datetime.fromtimestamp(int(related_anomaly_timestamp)).strftime('%H:%M_%Y%m%d')
                        related_unencoded_graph_title = '%s\nrelated with anomaly id %s' % (
                            related_metric_name, str(p_id))
                        related_graph_title_string = quote(related_unencoded_graph_title, safe='')
                        related_graph_title = '&title=%s' % related_graph_title_string
                        if settings.GRAPHITE_PORT != '':
                            # @modified 20200417 - Task #3294: py3 - handle system parameter in Graphite cactiStyle
                            # related_graphite_link = '%s://%s:%s/%s/?from=%s&until=%s&target=cactiStyle(%s)%s%s&colorList=blue' % (
                            related_graphite_link = '%s://%s:%s/%s/?from=%s&until=%s&target=cactiStyle(%s,%%27si%%27)%s%s&colorList=blue' % (
                                settings.GRAPHITE_PROTOCOL, settings.GRAPHITE_HOST,
                                settings.GRAPHITE_PORT, GRAPHITE_RENDER_URI,
                                str(related_graphite_from),
                                str(related_graphite_until), related_metric_name,
                                settings.GRAPHITE_GRAPH_SETTINGS, related_graph_title)
                        else:
                            # @modified 20200417 - Task #3294: py3 - handle system parameter in Graphite cactiStyle
                            # related_graphite_link = '%s://%s/%s/?from=%s&until=%starget=cactiStyle(%s)%s%s&colorList=blue' % (
                            related_graphite_link = '%s://%s/%s/?from=%s&until=%starget=cactiStyle(%s,%%27si%%27)%s%s&colorList=blue' % (
                                settings.GRAPHITE_PROTOCOL, settings.GRAPHITE_HOST,
                                GRAPHITE_RENDER_URI, str(related_graphite_from),
                                str(related_graphite_until), related_metric_name,
                                settings.GRAPHITE_GRAPH_SETTINGS, related_graph_title)
                        related_human_timestamp = datetime.datetime.fromtimestamp(int(related_anomaly_timestamp)).strftime('%Y-%m-%d %H:%M:%S')
                        related_with_graph_links.append([related_anomaly_id, related_metric_name, related_anomaly_timestamp, related_full_duration, related_human_timestamp, str(related_graphite_link)])
                related_metric = 'get_related_matches'
                related_metric_like = 'all'
                related_fp_id = None
                related_layer_id = None
                minus_two_minutes = int(requested_timestamp) - 120
                plus_two_minutes = int(requested_timestamp) + 120
                related_limited_by = None
                related_ordered_by = 'DESC'
                try:
                    related_matches, fail_msg, trace = get_fp_matches(related_metric, related_metric_like, related_fp_id, related_layer_id, minus_two_minutes, plus_two_minutes, related_limited_by, related_ordered_by)
                except:
                    trace = traceback.format_exc()
                    fail_msg = 'error :: Webapp error with get_fp_matches for related_matches'
                    logger.error(fail_msg)
                    return internal_error(fail_msg, trace)
                if len(related_matches) == 1:
                    # @modified 20200908 - Feature #3740: webapp - anomaly API endpoint
                    # Added match_anomaly_timestamp
                    # @modified 20210413 - Feature #4014: Ionosphere - inference
                    #                      Branch #3590: inference
                    # Added related_motifs_matched_id
                    for related_human_date, related_match_id, related_matched_by, related_fp_id, related_layer_id, related_metric, related_uri_to_matched_page, related_validated, match_anomaly_timestamp, related_motifs_matched_id in related_matches:
                        if related_matched_by == 'no matches were found':
                            related_matches = []
                if related_matches:
                    logger.info('%s possible related matches found' % (str(len(related_matches))))
                # @added 20200808 - Feature #3568: Ionosphere - report anomalies in training period
                if labelled_anomalies:
                    logger.info('%s labelled anomalies found in the trying period' % (str(len(labelled_anomalies))))

            # @added 20190510 - Feature #2990: Add metrics id to relevant web pages
            # By this point in the request the previous function calls will have
            # populated memcache with the metric details
            if not metric_id:
                metric_id = 0
                try:
                    cache_key = 'panorama.mysql_ids.metrics.metric.%s' % base_name
                    metric_id_msg_pack = None
                    # @modified 20191015 - Bug #3266: py3 Redis binary objects not strings
                    #                      Branch #3262: py3
                    # metric_id_msg_pack = REDIS_CONN.get(cache_key)
                    metric_id_msg_pack = REDIS_CONN_UNDECODE.get(cache_key)

                    if metric_id_msg_pack:
                        unpacker = Unpacker(use_list=False)
                        unpacker.feed(metric_id_msg_pack)
                        metric_id = [item for item in unpacker][0]
                        logger.info('metrics id is %s from Redis key -%s' % (str(metric_id), cache_key))
                    else:
                        logger.info('Webapp could not get metric id from Redis key - %s' % cache_key)
                except:
                    logger.info(traceback.format_exc())
                    logger.error('error :: Webapp could not get metric id from Redis key - %s' % cache_key)
            else:
                logger.info('metrics id is %s' % str(metric_id))

            # @added 20190502 - Branch #2646: slack
            if context == 'training_data':
                update_slack = True
                # Do not update if a fp_id is present
                if fp_id:
                    update_slack = False
                # Do not update slack when extract features is run
                if calculate_features:
                    update_slack = False
                if update_slack:
                    slack_updated = webapp_update_slack_thread(base_name, requested_timestamp, None, 'training_data_viewed')
                    logger.info('slack_updated for training_data_viewed %s' % str(slack_updated))

            show_motif_match = False
            if context == 'saved_training_data':
                inference_file = '%s/%s.%s.inference.matched_motifs.dict' % (
                    ionosphere_data_dir, str(requested_timestamp), base_name)
                if os.path.isfile(inference_file):
                    show_motif_match = True

            # @added 20210419 - Feature #4014: Ionosphere - inference
            # Plot the macthed motif
            if context == 'training_data' or context == 'saved_training_data' and matched_motif_id:
                show_motif_match = True
            if show_motif_match:
                inference_file = '%s/%s.%s.inference.matched_motifs.dict' % (
                    ionosphere_data_dir, str(requested_timestamp), base_name)
                matched_motifs_dict = {}
                if os.path.isfile(inference_file):
                    try:
                        with open(inference_file, 'r') as f:
                            matched_motifs_dict_str = f.read()
                        matched_motifs_dict = literal_eval(matched_motifs_dict_str)
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: failed to evaluate matched_motifs_dict from %s' % inference_file)
                        matched_motifs_dict = {}
                matched_motif = None
                if matched_motifs_dict:
                    try:
                        matched_motif = list(matched_motifs_dict.keys())[0]
                        inference_match_motif_dict = matched_motifs_dict[matched_motif]
                        matched_motif_fp_id = matched_motifs_dict[matched_motif]['fp_id']
                        f_id_matched = matched_motif_fp_id
                        matched_motif_timestamp = matched_motifs_dict[matched_motif]['timestamp']
                        matched_motif_fp_index = matched_motifs_dict[matched_motif]['index']
                        matched_motif_size = matched_motifs_dict[matched_motif]['size']
                        # Add the id to the dictionery, inference does not know
                        # the id when the dict is created
                        matched_motif_id, motif_validated, ionosphere_matched_id = get_matched_motif_id(
                            matched_motif_fp_id, matched_motif_timestamp,
                            matched_motif_fp_index, matched_motif_size)
                        inference_match_motif_dict['matched_motif_id'] = matched_motif_id
                        inference_match_motif_dict['ionosphere_matched_id'] = ionosphere_matched_id
                        inference_match_motif_dict['validated'] = motif_validated
                        match_validated_db_value = motif_validated
                        try:
                            matched_motif_distance = matched_motifs_dict[matched_motif]['dist']
                        except:
                            matched_motif_distance = matched_motifs_dict[matched_motif]['distance']
                        matched_motif_type_id = matched_motifs_dict[matched_motif]['type_id']
                        matched_motif_type = matched_motifs_dict[matched_motif]['type']
                        matched_motif_full_duration = matched_motifs_dict[matched_motif]['full_duration']
                        matched_motif_sequence = matched_motifs_dict[matched_motif]['motif_sequence']
                        matched_motif_fp_generation = matched_motifs_dict[matched_motif]['generation']
                        if matched_motif_fp_generation == 0:
                            generation_str = 'trained'
                        else:
                            generation_str = 'LEARNT'
                        relate_dataset = [item[1] for item in matched_motifs_dict[matched_motif]['fp_motif_sequence']]
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: failed to determine plot parameters from matched_motifs_dict')
                if matched_motif:
                    motif_graph_image_file = '%s/%s.%s.matched_motif.%s.fp_id.%s.index.%s.size.%s.%s.png' % (
                        ionosphere_data_dir, str(matched_motif_timestamp), base_name,
                        str(matched_motif_id), str(matched_motif_fp_id),
                        str(matched_motif_fp_index), str(matched_motif_size),
                        str(matched_motif_type))
                    # Plot an invalid motif with a red line
                    if motif_validated == 2:
                        motif_graph_image_file = '%s/%s.%s.matched_motif.%s.fp_id.%s.index.%s.size.%s.%s.INVALIDATED.png' % (
                            ionosphere_data_dir, str(matched_motif_timestamp), base_name,
                            str(matched_motif_id), str(matched_motif_fp_id),
                            str(matched_motif_fp_index), str(matched_motif_size),
                            str(matched_motif_type))
                        # Use type_id as 5 to indicate invalidated
                        matched_motif_type_id = 5

                    plotted_motif_image = False
                    if not path.isfile(motif_graph_image_file):
                        use_on_demand_motif_analysis = False
                        plotted_motif_image, inference_match_motif_image = plot_motif_match(
                            skyline_app, base_name, matched_motif_timestamp, matched_motif_fp_id,
                            matched_motif_full_duration,
                            generation_str, matched_motif_id,
                            matched_motif_fp_index, int(matched_motif_size),
                            matched_motif_distance, matched_motif_type_id,
                            relate_dataset, matched_motif_sequence,
                            motif_graph_image_file, use_on_demand_motif_analysis)
                        inference_match_motif_dict['image'] = inference_match_motif_image
                    else:
                        plotted_motif_image = True
                        inference_match_motif_image = motif_graph_image_file
                        logger.info('plot already exists - %s - %s' % (
                            str(plotted_motif_image), str(motif_graph_image_file)))
                        inference_match_motif_dict['image'] = inference_match_motif_image

            # @added 20191210 - Feature #3348: fp creation json response
            if create_feature_profile and fp_id:
                if response_format == 'json':
                    data_dict = {"status": "error"}
                    try:
                        fp_created_successful = fp_in_successful
                    except:
                        fp_created_successful = None
                    if fp_created_successful:
                        data_dict = {"status": {"created": "true"}, "data": {"fp_id": fp_id}}
                    else:
                        if fp_exists:
                            data_dict = {"status": {"created": "already exists"}, "data": {"fp_id": fp_id}}
                    return jsonify(data_dict), 200

            # @added 20200731 - Feature #3654: IONOSPHERE_GRAPHITE_NOW_GRAPHS_OVERRIDE
            graphite_now_graphs_title = 'NOW'
            if IONOSPHERE_GRAPHITE_NOW_GRAPHS_OVERRIDE:
                graphite_now_graphs_title = 'THEN'

            # @added 20200813 - Feature #3670: IONOSPHERE_CUSTOM_KEEP_TRAINING_TIMESERIES_FOR
            historical_training_data = False
            if mpaths:
                try:
                    if IONOSPHERE_HISTORICAL_DATA_FOLDER in mpaths[0][1]:
                        historical_training_data = True
                except:
                    logger.info(traceback.format_exc())
                    logger.error('error :: Webapp could not determine if this is historical training data')

            # @added 20210324 - Feature #3642: Anomaly type classification
            anomaly_types = None
            anomaly_type_ids = None
            if LUMINOSITY_CLASSIFY_ANOMALIES and p_id:
                try:
                    anomaly_types, anomaly_type_ids = get_anomaly_type(skyline_app, int(p_id))
                except Exception as e:
                    fail_msg = 'error :: Webapp error with get_anomaly_type - %s' % str(e)
                    logger.error(fail_msg)

            return render_template(
                'ionosphere.html', timestamp=requested_timestamp,
                for_metric=base_name, metric_vars=m_vars, metric_files=mpaths,
                metric_images=sorted_images, metric_images_str=str(sorted_images),
                human_date=hdate, timeseries=ts_json,
                data_ok=data_to_process, td_files=mpaths,
                panorama_anomaly_id=p_id, graphite_url=graph_url,
                extracted_features=features, calc_time=f_calc,
                features_profile_id=fp_id, features_profile_exists=fp_exists,
                fp_view=fp_view_on, features_profile_details=fp_details,
                redis_full_duration=full_duration,
                redis_full_duration_in_hours=full_duration_in_hours,
                metric_full_duration=m_full_duration,
                metric_full_duration_in_hours=m_full_duration_in_hours,
                metric_second_order_resolution_hours=second_order_resolution_hours,
                tsfresh_version=TSFRESH_VERSION, graphite_now_images=gimages,
                graphite_matched_images=gmimages, matched_count=times_matched,
                parent_id=par_id, generation=gen, learn=fp_learn,
                use_full_duration_days=fp_fd_days, countdown=countdown_to,
                ionosphere_metric=iono_metric, value_condition_list=vconds,
                criteria_types=crit_types, fp_layer=fp_layers,
                layer_id=l_id, layers_algorithms=new_l_algos,
                layers_algorithms_ids=new_l_algos_ids,
                layer_details=l_details,
                layer_details_object=l_details_object,
                layer_algorithms_details=la_details,
                existing_layers=metric_layers_details,
                existing_algorithms=metric_layers_algorithm_details,
                metric_layers_count=metric_lc,
                metric_layers_matched_count=metric_lmc,
                graphite_layers_matched_images=glm_images,
                layers_id_matched=l_id_matched, ts_full_duration=ts_fd,
                app_context=m_app_context, ionosphere_json=i_ts_json,
                baseline_fd=full_duration, last_ts_json=sample_ts_json,
                last_i_ts_json=sample_i_ts_json, layers_updated=layers_updated,
                fp_id_matched=f_id_matched, fp_id_created=f_id_created,
                fp_generation=fp_generation_created, validated=fp_validated,
                validated_fp_successful=validated_fp_success,
                profile_layer_algorithms=fp_layer_algorithms,
                current_layer=fp_current_layer,
                save_metric_td=save_training_data,
                saved_metric_td_label=saved_td_label,
                saved_metric_td=saved_training_data,
                metric_training_data_saved=training_data_saved,
                saved_metric_td_requested=saved_td_requested,
                saved_metric_td_details=saved_td_details,
                profile_enabled=fp_enabled, disable_feature_profile=disable_fp,
                disabled_fp_successful=disabled_fp_success,
                family_tree_ids=family_tree_fp_ids,
                matched_fp_id=matched_fp_id, matched_layer_id=matched_layer_id,
                matched_id_resources=matched_id_resources,
                matched_graph_image_file=matched_graph_image_file,
                # @added 20180620 - Feature #2404: Ionosphere - fluid approximation
                # Added minmax scaling
                minmax=minmax,
                correlations=correlations,
                # @added 20180723 - Feature #2470: Correlations Graphite graph links
                #                   Branch #2270: luminosity
                # Added Graphite graph links to the Correlations block in
                # the correlations.html and training_data.html templates
                correlations_with_graph_links=correlations_with_graph_links,
                matched_from_datetime=matched_from_datetime,
                # @added 20180921 - Feature #2558: Ionosphere - fluid approximation - approximately_close on layers
                approx_close=approx_close,
                # @added 20190328 - Feature #2484: FULL_DURATION feature profiles
                # Added ionosphere_echo
                echo_fp=echo_fp_value, echo_human_date=echo_hdate,
                metric_full_duration_in_hours_image_str=m_fd_in_hours_img_str,
                # @added 20190510 - Feature #2990: Add metrics id to relevant web pages
                metric_id=metric_id,
                # @added 20190601 - Feature #3084: Ionosphere - validated matches
                match_validated=match_validated,
                match_validated_db_value=match_validated_db_value,
                validated_match_successful=validated_match_successful,
                # @added 20190922 - Feature #2516: Add label to features profile
                fp_label=fp_label,
                # @added 20200113 - Feature #3390: luminosity related anomalies
                #                   Branch #2270: luminosity
                related=related, related_with_graph_links=related_with_graph_links,
                related_matches=related_matches,
                # @added 20200711 - Feature #3634: webapp - ionosphere - report number of data points
                ts_json_length=ts_json_length,
                anomalous_timeseries_length=anomalous_timeseries_length,
                # @added 20200731 - Feature #3654: IONOSPHERE_GRAPHITE_NOW_GRAPHS_OVERRIDE
                graphite_now_graphs_title=graphite_now_graphs_title,
                # @added 20200808 - Feature #3568: Ionosphere - report anomalies in training period
                labelled_anomalies=labelled_anomalies,
                # @added 20200813 - Feature #3670: IONOSPHERE_CUSTOM_KEEP_TRAINING_TIMESERIES_FOR
                historical_training_data=historical_training_data,
                # @added 20210324 - Feature #3642: Anomaly type classification
                anomaly_types=anomaly_types, anomaly_type_ids=anomaly_type_ids,
                # @added 20210413 - Feature #4014: Ionosphere - inference
                #                   Branch #3590: inference
                matched_motif_id=matched_motif_id,
                # @added 20210415 - Feature #4014: Ionosphere - inference
                #                   Branch #3590: inference
                matched_details_object=matched_details_object,
                motif_analysis=motif_analysis,
                motif_analysis_images=motif_analysis_images,
                motif_analysis_motif_id_by_distance=motif_analysis_motif_id_by_distance,
                # @added 20210416 - Feature #4014: Ionosphere - inference
                ionosphere_matched_id=ionosphere_matched_id,
                # @added 20210417 - Feature #4014: Ionosphere - inference
                batch_size=batch_size, top_matches=top_matches,
                max_distance=max_distance,
                # @added 20210419 - Feature #4014: Ionosphere - inference
                matched_motif_dict=inference_match_motif_dict,
                inference_match_motif_image=inference_match_motif_image,
                # @added 20210422 - Feature #4014: Ionosphere - inference
                # Allow the user to specify the range_padding
                motif_range_padding=motif_range_padding,
                # @added 20210425 - Feature #4014: Ionosphere - inference
                # Allow user to specify the difference between the areas under the
                # curve
                motif_max_area_percent_diff=motif_max_area_percent_diff,
                version=skyline_version, duration=(time.time() - start),
                print_debug=debug_on), 200
        except:
            message = 'Uh oh ... a Skyline 500 :('
            trace = traceback.format_exc()
            return internal_error(message, trace)

    try:
        message = 'Unknown request'
        return render_template(
            'ionosphere.html', display_message=message,
            version=skyline_version, duration=(time.time() - start),
            print_debug=debug_on), 200
    except:
        message = 'Uh oh ... a Skyline 500 :('
        trace = traceback.format_exc()
        return internal_error(message, trace)


@app.route('/ionosphere_images')
# @added 20210316 - Feature #3978: luminosity - classify_metrics
#                   Feature #3642: Anomaly type classification
# Serve the luminosity_images endpoint as well, for classify_metrics plots
@app.route('/luminosity_images')
# @added 20210328 - Feature #3994: Panorama - mirage not anomalous
# Serve the panorama_file endpoint as well
@app.route('/panorama_images')
# @added 20211103 - Branch #3068: SNAB
#                   Bug #4308: matrixprofile - fN on big drops
# Serve the snab_images endpoint as well
@app.route('/snab_images')
def ionosphere_images():

    request_args_present = False
    try:
        request_args_len = len(request.args)
        request_args_present = True
    except:
        request_args_len = 0
        logger.error('error :: request arguments have no length - %s' % str(request_args_len))

    IONOSPHERE_REQUEST_ARGS = ['image']

    if request_args_present:
        for i in request.args:
            key = str(i)
            if key not in IONOSPHERE_REQUEST_ARGS:
                logger.error('error :: invalid request argument - %s=%s' % (key, str(i)))
                # @modified 20190524 - Branch #3002: docker
                # Return data
                # return 'Bad Request', 400
                error_string = 'error :: invalid request argument - %s=%s' % (key, str(i))
                logger.error(error_string)
                resp = json.dumps(
                    {'400 Bad Request': error_string})
                return flask_escape(resp), 400

            value = request.args.get(key, None)
            logger.info('request argument - %s=%s' % (key, str(value)))

            if key == 'image':
                filename = str(value)

                # @added 20201214 - Feature #3890: metrics_manager - sync_cluster_files
                # Even though authenticated only allow specific paths
                IONOSPHERE_IMAGE_ALLOWED_PATHS = [
                    settings.IONOSPHERE_DATA_FOLDER,
                    settings.IONOSPHERE_PROFILES_FOLDER,
                    # @added 20210112 - Feature #3934: ionosphere_performance
                    performance_dir,
                    # @added 20210209 - Feature #1448: Crucible web UI
                    crucible_jobs_dir,
                    # @added 20210316 - Feature #3978: luminosity - classify_metrics
                    #                   Feature #3642: Anomaly type classification
                    luminosity_data_folder,
                    # @added 20210328 - Feature #3994: Panorama - mirage not anomalous
                    settings.SKYLINE_TMP_DIR,
                ]
                allowed_path = False
                for allowed_image_path in IONOSPHERE_IMAGE_ALLOWED_PATHS:
                    if allowed_image_path in filename:
                        allowed_path = True
                if not allowed_path:
                    logger.info('forbidden filename, returning 403 - %s' % filename)
                    return 'Forbidden', 403
                if '.png' not in filename:
                    logger.info('forbidden filename, not png, returning 403 - %s' % filename)
                    return 'Forbidden', 403

                # @added 20220112 - Bug #4374: webapp - handle url encoded chars
                if not os.path.isfile(filename):
                    r_url = iri_to_uri(request.url)
                    logger.info('r_url: %s' % r_url)

                if os.path.isfile(filename):
                    try:
                        return send_file(filename, mimetype='image/png')
                    except:
                        message = 'Uh oh ... a Skyline 500 :( - could not return %s' % filename
                        trace = traceback.format_exc()
                        return internal_error(message, trace)
                else:
                    image_404_path = 'webapp/static/images/skyline.ionosphere.image.404.png'
                    filename = path.abspath(
                        path.join(path.dirname(__file__), '..', image_404_path))
                    try:
                        return send_file(filename, mimetype='image/png')
                    except:
                        message = 'Uh oh ... a Skyline 500 :( - could not return %s' % filename
                        trace = traceback.format_exc()
                        return internal_error(message, trace)

    return 'Bad Request', 400


# @added 20201213 - Feature #3890: metrics_manager - sync_cluster_files
@app.route('/ionosphere_files')
# @added 20210316 - Feature #3978: luminosity - classify_metrics
#                   Feature #3642: Anomaly type classification
# Serve the luminosity_file endpoint as well, for classify_metrics plots
@app.route('/luminosity_files')
@requires_auth
def ionosphere_files():

    start = time.time()
    request_args_present = False
    try:
        request_args_len = len(request.args)
        request_args_present = True
    except:
        request_args_len = 0
        logger.error('error :: ionosphere_files request arguments have no length - %s' % str(request_args_len))

    # @modified 20210316 - Feature #3978: luminosity - classify_metrics
    #                      Feature #3642: Anomaly type classification
    # Added algorithm_name and classify_metrics
    IONOSPHERE_FILES_REQUEST_ARGS = [
        'source', 'timestamp', 'metric', 'algorithm_name',
    ]
    IONOSPHERE_FILES_ALLOWED_DIR_TYPES = [
        'training_data', 'features_profiles', 'classify_metrics',
    ]

    allowed_source_dir = False
    timestamp = None
    metric = None

    if request_args_present:
        for i in request.args:
            key = str(i)
            if key not in IONOSPHERE_FILES_REQUEST_ARGS:
                logger.error('error :: ionosphere_files invalid request argument - %s' % (key))
                return 'Bad Request', 400
            value = request.args.get(key, 0)
            if key == 'source':
                source = str(value)
                if source in IONOSPHERE_FILES_ALLOWED_DIR_TYPES:
                    allowed_source_dir = True
                if not allowed_source_dir:
                    logger.error('error :: ionosphere_files not in IONOSPHERE_FILES_ALLOWED_DIR_TYPES - %s' % source)
                    return 'Bad Request', 400
            if key == 'timestamp':
                timestamp_str = str(value)
                try:
                    timestamp = int(timestamp_str)
                except:
                    logger.error('error :: ionosphere_files timestamp is not an int - %s' % timestamp_str)
                    return 'Bad Request', 400
            if key == 'metric':
                metric = str(value)
            # @added 20210316 - Feature #3978: luminosity - classify_metrics
            #                   Feature #3642: Anomaly type classification
            # Added algorithm_name
            algorithm_name = None
            if key == 'algorithm_name':
                algorithm_name = str(value)

    if not metric:
        logger.error('error :: ionosphere_files no metric passed')
        return 'Bad Request', 400

    required_dir = None
    if allowed_source_dir and timestamp and metric:
        try:
            metric_timeseries_dir = metric.replace('.', '/')
            if source == 'features_profiles':
                required_dir = '%s/%s/%s' % (
                    settings.IONOSPHERE_PROFILES_FOLDER, metric_timeseries_dir,
                    str(timestamp))
            if source == 'training_data':
                required_dir = '%s/%s/%s' % (
                    settings.IONOSPHERE_DATA_FOLDER, str(timestamp),
                    metric_timeseries_dir)
            # @added 20210316 - Feature #3978: luminosity - classify_metrics
            #                   Feature #3642: Anomaly type classification
            # Added classify_metrics
            if source == 'classify_metrics':
                required_dir = '%s/%s/%s/%s/%s' % (
                    luminosity_data_folder, source, algorithm_name,
                    metric_timeseries_dir, str(timestamp))
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: ionosphere_files failed to interpolate required_dir')
            return 'Internal Server Error', 500

    files_dict = {}
    if required_dir:
        if not isdir(required_dir):
            # @modified 20201216 - Feature #3890: metrics_manager - sync_cluster_files
            # Remove the entry from the ionosphere.training data Redis set if
            # the directory does not exist on this node
            # logger.info('ionosphere_files required_dir does not exist - %s, returning 404' % str(required_dir))
            # return 'Not Found', 404
            data_dict = {"status": {"response": 404}, "data": {"features_profiles dir": "not found"}}
            if source == 'training_data':
                try:
                    ionosphere_training_data = list(REDIS_CONN.smembers('ionosphere.training_data'))
                    training_data_removed = False
                    for training_data_item in ionosphere_training_data:
                        training_data = literal_eval(training_data_item)
                        if training_data[0] == metric:
                            if training_data[1] == int(timestamp):
                                try:
                                    REDIS_CONN.srem('ionosphere.training_data', str(training_data))
                                    logger.info('ionosphere_files removed item from ionosphere.training_data - %s' % str(training_data))
                                    training_data_removed = True
                                except:
                                    logger.error(traceback.format_exc())
                                    logger.error('error :: ionosphere_files failed to remove item from ionosphere.training_data - %s' % str(training_data))
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: ionosphere_files failed to try and remove the unfound training data dir for %s from ionosphere.training_data' % required_dir)
                if not training_data_removed:
                    logger.info('ionosphere_files found no item to remove from ionosphere.training_data for %s at %s' % (metric, str(timestamp)))
                logger.info('ionosphere_files dir not found for %s at %s, returning 404' % (metric, str(timestamp)))
                data_dict = {"status": {"response": 404}, "data": {"training_data dir": "not found"}}
            return jsonify(data_dict), 404

        for dir_path, folders, files in os.walk(required_dir):
            try:
                if files:
                    for i in files:
                        path_and_file = '%s/%s' % (required_dir, i)
                        files_dict[i] = path_and_file
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: ionosphere_files failed to build files_list from %s' % str(required_dir))
    duration = time.time() - start
    if not files_dict:
        logger.warning('warning :: ionosphere_files no files in required_dir - %s - returning 404' % str(required_dir))
        # return 'Not Found', 404
        data_dict = {"status": {"response": 404, "request_time": duration}, "data": {"metric": metric, "timestamp": timestamp, "source": source, "files": "not found"}}
        if algorithm_name and source == 'classify_metrics':
            data_dict = {"status": {"response": 404, "request_time": duration}, "data": {"metric": metric, "timestamp": timestamp, "source": source, "algorithm_name": algorithm_name, "files": "not found"}}
        return jsonify(data_dict), 404
    if files_dict:
        data_dict = {"status": {"response": 200, "request_time": duration}, "data": {"metric": metric, "timestamp": timestamp, "source": source, "files": files_dict}}
        # @added 20210316 - Feature #3978: luminosity - classify_metrics
        #                   Feature #3642: Anomaly type classification
        # Added algorithm_name for classify_metrics
        if algorithm_name and source == 'classify_metrics':
            data_dict = {
                "status": {"response": 200, "request_time": duration},
                "data": {
                    "metric": metric, "timestamp": timestamp, "source": source,
                    "algorithm_name": algorithm_name, "files": files_dict}
            }
        logger.info('ionosphere_files returned json with %s files listed' % str(len(files_dict)))
        return jsonify(data_dict), 200
    return 'Bad Request', 400


# TODO @gzipped this?  But really bandwidth is more available than CPU
# @added 20201213 - Feature #3890: metrics_manager - sync_cluster_files
@app.route('/ionosphere_file')
# @added 20210316 - Feature #3978: luminosity - classify_metrics
#                   Feature #3642: Anomaly type classification
# Serve the luminosity_file endpoint as well, for classify_metrics plots
@app.route('/luminosity_file')
@requires_auth
def ionosphere_file():

    request_args_present = False
    try:
        request_args_len = len(request.args)
        request_args_present = True
    except:
        request_args_len = 0
        logger.error('error :: ionosphere_file request arguments have no length - %s' % str(request_args_len))

    IONOSPHERE_FILE_REQUEST_ARGS = ['file']
    IONOSPHERE_FILE_ALLOWED_PATHS = [
        settings.IONOSPHERE_DATA_FOLDER,
        settings.IONOSPHERE_PROFILES_FOLDER,
        # @added 20210112 - Feature #3934: ionosphere_performance
        performance_dir,
        # @added 20210223 - Feature #2054: ionosphere.save.training_data
        settings.IONOSPHERE_DATA_FOLDER + '_saved',
        # @added 20210316 - Feature #3978: luminosity - classify_metrics
        #                   Feature #3642: Anomaly type classification
        luminosity_data_folder,
    ]

    if request_args_present:
        for i in request.args:
            key = str(i)
            if key not in IONOSPHERE_FILE_REQUEST_ARGS:
                logger.error('error :: invalid request argument - %s=%s' % (key, str(i)))
                error_string = 'error :: invalid request argument - %s=%s' % (key, str(i))
                logger.error(error_string)
                resp = json.dumps(
                    {'400 Bad Request': error_string})
                return flask_escape(resp), 400

            value = request.args.get(key, 0)
            # logger.info('request argument - %s=%s' % (key, str(value)))

            allowed_file = False
            if key == 'file':
                filename = str(value)
                for allowed_path in IONOSPHERE_FILE_ALLOWED_PATHS:
                    if allowed_path in filename:
                        allowed_file = True
                if not allowed_file:
                    logger.error('error :: ionosphere_file not in allowed paths - %s' % filename)
                    return 'Bad Request', 400
            if allowed_file:
                if os.path.isfile(filename):
                    mimetype = 'application/octet-stream'
                    if filename.endswith('.png'):
                        mimetype = 'image/png'
                    if filename.endswith('.json'):
                        mimetype = 'application/json'
                    if filename.endswith('.txt'):
                        mimetype = 'text/plain'
                    # @added 20210112 - Feature #3934: ionosphere_performance
                    if filename.endswith('.csv'):
                        mimetype = 'text/csv'
                    try:
                        return send_file(filename, mimetype=mimetype)
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: ionosphere_file failed to send_file - %s' % filename)
                        return 'Internal Server Error', 500
                else:
                    logger.warning('warning :: ionosphere_file not found - %s' % filename)
                    return 'Not Found', 404
    return 'Bad Request', 400


# @added 20170102 - Feature #1838: utilites - ALERTS matcher
#                   Branch #922: ionosphere
#                   Task #1658: Patterning Skyline Ionosphere
# Added utilities TODO
@app.route("/utilities")
@requires_auth
def utilities():
    start = time.time()

    # @added 20210604 - Branch #1444: thunder
    metrics_match = True
    thunder_testing = False
    thunder_no_data_test = False
    thunder_no_data_test_key_added = False

    thunder_stale_metrics_test = False
    thunder_stale_metrics_test_key_added = False

    parent_namespaces = []
    try:
        request_args_len = len(request.args)
    except:
        request_args_len = 0
    UTILITIES_REQUEST_ARGS = [
        'metric', 'settings_list', 'match_with', 'thunder_testing',
        'thunder_no_data_test', 'namespace', 'stale_period', 'expiry',
        'thunder_no_data_test_key_added', 'thunder_stale_metrics_test',
        'thunder_stale_metrics_test_key_added',
    ]
    if request_args_len:
        for i in request.args:
            key = str(i)
            if key not in UTILITIES_REQUEST_ARGS:
                logger.error('error :: invalid request argument - %s=%s' % (key, str(i)))
                error_string = 'error :: invalid request argument - %s=%s' % (key, str(i))
                logger.error(error_string)
                resp = json.dumps(
                    {'400 Bad Request': error_string})
                return flask_escape(resp), 400
            value = request.args.get(key, None)
            logger.info('request argument - %s=%s' % (key, str(value)))
            if key == 'thunder_testing':
                thunder_testing = True
                metrics_match = False
                logger.info('thunder_testing running get_top_level_namespaces')
                try:
                    parent_namespaces = get_top_level_namespaces(skyline_app, True)
                except Exception as e:
                    message = 'get_top_level_namespaces failed: %s' % e
                    trace = traceback.format_exc()
                    return internal_error(message, trace)
            if key == 'thunder_no_data_test':
                thunder_no_data_test = True
                metrics_match = False
                logger.info('thunder_no_data_test running get_top_level_namespaces')
                try:
                    parent_namespaces = get_top_level_namespaces(skyline_app, True)
                except Exception as e:
                    message = 'get_top_level_namespaces failed: %s' % e
                    trace = traceback.format_exc()
                    return internal_error(message, trace)
            if key == 'thunder_stale_metrics_test':
                thunder_stale_metrics_test = True
                metrics_match = False
                logger.info('thunder_stale_metrics_test running get_top_level_namespaces')
                try:
                    parent_namespaces = get_top_level_namespaces(skyline_app, True)
                except Exception as e:
                    message = 'get_top_level_namespaces failed: %s' % e
                    trace = traceback.format_exc()
                    return internal_error(message, trace)

    try:
        return render_template(
            'utilities.html', metrics_match=metrics_match,
            thunder_testing=thunder_testing,
            thunder_no_data_test=thunder_no_data_test,
            parent_namespaces=parent_namespaces,
            thunder_no_data_test_key_added=thunder_no_data_test_key_added,
            thunder_stale_metrics_test=thunder_stale_metrics_test,
            thunder_stale_metrics_test_key_added=thunder_stale_metrics_test_key_added,
            version=skyline_version, duration=(time.time() - start)), 200
    except Exception as e:
        trace = traceback.format_exc()
        message = 'failed to render utilities.html: %s' % e
        return internal_error(message, trace)


# @added 20200516 - Feature #3538: webapp - upload_data endpoint
#                   Feature #3550: flux.uploaded_data_worker
@app.route("/flux_frontend", methods=['GET'])
@requires_auth
def flux_frontend():
    debug_on = False

    # file_uploads_enabled = True
    logger.info('/flux_frontend request')
    start = time.time()

    try:
        request_args_len = len(request.args)
    except:
        request_args_len = 0

    FLUX_UPLOAD_DATA_REQUEST_ARGS = [
        'parent_metric_namespace', 'data_file_uploaded', 'info_file_uploaded',
        'upload_id', 'upload_id_key'
    ]

    parent_metric_namespace = None
    data_file_uploaded = None
    info_file_uploaded = None
    upload_id = None
    upload_id_key = None
    request_arguments = []

    if request_args_len:
        for i in request.args:
            key = str(i)
            if key not in FLUX_UPLOAD_DATA_REQUEST_ARGS:
                logger.error('error :: invalid request argument - %s=%s' % (key, str(i)))
                error_string = 'error :: invalid request argument - %s=%s' % (key, str(i))
                logger.error(error_string)
                resp = json.dumps(
                    {'400 Bad Request': error_string})
                return flask_escape(resp), 400
            value = request.args.get(key, None)
            request_arguments.append([key, value])
            logger.info('request argument - %s=%s' % (key, str(value)))
            if key == 'parent_metric_namespace':
                parent_metric_namespace = str(value)
            if key == 'data_file_uploaded':
                data_file_uploaded = str(value)
            if key == 'info_file_uploaded':
                info_file_uploaded = str(value)
            if key == 'upload_id':
                upload_id = str(value)
            if key == 'upload_id_key':
                upload_id_key = str(value)

    if not upload_id_key:
        if upload_id:
            upload_id_key = upload_id.replace('/', '.')

    # To enable uplaods via the webapp Flux endpoint using the
    # settings.FLUX_SELF_API_KEY a shortlived FLUX_UPLOADS_KEYS is created in
    # in Redis and passed the the upload_data template to submit as the key
    temporary_key = str(uuid.uuid4())
    redis_temporary_upload_key = 'flux.tmp.temporary_upload_key.%s' % temporary_key
    try:
        REDIS_CONN.setex(redis_temporary_upload_key, 600, str(temporary_key))
        logger.info('added Redis key %s' % redis_temporary_upload_key)
    except:
        trace = traceback.format_exc()
        message = 'could not add Redis key - %s' % redis_temporary_upload_key
        logger.error(trace)
        logger.error('error :: %s' % message)

    try:
        return render_template(
            'flux_frontend.html', upload_data_enabled=file_uploads_enabled,
            parent_metric_namespace=parent_metric_namespace,
            data_file_uploaded=data_file_uploaded,
            info_file_uploaded=info_file_uploaded,
            upload_id=upload_id, upload_id_key=upload_id_key,
            temporary_upload_key=temporary_key,
            flux_identifier=temporary_key,
            version=skyline_version, duration=(time.time() - start),
            print_debug=debug_on), 200
    except:
        message = 'Uh oh ... a Skyline 500 :('
        trace = traceback.format_exc()
        return internal_error(message, trace)


# @added 20200514 - Feature #3538: webapp - upload_data endpoint
#                   Feature #3550: flux.uploaded_data_worker
@app.route("/upload_data", methods=['POST'])
def upload_data():

    logger.info('/upload_data request')

    if not file_uploads_enabled:
        return 'Not Found', 404

    start = time.time()

    api_key = None
    parent_metric_namespace = None
    timezone = None
    format = None
    archive = None
    data_file = None
    info_file = None
    info_file_in_archive = False
    date_orientation = 'rows'
    skip_rows = None
    header_row = None
    columns_to_ignore = None
    columns_to_process = None
    resample_method = 'mean'
    json_response = False
    flux_identifier = None
    # @added 20200521 - Feature #3538: webapp - upload_data endpoint
    #                   Feature #3550: flux.uploaded_data_worker
    # Added the ability to ignore_submitted_timestamps and not
    # check flux.last metric timestamp
    ignore_submitted_timestamps = False

    upload_id = None
    data_dict = {}

    if request.method != 'POST':
        logger.error('error :: not a POST requests, returning 400')
        return 'Method Not Allowed', 405

    if request.method == 'POST':
        logger.info('handling upload_data POST request')
        if 'json_response' in request.form:
            json_response = request.form['json_response']
            if json_response == 'true':
                json_response = True
        logger.info('handling upload_data POST with variable json_response - %s' % str(json_response))

        # If there is no key a 401 is returned with no info
        if 'key' not in request.form:
            error_string = 'no key in the POST variables'
            logger.error('error :: ' + error_string)
            return 'Unauthorized', 401
        else:
            api_key = str(request.form['key'])
        logger.info('handling upload_data POST request with key - %s, using as api_key' % str(api_key))

        # Do not process requests that do not come from the Flux frontend if
        # they do not have json_response set.
        if 'flux_identifier' not in request.form:
            if not json_response:
                return 'Bad Request', 400
        flux_frontend_request = False
        if 'flux_identifier' in request.form:
            flux_identifier = str(request.form['flux_identifier'])
            if flux_identifier:
                flux_identifier_key = 'flux.tmp.temporary_upload_key.%s' % flux_identifier
                try:
                    flux_frontend_request = REDIS_CONN.get(flux_identifier_key)
                except:
                    trace = traceback.format_exc()
                    message = 'could query Redis for flux_identifier_key - %s' % flux_identifier_key
                    logger.error(trace)
                    logger.error('error :: %s' % message)
            if not flux_frontend_request:
                error_string = 'flux_identifier has expired or is not valid, please reload the Flux page'
                logger.info('flux_identifier - %s - has expired or is not valid, returning 400' % str(flux_identifier))
                data_dict = {"status": {"error": error_string},
                             "data": {
                                 "upload": "failed"}}
                return jsonify(data_dict), 400

        required_post_variables = [
            'parent_metric_namespace', 'timezone', 'format', 'archive',
            'date_orientation', 'header_row', 'columns_to_metrics',
            'info_file_in_archive'
        ]
        for r_var in required_post_variables:
            if r_var not in request.form:
                error_string = 'no %s in the POST variables' % r_var
                logger.error('error :: ' + error_string)
                data_dict = {"status": {"error": error_string},
                             "data": {
                                 "upload": "failed"}}
                return jsonify(data_dict), 400
        return_400 = False
        parent_metric_namespace = str(request.form['parent_metric_namespace'])
        if parent_metric_namespace == '':
            error_string = 'blank parent_metric_namespace variable passed'
            logger.error('error :: ' + error_string)
            return_400 = True

        known_key = None
        if flux_upload_keys:
            try:
                parent_metric_namespace_key = flux_upload_keys[parent_metric_namespace]
                logger.info('a key found for %s in flux_upload_keys' % parent_metric_namespace)
                if parent_metric_namespace_key == api_key:
                    known_key = api_key
                    logger.info('the key matches the key found for %s in flux_upload_keys' % parent_metric_namespace)
                else:
                    logger.info('the key variable passed does not match the key found for %s in flux_upload_keys' % parent_metric_namespace)
            except:
                logger.info('no known key found for %s in flux_upload_keys' % parent_metric_namespace)
                known_key = None
            # @added 20210114 - Task #3936: Allow for FLUX_API_KEYS rotation
            # Allow multiple keys per namespace to allow for key rotation
            if not known_key:
                try:
                    for i_key in flux_upload_keys:
                        parent_metric_namespace_for_key = flux_upload_keys[i_key]
                        if parent_metric_namespace_for_key == parent_metric_namespace:
                            known_key = i_key
                            logger.info('a key match for the key found for %s in flux_upload_keys was found' % parent_metric_namespace)
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: iterating flux_upload_keys')
                    known_key = None
                if not known_key:
                    logger.info('no key match was found for %s in flux_upload_keys' % parent_metric_namespace)

        if not known_key:
            redis_temporary_upload_key = 'flux.tmp.temporary_upload_key.%s' % api_key
            try:
                known_key = REDIS_CONN_UNDECODE.get(redis_temporary_upload_key)
                logger.info('attempt to check %s key in Redis got - %s' % (
                    redis_temporary_upload_key, str(known_key)))
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: could query Redis for %s' % redis_temporary_upload_key)
        if not known_key:
            logger.error('error :: unknown key passed %s' % api_key)
            return 'Unauthorized', 401

        if not return_400:
            logger.info('handling upload_data POST with variable parent_metric_namespace - %s' % str(parent_metric_namespace))
            timezone = str(request.form['timezone'])
            if timezone == '':
                error_string = '%s is not an accepted timezone' % timezone
                logger.error('error :: ' + error_string)
                return_400 = True
        if not return_400:
            logger.info('handling upload_data POST with variable timezone - %s' % str(timezone))
            format = str(request.form['format'])
            if format not in ALLOWED_FORMATS:
                error_string = '%s is not an accepted format' % format
                logger.error('error :: ' + error_string)
                return_400 = True
        if not return_400:
            logger.info('handling upload_data POST with variable format - %s' % str(format))
            archive = str(request.form['archive'])
            if archive not in ALLOWED_ARCHIVES:
                error_string = '%s is not an accepted archive type' % archive
                logger.error('error :: ' + error_string)
                return_400 = True
        if not return_400:
            logger.info('handling upload_data POST with variable archive - %s' % str(archive))
            date_orientation = str(request.form['date_orientation'])
            if date_orientation not in ['rows', 'columns']:
                error_string = '%s is not a valid date_orientation' % date_orientation
                logger.error('error :: ' + error_string)
                return_400 = True
        if not return_400:
            logger.info('handling upload_data POST with variable date_orientation - %s' % str(date_orientation))
            header_row_str = str(request.form['header_row'])
            try:
                header_row = int(header_row_str)
            except:
                error_string = '%s is not a valid header_row' % header_row_str
                logger.error('error :: ' + error_string)
                return_400 = True
        if not return_400:
            logger.info('handling upload_data POST with variable header_row - %s' % str(header_row))
            columns_to_metrics = str(request.form['columns_to_metrics'])
        if return_400:
            data_dict = {"status": {"error": error_string},
                         "data": {
                             "upload": "failed"}}
            return jsonify(data_dict), 400
        logger.info('handling upload_data POST with variable columns_to_metrics - %s' % str(columns_to_metrics))
        info_file_in_archive_str = str(request.form['info_file_in_archive'])
        if info_file_in_archive_str == 'true':
            info_file_in_archive = True
        logger.info('handling upload_data POST with variable info_file_in_archive - %s' % str(info_file_in_archive))

        info_filename = None
        data_filename = None
        # Check if the POST request has the data_file part
        no_data_file = False
        if 'data_file' not in request.files:
            no_data_file = True
            error_string = 'error :: no data_file in the POST'
            logger.error(error_string)
        if 'data_file' in request.files:
            data_file = request.files['data_file']
            if data_file.filename == '':
                error_string = 'error :: blank data_file variable'
                logger.error(error_string)
                no_data_file = True
        if no_data_file:
            data_dict = {"status": {"error": error_string},
                         "data": {
                             "upload": "failed"}}
            return jsonify(data_dict), 400

        create_info_file = False
        info_file_dict = None
        if 'skip_rows' in request.form:
            if request.form['skip_rows'] == 'none':
                skip_rows = None
            else:
                try:
                    skip_rows = int(request.form['skip_rows'])
                except:
                    error_string = 'skip_row must be none or int'
                    data_dict = {"status": {"error": error_string},
                                 "data": {
                                     "upload": "failed"}}
                    return jsonify(data_dict), 400
            logger.info('handling upload_data POST with variable skip_rows - %s' % str(skip_rows))
        if 'columns_to_ignore' in request.form:
            columns_to_ignore = str(request.form['columns_to_ignore'])
            logger.info('handling upload_data POST with variable columns_to_ignore - %s' % str(columns_to_ignore))
        if 'columns_to_process' in request.form:
            columns_to_process = str(request.form['columns_to_process'])
            logger.info('handling upload_data POST with variable columns_to_process - %s' % str(columns_to_process))
        resample_method = 'mean'
        if 'resample_method' in request.form:
            resample_method_str = str(request.form['resample_method'])
            if resample_method_str == 'sum':
                resample_method = 'sum'
                logger.info('handling upload_data POST with variable columns_to_process - %s' % str(columns_to_process))
        # @added 20200521 - Feature #3538: webapp - upload_data endpoint
        #                   Feature #3550: flux.uploaded_data_worker
        # Added the ability to ignore_submitted_timestamps and not
        # check flux.last metric timestamp
        if 'ignore_submitted_timestamps' in request.form:
            ignore_submitted_timestamps_str = request.form['ignore_submitted_timestamps']
            if ignore_submitted_timestamps_str == 'true':
                ignore_submitted_timestamps = True
                logger.info('handling upload_data POST with variable json_response - %s' % str(json_response))

        if 'info_file' not in request.files:
            create_info_file = True
        if 'info_file' in request.files:
            info_file = request.files['info_file']
            logger.info('handling upload_data POST with variable info_file - %s' % str(info_file))
            if info_file.filename == '':
                create_info_file = True
            else:
                create_info_file = False
        if create_info_file:
            logger.info('handling upload_data POST request with no info_file uploaded, attempting to create from variables')
            return_400 = False
            info_file_dict = {
                "parent_metric_namespace": parent_metric_namespace,
                "timezone": timezone,
                "archive": archive,
                "skip_rows": skip_rows,
                "header_row": header_row,
                "date_orientation": date_orientation,
                "columns_to_metrics": columns_to_metrics,
                "columns_to_ignore": columns_to_ignore,
                "columns_to_process": columns_to_process,
                "info_file_in_archive": info_file_in_archive,
                "resample_method": resample_method,
                "ignore_submitted_timestamps": ignore_submitted_timestamps,
            }

        info_file_saved = False
        parent_metric_namespace_dirname = secure_filename(parent_metric_namespace)
        upload_data_dir = '%s/%s/%s' % (DATA_UPLOADS_PATH, parent_metric_namespace_dirname, str(start))
        upload_id = '%s/%s' % (parent_metric_namespace_dirname, str(start))
        upload_id_key = '%s.%s' % (parent_metric_namespace_dirname, str(start))
        if not create_info_file:
            info_file = request.files['info_file']
            if info_file and allowed_file(info_file.filename):
                try:
                    info_filename = secure_filename(info_file.filename)
                    logger.info('handling upload_data POST request with info file - %s' % str(info_filename))
                    if not path.exists(upload_data_dir):
                        mkdir_p(upload_data_dir)
                    info_file.save(os.path.join(upload_data_dir, info_filename))
                    info_file_saved = True
                    logger.info('handling upload_data POST request saved info file - %s/%s' % (upload_data_dir, str(info_filename)))
                except:
                    message = 'failed to save info file'
                    trace = traceback.format_exc()
                    logger.error(trace)
                    logger.error(message)
                    if json_response:
                        return 'Internal Server Error', 500
                    else:
                        return internal_error(message, trace)

        data_filename = None
        if data_file and allowed_file(data_file.filename):
            try:
                data_filename = secure_filename(data_file.filename)
                logger.info('handling upload_data POST request with data file - %s' % str(data_filename))
                # @modified 20200520 - Bug #3552: flux.uploaded_data_worker - tar.gz
                # tar.gz needs more work
                if data_filename.endswith('tar.gz'):
                    error_string = 'error - tar.gz archives are not accepted'
                    logger.info('tar.gz archives are not accepted - %s, returning 400' % str(data_file))
                    data_dict = {"status": {"error": error_string},
                                 "data": {
                                     "upload": "failed"}}
                    return jsonify(data_dict), 400

                if not path.exists(upload_data_dir):
                    mkdir_p(upload_data_dir)
                data_file.save(os.path.join(upload_data_dir, data_filename))
                logger.info('handling upload_data POST request saved data file - %s/%s' % (upload_data_dir, str(data_filename)))
            except:
                trace = traceback.format_exc()
                message = 'failed to save data file'
                logger.error(trace)
                logger.error(message)
                if json_response:
                    return 'Internal Server Error', 500
                else:
                    return internal_error(message, trace)

        if info_file_dict and data_filename:
            info_filename = '%s.info.json' % data_filename
            logger.info('handling upload_data POST creating info file from POST variables - %s' % str(info_filename))
            if info_file_in_archive:
                logger.info('handling upload_data POST request with info_file_in_archive True, but still creating a parent info file from variables')
            if not path.exists(upload_data_dir):
                mkdir_p(upload_data_dir)
            info_file = '%s/%s' % (upload_data_dir, info_filename)
            # info_file_data = jsonify(info_file_dict)
            try:
                write_data_to_file(skyline_app, info_file, 'w', str(info_file_dict))
                logger.info('handling upload_data POST - saved inferred info file - %s' % info_file)
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: failed to save inferred info file - %s' % info_file)

        upload_data_dict = {
            "parent_metric_namespace": parent_metric_namespace,
            "timezone": timezone,
            "upload_id": upload_id,
            "status": 'pending',
            "format": format,
            "archive": archive,
            "data_filename": data_filename,
            "info_filename": info_filename,
            "info_file_in_archive": info_file_in_archive,
            "skip_rows": skip_rows,
            "header_row": header_row,
            "resample_method": resample_method,
            "ignore_submitted_timestamps": ignore_submitted_timestamps,
        }
        try:
            REDIS_CONN.sadd('flux.uploaded_data', str(upload_data_dict))
        except Exception as e:
            trace = traceback.format_exc()
            message = 'could not add item to flux.uploaded_data Redis set - %s' % str(e)
            logger.error(trace)
            logger.error('error :: %s' % message)
            if json_response:
                return 'Internal Server Error', 500
            else:
                return internal_error(message, trace)

        upload_status_redis_key = 'flux.upload_status.%s' % upload_id_key
        try:
            REDIS_CONN.setex(upload_status_redis_key, 2592000, str(upload_data_dict))
            logger.info('added Redis key %s with new status' % upload_status_redis_key)
        except Exception as e:
            trace = traceback.format_exc()
            message = 'could not add item to flux.uploaded_data Redis set - %s' % str(e)
            logger.error(trace)
            logger.error('error :: %s' % message)

        if json_response:
            data_dict = {"status": {},
                         "data": {
                             "feature maturity": "EXPERIMENTAL",
                             "upload": "successful",
                             "upload_id": upload_id,
                             "upload_id_key": upload_id_key,
                             "key": api_key,
                             "parent_metric_namespace": parent_metric_namespace,
                             "data file": data_filename}}
            if info_file_saved:
                data_dict['data']['info file'] = info_filename
            logger.info('responding to upload_data POST request with json response - %s' % str(data_dict))
            return jsonify(data_dict), 200
        if flux_frontend_request:
            return redirect(url_for('flux_frontend',
                                    parent_metric_namespace=parent_metric_namespace,
                                    data_file_uploaded=data_filename,
                                    info_file_uploaded=info_filename,
                                    upload_id=upload_id,
                                    upload_id_key=upload_id_key))
    return 'Bad Request', 400


# @added 20201212 - Feature #3880: webapp - utilities - match_metric
@app.route("/match_metric", methods=['POST'])
def match_metric():
    start = time.time()
    logger.info('/match_metric request')

    metric = None
    settings_list = None
    use_settings_list = None
    match_with = None

    if request.method != 'POST':
        logger.error('error :: not a POST requests, returning 400')
        return 'Method Not Allowed', 405

    if request.method == 'POST':
        logger.info('handling match_metric POST request')

        def get_settings(str):
            return getattr(sys.modules['settings'], str)

        if 'metric' in request.form:
            metric = request.form['metric']
            logger.info('handling match_metric POST with variable metric - %s' % str(metric))
        if not metric:
            logger.info('handling match_metric, return 400 no metric')
            return 'Bad request', 400
        if 'settings_list' in request.form:
            settings_list = request.form['settings_list']
            logger.info('handling match_metric POST with variable settings_list - %s' % str(settings_list))
        if not settings_list:
            settings_list = None
        if settings_list:
            use_settings_list = None
            try:
                use_settings_list = get_settings(settings_list)
            except Exception as e:
                trace = traceback.format_exc()
                message = 'could not interpolate settings_list - %s - %s' % (str(settings_list), e)
                logger.error(trace)
                logger.error('error :: %s' % message)
                return internal_error(message, trace)
        if not use_settings_list:
            if 'match_with' in request.form:
                match_with = request.form['match_with']
                logger.info('handling match_metric POST with variable match_with - %s' % str(match_with))
        if metric:
            if not use_settings_list:
                if not match_with:
                    logger.info('handling match_metric, return 400 no settings_list or match_with')
                    return 'Bad request', 400
        pattern_match = None
        try:
            if use_settings_list:
                pattern_match, matched_by = matched_or_regexed_in_list(skyline_app, metric, use_settings_list, True)
            if not use_settings_list and match_with:
                pattern_match, matched_by = matched_or_regexed_in_list(skyline_app, metric, [match_with], True)
        except Exception as e:
            trace = traceback.format_exc()
            message = 'could not determine if metric matched - error - %s' % (e)
            logger.error(trace)
            logger.error('error :: %s' % message)
            return internal_error(message, trace)
        if pattern_match is None:
            logger.info('handling match_metric, pattern_match is None, return 400')
            return 'Bad request', 400
        try:
            return render_template(
                'utilities.html', match_metric=True, metric=metric,
                settings_list=settings_list, match_with=match_with,
                pattern_match=pattern_match, matched_by=matched_by,
                version=skyline_version, duration=(time.time() - start),
                print_debug=False), 200
        except Exception as e:
            trace = traceback.format_exc()
            message = 'could not render_template utilities.html - error - %s' % (e)
            logger.error(trace)
            logger.error('error :: %s' % message)
            return internal_error(message, trace)


# @added 20210604 - Branch #1444: thunder
@app.route("/thunder_test", methods=['POST'])
def thunder_test():
    start = time.time()
    logger.info('/thunder_test request')

    thunder_test_type = None
    namespace = None
    stale_period = None
    expiry = None
    stale_count = None
    thunder_no_data_test = False
    thunder_stale_metrics_test = False
    thunder_no_data_test_key_added = None
    thunder_stale_metrics_test_key_added = None

    if request.method != 'POST':
        logger.error('error :: not a POST requests, returning 405')
        return 'Method Not Allowed', 405

    if request.method == 'POST':
        logger.info('handling thunder_test POST request')
        if 'thunder_test_type' in request.form:
            thunder_test_type = request.form['thunder_test_type']
            logger.info('handling thunder_test POST with variable thunder_test - %s' % str(thunder_test_type))
        if not thunder_test_type:
            logger.info('handling thunder_test, return 400 no thunder_test')
            return 'Bad request', 400
        if 'namespace' in request.form:
            namespace = request.form['namespace']
            logger.info('handling thunder_test POST with variable namespace - %s' % str(namespace))
        if not namespace:
            logger.info('handling thunder_test, return 400 no namespace')
            return 'Bad request', 400
        if 'stale_period' in request.form:
            stale_period = int(request.form['stale_period'])
            logger.info('handling thunder_test POST with variable stale_period - %s' % str(stale_period))
        if not stale_period:
            if stale_period != 0:
                logger.info('handling thunder_test, return 400 no stale_period')
                return 'Bad request', 400
        if 'stale_count' in request.form:
            stale_count = int(request.form['stale_count'])
            logger.info('handling stale_count POST with variable stale_count - %s' % str(stale_count))
        if not stale_count:
            if stale_count != 0:
                logger.info('handling thunder_test, return 400 no stale_count')
                return 'Bad request', 400
        if 'expiry' in request.form:
            expiry = int(request.form['expiry'])
            logger.info('handling thunder_test POST with variable expiry - %s' % str(expiry))
        if not expiry:
            if expiry != 0:
                logger.info('handling thunder_test, return 400 no expiry')
                return 'Bad request', 400
        if thunder_test_type == 'no_data':
            thunder_test_alert_key = 'thunder.test.alert.no_data.%s' % namespace
            data_dict = {'stale_period': stale_period, 'expiry': expiry}
        if thunder_test_type == 'stale_metrics':
            thunder_test_alert_key = 'thunder.test.alert.stale_metrics.%s' % namespace
            data_dict = {'stale_count': stale_count, 'stale_period': stale_period, 'expiry': expiry}

        thunder_test_key_added = False
        try:
            thunder_test_key_added = REDIS_CONN.setex(thunder_test_alert_key, expiry, str(data_dict))
            if thunder_test_key_added:
                thunder_test_key_added = thunder_test_alert_key
                if thunder_test_type == 'no_data':
                    thunder_no_data_test_key_added = thunder_test_alert_key
                    thunder_no_data_test = True
                if thunder_test_type == 'stale_metrics':
                    thunder_stale_metrics_test_key_added = thunder_test_alert_key
                    thunder_stale_metrics_test = True
            logger.info('added Redis key %s' % thunder_test_alert_key)
        except Exception as e:
            trace = traceback.format_exc()
            message = 'could not add Redis key %s - %s' % (thunder_test_alert_key, e)
            logger.error(trace)
            logger.error('error :: %s' % message)
            return internal_error(message, trace)
        try:
            return render_template(
                'utilities.html', match_metric=False,
                thunder_testing=True,
                thunder_no_data_test=thunder_no_data_test,
                thunder_no_data_test_key_added=thunder_no_data_test_key_added,
                thunder_stale_metrics_test=thunder_stale_metrics_test,
                thunder_stale_metrics_test_key_added=thunder_stale_metrics_test_key_added,
                version=skyline_version, duration=(time.time() - start),
                print_debug=False), 200
        except Exception as e:
            trace = traceback.format_exc()
            message = 'could not render_template utilities.html - error - %s' % (e)
            logger.error(trace)
            logger.error('error :: %s' % message)
            return internal_error(message, trace)


# @added 20220114 - Feature #4376: webapp - update_external_settings
@app.route('/update_external_settings', methods=['POST'])
@requires_auth
def update_external_settings():
    start = time.time()
    start_timer = timer()
    logger.info('/update_external_settings')
    key = None
    if request.method != 'POST':
        logger.error('error :: not a POST requests, returning 405')
        return 'Method Not Allowed', 405
    cluster_data = False
    if request.method == 'POST':
        post_data = {}
        try:
            post_data = request.get_json()
        except Exception as err:
            logger.error(traceback.format_exc())
            logger.error('error :: update_external_settings - no POST data - %s' % (
                err))
            logger.info('update_external_settings, return 400 no POST data')
            return 'Bad request', 400
        try:
            cluster_data = post_data['data']['cluster_data']
        except KeyError:
            cluster_data = False
        try:
            key = post_data['data']['key']
        except KeyError:
            key = None
        if not key:
            logger.info('update_external_settings, return 400 no key')
            return 'Bad request', 400
        if key != settings.FLUX_SELF_API_KEY:
            bad_key = str(key)
            logger.info('update_external_settings, return 401 bad key - %s' % bad_key)
            return 'Unauthorized', 401
    external_settings = {}
    external_from_cache = False
    remote_external_settings = {}
    if key:
        if settings.EXTERNAL_SETTINGS:
            logger.info('update_external_settings - managing external_settings')
            try:
                external_settings, external_from_cache = manage_external_settings(skyline_app)
            except Exception as err:
                logger.error(traceback.format_exc())
                logger.error('error :: update_external_settings - fetch_external_settings failed - %s' % (
                    err))
            if external_settings:
                logger.info('update_external_settings - %s external_settings from cache %s' % (
                    str(len(list(external_settings.keys()))), str(external_from_cache)))
                try:
                    REDIS_CONN.set('skyline.external_settings.update.flux', int(start))
                    logger.info('added Redis key skyline.external_settings.update.flux')
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: update_external_settings - failed to create Redis key skyline.external_settings.update.flux')
                # @added 20220117 - Feature #4324: flux - reload external_settings
                #                   Feature #4376: webapp - update_external_settings
                flux_pid_file = '%s/flux.pid' % settings.PID_PATH
                if os.path.isfile(flux_pid_file):
                    # logger.info('restarting flux')
                    # try:
                    #     restart_flux = subprocess.getstatusoutput('/usr/bin/systemctl restart flux')
                    #     if restart_flux:
                    #         if restart_flux[0] == 0:
                    #             logger.info('restarted flux OK -  exit code and output - %s' % str(restart_flux))
                    #         else:
                    #             logger.error('error :: failed to restart flux -  exit code and output - %s' % str(restart_flux))
                    # except Exception as err:
                    #     logger.error(traceback.format_exc())
                    #     logger.error('error :: update_external_settings - fetch_external_settings failed - %s' % (
                    #         err))
                    logger.info('initiating reload_flux')
                    try:
                        flux_pids = reload_flux(skyline_app, flux_pid_file)
                        if flux_pids:
                            logger.info('update_external_settings - reload_flux reports %s flux pids' % str(len(flux_pids)))
                    except Exception as err:
                        logger.error('error :: update_external_settings - reload_flux error - %s' % err)

        if not settings.REMOTE_SKYLINE_INSTANCES:
            cluster_data = False

        if cluster_data:
            endpoint_params = {
                'api_endpoint': '/update_external_settings',
                'data_required': 'updated',
                'only_host': 'all',
                'method': 'POST',
                'post_data': {
                    'data': {
                        'key': key,
                        'cluster_data': False,
                    }
                }
            }
            try:
                remote_external_settings = get_cluster_data('update_external_settings', 'updated', only_host='all', endpoint_params=endpoint_params)
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: webapp could not get update_external_settings from the remote Skyline instances')

    end = timer()
    request_time = (end - start_timer)

    if not cluster_data:
        if external_settings:
            status_code = 200
            data_dict = {"status": {"update_external_settings": "success", "response": 200, "request_time": request_time}, "data": {"updated": True}}
        else:
            status_code = 500
            data_dict = {"status": {"update_external_settings": "failed", "response": 500, "request_time": request_time}, "data": {"updated": False, "external_settings": external_settings}}
        return jsonify(data_dict), status_code
    if external_settings and remote_external_settings:
        status_code = 200
        data_dict = {"status": {"update_external_settings": "success", "response": 200, "request_time": request_time}, "data": {"updated": True, "remote_external_settings": remote_external_settings}}
    else:
        status_code = 500
        data_dict = {"status": {"update_external_settings": "failed", "response": 500, "request_time": request_time}, "data": {"updated": False, "remote_external_settings": remote_external_settings}}
    return jsonify(data_dict), status_code


# @added 20210316 - Feature #3978: luminosity - classify_metrics
#                   Feature #3642: Anomaly type classification
@app.route("/luminosity", methods=['GET'])
@requires_auth
def luminosity():
    debug_on = False

    # file_uploads_enabled = True
    logger.info('/luminosity request')
    start = time.time()

    try:
        request_args_len = len(request.args)
    except:
        request_args_len = 0

    LUMINOSITY_REQUEST_ARGS = [
        'classify_metrics', 'algorithm', 'metric', 'timestamp', 'significant',
        'classification', 'classify_metric',
        # @added 20210825 - Feature #4164: luminosity - cloudbursts
        'cloudbursts', 'base_name', 'namespaces', 'from_timestamp',
        'until_timestamp', 'cloudbursts_plot', 'plot_cloudburst',
        'cloudburst_id', 'all_in_period', 'shift',
        # @added 20211001 - Feature #4264: luminosity - cross_correlation_relationships
        'related_metrics', 'related_to_metrics', 'metric_id', 'cluster_data',
        'metric_related_metrics', 'namespaces',
    ]

    classify_metrics = False
    algorithm = 'all'
    metric = 'all'
    timestamp = 'all'
    significant = False
    classification = False
    classify_metric = False
    request_arguments = []

    # @added 20210825 - Feature #4164: luminosity - cloudbursts
    cloudbursts_request = False
    from_timestamp = 0
    until_timestamp = 0
    namespaces = []
    cloudbursts_dict = {}
    cloudburst_keys = []
    plot_cloudburst = False
    cloudbursts_plot = False
    cloudburst_id = 0
    all_in_period = False
    shift = 3

    # @added 20211001 - Feature #4264: luminosity - cross_correlation_relationships
    related_metrics = False
    related_to_metrics = False
    metric_related_metrics = False
    related_metrics_dict = {}
    related_metrics_keys = []
    related_to_metrics_dict = {}

    metric_id = 0
    namespaces = []

    if request_args_len == 0:
        classify_metrics = True
        timestamp = str(int(time.time()) - 86400)

    # IMPORTANT: cluster_data MUST be the first argument that is evaluated as it
    #            is used and required by many of the following API methods
    cluster_data = False
    if 'cluster_data' in request.args:
        try:
            cluster_data_argument = request.args.get('cluster_data', 'false')
            if cluster_data_argument == 'true':
                cluster_data = True
                logger.info('luminosity request with cluster_data=true')
            elif cluster_data_argument == 'false':
                cluster_data = False
                logger.info('luminosity request with cluster_data=false')
            else:
                logger.error('error :: api request with invalid cluster_data argument - %s' % str(cluster_data_argument))
                return 'Bad Request', 400
        except:
            logger.error('error :: /luminosity request with invalid cluster_data argument')
            return 'Bad Request', 400

    if request_args_len:
        for key in request.args:
            if key == 'classification':
                value = request.args.get(key, None)
                if str(value) == 'true':
                    classification = True

            # @added 20211001 - Feature #4264: luminosity - cross_correlation_relationships
            if key == 'related_metrics':
                value = request.args.get(key, 'false')
                if str(value) == 'true':
                    related_metrics = True
            if key == 'metric_related_metrics':
                value = request.args.get(key, 'false')
                if str(value) == 'true':
                    metric_related_metrics = True
            if key == 'related_to_metrics':
                value = request.args.get(key, 'false')
                if str(value) == 'true':
                    related_to_metrics = True
            if key == 'metric_id':
                value = request.args.get(key, 0)
                try:
                    metric_id = int(value)
                except Exception as err:
                    error_string = 'error :: invalid request argument - %s - %s' % (key, str(err))
                    logger.error(error_string)
                    resp = {'400 Bad Request': error_string}
                    return jsonify(resp), 400
            if key == 'namespaces':
                value = request.args.get(key, '')
                try:
                    namespaces_str = str(value)
                    namespaces = namespaces_str.split(',')
                    logger.info('filtering on namespaces: %s' % str(namespaces))

                except Exception as err:
                    error_string = 'error :: invalid request argument - %s - %s' % (key, str(err))
                    logger.error(error_string)
                    resp = {'400 Bad Request': error_string}
                    return jsonify(resp), 400

        if classification:
            algorithm = None
            metric = None

        for i in request.args:
            key = str(i)
            if key not in LUMINOSITY_REQUEST_ARGS:
                error_string = 'error :: invalid request argument - %s=%s' % (key, str(i))
                logger.error(error_string)
                resp = {'400 Bad Request': error_string}
                return jsonify(resp), 400
            value = request.args.get(key, None)
            request_arguments.append([key, value])
            if key == 'classify_metrics':
                classify_metrics = True
            if key == 'algorithm':
                algorithm = str(value)
            if key == 'metric':
                metric = str(value)
            if key == 'timestamp':
                timestamp = str(value)
            if key == 'significant':
                if str(value) == 'true':
                    significant = True
            if key == 'classification':
                if str(value) == 'true':
                    classification = True
            if key == 'classify_metric':
                if str(value) == 'true':
                    classify_metric = True

            # @added 20210825 - Feature #4164: luminosity - cloudbursts
            if key == 'cloudbursts':
                cloudbursts_request = True
            if not namespaces:
                if key == 'namespaces' and value != 'all':
                    try:
                        namespaces = value.split[',']
                    except TypeError:
                        logger.error('error :: TypeError from namespaces parameter' % str(value))
                        namespaces = [value]
            if key in ['from_timestamp', 'until_timestamp']:
                timestamp_format_invalid = True
                if value == 'all':
                    timestamp_format_invalid = False
                # unix timestamp
                if value.isdigit():
                    timestamp_format_invalid = False
                # %Y%m%d %H:%M timestamp
                if timestamp_format_invalid:
                    value_strip_colon = value.replace(':', '')
                    new_value = value_strip_colon.replace(' ', '')
                    if new_value.isdigit():
                        timestamp_format_invalid = False
                if timestamp_format_invalid:
                    error_string = 'error :: invalid %s value passed %s' % (key, value)
                    logger.error('error :: invalid %s value passed %s' % (key, value))
                    error_string = 'error :: invalid request argument - %s' % (key)
                    logger.error(error_string)
                    resp = json.dumps(
                        {'400 Bad Request': error_string})
                    return flask_escape(resp), 400
            if key == 'from_timestamp':
                from_timestamp = str(value)
                if ":" in from_timestamp:
                    new_from_timestamp = time.mktime(datetime.datetime.strptime(from_timestamp, '%Y%m%d %H:%M').timetuple())
                    from_timestamp = int(new_from_timestamp)
                elif from_timestamp == 'all':
                    from_timestamp = 0
                else:
                    from_timestamp = int(from_timestamp)
            if key == 'until_timestamp':
                until_timestamp = str(value)
                if ":" in until_timestamp:
                    new_until_timestamp = time.mktime(datetime.datetime.strptime(until_timestamp, '%Y%m%d %H:%M').timetuple())
                    from_timestamp = int(new_until_timestamp)
                elif until_timestamp == 'all':
                    until_timestamp = 0
                else:
                    until_timestamp = int(until_timestamp)
            if key == 'plot_cloudburst':
                if str(value) == 'true':
                    plot_cloudburst = True
            if key == 'cloudburst_id':
                cloudburst_id = int(value)
            if key == 'all_in_period':
                if str(value) == 'true':
                    all_in_period = True
            if key == 'shift':
                shift = int(value)

    logger.info('/luminosity request_arguments: %s' % str(request_arguments))

    valid_request = True
    if classify_metrics:
        if not metric or not timestamp or not algorithm:
            valid_request = False
        if not valid_request:
            status_code = 400
            data_dict = {"status": {"classify_metrics": True, "reason": "required parameter not passed", "response": 400}, "data": {"metric": metric, "timestamp": timestamp, "algorithm": algorithm}}
            return jsonify(data_dict), status_code
        if request_args_len == 1:
            timestamp = str(int(time.time()) - 86400)

    if classification and valid_request:
        if not metric or not algorithm:
            valid_request = False
        if not valid_request:
            status_code = 400
            data_dict = {"status": {"classification": True, "reason": "required parameter not passed", "response": 400}, "data": {"metric": metric, "algorithm": algorithm}}

    # @added 20210825 - Feature #4164: luminosity - cloudbursts
    if cloudbursts_request and valid_request:
        if not metric and not from_timestamp and not until_timestamp:
            valid_request = False
        if not valid_request:
            status_code = 400
            data_dict = {"status": {"cloudbursts": True, "reason": "required parameter not passed", "response": 400}, "data": {"metric": metric, "from_timestamp": from_timestamp, "until_timestamp": until_timestamp}}

    if not valid_request:
        return jsonify(data_dict), status_code

    classify_metrics_dict = {}
    classified_metrics = []
    if classify_metrics or classify_metric or classification:
        try:
            if metric != 'all':
                classify_metrics_dict, classified_metrics, fail_msg, trace = get_classify_metrics(metric, timestamp, algorithm, significant)
        except Exception as e:
            trace = traceback.format_exc()
            message = 'could not determine if metric matched - error - %s' % (e)
            logger.error(trace)
            logger.error('error :: %s' % message)
            return internal_error(message, trace)

    if classification:
        if metric == 'all' and algorithm == 'all':
            classification = False
            classify_metrics = True
            timestamp = 'all'

    classification_count = 0
    siginificant_count = 0
    if len(classify_metrics_dict) > 0:
        if metric != 'all':
            # classification_count = classify_metrics_dict[metric][algorithm]['classifications']
            # siginificant_count = classify_metrics_dict[metric][algorithm]['significant']
            for current_algorithm in list(classify_metrics_dict[metric].keys()):
                classification_count += classify_metrics_dict[metric][current_algorithm]['classifications']
                siginificant_count += classify_metrics_dict[metric][current_algorithm]['significant']
            classify_metrics = False
            classification = True

    # @added 20210825 - Feature #4164: luminosity - cloudbursts
    if cloudbursts_request:
        if from_timestamp == 0 and metric == 'all':
            from_timestamp = int(time.time()) - (86400 * 3)
        try:
            cloudbursts_dict = get_cloudbursts(metric, namespaces, from_timestamp, until_timestamp)
        except Exception as err:
            trace = traceback.format_exc()
            message = 'get_cloudbursts failed - %s' % (err)
            logger.error(trace)
            logger.error('error :: %s' % message)
            return internal_error(message, trace)
    if cloudbursts_dict:
        cloudburst_id = list(cloudbursts_dict.keys())[0]
        cloudburst_keys = list(cloudbursts_dict[cloudburst_id].keys())
        logger.info('returning %s cloudbursts with cloudburst_keys: %s' % (
            str(len(cloudbursts_dict)), str(cloudburst_keys)))

    # @added 20210826 - Feature #4164: luminosity - cloudbursts
    cloudburst_plot_image = None
    cloudburst_dict = {}
    if plot_cloudburst:
        try:
            cloudburst_dict, cloudburst_plot_image = get_cloudburst_plot(cloudburst_id, metric, shift, all_in_period)
        except Exception as err:
            trace = traceback.format_exc()
            message = 'get_cloudbursts failed - %s' % (err)
            logger.error(trace)
            logger.error('error :: %s' % message)
            return internal_error(message, trace)
        if cloudburst_dict:
            cloudburst_keys = list(cloudburst_dict.keys())

    # @added 20211004 - Feature #4264: luminosity - cross_correlation_relationships
    if related_metrics and not metric_id:
        try:
            metric_group_info = get_metric_group_info(skyline_app, 0, {'namespaces': namespaces})
            if metric_group_info:
                for metric_id in list(metric_group_info.keys()):
                    related_metrics_dict[metric_id] = metric_group_info[metric_id]
                    if not related_metrics_keys:
                        for key in list(related_metrics_dict[metric_id].keys()):
                            related_metrics_keys.append(key)
        except Exception as err:
            trace = traceback.format_exc()
            message = 'get_cloudbursts failed - %s' % (err)
            logger.error(trace)
            logger.error('error :: %s' % message)
            return internal_error(message, trace)

    metric_related_metrics_keys = []
    if metric_related_metrics:
        try:
            full_details = True
            if metric == 'all':
                metric = None
            else:
                if not metric_id:
                    try:
                        metric_id = get_metric_id_from_base_name(skyline_app, metric)
                    except Exception as err:
                        message = 'related_to_metric request :: get_metric_id_from_base_name failed to determine metric_id from metric: %s - %s' % (
                            str(metric), str(err))
                        logger.error('error :: %s' % message)
                        return internal_error(message, trace)
            metric_related_metrics = get_related_metrics(skyline_app, cluster_data, full_details, metric, metric_id)
            if metric_related_metrics:
                for related_metric in list(metric_related_metrics['related_metrics'].keys()):
                    if not metric_related_metrics_keys:
                        for key in list(metric_related_metrics['related_metrics'][related_metric].keys()):
                            metric_related_metrics_keys.append(key)
        except Exception as err:
            logger.error(traceback.format_exc())
            logger.error('error :: get_related_metrics failed to set metric_related_metrics - %s' % str(err))

    # @added 20211006 - Feature #4264: luminosity - cross_correlation_relationships
    related_to_metrics_keys = []
    if related_to_metrics:
        if not metric:
            error_string = 'error :: invalid request no metric parameter'
            logger.error(error_string)
            resp = {'400 Bad Request': error_string}
            return jsonify(resp), 400
        if metric != 'all':
            if not metric_id:
                try:
                    metric_id = get_metric_id_from_base_name(skyline_app, metric)
                except Exception as err:
                    message = 'related_to_metric request :: get_metric_id_from_base_name failed to determine metric_id from metric: %s - %s' % (
                        str(metric), str(err))
                    logger.error('error :: %s' % message)
                    return internal_error(message, trace)
            try:
                related_to_metrics_dict = get_related_to_metric_groups(skyline_app, metric, metric_id)
            except Exception as err:
                logger.error(traceback.format_exc())
                logger.error('error :: get_related_to_metric_groups failed - %s' % str(err))
            if not related_to_metrics_dict:
                trace = 'None'
                message = '%s - metric not found' % metric
                logger.error('error :: %s' % message)
                return internal_error(message, trace)

            if related_to_metrics_dict:
                try:
                    for related_to_metric in list(related_to_metrics_dict[metric]['related_to_metrics'].keys()):
                        if related_to_metrics_keys:
                            break
                        for key in list(related_to_metrics_dict[metric]['related_to_metrics'][related_to_metric].keys()):
                            related_to_metrics_keys.append(key)
                    logger.info('debug :: related_to_metrics_dict: %s' % str(related_to_metrics_dict))
                    logger.info('debug :: related_to_metrics_keys: %s' % str(related_to_metrics_keys))
                except Exception as err:
                    trace = traceback.format_exc()
                    message = 'get_related_to_metric_groups output failed - %s' % str(err)
                    logger.error(trace)
                    logger.error('error :: %s' % message)
                    return internal_error(message, trace)
        if metric == 'all':
            related_to_metrics_dict = True
            metric = None

    try:
        return render_template(
            'luminosity.html', classify_metrics=classify_metrics,
            metric=metric, timestamp=timestamp, algorithm=algorithm,
            classify_metrics_dict=classify_metrics_dict,
            classification_count=classification_count,
            siginificant_count=siginificant_count,
            classified_metrics=classified_metrics,
            classification=classification,
            classify_metric=classify_metric,
            # @added 20210825 - Feature #4164: luminosity - cloudbursts
            cloudbursts=cloudbursts_request, cloudbursts_dict=cloudbursts_dict,
            cloudburst_keys=cloudburst_keys, plot_cloudburst=plot_cloudburst,
            cloudburst_plot_image=cloudburst_plot_image,
            cloudburst_id=cloudburst_id, cloudburst_dict=cloudburst_dict,
            all_in_period=all_in_period,
            related_metrics=related_metrics_dict,
            related_metrics_keys=related_metrics_keys,
            metric_related_metrics=metric_related_metrics,
            metric_related_metrics_keys=metric_related_metrics_keys,
            namespaces=namespaces, metric_id=metric_id,
            related_to_metrics=related_to_metrics_dict,
            related_to_metrics_keys=related_to_metrics_keys,
            version=skyline_version, duration=(time.time() - start),
            print_debug=debug_on), 200
    except:
        message = 'Uh oh ... a Skyline 500 :('
        trace = traceback.format_exc()
        return internal_error(message, trace)


# @added 20211102 - Branch #3068: SNAB
#                   Bug #4308: matrixprofile - fN on big drops
@app.route('/snab', methods=['GET'])
def snab():

    logger.info('/snab request')
    start = time.time()
    results_data = {}
    filter_on = {}
    filter_on['default'] = True
    filter_on['algorithm'] = None
    filter_on['algorithm_group'] = None

    algorithms = {}
    try:
        algorithms = get_algorithms(skyline_app)
    except Exception as err:
        logger.error(traceback.format_exc())
        logger.error('error :: /snab :: failed to get algorithms - %s' % str(err))

    algorithm_groups = {}
    try:
        algorithm_groups = get_algorithm_groups(skyline_app)
    except Exception as err:
        logger.error(traceback.format_exc())
        logger.error('error :: /snab :: failed to get algorithm_groups - %s' % str(err))

    namespaces = []
    filter_on['namespaces'] = namespaces
    if 'namespaces' in request.args:
        namespaces_list = []
        namespaces = request.args.get('namespaces', 'all')
        if namespaces != 'all':
            namespaces_list = namespaces.split(',')
        filter_on['namespaces'] = namespaces_list
        filter_on['default'] = False

    algorithm = None
    algorithm_id = 0
    filter_on['algorithm_id'] = algorithm_id
    if 'algorithm' in request.args:
        algorithm = request.args.get('algorithm', 'all')
        if algorithm != 'all':
            try:
                algorithm_id = algorithms[algorithm]
            except KeyError:
                message = 'Unknown algorithm - %s, no ID found' % str(algorithm)
                trace = traceback.format_exc()
                return internal_error(message, trace)
            if algorithm_id:
                filter_on['algorithm_id'] = algorithm_id
                filter_on['default'] = False
                filter_on['algorithm'] = algorithm

    algorithm_group = None
    algorithm_group_id = 0
    filter_on['algorithm_group_id'] = algorithm_group_id
    if 'algorithm_group' in request.args:
        algorithm_group = request.args.get('algorithm_group', 'all')
        if algorithm_group != 'all':
            try:
                algorithm_group_id = algorithm_groups[algorithm_group]
            except KeyError:
                message = 'Unknown algorithm_group - %s, no ID found' % str(algorithm_group)
                trace = traceback.format_exc()
                return internal_error(message, trace)
            if algorithm_group_id:
                filter_on['algorithm_group_id'] = algorithm_group_id
                filter_on['default'] = False
                filter_on['algorithm_group'] = algorithm_group

    from_timestamp = 0
    filter_on['from_timestamp'] = from_timestamp
    if 'from_timestamp' in request.args:
        from_timestamp = request.args.get('from_timestamp', 0)
        if from_timestamp:
            if ":" in from_timestamp:
                try:
                    new_from_timestamp = time.mktime(datetime.datetime.strptime(from_timestamp, '%Y-%m-%d %H:%M').timetuple())
                except Exception as err:
                    trace = traceback.format_exc()
                    logger.error('%s' % trace)
                    fail_msg = 'error :: /snab :: failed to unix timestamp from from_timestamp - %s' % str(err)
                    logger.error('%s' % fail_msg)
                    return internal_error(fail_msg, trace)
                from_timestamp = int(new_from_timestamp)
            filter_on['from_timestamp'] = int(from_timestamp)
    if not from_timestamp:
        try:
            from_timestamp = int(from_timestamp)
        except Exception as err:
            trace = traceback.format_exc()
            logger.error('%s' % trace)
            fail_msg = 'error :: /snab :: no from_timestamp parameter was passed - %s' % str(err)
            logger.error('%s' % fail_msg)
            return internal_error(fail_msg, trace)

    until_timestamp = 0
    if 'until_timestamp' in request.args:
        until_timestamp = request.args.get('until_timestamp', 0)
        if until_timestamp:
            if ":" in until_timestamp:
                try:
                    new_until_timestamp = time.mktime(datetime.datetime.strptime(until_timestamp, '%Y-%m-%d %H:%M').timetuple())
                except Exception as err:
                    trace = traceback.format_exc()
                    logger.error('%s' % trace)
                    fail_msg = 'error :: snab :: failed to unix timestamp from until_timestamp - %s' % str(err)
                    logger.error('%s' % fail_msg)
                    return internal_error(fail_msg, trace)
                until_timestamp = int(new_until_timestamp)
            else:
                until_timestamp = int(until_timestamp)
    if not until_timestamp:
        until_timestamp = int(time.time())

    filter_on['until_timestamp'] = int(until_timestamp)

    result = None
    filter_on['result'] = result
    if 'result' in request.args:
        result = request.args.get('result', 'all')
        if result in ['tP', 'fP', 'fN', 'tN', 'unsure']:
            filter_on['result'] = result
            filter_on['default'] = False

    plot = False
    if 'plot' in request.args:
        plot = request.args.get('plot', 'false')
        if plot == 'true':
            plot = True
    filter_on['plot'] = plot

    if not filter_on['default']:
        try:
            results_data = get_snab_results(filter_on)
        except Exception as err:
            trace = traceback.format_exc()
            logger.error('%s' % trace)
            fail_msg = 'error :: snab :: failed to unix timestamp from until_timestamp - %s' % str(err)
            logger.error('%s' % fail_msg)
            return internal_error(fail_msg, trace)
    results_data_keys = []
    do_not_use_keys = ['runtime', 'app_id', 'snab_timestamp', 'slack_thread_ts', 'plot']
    if results_data:
        snab_id = list(results_data.keys())[0]
        for key in list(results_data[snab_id].keys()):
            if key not in do_not_use_keys:
                results_data_keys.append(key)
    try:
        return render_template(
            'snab.html', algorithms=algorithms,
            algorithm_groups=algorithm_groups,
            namespaces=namespaces, from_timestamp=from_timestamp,
            until_timestamp=until_timestamp, result=result,
            results_data=results_data, results_data_keys=results_data_keys,
            filter_on=filter_on, plot=plot,
            version=skyline_version, duration=(time.time() - start)), 200
    except:
        message = 'Uh oh ... a Skyline 500 :('
        trace = traceback.format_exc()
        return internal_error(message, trace)


# @added 20210612 - Branch #1444: thunder
@app.route('/webapp_up', methods=['GET'])
def webapp_up():

    logger.info('/webapp_up request')
    start = time.time()
    timestamp = int(time.time())
    thunder_key_set = None
    try:
        thunder_key_set = REDIS_CONN.setex('webapp', 120, timestamp)
        logger.info('added Redis key webapp')
    except Exception as e:
        trace = traceback.format_exc()
        message = 'could not add item to flux.uploaded_data Redis set - %s' % str(e)
        logger.error(trace)
        logger.error('error :: %s' % message)
    end = timer()
    request_time = (end - start)
    if thunder_key_set:
        data_dict = {"status": {"request_time": request_time, "response": 200}, "data": {'up': True, 'timestamp': timestamp}}
        return jsonify(data_dict), 200
    else:
        data_dict = {"status": {"request_time": request_time, "response": 404}, "data": {'up': False, 'timestamp': timestamp}}
        return jsonify(data_dict), 404


# @added 20160703 - Feature #1464: Webapp Redis browser
# A port of Marian Steinbach's rebrow - https://github.com/marians/rebrow
# Description of info keys
# TODO: to be continued.
serverinfo_meta = {
    'aof_current_rewrite_time_sec': "Duration of the on-going <abbr title='Append-Only File'>AOF</abbr> rewrite operation if any",
    'aof_enabled': "Flag indicating <abbr title='Append-Only File'>AOF</abbr> logging is activated",
    'aof_last_bgrewrite_status': "Status of the last <abbr title='Append-Only File'>AOF</abbr> rewrite operation",
    'aof_last_rewrite_time_sec': "Duration of the last <abbr title='Append-Only File'>AOF</abbr> rewrite operation in seconds",
    'aof_last_write_status': "Status of last <abbr title='Append-Only File'>AOF</abbr> write operation",
    'aof_rewrite_in_progress': "Flag indicating a <abbr title='Append-Only File'>AOF</abbr> rewrite operation is on-going",
    'aof_rewrite_scheduled': "Flag indicating an <abbr title='Append-Only File'>AOF</abbr> rewrite operation will be scheduled once the on-going RDB save is complete",
    'arch_bits': 'Architecture (32 or 64 bits)',
    'blocked_clients': 'Number of clients pending on a blocking call (BLPOP, BRPOP, BRPOPLPUSH)',
    'client_biggest_input_buf': 'biggest input buffer among current client connections',
    'client_longest_output_list': None,
    'cmdstat_client': 'Statistics for the client command',
    'cmdstat_config': 'Statistics for the config command',
    'cmdstat_dbsize': 'Statistics for the dbsize command',
    'cmdstat_del': 'Statistics for the del command',
    'cmdstat_dump': 'Statistics for the dump command',
    'cmdstat_expire': 'Statistics for the expire command',
    'cmdstat_flushall': 'Statistics for the flushall command',
    'cmdstat_get': 'Statistics for the get command',
    'cmdstat_hgetall': 'Statistics for the hgetall command',
    'cmdstat_hkeys': 'Statistics for the hkeys command',
    'cmdstat_hmset': 'Statistics for the hmset command',
    'cmdstat_info': 'Statistics for the info command',
    'cmdstat_keys': 'Statistics for the keys command',
    'cmdstat_llen': 'Statistics for the llen command',
    'cmdstat_ping': 'Statistics for the ping command',
    'cmdstat_psubscribe': 'Statistics for the psubscribe command',
    'cmdstat_pttl': 'Statistics for the pttl command',
    'cmdstat_sadd': 'Statistics for the sadd command',
    'cmdstat_scan': 'Statistics for the scan command',
    'cmdstat_select': 'Statistics for the select command',
    'cmdstat_set': 'Statistics for the set command',
    'cmdstat_smembers': 'Statistics for the smembers command',
    'cmdstat_sscan': 'Statistics for the sscan command',
    'cmdstat_ttl': 'Statistics for the ttl command',
    'cmdstat_type': 'Statistics for the type command',
    'cmdstat_zadd': 'Statistics for the zadd command',
    'cmdstat_zcard': 'Statistics for the zcard command',
    'cmdstat_zrange': 'Statistics for the zrange command',
    'cmdstat_zremrangebyrank': 'Statistics for the zremrangebyrank command',
    'cmdstat_zrevrange': 'Statistics for the zrevrange command',
    'cmdstat_zscan': 'Statistics for the zscan command',
    'config_file': None,
    'connected_clients': None,
    'connected_slaves': None,
    'db0': None,
    'evicted_keys': None,
    'expired_keys': None,
    'gcc_version': None,
    'hz': None,
    'instantaneous_ops_per_sec': None,
    'keyspace_hits': None,
    'keyspace_misses': None,
    'latest_fork_usec': None,
    'loading': None,
    'lru_clock': None,
    'master_repl_offset': None,
    'mem_allocator': None,
    'mem_fragmentation_ratio': None,
    'multiplexing_api': None,
    'os': None,
    'process_id': None,
    'pubsub_channels': None,
    'pubsub_patterns': None,
    'rdb_bgsave_in_progress': None,
    'rdb_changes_since_last_save': None,
    'rdb_current_bgsave_time_sec': None,
    'rdb_last_bgsave_status': None,
    'rdb_last_bgsave_time_sec': None,
    'rdb_last_save_time': None,
    'redis_build_id': None,
    'redis_git_dirty': None,
    'redis_git_sha1': None,
    'redis_mode': None,
    'redis_version': None,
    'rejected_connections': None,
    'repl_backlog_active': None,
    'repl_backlog_first_byte_offset': None,
    'repl_backlog_histlen': None,
    'repl_backlog_size': None,
    'role': None,
    'run_id': None,
    'sync_full': None,
    'sync_partial_err': None,
    'sync_partial_ok': None,
    'tcp_port': None,
    'total_commands_processed': None,
    'total_connections_received': None,
    'uptime_in_days': None,
    'uptime_in_seconds': None,
    'used_cpu_sys': None,
    'used_cpu_sys_children': None,
    'used_cpu_user': None,
    'used_cpu_user_children': None,
    'used_memory': None,
    'used_memory_human': None,
    'used_memory_lua': None,
    'used_memory_peak': None,
    'used_memory_peak_human': None,
    'used_memory_rss': None
}


# @added 20180520 - Feature #2378: Add redis auth to Skyline and rebrow
# Added auth to rebrow as per https://github.com/marians/rebrow/pull/20 by
# elky84
# @modified 20191014 - Bug #3266: py3 Redis binary objects not strings
#                      Branch #3262: py3
# def get_redis(host, port, db, password):
def get_redis(host, port, db, password, decode):
    if password == "":
        # @modified 20190517 - Branch #3002: docker
        # Allow rebrow to connect to Redis on the socket too
        if host == 'unix_socket':
            # @modified 20191014 - Bug #3266: py3 Redis binary objects not strings
            #                      Branch #3262: py3
            # return redis.StrictRedis(unix_socket_path=settings.REDIS_SOCKET_PATH, db=db)
            if decode:
                return redis.StrictRedis(unix_socket_path=settings.REDIS_SOCKET_PATH, db=db, charset='utf-8', decode_responses=True)
            else:
                return redis.StrictRedis(unix_socket_path=settings.REDIS_SOCKET_PATH, db=db)
        else:
            # @modified 20191014 - Bug #3266: py3 Redis binary objects not strings
            #                      Branch #3262: py3
            # return redis.StrictRedis(host=host, port=port, db=db)
            if decode:
                return redis.StrictRedis(host=host, port=port, db=db, charset='utf-8', decode_responses=True)
            else:
                return redis.StrictRedis(host=host, port=port, db=db)
    else:
        if host == 'unix_socket':
            # @modified 20191014 - Bug #3266: py3 Redis binary objects not strings
            #                      Branch #3262: py3
            # return redis.StrictRedis(password=settings.REDIS_PASSWORD, unix_socket_path=settings.REDIS_SOCKET_PATH, db=db)
            if decode:
                return redis.StrictRedis(password=settings.REDIS_PASSWORD, unix_socket_path=settings.REDIS_SOCKET_PATH, db=db, charset='utf-8', decode_responses=True)
            else:
                return redis.StrictRedis(password=settings.REDIS_PASSWORD, unix_socket_path=settings.REDIS_SOCKET_PATH, db=db)
        else:
            # @modified 20191014 - Bug #3266: py3 Redis binary objects not strings
            #                      Branch #3262: py3
            # return redis.StrictRedis(host=host, port=port, db=db, password=password)
            if decode:
                return redis.StrictRedis(host=host, port=port, db=db, password=password, charset='utf-8', decode_responses=True)
            else:
                return redis.StrictRedis(host=host, port=port, db=db, password=password)


# @added 20180527 - Feature #2378: Add redis auth to Skyline and rebrow
# Added token, client_id and salt to replace password parameter and determining
# client protocol
def get_client_details():
    """
    Gets the first X-Forwarded-For address and sets as the IP address.
    Gets the client_id by simply using a md5 hash of the client IP address
    and user agent.
    Determines whether the request was proxied.
    Determines the client protocol.

    :return: client_id, protocol, proxied, salt
    :rtype: str, str, boolean, str

    """
    proxied = False
    if request.headers.getlist('X-Forwarded-For'):
        client_ip = str(request.headers.getlist('X-Forwarded-For')[0])
        logger.info('rebrow access :: client ip set from X-Forwarded-For[0] to %s' % (str(client_ip)))
        proxied = True
    else:
        client_ip = str(request.remote_addr)
        logger.info('rebrow access :: client ip set from remote_addr to %s, no X-Forwarded-For header was found' % (str(client_ip)))
    client_user_agent = request.headers.get('User-Agent')
    logger.info('rebrow access :: %s client_user_agent set to %s' % (str(client_ip), str(client_user_agent)))
    client_id = '%s_%s' % (client_ip, client_user_agent)
    if python_version == 2:
        # @modified 20200808 - Task #3608: Update Skyline to Python 3.8.3 and deps
        # Added nosec for bandit [B303:blacklist] Use of insecure MD2, MD4, MD5,
        # or SHA1 hash function.  For internal use only.
        # @modified 20210421 - Task #4030: refactoring
        # semgrep - python.lang.security.insecure-hash-algorithms.insecure-hash-algorithm-md5
        # client_id = hashlib.md5(client_id).hexdigest()  # nosec
        client_id = hashlib.sha256(client_id).hexdigest()
    else:
        # @modified 20200808 - Task #3608: Update Skyline to Python 3.8.3 and deps
        # Added nosec for bandit [B303:blacklist] Use of insecure MD2, MD4, MD5,
        # or SHA1 hash function.  For internal use only.
        # @modified 20210421 - Task #4030: refactoring
        # semgrep - python.lang.security.insecure-hash-algorithms.insecure-hash-algorithm-md5
        # client_id = hashlib.md5(client_id.encode('utf-8')).hexdigest()  # nosec
        client_id = hashlib.sha256(client_id.encode('utf-8')).hexdigest()

    logger.info('rebrow access :: %s has client_id %s' % (str(client_ip), str(client_id)))

    if request.headers.getlist('X-Forwarded-Proto'):
        protocol_list = request.headers.getlist('X-Forwarded-Proto')
        protocol = str(protocol_list[0])
        logger.info('rebrow access :: protocol for %s was set from X-Forwarded-Proto to %s' % (client_ip, str(protocol)))
    else:
        protocol = 'unknown'
        logger.info('rebrow access :: protocol for %s was not set from X-Forwarded-Proto to %s' % (client_ip, str(protocol)))

    if not proxied:
        logger.info('rebrow access :: Skyline is not set up correctly, the expected X-Forwarded-For header was not found')

    return client_id, protocol, proxied


def decode_token(client_id):
    """
    Use the app.secret, client_id and salt to decode the token JWT encoded
    payload and determine the Redis password.

    :param client_id: the client_id string
    :type client_id: str
    :return: token, decoded_redis_password, fail_msg, trace
    :rtype: str, str, str, str

    """
    fail_msg = False
    trace = False
    token = False
    # @modified 20210421 - Task #4030: refactoring
    # semgrep - python-logger-credential-disclosure
    # logger.info('decode_token for client_id - %s' % str(client_id))
    logger.info('decode_token for client_id OK')

    if not request.args.getlist('token'):
        fail_msg = 'No token url parameter was passed, please log into Redis again through rebrow'
    else:
        token = request.args.get('token', type=str)
        # @modified 20210421 - Task #4030: refactoring
        # semgrep - python-logger-credential-disclosure
        # logger.info('token found in request.args - %s' % str(token))
        logger.info('token found in request.args - OK')

    if not token:
        client_id, protocol, proxied = get_client_details()
        fail_msg = 'No token url parameter was passed, please log into Redis again through rebrow'
        trace = 'False'

    client_token_data = False
    if token:
        try:
            if settings.REDIS_PASSWORD:
                # @modified 20191014 - Bug #3266: py3 Redis binary objects not strings
                #                      Branch #3262: py3
                # redis_conn = redis.StrictRedis(password=settings.REDIS_PASSWORD, unix_socket_path=settings.REDIS_SOCKET_PATH)
                redis_conn = redis.StrictRedis(password=settings.REDIS_PASSWORD, unix_socket_path=settings.REDIS_SOCKET_PATH, charset='utf-8', decode_responses=True)
            else:
                # redis_conn = redis.StrictRedis(unix_socket_path=settings.REDIS_SOCKET_PATH)
                redis_conn = redis.StrictRedis(unix_socket_path=settings.REDIS_SOCKET_PATH, charset='utf-8', decode_responses=True)
            key = 'rebrow.token.%s' % token
            client_token_data = redis_conn.get(key)
        except:
            trace = traceback.format_exc()
            fail_msg = 'Failed to get client_token_data from Redis key - %s' % key
            logger.error('%s' % trace)
            logger.error('%s' % fail_msg)
            client_token_data = False
            token = False

    client_id_match = False
    # @modified 20191014 - Bug #3266: py3 Redis binary objects not strings
    #                      Branch #3262: py3
    # if client_token_data is not None:
    if client_token_data:
        # @modified 20210421 - Task #4030: refactoring
        # semgrep - python-logger-credential-disclosure
        # logger.info('client_token_data retrieved from Redis - %s' % str(client_token_data))
        logger.info('client_token_data retrieved from Redis - OK')
        try:
            client_data = literal_eval(client_token_data)
            # @modified 20210421 - Task #4030: refactoring
            # semgrep - python-logger-credential-disclosure
            # logger.info('client_token_data - %s' % str(client_token_data))
            logger.info('client_token_data - OK')
            client_data_client_id = str(client_data[0])
            logger.info('client_data_client_id - %s' % str(client_data_client_id))
        except:
            trace = traceback.format_exc()
            logger.error('%s' % trace)
            err_msg = 'error :: failed to get client data from Redis key'
            logger.error('%s' % err_msg)
            fail_msg = 'Invalid token. Please log into Redis through rebrow again.'
            client_data_client_id = False

        if client_data_client_id != client_id:
            logger.error(
                # @modified 20210421 - Task #4030: refactoring
                # semgrep - python-logger-credential-disclosure
                # 'rebrow access :: error :: the client_id does not match the client_id of the token - %s - %s' %
                # (str(client_data_client_id), str(client_id)))
                'rebrow access :: error :: the client_id does not match the client_id of the token')
            try:
                if settings.REDIS_PASSWORD:
                    # @modified 20191014 - Bug #3266: py3 Redis binary objects not strings
                    #                      Branch #3262: py3
                    # redis_conn = redis.StrictRedis(password=settings.REDIS_PASSWORD, unix_socket_path=settings.REDIS_SOCKET_PATH)
                    redis_conn = redis.StrictRedis(password=settings.REDIS_PASSWORD, unix_socket_path=settings.REDIS_SOCKET_PATH, charset='utf-8', decode_responses=True)
                else:
                    # redis_conn = redis.StrictRedis(unix_socket_path=settings.REDIS_SOCKET_PATH)
                    redis_conn = redis.StrictRedis(unix_socket_path=settings.REDIS_SOCKET_PATH, charset='utf-8', decode_responses=True)
                key = 'rebrow.token.%s' % token
                redis_conn.delete(key)
                # @modified 20210421 - Task #4030: refactoring
                # semgrep - python-logger-credential-disclosure
                # logger.info('due to possible attempt at unauthorised use of the token, deleted the Redis key - %s' % str(key))
                logger.info('due to possible attempt at unauthorised use of the token, deleted the Redis key')
            except:
                pass
            fail_msg = 'The request data did not match the token data, due to possible attempt at unauthorised use of the token it has been deleted.'
            trace = 'this was a dodgy request'
            token = False
        else:
            client_id_match = True
    else:
        fail_msg = 'Invalid token, there was no data found associated with the token, it has probably expired.  Please log into Redis again through rebrow'
        trace = client_token_data
        token = False

    client_data_salt = False
    client_data_jwt_payload = False
    if client_id_match:
        client_data_salt = str(client_data[1])
        client_data_jwt_payload = str(client_data[2])

    decoded_redis_password = False
    if client_data_salt and client_data_jwt_payload:
        try:
            jwt_secret = '%s.%s.%s' % (app.secret_key, client_id, client_data_salt)
            jwt_decoded_dict = jwt.decode(client_data_jwt_payload, jwt_secret, algorithms=['HS256'])
            jwt_decoded_redis_password = str(jwt_decoded_dict['auth'])
            decoded_redis_password = jwt_decoded_redis_password
        except:
            trace = traceback.format_exc()
            logger.error('%s' % trace)
            err_msg = 'error :: failed to decode the JWT token with the salt and client_id'
            logger.error('%s' % err_msg)
            fail_msg = 'failed to decode the JWT token with the salt and client_id. Please log into rebrow again.'
            token = False

    return token, decoded_redis_password, fail_msg, trace


@app.route('/rebrow', methods=['GET', 'POST'])
@requires_auth
# def login():
def rebrow():
    """
    Start page
    """
    if request.method == 'POST':
        # TODO: test connection, handle failures
        host = str(request.form['host'])
        port = int(request.form['port'])
        db = int(request.form['db'])
        # @modified 20180520 - Feature #2378: Add redis auth to Skyline and rebrow
        # Added auth to rebrow as per https://github.com/marians/rebrow/pull/20 by
        # elky84
        # url = url_for('rebrow_server_db', host=host, port=port, db=db)
        password = str(request.form['password'])

        # @added 20180529 - Feature #2378: Add redis auth to Skyline and rebrow
        token_valid_for = int(request.form['token_valid_for'])
        # if token_valid_for > 3600:
        #     token_valid_for = 3600
        if token_valid_for > 86400:
            token_valid_for = 86400
        if token_valid_for < 30:
            token_valid_for = 30

        # @added 20180520 - Feature #2378: Add redis auth to Skyline and rebrow
        # Added auth to rebrow as per https://github.com/marians/rebrow/pull/20 by
        # elky84 and add encryption to the password URL parameter trying to use
        # pycrypto/pycryptodome to encode it, but no, used PyJWT instead
        # padded_password = password.rjust(32)
        # secret_key = '1234567890123456' # create new & store somewhere safe
        # cipher = AES.new(app.secret_key,AES.MODE_ECB) # never use ECB in strong systems obviously
        # encoded = base64.b64encode(cipher.encrypt(padded_password))

        # @added 20180527 - Feature #2378: Add redis auth to Skyline and rebrow
        # Added client_id, token and salt
        salt = str(uuid.uuid4())
        client_id, protocol, proxied = get_client_details()

        # @added 20180526 - Feature #2378: Add redis auth to Skyline and rebrow
        # Use pyjwt - JSON Web Token implementation to encode the password and
        # pass a token in the URL password parameter, the password in the POST
        # data should be encrypted via the reverse proxy SSL endpoint
        # encoded = jwt.encode({'some': 'payload'}, 'secret', algorithm='HS256')
        # jwt.decode(encoded, 'secret', algorithms=['HS256'])
        # {'some': 'payload'}
        try:
            jwt_secret = '%s.%s.%s' % (app.secret_key, client_id, salt)
            jwt_encoded_payload = jwt.encode({'auth': str(password)}, jwt_secret, algorithm='HS256')
            # @added 20191014 - Bug #3268: webapp - rebrow - jwt.encode generating bytes instead of a string in py3
            #                   Branch #3262: py3
            # As per https://github.com/jpadilla/pyjwt/issues/391 - jwt.encode generating bytes instead of a string
            if python_version == 3:
                # @modified 20220110 - Task #4344: Update dependencies
                #                      Task #4362: snyk - numpy and pillow updates
                # PyJWT change no longer str
                # jwt_encoded_payload = jwt_encoded_payload.decode('UTF-8')
                # AttributeError: 'str' object has no attribute 'decode'
                # jwt_encoded_payload = jwt_encoded_payload.decode('UTF-8')
                jwt_encoded_payload = jwt_encoded_payload
        except:
            message = 'Failed to create set jwt_encoded_payload for %s' % client_id
            trace = traceback.format_exc()
            return internal_error(message, trace)

        # HERE WE WANT TO PUT THIS INTO REDIS with a TTL key and give the key
        # a salt and have the client use that as their token
        client_token = str(uuid.uuid4())
        # @modified 20210421 - Task #4030: refactoring
        # semgrep - python-logger-credential-disclosure
        # logger.info('rebrow access :: generated client_token %s for client_id %s' % (client_token, client_id))
        logger.info('rebrow access :: generated client_token for client_id OK')

        try:
            if settings.REDIS_PASSWORD:
                # @modified 20191014 - Bug #3266: py3 Redis binary objects not strings
                #                      Branch #3262: py3
                # redis_conn = redis.StrictRedis(password=settings.REDIS_PASSWORD, unix_socket_path=settings.REDIS_SOCKET_PATH)
                redis_conn = redis.StrictRedis(password=settings.REDIS_PASSWORD, unix_socket_path=settings.REDIS_SOCKET_PATH, charset='utf-8', decode_responses=True)
            else:
                # @modified 20191014 - Bug #3266: py3 Redis binary objects not strings
                #                      Branch #3262: py3
                # redis_conn = redis.StrictRedis(unix_socket_path=settings.REDIS_SOCKET_PATH)
                redis_conn = redis.StrictRedis(unix_socket_path=settings.REDIS_SOCKET_PATH, charset='utf-8', decode_responses=True)
            key = 'rebrow.token.%s' % client_token
            # @modified 20191014 - Bug #3266: py3 Redis binary objects not strings
            #                      Branch #3262: py3
            value = '[\'%s\',\'%s\',\'%s\']' % (client_id, salt, jwt_encoded_payload)
            redis_conn.setex(key, token_valid_for, value)
            logger.info('rebrow access :: set Redis key - %s' % (key))
        except:
            message = 'Failed to set Redis key - %s' % key
            trace = traceback.format_exc()
            return internal_error(message, trace)

        # @modified 20180526 - Feature #2378: Add redis auth to Skyline and rebrow
        # Change password parameter to token parameter
        # url = url_for("rebrow_server_db", host=host, port=port, db=db, password=password)
        # @modified 20201103 - Feature #3824: get_cluster_data
        # Change default page from Server to Keys
        # url = url_for(
        #     "rebrow_server_db", host=host, port=port, db=db, token=client_token)
        url = url_for(
            "rebrow_keys", host=host, port=port, db=db, token=client_token)
        return redirect(url)
    else:
        start = time.time()

        # @added 20180527 - Feature #2378: Add redis auth to Skyline and rebrow
        # Added client_id
        client_id, protocol, proxied = get_client_details()

        # @added 20180527 - Feature #2378: Add redis auth to Skyline and rebrow
        # Added client message to give relevant messages on the login page
        client_message = False

        # @added 20190519 - Branch #3002: docker
        display_redis_password = False
        host_input_value = 'localhost'
        rebrow_redis_password = False
        try:
            running_on_docker = settings.DOCKER
        except:
            running_on_docker = False
        if running_on_docker:
            host_input_value = 'unix_socket'
            try:
                display_redis_password = settings.DOCKER_DISPLAY_REDIS_PASSWORD_IN_REBROW
            except:
                display_redis_password = False
            if display_redis_password:
                rebrow_redis_password = settings.REDIS_PASSWORD

        return render_template(
            'rebrow_login.html',
            # @modified 20180527 - Feature #2378: Add redis auth to Skyline and rebrow
            # Change password parameter to token parameter and added protocol,
            # proxied
            # redis_password=redis_password,
            protocol=protocol, proxied=proxied, client_message=client_message,
            version=skyline_version,
            # @added 20190519 - Branch #3002: docker
            running_on_docker=running_on_docker,
            rebrow_redis_password=rebrow_redis_password,
            display_redis_password=display_redis_password,
            host_input_value=host_input_value,
            duration=(time.time() - start))


@app.route("/rebrow_server_db/<host>:<int:port>/<int:db>/")
@requires_auth
def rebrow_server_db(host, port, db):
    """
    List all databases and show info on server
    """
    start = time.time()
    # @modified 20180520 - Feature #2378: Add redis auth to Skyline and rebrow
    # r = redis.StrictRedis(host=host, port=port, db=0)
    # @modified 20180527 - Feature #2378: Add redis auth to Skyline and rebrow
    # Use client_id and JWT token
    # password = False
    # url_password = False
    # if request.args.getlist('password'):
    #     password = request.args.get('password', default='', type=str)
    #     url_password = quote(password, safe='')
    # @added 20180527 - Feature #2378: Add redis auth to Skyline and rebrow
    # Added client_id and token
    client_id, protocol, proxied = get_client_details()
    token, redis_password, fail_msg, trace = decode_token(client_id)
    if not token:
        if fail_msg:
            return internal_error(fail_msg, trace)

    try:
        # @modified 20191014 - Bug #3266: py3 Redis binary objects not strings
        #                      Branch #3262: py3
        # r = get_redis(host, port, db, redis_password)
        r = get_redis(host, port, db, redis_password, True)
    except:
        logger.error(traceback.format_exc())
        logger.error('error :: rebrow access :: failed to login to Redis with token')

    try:
        info = r.info('all')
    except:
        message = 'Failed to get INFO all from Redis, this could be an issue with the Redis password you entered.'
        trace = traceback.format_exc()
        return internal_error(message, trace)

    dbsize = r.dbsize()
    return render_template(
        'rebrow_server_db.html',
        host=host,
        port=port,
        db=db,
        # @added 20180520 - Feature #2378: Add redis auth to Skyline and rebrow
        # password=password,
        # url_password=url_password,
        # @added 20180527 - Feature #2378: Add redis auth to Skyline and rebrow
        token=token,
        info=info,
        dbsize=dbsize,
        serverinfo_meta=serverinfo_meta,
        version=skyline_version,
        duration=(time.time() - start))


@app.route("/rebrow_keys/<host>:<int:port>/<int:db>/keys/", methods=['GET', 'POST'])
@requires_auth
def rebrow_keys(host, port, db):
    """
    List keys for one database
    """
    start = time.time()
    # @modified 20180520 - Feature #2378: Add redis auth to Skyline and rebrow
    # r = redis.StrictRedis(host=host, port=port, db=db)
    # @modified 20180527 - Feature #2378: Add redis auth to Skyline and rebrow
    # password = request.args.get('password', default='', type=str)
    # url_password = quote(password, safe='')
    # r = get_redis(host, port, db, password)
    # @added 20180527 - Feature #2378: Add redis auth to Skyline and rebrow
    # Added client_id and token
    client_id, protocol, proxied = get_client_details()
    token, redis_password, fail_msg, trace = decode_token(client_id)
    if not token:
        if fail_msg:
            return internal_error(fail_msg, trace)

    try:
        # @modified 20191014 - Bug #3266: py3 Redis binary objects not strings
        #                      Branch #3262: py3
        # r = get_redis(host, port, db, redis_password)
        r = get_redis(host, port, db, redis_password, True)
    except:
        logger.error(traceback.format_exc())
        logger.error('error :: rebrow access :: failed to login to Redis with token')

    if request.method == 'POST':
        action = request.form['action']
        app.logger.debug(action)
        if action == 'delkey':
            if request.form['key'] is not None:
                try:
                    result = r.delete(request.form['key'])
                except:
                    message = 'Failed to delete Redis key - %s' % str(request.form['key'])
                    trace = traceback.format_exc()
                    return internal_error(message, trace)
                if result == 1:
                    flash('Key %s has been deleted.' % request.form['key'], category='info')
                    logger.info('rebrow :: deleted Redis key - %s' % str(request.form['key']))
                else:
                    flash('Key %s could not be deleted.' % request.form['key'], category='error')
                    logger.info('rebrow :: could not deleted Redis key - %s' % str(request.form['key']))
        return redirect(request.url)
    else:
        offset = int(request.args.get('offset', '0'))
        # @modified 20180527 - Feature #2378: Add redis auth to Skyline and rebrow
        # List more keys per page
        # perpage = int(request.args.get('perpage', '10'))
        perpage = int(request.args.get('perpage', '50'))
        pattern = request.args.get('pattern', '*')
        try:
            dbsize = r.dbsize()
        except:
            message = 'Failed to determine Redis dbsize'
            trace = traceback.format_exc()
            return internal_error(message, trace)

        keys = sorted(r.keys(pattern))
        limited_keys = keys[offset:(perpage + offset)]
        types = {}
        for key in limited_keys:
            types[key] = r.type(key)
        logger.info('rebrow :: returned keys list')

        return render_template(
            'rebrow_keys.html',
            host=host,
            port=port,
            db=db,
            # @added 20180520 - Feature #2378: Add redis auth to Skyline and rebrow
            # password=password,
            # url_password=url_password,
            # @added 20180527 - Feature #2378: Add redis auth to Skyline and rebrow
            token=token,
            dbsize=dbsize,
            keys=limited_keys,
            types=types,
            offset=offset,
            perpage=perpage,
            pattern=pattern,
            num_keys=len(keys),
            version=skyline_version,
            duration=(time.time() - start))


@app.route("/rebrow_key/<host>:<int:port>/<int:db>/keys/<key>/")
@requires_auth
def rebrow_key(host, port, db, key):
    """
    Show a specific key.
    key is expected to be URL-safe base64 encoded
    """
    # @added 20160703 - Feature #1464: Webapp Redis browser
    # metrics encoded with msgpack
    # @modified 20190503 - Branch #2646: slack - linting
    # original_key not used
    # original_key = key

    msg_pack_key = False
    # if key.startswith('metrics.'):
    #     msg_packed_key = True
    key = base64.urlsafe_b64decode(key.encode('utf8'))
    start = time.time()
    # @modified 20180520 - Feature #2378: Add redis auth to Skyline and rebrow
    # r = redis.StrictRedis(host=host, port=port, db=db)
    # @modified 20180527 - Feature #2378: Add redis auth to Skyline and rebrow
    # Use client_id and token
    # password = request.args.get('password', default='', type=str)
    # url_password = quote(password, safe='')
    # r = get_redis(host, port, db, password)
    # @added 20180527 - Feature #2378: Add redis auth to Skyline and rebrow
    # Added client_id and token
    client_id, protocol, proxied = get_client_details()
    token, redis_password, fail_msg, trace = decode_token(client_id)
    if not token:
        if fail_msg:
            return internal_error(fail_msg, trace)

    try:
        r = get_redis(host, port, db, redis_password, False)
    except:
        logger.error(traceback.format_exc())
        logger.error('error :: rebrow access :: failed to login to Redis with token')
    # @modified 20210504 -
    # try:
    #    r_d = get_redis(host, port, db, redis_password, True)
    # except:
    #     logger.error(traceback.format_exc())
    #     logger.error('error :: rebrow access :: failed to login to Redis with token')

    try:
        # @modified 20191014 - Bug #3266: py3 Redis binary objects not strings
        #                      Branch #3262: py3
        # dump = r.dump(key)
        if python_version == 2:
            dump = r.dump(key)
        dump_key = key.decode('utf-8')
        if python_version == 3:
            dump = r.dump(dump_key)
        logger.info('rebrow :: got key - %s' % str(dump_key))
    except:
        message = 'Failed to dump Redis key - %s, decoded key - %s' % (str(key), str(dump_key))
        trace = traceback.format_exc()
        return internal_error(message, trace)

    if dump is None:
        abort(404)
    # if t is None:
    #    abort(404)
    size = len(dump)
    # @modified 20170809 - Bug #2136: Analyzer stalling on no metrics
    # Added except to all del methods to prevent stalling if any object does
    # not exist
    try:
        del dump
    except:
        logger.error('error :: failed to del dump')

    # @modified 20191014 - Bug #3266: py3 Redis binary objects not strings
    #                      Branch #3262: py3
    # t = r.type(key)
    if python_version == 2:
        t = r.type(key)
    if python_version == 3:
        key = dump_key
        t = r.type(key).decode('utf-8')
    logger.info('rebrow :: got key - %s, of type %s' % (str(dump_key), str(t)))

    ttl = r.pttl(key)
    if t == 'string':
        # @modified 20160703 - Feature #1464: Webapp Redis browser
        # metrics encoded with msgpack
        # val = r.get(key)
        try:
            val = r.get(key)
        except:
            abort(404)

        # @modified 20200610 - Bug #3266: py3 Redis binary objects not strings
        #                      Branch #3262: py3
        # Try decode val so that bytes-like objects are decoded, this will fail
        # sliently and if it is a msg_packed_key
        try:
            val = val.decode('utf-8')
        except (UnicodeDecodeError, AttributeError):
            pass

        # @modified 20191014 - Bug #3266: py3 Redis binary objects not strings
        #                      Branch #3262: py3
        # test_string = all(c in string.printable for c in val)
        try:
            test_string = all(c in string.printable for c in val)
        except:
            test_string = False

        # @added 20170920 - Bug #2166: panorama incorrect mysql_id cache keys
        # There are SOME cache key msgpack values that DO == string.printable
        # for example [73] msgpacks to I
        # panorama.mysql_ids will always be msgpack
        if 'panorama.mysql_ids' in str(key):
            test_string = False
        if not test_string:
            raw_result = r.get(key)
            unpacker = Unpacker(use_list=False)
            unpacker.feed(raw_result)
            val = list(unpacker)
            msg_pack_key = True
            logger.info('rebrow :: msgpack key unpacked - %s' % str(dump_key))
    elif t == 'list':
        val = r.lrange(key, 0, -1)
    elif t == 'hash':
        val = r.hgetall(key)
    elif t == 'set':
        val = r.smembers(key)
        # @modified 20200610 - Bug #3266: py3 Redis binary objects not strings
        #                      Branch #3262: py3
        # Try decode val so that bytes-like objects are decoded
        try:
            val = val.decode('utf-8')
        except (UnicodeDecodeError, AttributeError):
            pass
        logger.info('rebrow :: set key - %s' % str(key))
    elif t == 'zset':
        val = r.zrange(key, 0, -1, withscores=True)
    return render_template(
        'rebrow_key.html',
        host=host,
        port=port,
        db=db,
        # @added 20180520 - Feature #2378: Add redis auth to Skyline and rebrow
        # password=password,
        # url_password=url_password,
        # @added 20180527 - Feature #2378: Add redis auth to Skyline and rebrow
        token=token,
        key=key,
        value=val,
        type=t,
        size=size,
        ttl=ttl / 1000.0,
        now=datetime.datetime.utcnow(),
        expiration=datetime.datetime.utcnow() + timedelta(seconds=ttl / 1000.0),
        version=skyline_version,
        duration=(time.time() - start),
        msg_packed_key=msg_pack_key)


@app.template_filter('urlsafe_base64')
def urlsafe_base64_encode(s):
    # @modified 20180520 - Feature #2378: Add redis auth to Skyline and rebrow
    # if type(s) == 'Markup':
    #     s = s.unescape()

    # @modified 20191014 - Bug #3266: py3 Redis binary objects not strings
    #                      Branch #3262: py3
    # if isinstance(s, Markup):
    #     s = s.unescape()
    # elif isinstance(s, bytes):
    #     s = s.decode('utf-8')
    # s = s.encode('utf8')
    # s = base64.urlsafe_b64encode(s)
    s_original = s
    try:
        if isinstance(s, Markup):
            s = s.unescape()
        elif isinstance(s, bytes):
            s = s.decode('utf-8')
        s = s.encode('utf8')
        s = base64.urlsafe_b64encode(s)
        # logger.info('debug :: rebrow :: %s urlsafe_b64encode as %s' % (str(s_original), str(s)))
    except:
        message = 'Failed to urlsafe_b64encode - %s' % str(s)
        trace = traceback.format_exc()
        return internal_error(message, trace)

    # @added 20191014 - Bug #3266: py3 Redis binary objects not strings
    #                   Branch #3262: py3
    if python_version == 3:
        decoded_s = s.decode('utf-8')
        s = decoded_s
        # logger.info('debug more :: rebrow :: %s urlsafe_b64encode decoded as %s' % (str(s_original), str(decoded_s)))

    return Markup(s)
# END rebrow


class App():
    def __init__(self):
        self.stdin_path = '/dev/null'
        self.stdout_path = '%s/%s.log' % (settings.LOG_PATH, skyline_app)
        self.stderr_path = '%s/%s.log' % (settings.LOG_PATH, skyline_app)
        self.pidfile_path = '%s/%s.pid' % (settings.PID_PATH, skyline_app)
        self.pidfile_timeout = 5

    def run(self):

        # Log management to prevent overwriting
        # Allow the bin/<skyline_app>.d to manage the log
        if os.path.isfile(skyline_app_logwait):
            try:
                os_remove(skyline_app_logwait)
            except OSError:
                logger.error('error - failed to remove %s, continuing' % skyline_app_logwait)
                pass

        now = time.time()
#        log_wait_for = now + 5
        log_wait_for = now + 1
        while now < log_wait_for:
            if os.path.isfile(skyline_app_loglock):
                sleep(.1)
                now = time.time()
            else:
                now = log_wait_for + 1

        logger.info('starting %s run' % skyline_app)
        if os.path.isfile(skyline_app_loglock):
            logger.error('error - bin/%s.d log management seems to have failed, continuing' % skyline_app)
            try:
                os_remove(skyline_app_loglock)
                logger.info('log lock file removed')
            except OSError:
                logger.error('error - failed to remove %s, continuing' % skyline_app_loglock)
                pass
        else:
            logger.info('bin/%s.d log management done' % skyline_app)

        try:
            logger.info('starting %s - %s' % (skyline_app, skyline_version))
        except:
            logger.info('starting %s - version UNKNOWN' % (skyline_app))
        logger.info('hosted at %s' % settings.WEBAPP_IP)
        logger.info('running on port %d' % settings.WEBAPP_PORT)

        app.run(settings.WEBAPP_IP, settings.WEBAPP_PORT)


def run():
    """
    Start the Webapp server
    """
    if not isdir(settings.PID_PATH):
        print('pid directory does not exist at %s' % settings.PID_PATH)
        sys.exit(1)

    if not isdir(settings.LOG_PATH):
        print('log directory does not exist at %s' % settings.LOG_PATH)
        sys.exit(1)

    logger.setLevel(logging.DEBUG)

    formatter = logging.Formatter("%(asctime)s :: %(process)s :: %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    handler = logging.handlers.TimedRotatingFileHandler(
        logfile,
        when="midnight",
        interval=1,
        backupCount=5)

    memory_handler = logging.handlers.MemoryHandler(100,
                                                    flushLevel=logging.DEBUG,
                                                    target=handler)
    handler.setFormatter(formatter)
    logger.addHandler(memory_handler)

    # Validate settings variables
    valid_settings = validate_settings_variables(skyline_app)

    if not valid_settings:
        print('error :: invalid variables in settings.py - cannot start')
        sys.exit(1)

    try:
        settings.WEBAPP_SERVER
    except:
        logger.error('error :: failed to determine %s from settings.py' % str('WEBAPP_SERVER'))
        print('Failed to determine %s from settings.py' % str('WEBAPP_SERVER'))
        sys.exit(1)
    try:
        settings.WEBAPP_IP
    except:
        logger.error('error :: failed to determine %s from settings.py' % str('WEBAPP_IP'))
        print('Failed to determine %s from settings.py' % str('WEBAPP_IP'))
        sys.exit(1)
    try:
        settings.WEBAPP_PORT
    except:
        logger.error('error :: failed to determine %s from settings.py' % str('WEBAPP_PORT'))
        print('Failed to determine %s from settings.py' % str('WEBAPP_PORT'))
        sys.exit(1)
    try:
        settings.WEBAPP_AUTH_ENABLED
    except:
        logger.error('error :: failed to determine %s from settings.py' % str('WEBAPP_AUTH_ENABLED'))
        print('Failed to determine %s from settings.py' % str('WEBAPP_AUTH_ENABLED'))
        sys.exit(1)
    try:
        settings.WEBAPP_IP_RESTRICTED
    except:
        logger.error('error :: failed to determine %s from settings.py' % str('WEBAPP_IP_RESTRICTED'))
        print('Failed to determine %s from settings.py' % str('WEBAPP_IP_RESTRICTED'))
        sys.exit(1)
    try:
        settings.WEBAPP_AUTH_USER
    except:
        logger.error('error :: failed to determine %s from settings.py' % str('WEBAPP_AUTH_USER'))
        print('Failed to determine %s from settings.py' % str('WEBAPP_AUTH_USER'))
        sys.exit(1)
    try:
        settings.WEBAPP_AUTH_USER_PASSWORD
    except:
        logger.error('error :: failed to determine %s from settings.py' % str('WEBAPP_AUTH_USER_PASSWORD'))
        print('Failed to determine %s from settings.py' % str('WEBAPP_AUTH_USER_PASSWORD'))
        sys.exit(1)
    try:
        settings.WEBAPP_ALLOWED_IPS
    except:
        logger.error('error :: failed to determine %s from settings.py' % str('WEBAPP_ALLOWED_IPS'))
        print('Failed to determine %s from settings.py' % str('WEBAPP_ALLOWED_IPS'))
        sys.exit(1)

    webapp = App()

# Does this make it log?
#    if len(sys.argv) > 1 and sys.argv[1] == 'run':
#        webapp.run()
#    else:
#        daemon_runner = runner.DaemonRunner(webapp)
#        daemon_runner.daemon_context.files_preserve = [handler.stream]
#        daemon_runner.do_action()

    daemon_runner = runner.DaemonRunner(webapp)
    daemon_runner.daemon_context.files_preserve = [handler.stream]
    daemon_runner.do_action()


if __name__ == "__main__":
    run()
