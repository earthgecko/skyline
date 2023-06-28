"""
listen.py
"""
import socket
from os import kill, getpid
try:
    from Queue import Full
except ImportError:
    from queue import Full
from multiprocessing import Process
from struct import Struct, unpack
import sys
from time import time, sleep
import traceback

import logging
import os.path
from os import remove as os_remove

from msgpack import unpackb

import settings
# @modified 20220726 - Task #2732: Prometheus to Skyline
#                      Branch #4300: prometheus
# Moved send_graphite_metric
# from skyline_functions import send_graphite_metric
# @added 20220726 - Task #2732: Prometheus to Skyline
#                   Branch #4300: prometheus
from functions.graphite.send_graphite_metric import send_graphite_metric

parent_skyline_app = 'horizon'
child_skyline_app = 'listen'
skyline_app_logger = '%sLog' % parent_skyline_app
logger = logging.getLogger(skyline_app_logger)
skyline_app = '%s.%s' % (parent_skyline_app, child_skyline_app)
skyline_app_logfile = '%s/%s.log' % (settings.LOG_PATH, parent_skyline_app)
skyline_app_loglock = '%s.lock' % skyline_app_logfile
skyline_app_logwait = '%s.wait' % skyline_app_logfile

try:
    SERVER_METRIC_PATH = '.%s' % settings.SERVER_METRICS_NAME
    if SERVER_METRIC_PATH == '.':
        SERVER_METRIC_PATH = ''
except:
    SERVER_METRIC_PATH = ''

skyline_app_graphite_namespace = 'skyline.%s%s.%s' % (
    parent_skyline_app, SERVER_METRIC_PATH, child_skyline_app)

python_version = int(sys.version_info[0])

# SafeUnpickler taken from Carbon: https://github.com/graphite-project/carbon/blob/master/lib/carbon/util.py
if python_version == 2:
    try:
        from cStringIO import StringIO
    except ImportError:
        from StringIO import StringIO
# @added 20191016 - Task #3278: py3 handle bytes and not str in pickles
# For backwards compatibility Horizon needs to load BytesIO in py2
    from io import BytesIO

if python_version == 3:
    # @modified 20191016 - Task #3278: py3 handle bytes and not str in pickles
    #                      Branch #3262: py3
    # import io
    from io import StringIO
    from io import BytesIO

try:
    # @modified 20170913 - Task #2160: Test skyline with bandit
    # Added nosec to exclude from bandit tests
    import cPickle as pickle  # nosec B403
    USING_CPICKLE = True
except:
    import pickle  # nosec B403
    USING_CPICKLE = False

# @added 20191016 - Task #3278: py3 handle bytes and not str in pickles
#                   Branch #3262: py3
# Add ability to log for debugging
LOCAL_DEBUG = False

# This whole song & dance is due to pickle being insecure
# yet performance critical for carbon. We leave the insecure
# mode (which is faster) as an option (USE_INSECURE_UNPICKLER).
# The SafeUnpickler classes were largely derived from
# http://nadiana.com/python-pickle-insecure
if USING_CPICKLE:
    class SafeUnpickler(object):
        PICKLE_SAFE = {
            'copy_reg': set(['_reconstructor']),
            '__builtin__': set(['object']),
        }

        @classmethod
        def find_class(cls, module, name):
            if module not in cls.PICKLE_SAFE:
                raise pickle.UnpicklingError('Attempting to unpickle unsafe module %s' % module)
            __import__(module)
            mod = sys.modules[module]
            if name not in cls.PICKLE_SAFE[module]:
                raise pickle.UnpicklingError('Attempting to unpickle unsafe class %s' % name)
            return getattr(mod, name)

        @classmethod
        def loads(cls, pickle_string):
            # @modified 20170913 - Task #2160: Test skyline with bandit
            # Added nosec to exclude from bandit tests
            # @modified 20191016 - Task #3278: py3 handle bytes and not str in pickles
            #                      Branch #3262: py3
            # pickle_obj = pickle.Unpickler(StringIO(pickle_string))  # nosec
            if python_version == 2:
                pickle_obj = pickle.Unpickler(StringIO(pickle_string))  # nosec B301
            if python_version == 3:
                pickle_obj = pickle.Unpickler(BytesIO(pickle_string))  # nosec B301

            pickle_obj.find_global = cls.find_class
            return pickle_obj.load()

else:
    class SafeUnpickler(pickle.Unpickler):
        PICKLE_SAFE = {
            'copy_reg': set(['_reconstructor']),
            '__builtin__': set(['object']),
        }

        def find_class(self, module, name):
            if module not in self.PICKLE_SAFE:
                raise pickle.UnpicklingError('Attempting to unpickle unsafe module %s' % module)
            __import__(module)
            mod = sys.modules[module]
            if name not in self.PICKLE_SAFE[module]:
                raise pickle.UnpicklingError('Attempting to unpickle unsafe class %s' % name)
            return getattr(mod, name)

        @classmethod
        def loads(cls, pickle_string):
            # @modified 20191016 - Task #3278: py3 handle bytes and not str in pickles
            #                      Branch #3262: py3
            # return cls(StringIO(pickle_string)).load()
            if python_version == 2:
                return cls(StringIO(pickle_string)).load()
            if python_version == 3:
                return cls(BytesIO(pickle_string)).load()
# //SafeUnpickler


class Listen(Process):
    """
    The listener is responsible for listening on a port.
    """
    def __init__(self, port, queue, parent_pid, d_type="pickle"):
        super(Listen, self).__init__()
        try:
            self.ip = settings.HORIZON_IP
        except AttributeError:
            # Default for backwards compatibility
            self.ip = socket.gethostname()
        self.port = port
        self.q = queue
        self.daemon = True
        self.parent_pid = parent_pid
        self.current_pid = getpid()
        self.d_type = d_type

        # Use the safe unpickler that comes with carbon rather than standard python pickle/cpickle
        self.unpickler = SafeUnpickler

    def gen_unpickle(self, infile):
        """
        Generate a pickle from a stream
        """
        try:
            bunch = self.unpickler.loads(infile)
            yield bunch
        except EOFError:
            return

    def read_all(self, sock, n):
        """
        Read n bytes from a stream
        """

        # @modified 20191016 - Task #3278: py3 handle bytes and not str in pickles
        #                      Branch #3262: py3
        # The data is type str in py2 and class bytes in py3 and using bytes in
        # the data object does not allow for concatenation as was possible with
        # strings
        # data = ''
        if python_version == 2:
            data = ''
        if python_version == 3:
            data = b''
        while n > 0:
            # Break the loop when connection closes. #8 @earthgecko
            # https://github.com/earthgecko/skyline/pull/8/files
            # @earthgecko merged 1 commit into earthgecko:master from
            # mlowicki:fix_infinite_loop on 16 Mar 2015
            # Break the loop when connection closes. #115 @etsy
            chunk = sock.recv(n)

            count = len(chunk)

            if count == 0:
                break

            n -= count
            # @modified 20191016 - Task #3278: py3 handle bytes and not str in pickles
            #                      Branch #3262: py3
            # In py3 he data is bytes not str and bytes can not be concatenated
            # like str.  Also added debug logging.
            # data += chunk
            try:
                if python_version == 2:
                    data += chunk
                if python_version == 3:
                    new_data = data + chunk
                    data = new_data
                if LOCAL_DEBUG:
                    logger.debug('debug :: listen :: read_all with chunk - %s' % str(chunk))
            except:
                if LOCAL_DEBUG:
                    logger.error(traceback.format_exc())
                    logger.error('error :: listen :: read_all with chunk - %s' % str(chunk))
                data = False

        return data

    def check_if_parent_is_alive(self):
        """
        Self explanatory
        """
        try:
            kill(self.current_pid, 0)
            kill(self.parent_pid, 0)
        except:
            # @added 20201203 - Bug #3856: Handle boring sparsely populated metrics in derivative_metrics
            # Log warning
            logger.warning('warning :: parent or current process dead')
            sys.exit(0)

    def listen_pickle(self):
        """
        Listen for pickles over tcp
        """
        while 1:
            try:
                # Set up the TCP listening socket
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                s.bind((self.ip, self.port))
                s.setblocking(1)
                s.listen(5)
                logger.info('%s :: listening over tcp for pickles on %s' % (skyline_app, str(self.port)))

                (conn, address) = s.accept()
                logger.info('%s :: connection from %s on %s' % (skyline_app, str(address[0]), str(self.port)))

                chunk = []
                while 1:
                    self.check_if_parent_is_alive()
                    try:
                        # @modified 20191016 - Task #3278: py3 handle bytes and not str in pickles
                        #                      Branch #3262: py3
                        # Added ability to log and debug
                        if LOCAL_DEBUG:
                            length = None
                            body = None
                            try:
                                length = Struct('!I').unpack(self.read_all(conn, 4))
                            except:
                                logger.error(traceback.format_exc())
                                logger.error('error :: listen :: could not determine length')
                            if length:
                                try:
                                    body = self.read_all(conn, length[0])
                                except:
                                    logger.error(traceback.format_exc())
                                    logger.error('error :: listen :: could not determine body')
                        else:
                            length = Struct('!I').unpack(self.read_all(conn, 4))
                            body = self.read_all(conn, length[0])

                        # Iterate and chunk each individual datapoint
                        for bunch in self.gen_unpickle(body):
                            for metric in bunch:
                                chunk.append(metric)

                                # Queue the chunk and empty the variable
                                if len(chunk) > settings.CHUNK_SIZE:
                                    try:
                                        self.q.put(list(chunk), block=False)
                                        chunk[:] = []

                                    # Drop chunk if queue is full
                                    except Full:
                                        chunks_dropped = str(len(chunk))
                                        logger.info(
                                            '%s :: pickle queue is full, dropping %s datapoints'
                                            % (skyline_app, chunks_dropped))
#                                        self.send_graphite_metric(
#                                            'skyline.horizon.' + SERVER_METRIC_PATH + 'pickle_chunks_dropped',
#                                            chunks_dropped)
                                        send_metric_name = '%s.pickle_chunks_dropped' % skyline_app_graphite_namespace
                                        send_graphite_metric(self, skyline_app, send_metric_name, chunks_dropped)
                                        chunk[:] = []

                    except Exception as e:
                        logger.info(e)
                        logger.info('%s :: incoming pickle connection dropped, attempting to reconnect' % skyline_app)
                        break

            except Exception as e:
                logger.info('%s :: can not connect to socket: %s' % (skyline_app, str(e)))
                break

    def listen_udp(self):
        """
        Listen over udp for MessagePack strings
        """
        while 1:
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                s.bind((self.ip, self.port))
                logger.info('%s :: listening over udp for messagepack on %s' % (skyline_app, self.port))

                chunk = []
                while 1:
                    self.check_if_parent_is_alive()
                    data, addr = s.recvfrom(1024)

                    # @modified 20191014 - Task #3272: horizon - listen - py3 handle msgpack bytes
                    #                      Branch #3262: py3
                    #                      Bug #3266: py3 Redis binary objects not strings
                    # msgpack encoding of bytes and not str as per
                    # https://msgpack.org/#string-and-binary-type Python/msgpack
                    # and https://stackoverflow.com/a/47070687/107406
                    # metric = unpackb(data)
                    if python_version == 3:
                        # @added 20210328 - [Q] The "horizon.test.pickle" test is getting an error. #419
                        # Wrap in try and except and try without encoding if
                        # with encoding fails
                        try:
                            metric = unpackb(data, encoding='utf-8')
                        except Exception as e:
                            logger.error('%s :: unpackb error : %s' % (skyline_app, str(e)))
                            try:
                                logger.info('%s :: trying unpackb without encoding' % (skyline_app))
                                metric = unpackb(data)
                            except Exception as e:
                                logger.info('%s :: unpackb without encoding error : %s' % (skyline_app, str(e)))
                    else:
                        metric = unpackb(data)

                    chunk.append(metric)

                    # Queue the chunk and empty the variable
                    if len(chunk) > settings.CHUNK_SIZE:
                        try:
                            self.q.put(list(chunk), block=False)
                            chunk[:] = []

                        # Drop chunk if queue is full
                        except Full:
                            chunks_dropped = str(len(chunk))
                            logger.info(
                                '%s :: UDP queue is full, dropping %s datapoints'
                                % (skyline_app, chunks_dropped))
                            send_metric_name = '%s.udp_chunks_dropped' % skyline_app_graphite_namespace
                            send_graphite_metric(self, skyline_app, send_metric_name, chunks_dropped)
                            chunk[:] = []

            except Exception as e:
                logger.info('%s :: cannot connect to socket: %s' % (skyline_app, str(e)))
                break

    def run(self):
        """
        Called when process intializes.
        """

        # Log management to prevent overwriting
        # Allow the bin/<skyline_app>.d to manage the log
        if os.path.isfile(skyline_app_logwait):
            try:
                os_remove(skyline_app_logwait)
            except OSError:
                logger.error('error - failed to remove %s, continuing' % skyline_app_logwait)

        now = time()
        log_wait_for = now + 5
        while now < log_wait_for:
            if os.path.isfile(skyline_app_loglock):
                sleep(.1)
                now = time()
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
        else:
            logger.info('bin/%s.d log management done' % skyline_app)

        logger.info('%s :: started listener' % skyline_app)

        if self.d_type == 'pickle':
            self.listen_pickle()
        elif self.d_type == 'udp':
            self.listen_udp()
        else:
            logger.error('%s :: unknown listener format' % skyline_app)
