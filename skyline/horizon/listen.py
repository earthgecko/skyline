import socket
from os import kill, getpid
try:
    from Queue import Full
except ImportError:
    from queue import Full
from multiprocessing import Process
from struct import Struct, unpack
from msgpack import unpackb
import sys
from time import time, sleep

import logging
import os.path
from os import remove as os_remove
import settings
from skyline_functions import send_graphite_metric

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
if python_version == 3:
    import io

try:
    import cPickle as pickle
    USING_CPICKLE = True
except:
    import pickle
    USING_CPICKLE = False

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
            pickle_obj = pickle.Unpickler(StringIO(pickle_string))
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
            return cls(StringIO(pickle_string)).load()
# //SafeUnpickler


class Listen(Process):
    """
    The listener is responsible for listening on a port.
    """
    def __init__(self, port, queue, parent_pid, type="pickle"):
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
        self.type = type

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
        data = ''
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
            data += chunk
        return data

    def check_if_parent_is_alive(self):
        """
        Self explanatory
        """
        try:
            kill(self.current_pid, 0)
            kill(self.parent_pid, 0)
        except:
            exit(0)

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
                logger.info('%s :: listening over tcp for pickles on %s' % (skyline_app, self.port))

                (conn, address) = s.accept()
                logger.info('%s :: connection from %s:%s' % (skyline_app, address[0], self.port))

                chunk = []
                while 1:
                    self.check_if_parent_is_alive()
                    try:
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
                                        send_graphite_metric(skyline_app, send_metric_name, chunks_dropped)
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
                            send_graphite_metric(skyline_app, send_metric_name, chunks_dropped)
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
                pass

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
                pass
        else:
            logger.info('bin/%s.d log management done' % skyline_app)

        logger.info('%s :: started listener' % skyline_app)

        if self.type == 'pickle':
            self.listen_pickle()
        elif self.type == 'udp':
            self.listen_udp()
        else:
            logger.error('%s :: unknown listener format' % skyline_app)
