from debugtools import *
from userpref import *
#from multiprocessing.connection import Listener, Client, AuthenticationError
import multiprocessing.connection
import cPickle
import time
import socket
import sys
import threading

BUFSIZE = 1024 * 32
try:
    LOCAL_IP = socket.gethostbyname(socket.gethostname())
except:
    LOCAL_IP = '127.0.0.1'


__all__ = ['accept', 'connect', 'LOCAL_IP']


class Connection(object):
    """
    Handles chunking and compression of data.

    To minimise data transfers between machines, we can use data compression,
    which this Connection handles automatically.
    """
    def __init__(self, conn, chunked=False, compressed=False):
        self.conn = conn
        self.chunked = chunked
        self.compressed = compressed
        self.BUFSIZE = BUFSIZE

    def send(self, obj):
        s = cPickle.dumps(obj, -1)
        self.conn.send(s)

    def _recv(self, bool_verbose):
        trials = 5
        for i in xrange(trials):
            try:
                s = self.conn.recv()
                break
            except Exception as e:
                if bool_verbose:
                    log_warn("current connection is : %s" % (self.conn))
                    log_warn("Connection error (%d/%d): %s" %
                        (i + 1, trials, str(e)))
                time.sleep(.1 * 2 ** i)
                if i == trials - 1:
                    return None
        return cPickle.loads(s)
    
    def recv(self):
        return self._recv(True)
        
    
    def blckng_recv(self, recv_items):
        recv_items.append(self._recv(False))
     
    def nonblckng_recv(self,timeout):
        recv_items = []
        receiver = threading.Thread(target=self.blckng_recv, args=(recv_items,))
        receiver.start()
        receiver.join(timeout)
        if len(recv_items) == 0:
            return None
        return recv_items[0]
    
    def is_alive(self, timeout):
        return self.nonblckng_recv(timeout) != None

    def close(self):
        if self.conn is not None:
            self.conn.close()
            self.conn = None

    def ping(self):
        self.send("ping")

def accept(address):
    """
    Accept a connection and return a Connection object.
    """
    while True:
        try:
            listener = multiprocessing.connection.Listener(address, authkey=USERPREF['authkey'])
            conn = listener.accept()
            break
        except:
            listener.close()
            del listener
            #time.sleep(.1)
            raise Exception(sys.exc_info()[1])
        
    client = listener.last_accepted
    return Connection(conn), client[0]

# def _new_init_timeout():
#    return time.time() + USERPREF['connectiontimeout']
#
# sys.modules['multiprocessing'].__dict__['connection']._init_timeout = _new_init_timeout

def connect(address, trials=None):
    """
    Connect to a server and return a Connection object.
    """
    if trials is None:
        trials = USERPREF['connectiontrials']
    conn = None
    t0 = time.time()
    timeout = USERPREF['connectiontimeout']
#     multiprocessing.connection.CONNECTION_TIMEOUT = timeout
    for i in xrange(trials):
        try:
            conn = multiprocessing.connection.Client(address, authkey=USERPREF['authkey'])
            break
        except multiprocessing.connection.AuthenticationError as e:
            log_warn("Authentication error: %s" % str(e))
            break
        except Exception as e:
            if time.time() > t0 + timeout:
                log_warn("Connection timed out, unable to connect to %s"\
                         % str(address))
                break
            log_debug("Connection error: %s, trying again... (%d/%d)" %
                (str(e), i + 1, trials))
            if i == trials - 1:
                log_warn("Connection error: %s" % e)
            time.sleep(.1 * 2 ** i)
    if conn is None:
        return None
    return Connection(conn)

# connect with a different way of returning values, 
# convinient when using it inside a thread
def try_to_connect(address, result):
    conn = connect(address, trials=None)
    result[0] = conn
    
# check to see if the server running @address is reachable
def is_server_connected(address, result):
    thread = threading.Thread(target=try_to_connect, args=(address,result))
    thread.start()
    thread.join(1)
    return result[0] != None

# checks to find all machines that are reachable and it returns
# a map with machines as keys and connections as values
# machines that are unreachable are not returned
def validate_servers(machines, port):
    conn_map = {}
    for machine in machines:
        result = [None]
        if is_server_connected((machine, port), result):
            conn_map[machine] = result[0]
        else:
            log_warn("Unable to connect to %s "%machine)
            
    return conn_map


# keep sending pings to elements of @connections 
# on regular time @intervals until @done[0] is true        
def bulk_ping_loop(connections, interval, done):
    while not done[0]:
        time.sleep(interval)
        for conn in connections:
            conn.ping()
    for conn in connections:
        conn.send("close_connection")
