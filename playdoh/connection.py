from debugtools import *
from userpref import *
from multiprocessing.connection import Listener, Client, AuthenticationError
import cPickle
import time
import socket
import sys

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

    def recv(self):
        trials = 5
        for i in xrange(trials):
            try:
                s = self.conn.recv()
                break
            except Exception as e:
                log_warn("current connection is : %s" % (self.conn))
                log_warn("Connection error (%d/%d): %s" %
                    (i + 1, trials, str(e)))
                time.sleep(.1 * 2 ** i)
                if i == trials - 1:
                    return None
        return cPickle.loads(s)

    def close(self):
        if self.conn is not None:
            self.conn.close()
            self.conn = None


def accept(address):
    """
    Accept a connection and return a Connection object.
    """
    while True:
        try:
            listener = Listener(address, authkey=USERPREF['authkey'])
            conn = listener.accept()
            break
        except:
            listener.close()
            del listener
            #time.sleep(.1)
            raise Exception(sys.exc_info()[1])
        
    client = listener.last_accepted
    return Connection(conn), client[0]


def connect(address, trials=None):
    """
    Connect to a server and return a Connection object.
    """
    if trials is None:
        trials = USERPREF['connectiontrials']
    conn = None
    t0 = time.time()
    timeout = USERPREF['connectiontimeout']
    for i in xrange(trials):
        try:
            conn = Client(address, authkey=USERPREF['authkey'])
            break
        except AuthenticationError as e:
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


def is_server_connected(address):
    conn = connect(address, trials=None)
    conn.send("close_connection")
    if conn != None:
        conn.close()
    return (conn != None)

def validate_servers(machines, port):
    valid_machines = []
    for machine in machines:
        if is_server_connected((machine, port)):
            valid_machines.append(machine)
            
    return valid_machines
