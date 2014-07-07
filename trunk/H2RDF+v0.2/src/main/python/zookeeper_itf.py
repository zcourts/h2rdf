# Adapted from Henry Robinson's queue recipe at
# https://github.com/henryr/pyzk-recipes/blob/db8a3bf89648d8c1740351c1d43cdf1efb7e2a4c/queue.py

import zookeeper
import threading
import sys
import json

ZOO_OPEN_ACL_UNSAFE = {"perms": 0x1f, "scheme": "world", "id": "anyone"}

class ZooKeeperQueue(object):
    def __init__(self, host, port, queuename):
        self.connected = False
        self.queuename = "/" + queuename
        self.cv = threading.Condition()
        zookeeper.set_log_stream(open("/dev/null"))
        def watcher(handle, type, state, path):
            # print "Connected"
            self.cv.acquire()
            self.connected = True
            self.cv.notify()
            self.cv.release()
        self.cv.acquire()
        host_s = host + ":" + str(port)
        self.handle = zookeeper.init(host_s, watcher, 2000000)
        self.cv.wait(10.0)
        if not self.connected:
            print ("Connection to ZooKeeper cluster timed out "
                   "- is a server running on %s?" % host_s)
            sys.exit()
        self.cv.release()
        # try:
        #    zookeeper.create(self.handle, self.queuename, "queue top level",
        #                     [ZOO_OPEN_ACL_UNSAFE], 0)
        # except IOError, e:
        #    if e.message == zookeeper.zerror(zookeeper.NODEEXISTS):
        #        print "Queue already exists"
        #    else:
        #        raise e

    def enqueue(self, val):
        return zookeeper.create(self.handle, self.queuename + "/element", val,
                                [ZOO_OPEN_ACL_UNSAFE], zookeeper.SEQUENCE)

    def wait(self, file):
        def ret_watcher(handle, event, state, path):
            self.cv.acquire()
            self.cv.notify()
            self.cv.release()
        while True:
            self.cv.acquire()
            data = zookeeper.exists(self.handle, file, ret_watcher)
            if data != None:
                 data = self.get_and_delete(file)
                 self.cv.release()
                 return data
            self.cv.wait()
            self.cv.release()

    def dequeue(self):
        while True:
            children = sorted(
                zookeeper.get_children(self.handle, self.queuename, None))
            if len(children) == 0:
                return None
            for child in children:
                data = self.get_and_delete(self.queuename + "/" + children[0])
                if data:
                    return data

    def get_and_delete(self, node):
        try:
            (data, stat) = zookeeper.get(self.handle, node, None)
            zookeeper.delete(self.handle, node, stat["version"])
            return data
        except IOError, e:
            if e.message == zookeeper.zerror(zookeeper.NONODE):
                return None
            raise e

    def block_dequeue(self):
        def queue_watcher(handle, event, state, path):
            self.cv.acquire()
            self.cv.notify()
            self.cv.release()
        while True:
            self.cv.acquire()
            children = sorted(
                zookeeper.get_children(self.handle, self.queuename,
                                       queue_watcher))
            for child in children:
                data = self.get_and_delete(self.queuename + "/" + children[0])
                if data != None:
		    self.cv.wait()
                    self.cv.release()
                    return data
            self.cv.wait()
            self.cv.release()

class ZooKeeperRpc(ZooKeeperQueue):
    def __init__(self, host, port):
        ZooKeeperQueue.__init__(self, host, port, 'in')

    def call(self, func, value):
        b = func + value
        file = self.enqueue(buffer(b, 0))
        outfile = "/out/" + file[4:]
        res = self.wait(outfile)
        try:
            ret = json.loads(res)
            for k, v in ret.iteritems():
                if k == "error" :
                    print k, v
		    raise Exception(str((k, v)))
		else:
		    return ret 
        except:
            print "exception trying to decode JSON response, got: ", res
            raise 

    def call2(self, func, args):
        b = '\x02' + json.dumps([func, args])
        file = self.enqueue(buffer(b, 0))
        outfile = "/out/" + file[4:]
        res = self.wait(outfile)
        try:
            return json.loads(res)
        except:
            print "exception trying to decode JSON response, got: ", res
            raise
