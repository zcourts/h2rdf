import json
import zookeeper_itf

def low_level_comm():
    #connect to queue with name "in"
    zk = zookeeper_itf.ZooKeeperQueue("ia200127", 2181, "in")
    #type of request != \x00 reserved for SparQL queries
    b = '\x01'
    val = "Hello from client!!"
    b += val
    #send request
    print "Sending request"
    file = zk.enqueue(buffer(b, 0))
    #wait for reply
    outfile = "/out/" + file[4:]
    reply = zk.wait(outfile)
    print "Reply: " + reply

def bulkPutTriples(table, triples):
    rpcq = zookeeper_itf.ZooKeeperRpc("ia200127", 2181)
    res = rpcq.call('\x05', table+'$'+triples)

def bulkLoadTriples(table):
    rpcq = zookeeper_itf.ZooKeeperRpc("ia200127", 2181)
    res = rpcq.call('\x06', table)

def rpc():
    rpcq = zookeeper_itf.ZooKeeperRpc("ia200127", 2181)
    return rpcq.call2('echo', ['some message'])
