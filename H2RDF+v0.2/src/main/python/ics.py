import json


## Returns the ICS matching ics_id or None if not found.
# @param zkrpc: a zookeeper_itf.ZooKeeperRpc
def ics(zkrpc, campaign_id):
    return zkrpc.call('\x03', campaign_id)


## Inserts or updates the ICS. TODO decide whether we allow partial dictionary
# to update only some fields.
# @param zkrpc: a zookeeper_itf.ZooKeeperRpc
def put_ics(zkrpc, json_ics):
    return zkrpc.call('\x04', json.dumps(json_ics))
