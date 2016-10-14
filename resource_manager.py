"""

Each resource can get its own limit.  The resource name will typically be
the server name but could be anything unique and refer to specific datatypes, uuids,
or anything that should have its own resource quota.  Currently, these quotas
are independent.  It might make sense to make concurrent requests limits a server wide.

# CLIENT
 make request (e.g., for a resource)
 (if resource request) subscribe to provided id or start job (should be temporary or will keep queueing)
 (if resource request) indicate that job is starting (if from pub)
 (if resource request) indicate that job is finishing

# SERVER
 receive request
 reply with resource or resource id, or ack
 if resource becomes available publish resource id
 keep publishing if start req not made

\author Stephen Plaza (plazas@janelia.hhmi.org)

"""

# TODO: potentially time-out the pub/sub reservation

import zmq
import sys
from collections import deque

# usage: prog <port> -c config.json
comm_port = int(sys.argv[1])
# ?! arg parser

# poll delay when un-acked pub 
PUBDELAY = 2000 # ms

# assume that publish port is always port + 1
# 0mq requires that the pub/sub port is different than recv/rep
pub_port = comm_port + 1


# create server to listen to provided port
context = zmq.Context()
comm_socket = context.socket(zmq.REP)
comm_socket.bind("tcp://*:%s" % comm_port)

# create publisher port
pub_socket = context.socket(zmq.PUB)
pub_socket.bind("tcp://*:%s" % pub_port)

# create poller 
poller = zmq.Poller()
poller.register(comm_socket, zmq.POLLIN)

# !! PROBLEMS: if server gets an error can I gracefully unlock or reply; is it problematic that I require the client to respond to pub

# datastructure for outstanding requests []{type, size, priority, client#, requests, etc -- can be greater than 1 but default 1, etc}
work_items = deque() # store data as (priority, request_info) -- use simple FIFO for now but could sort on priority if given)

# outstanding work (client id -> request)
current_work_items = {}


# dict for current config
# TODO: allow per resource config along with master config
config = {
            "read_reqs": 96, # some multiple of the available worker threads in DVID
            "read_data": 200, # in MBs
            "write_reqs": 96, # some multiple of the available worker threads in DVID
            "write_data": 150 # in MBs
         }

clientid = 0 # increment for each resource request

# stats unique per server (does not distinguish between
# dvids on different ports)
class ServerStats:
    def __init__(self):
        self.read_reqs = 0
        self.read_data = 0
        self.write_reqs = 0
        self.write_data = 0

resource_limits = {}

# TODO proper error handling, json schema request
# { type=request, resource, read=true|false, numopts, datasize, id (set by server) }
def request_resource(request):
    resourcename = request["resource"]
    if resourcename not in resource_limits:
        resource_limits[resourcename] = ServerStats()
    stats = resource_limits[resourcename]

    if is_available(request, stats):
        if request["read"]:
            stats.read_reqs += request["numopts"]
            stats.read_data += request["datasize"]
        else:
            stats.write_reqs += request["numopts"]
            stats.write_data += request["datasize"]
        current_work_items[request["id"]] = request 
        return True

    return False


# resource is not available if any of the resources are completely used
# probably overly simplistic but usually calls are in batches of reads and writes separately
def is_available(request, curr_stats):
    if curr_stats.read_reqs >= config["read_reqs"] or curr_stats.read_data >= config["read_data"] or curr_stats.write_reqs >= config["write_reqs"] or curr_stats.write_data >= config["write_data"]: 
        return False
    return True

# grab next resource (just grab oldest one)
# TODO use priority to sort
def find_work():
    priority, work_item = work_items[0]
    work_items.popleft()
    if request_resource(work_item):
        return work_item["id"]
    else:
        work_items.appendleft((priority,work_item))
        return -1

# resource finished
def finish_work(curr_id):
    request = current_work_items[curr_id]
    del current_work_items[curr_id] 
    stats = resource_limits[request["resource"]]
 
    # update usage
    if request["read"]:
        stats.read_reqs -= request["numopts"]
        stats.read_data -= request["datasize"]
    else:
        stats.write_reqs -= request["numopts"]
        stats.write_data -= request["datasize"]

publish_list = set() 

# infinite server loop
while True:
    # get next request
    request = None
    if len(publish_list) == 0:
        # block on next request
        request = comm_socket.recv_json()
    else:
        # outstanding pubs not ack'd, poll for PUBDELAY and repub
        res = poller.poll(PUBDELAY)        
        if len(res) > 0:
            request = comm_socket.recv_json()
        else:
            # republish in case dropped
            for cid in publish_list:
                pub_socket.send("%d %d" % (cid, 1))
            continue

    # process request
    if request["type"] == "request":
        # request for resource
        request["id"] = clientid
        clientid += 1
        if request_resource(request):
            # resource available
            comm_socket.send_json({"available": True, "id": request["id"]})
        else:
            # give everything the same priority now
            # TODO: allow optional priority and sort work items accordingly
            work_items.append((0, request))
            comm_socket.send_json({"available": False, "id": request["id"]})
    elif request["type"] == "hold":
        # grab resource, removed from pub list
        publish_list.remove(request["id"])
        comm_socket.send_json({})
    elif request["type"] == "release":
        finish_work(request["id"])
        comm_socket.send_json({})

        # find work and publish
        while len(work_items) > 0: 
            cid = find_work()
            if cid != -1:
                publish_list.add(cid)
                pub_socket.send("%d %d" % (cid, 1))
            else:
                break 
    elif request["type"] == "config":
        # reset config
        config = request["config"] 
        comm_socket.send_json(request["config"])
    else:
        comm_socket.send_json({})
        raise Exception # error!

   

