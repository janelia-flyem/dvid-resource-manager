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

import sys
import json
import copy
import argparse
from collections import deque

import jsonschema

# Terminate results in normal shutdown
import signal
signal.signal(signal.SIGTERM, lambda signum, stack_frame: sys.exit(0))

import zmq

from dvid_resource_manager.schemas import ReceivedMessageSchema

# poll delay when un-acked pub 
PUBDELAY = 2000 # ms

DEFAULT_CONFIG = {
    "read_reqs": 96, # some multiple of the available worker threads in DVID
    "read_data": 200000000, # in bytes
    "write_reqs": 96, # some multiple of the available worker threads in DVID
    "write_data": 150000000 # in bytes
}

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("comm_port", type=int, help="Port to use for listening . The next port (+1) will also be used, for publishing.")
    parser.add_argument("--config-file")
    parser.add_argument("--debug", action='store_true')
    args = parser.parse_args()

    comm_port = args.comm_port

    # assume that publish port is always port + 1
    # 0mq requires that the pub/sub port is different than recv/rep
    pub_port = comm_port + 1

    # dict for current config
    # TODO: allow per resource config along with master config
    if args.config_file:
        config = json.load(open(args.config_file, 'r'))
    else:
        config = DEFAULT_CONFIG

    if set(config.keys()) != set(DEFAULT_CONFIG.keys()):
        sys.stderr.write("Config file does not have the expected keys!\n")
        sys.exit(1)

    server = ResourceManagerServer(comm_port, pub_port, config)

    try:
        server.run(args.debug)
    except (KeyboardInterrupt, SystemExit) as ex:
        print("Resource manager killed via external signal: {}".format(ex.__class__.__name__))

class ResourceManagerServer(object):

    # stats unique per server
    # (does not distinguish between dvids on different ports)
    class ServerStats(object):
        def __init__(self):
            self.read_reqs = 0
            self.read_data = 0
            self.write_reqs = 0
            self.write_data = 0

    def __init__(self, comm_port, pub_port, config):
        self.comm_port = comm_port
        self.pub_port = pub_port
        self.config = config

    def run(self, debug):
        self.resource_limits = {}
        
        # outstanding work (client id -> request)
        self.current_work_items = {}
        
        # datastructure for outstanding requests []{type, size, priority, client#, requests, etc -- can be greater than 1 but default 1, etc}
        self.work_items = deque() # store data as (priority, request_info) -- use simple FIFO for now but could sort on priority if given)
        
        # create server to listen to provided port
        context = zmq.Context()
        comm_socket = context.socket(zmq.REP)
        comm_socket.bind("tcp://*:%s" % self.comm_port)
        
        # create publisher port
        pub_socket = context.socket(zmq.PUB)
        pub_socket.bind("tcp://*:%s" % self.pub_port)
        
        # create poller 
        poller = zmq.Poller()
        poller.register(comm_socket, zmq.POLLIN)
        
        # !! PROBLEMS: if server gets an error can I gracefully unlock or reply; is it problematic that I require the client to respond to pub
            
        clientid = 0 # increment for each resource request
        
        publish_list = set()

        try:

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
                            pub_socket.send(b"%d %d" % (cid, 1))
                        continue
            
                if debug:
                    jsonschema.validate(request, ReceivedMessageSchema)
                
                # process request
                if request["type"] == "request":
                    # request for resource
                    request["id"] = clientid
                    clientid += 1
                    if self.request_resource(request):
                        # resource available
                        comm_socket.send_json({"available": True, "id": request["id"]})
                    else:
                        # give everything the same priority now
                        # TODO: allow optional priority and sort work items accordingly
                        self.work_items.append((0, request))
                        comm_socket.send_json({"available": False, "id": request["id"]})
                elif request["type"] == "hold":
                    # grab resource, removed from pub list
                    publish_list.remove(request["id"])
                    comm_socket.send_json({})
                elif request["type"] == "release":
                    self.finish_work(request["id"])
                    comm_socket.send_json({})
            
                    # find work and publish
                    while len(self.work_items) > 0: 
                        cid = self.find_work()
                        if cid != -1:
                            publish_list.add(cid)
                            pub_socket.send(b"%d %d" % (cid, 1))
                        else:
                            break 
                elif request["type"] == "config":
                    # reset config
                    self.config = request["config"]
                    comm_socket.send_json(request["config"])
                else:
                    comm_socket.send_json({})
                    raise Exception("Unknown request type")
        finally:
            context.destroy()

    # TODO proper error handling, json schema request
    # { type=request, resource, read=true|false, numopts, datasize, id (set by server) }
    def request_resource(self, request):
        resourcename = request["resource"]
        if resourcename not in self.resource_limits:
            self.resource_limits[resourcename] = ResourceManagerServer.ServerStats()
        stats = self.resource_limits[resourcename]
    
        if self.is_available(request, stats):
            if request["read"]:
                stats.read_reqs += request["numopts"]
                stats.read_data += request["datasize"]
            else:
                stats.write_reqs += request["numopts"]
                stats.write_data += request["datasize"]
            self.current_work_items[request["id"]] = request 
            return True
    
        return False
    
    
    # resource is not available if any of the resources are completely used
    # probably overly simplistic but usually calls are in batches of reads and writes separately
    def is_available(self, request, curr_stats):
        new_stats = copy.copy(curr_stats)
        if request["read"]:
            new_stats.read_reqs += request["numopts"]
            new_stats.read_data += request["datasize"]
        else:
            new_stats.write_reqs += request["numopts"]
            new_stats.write_data += request["datasize"]
        
        return (    new_stats.read_reqs <= self.config["read_reqs"]
                and new_stats.read_data <= self.config["read_data"]
                and new_stats.write_reqs <= self.config["write_reqs"]
                and new_stats.write_data <= self.config["write_data"] )
    
    # grab next resource (just grab oldest one)
    # TODO use priority to sort
    def find_work(self):
        priority, work_item = self.work_items[0]
        self.work_items.popleft()
        if self.request_resource(work_item):
            return work_item["id"]
        else:
            self.work_items.appendleft((priority,work_item))
            return -1
    
    # resource finished    
    def finish_work(self, curr_id):
        request = self.current_work_items[curr_id]
        del self.current_work_items[curr_id] 
        stats = self.resource_limits[request["resource"]]
     
        # update usage
        if request["read"]:
            stats.read_reqs -= request["numopts"]
            stats.read_data -= request["datasize"]
        else:
            stats.write_reqs -= request["numopts"]
            stats.write_data -= request["datasize"]

if __name__ == "__main__":
    sys.exit( main() )
