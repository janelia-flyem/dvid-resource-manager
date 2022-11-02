import os
import json
import threading
from contextlib import closing, contextmanager

import jsonschema
import zmq

from dvid_resource_manager.schemas import RequestMessageSchema, HoldMessageSchema, ReleaseMessageSchema, ConfigSchema

class TimeoutError(RuntimeError):
    pass


class ResourceManagerClient:
    """
    Manages multiple low-level _ResourceManagerClient instances,
    one for each thread in which this object is used.
    
    Also, if server_ip is None, then a _DummyClient is used.
    That helps avoid boilerplate if-statements like this:
    
        if resource_manager_server_ip:
            client = ResourceManagerClient(resource_manager_server_ip, port)
            with client.access_context( data_server_ip, False, 1, volume_bytes ):
                send_data(...)
        else:
            send_data(...)
    
    Instead, just write this:

        client = ResourceManagerClient(resource_manager_server_ip, port)
        with client.access_context( data_server_ip, False, 1, volume_bytes ):
            send_data(...)
    """
    def __init__(self, server_ip, server_port, _debug=False):
        assert server_ip != "driver", "Invalid server_ip"  # Possible mistake when using flyemflows.
        self.server_ip = server_ip
        self.server_port = server_port
        self._debug = _debug
        self._clients = {}
    
    def _get_client(self):
        if not self.server_ip:
            return _DummyClient()

        thread_id = threading.current_thread().ident
        pid = os.getpid()
    
        try:
            client = self._clients[(thread_id, pid)]
        except KeyError:
            client = _ResourceManagerClient(self.server_ip, self.server_port, self._debug)
            self._clients[(thread_id, pid)] = client
    
        return client
    
    def access_context(self, resource_name, is_read, num_reqs, data_size):
        return self._get_client().access_context(resource_name, is_read, num_reqs, data_size)
    
    def reconfigure_server(self, config):
        return self._get_client().reconfigure_server(config)
    
    def read_config(self):
        return self._get_client().read_config()
    
    def close(self):
        for client in self._clients.values():
            client.close()

    def __getstate__(self):
        """
        Pickle support
        """
        d = self.__dict__.copy()
        d['_clients'] = {}
        return d


class _DummyClient:
    """
    Mimics the public API of _ResourceManagerClient, but otherwise does nothing.
    """
    server_ip = ""
    server_port = 0

    @contextmanager
    def access_context(self, *args, **kwargs):
        yield
    
    
class _ResourceManagerClient:
    """
    Usage:
    
        >>> volume_bytes = np.prod(my_volume.size) * my_volume.dtype.type().nbytes
        >>> client = ResourceManagerClient(resource_manager_server_ip, port)
        >>> with client.access_context( data_server_ip, False, 1, volume_bytes ):
        ...    send_volume(data_server_ip, my_volume)

    Note: This class is NOT threadsafe, due to the way it uses zeromq.
          If you need to use multiple threads, create a separate
          ResourceManagerClient instance for each thread.
    
    Pickling: This class can be pickled, but it's an error to do
              so while an AccessContext is active.
    """

    def __init__(self, server_ip, server_port, _debug=False):
        """
        _debug: Used during testing.  Forces validation of json schemas
        """
        self.server_ip = server_ip
        self.server_port = server_port
        self._debug = _debug
        self._currently_accessing = False
        self._initialize_zmq()

    def _initialize_zmq(self):
        self._context = zmq.Context()
        self._init_socket()
    
    def _init_socket(self):
        self._commsocket = self._context.socket(zmq.REQ)
        self._commsocket.connect(f'tcp://{self.server_ip}:{self.server_port}')
        self._poller = zmq.Poller()
        self._poller.register(self._commsocket, zmq.POLLIN)

    def _close_socket(self):
        # Note: Docs say that destroy() is not threadsafe, so it's not safe to
        #       call this if zeromq sockets are being used in any other threads.
        assert self._commsocket is not None
        assert self._context is not None
        self._commsocket.setsockopt(zmq.LINGER, 1000) # timeout of 1 second
        self._commsocket.close()
        self._poller.unregister(self._commsocket)

    def close(self):
        self._close_socket()
        self._context.term()
        self._context = None

    def __del__(self):
        if self._context is not None:
            self.close()

    def access_context(self, resource_name, is_read, num_reqs, data_size):
        """
        Primary API function. (See usage above.)
        Returns a contextmanager object.
        While the context is active, access is granted to the the requested resource.  
        """
        return _ResourceManagerClient.AccessContext(self, resource_name, is_read, num_reqs, data_size)

    def reconfigure_server(self, config):
        jsonschema.validate(config, ConfigSchema)
        self._commsocket.send_json( { "type": "config",
                                      "config": config } )
        response = self._recv_json_safe()
        if response != config:
            raise RuntimeError("Server failed to apply the new config")

    def read_config(self):
        self._commsocket.send_json( { "type": "read-config" } )
        response = self._recv_json_safe()
        if response["type"] != "read-config":
            raise RuntimeError(f"Bad response from resource manager server: {response}")
        return response["config"]

    def __getstate__(self):
        """
        Pickle support.
        """
        assert not self._currently_accessing, \
            "Not allowed to pickle _ResourceManagerClient while an AcessContext is active."
        d = self.__dict__.copy()
        d['_context'] = None
        d['_commsocket'] = None
        d['_poller'] = None
        return d
    
    def __setstate__(self, state):
        self.__dict__.update(state)
        self._initialize_zmq()

    def _recv_json_safe(self, timeout_ms=4000):
        """
        Receive a json message from the socket,
        or if the server sends no response at all,
        raise a TimeoutError (rather than hanging indefinitely).
        
        timeout_ms:
            How long the poller should wait to receive the reply from the server,
            which might be busy with other clients and therefore take a while to respond.
            When there are 1000 clients, a timeout of 2 seconds seems to be too short,
            but 4 seconds is enough. For 2000 clients, 4 seconds is still too short.
        """
        events = dict(self._poller.poll(timeout_ms))
        if events.get(self._commsocket, None):
            return self._commsocket.recv_json()
        else:
            # With paired REQ (client) and REP (server) sockets,
            # It's forbidden to call recv again if you haven't received a reply.
            # We must reinitialize the socket before trying again.
            # http://zguide.zeromq.org/page%3aall#Client-Side-Reliability-Lazy-Pirate-Pattern
            # http://zguide.zeromq.org/py:lpclient
            self._close_socket()
            self._init_socket()
            raise TimeoutError(f"Timed out after {timeout_ms/1000} seconds")
    
    def _attempt_acquire(self, resource_name, is_read, num_reqs, data_size):
        """
        Attempt to acquire the given resource from the resource manager.
        If it is currently available, we will be the owner (we acquired it).
        if it isn't, we will have to wait, using the ID in the server response.
        
        Returns (request_id, acquired)
        """
        req_data = {
            "type": "request",
            "resource": resource_name,
            "read": is_read,
            "numopts": num_reqs,
            "datasize": data_size
        }
        if self._debug:
            jsonschema.validate(req_data, RequestMessageSchema)
        
        self._commsocket.send_json( req_data )
        response = self._recv_json_safe()
        if 'invalid' in response:
            config = self.read_config()
            msg = (
                "Cannot acquire access to the shared resource because "
                "your request exceeds the server's config maximums.\n"
                f"Config maximums: {json.dumps(config)}\n"
                f"Your request: {json.dumps(req_data)}")
            raise RuntimeError(msg)
        return (response["id"], response["available"])
    
    def _wait_for_acquire(self, request_id):
        sub_socket = self._context.socket(zmq.SUB)
        sub_socket.setsockopt(zmq.SUBSCRIBE, str(request_id).encode('utf-8'))
        sub_port = self.server_port + 1
        sub_socket.connect(f'tcp://{self.server_ip}:{sub_port}')
        with closing( sub_socket ):
            # Wait happens here:
            # We won't receive anything until the server grants us access to the resource.
            sub_socket.recv()

        msg = { "type": "hold", "id": request_id }
        if self._debug:
            jsonschema.validate(msg, HoldMessageSchema)

        self._commsocket.send_json( msg  )
        self._recv_json_safe(None) # No timeout here: We could be waiting for a long time.

    def _release(self, request_id):
        msg = { "type": "release", "id": request_id }
        if self._debug:
            jsonschema.validate(msg, ReleaseMessageSchema)
        self._commsocket.send_json( msg )
        self._recv_json_safe()

    class AccessContext:
        """
        Context manager to obtain access to a resource upon entering the
        context and release it upon exiting the context.
        """
        def __init__(self, client, resource_name, is_read, num_reqs, data_size):
            self.client = client
            self.request_id = None
            self.resource_name = resource_name
            self.is_read = bool(is_read)
            self.num_reqs = int(num_reqs)
            self.data_size = int(data_size)

        def __enter__(self):
            assert not self.client._currently_accessing, \
                "Not allowed to use AccessContext in parallel or (nested)"
            self.client._currently_accessing = True
            try:
                self.request_id, success = self.client._attempt_acquire(self.resource_name, self.is_read, self.num_reqs, self.data_size)
                if not success:
                    self.client._wait_for_acquire(self.request_id)
            except BaseException:
                self.client._currently_accessing = False
                raise
            return self

        def __exit__(self, *_args):
            self.client._release(self.request_id)
            self.client._currently_accessing = False
