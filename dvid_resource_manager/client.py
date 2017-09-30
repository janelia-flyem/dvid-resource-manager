from contextlib import closing
import jsonschema
import zmq

from dvid_resource_manager.schemas import RequestMessageSchema, HoldMessageSchema, ReleaseMessageSchema

class ResourceManagerClient:
    """
    Usage:
    
        >>> volume_bytes = np.prod(my_volume.size) * my_volume.dtype.type().nbytes
        >>> client = ResourceManagerClient(resource_manager_server_ip, port)
        >>> with client.access_context( data_server_ip, False, 1, volume_bytes ):
        ...    send_volume(data_server_ip, my_volume)
    """

    def __init__(self, server_ip, server_port, _debug=False):
        """
        _debug: Used during testing.  Forces validation of json schemas
        """
        self.server_ip = server_ip
        self.server_port = server_port

        self._context = zmq.Context()
        self._commsocket = self._context.socket(zmq.REQ)
        self._commsocket.connect(f'tcp://{server_ip}:{server_port}')
        self._debug = _debug

    def access_context(self, resource_name, is_read, numopts, data_size):
        """
        Primary API function. (See usage above.)
        Returns a contextmanager object.
        While the context is active, access is granted to the the requested resource.  
        """
        return ResourceManagerClient.AccessContext(self, resource_name, is_read, numopts, data_size)

    def close(self):
        # Note: Docs say that destroy() is not threadsafe, so it's not safe to
        #       call this if zeromq sockets are being used in any other threads.
        assert self._commsocket is not None
        assert self._context is not None
        self._commsocket.close()
        self._context.term()
        self._context = None

    def __del__(self):
        if self._context is not None:
            self.close()

    def _attempt_acquire(self, resource_name, is_read, num_reqs, data_size):
        """
        Acquire the given resource from the resource manager.
        Blocks until the resource is available.
        
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
        response = self._commsocket.recv_json()
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
        self._commsocket.recv_json()

    def _release(self, request_id):
        msg = { "type": "release", "id": request_id }
        if self._debug:
            jsonschema.validate(msg, ReleaseMessageSchema)
        self._commsocket.send_json( msg )
        self._commsocket.recv_json()

    class AccessContext:
        """
        Context manager to obtain access to a resource upon entering the
        context and release it upon exiting the context.
        """
        def __init__(self, client, resource_name, is_read, num_reqs, data_size):
            self.client = client
            self.request_id = None
            self.resource_name = resource_name
            self.is_read = is_read
            self.num_reqs = num_reqs
            self.data_size = data_size

        def __enter__(self):
            self.request_id, success = self.client._attempt_acquire(self.resource_name, self.is_read, self.num_reqs, self.data_size)
            if not success:
                self.client._wait_for_acquire(self.request_id)
            return self
        
        def __exit__(self, *_args):
            self.client._release(self.request_id)

