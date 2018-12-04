import os
import sys
import json
import time
import pickle
import unittest
import threading
import subprocess

import dvid_resource_manager.server
from dvid_resource_manager.client import ResourceManagerClient

SERVER_PORT = 3000

def with_server(config_updates):
    """
    Decorator.
    
    Launch the resource manager server with the given config.
    (Any keys missing in the config will be supplied from server.DEFAULT_CONFIG.)
    
    Then run the decorated function, and kill the server after that function exits.
    """
    server_config = dvid_resource_manager.server.DEFAULT_CONFIG.copy()
    server_config.update(config_updates)
    
    def decorator(func):
        def wrapper( *args, **kwargs ):
            server_config_path = '/tmp/test-resource-server-config.json'
            with open(server_config_path, 'w') as f:
                json.dump(server_config, f)
    
            server_script = dvid_resource_manager.server.__file__
            cmd = f"{sys.executable} {server_script} {SERVER_PORT} --config-file={server_config_path} --debug"
            server_process = subprocess.Popen(cmd, shell=True)

            try:
                func(*args, **kwargs)
                assert server_process.poll() is None, \
                    f"Server exited prematurely with code {server_process.poll()}"
            finally:
                if server_process.poll() is None:
                    server_process.kill()
                    time.sleep(0.1)
                    assert server_process.poll() is not None, \
                        "Server process did not respond to SIGTERM"
                os.unlink(server_config_path)
        return wrapper
    return decorator


class Test(unittest.TestCase):
    ##
    ## TODO: This test suite does not check the following:
    ##       - data size limits
    ##
    
    @with_server({"write_reqs": 2})
    def test_basic(self):
        """
        Request access to a resource.
        """
        resource = 'my-resource'
        client = ResourceManagerClient('127.0.0.1', SERVER_PORT, _debug=True)
        assert client.read_config()["write_reqs"] == 2
        with client.access_context( resource, False, 1, 1000 ):
            pass

    @with_server( { "write_reqs": 1 } )
    def test_exclusive_access(self):
        """
        Verify that the server does not grant simultaneous access
        to two clients if it is configured to allow only one at a time.
        """
        resource = 'my-resource'
        DELAY = 0.5
        
        client_1 = ResourceManagerClient('127.0.0.1', SERVER_PORT, _debug=True)
        client_2 = ResourceManagerClient('127.0.0.1', SERVER_PORT, _debug=True)
        
        task_started = threading.Event()
        def long_task():
            with client_1.access_context( resource, False, 1, 1000 ):
                task_started.set()
                time.sleep(DELAY)

        start = time.time()
        th = threading.Thread(target=long_task)
        th.start()

        task_started.wait()
        with client_2.access_context( resource, False, 1, 1000 ):
            assert time.time() - start >= DELAY, \
                "We shouldn't have been granted access to the resource so quickly!"

        th.join()

    @with_server( { "write_reqs": 1, "read_reqs": 1 } )
    def test_parallel_read_write_access(self):
        """
        Verify that the server DOES grant simultaneous access
        to two clients if one is reading and the other is writing,
        as long as neither is over capacity already.
        """
        resource = 'my-resource'
        DELAY = 0.5

        client_1 = ResourceManagerClient('127.0.0.1', SERVER_PORT, _debug=True)
        client_2 = ResourceManagerClient('127.0.0.1', SERVER_PORT, _debug=True)
        
        task_started = threading.Event()
        def long_task():
            with client_1.access_context( resource, True, 1, 1000 ):
                task_started.set()
                time.sleep(DELAY)

        start = time.time()
        th = threading.Thread(target=long_task)
        th.start()

        task_started.wait()
        with client_2.access_context( resource, False, 1, 1000 ):
            assert (time.time() - start) < DELAY, \
                "The server seems to have incorrectly forbidden parallel access for reading and writing."

        th.join()

    def test_dummy_client(self):
        with ResourceManagerClient("", "").access_context():
            assert True

    @with_server({"write_reqs": 2})
    def test_pickle(self):
        """
        Copy the client via pickling and then use the copy after unpickling.
        """
        resource = 'my-resource'
        client = ResourceManagerClient('127.0.0.1', SERVER_PORT, _debug=True)
        with client.access_context( resource, False, 1, 1000 ):
            pass
        
        pickled_client = pickle.dumps(client)
        unpickled_client = pickle.loads(pickled_client)
        
        with unpickled_client.access_context( resource, False, 1, 1000 ):
            pass

    @with_server({"write_reqs": 2})
    def test_pickle_fails_during_access_context(self):
        """
        Pickling is forbidden while an access context is active.
        Verify that an exception is raised.
        """
        resource = 'my-resource'
        client = ResourceManagerClient('127.0.0.1', SERVER_PORT, _debug=True)

        try:
            with client.access_context( resource, False, 1, 1000 ):
                _pickled_client = pickle.dumps(client)
        except AssertionError:
            pass
        else:
            assert False, "Expected pickle to fail!"


    @with_server({})
    def test_reconfigure(self):
        client = ResourceManagerClient('127.0.0.1', SERVER_PORT, _debug=True)
        orig_config = client.read_config()
        
        new_config = orig_config.copy()
        new_config["read_reqs"] = 123
        new_config["write_reqs"] = 456
        
        client.reconfigure_server(new_config)
        assert client.read_config() == new_config

if __name__ == "__main__":
    unittest.main()
