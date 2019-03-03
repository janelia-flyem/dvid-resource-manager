"""
This file defines functions needed to test multiprocess behavior,
which can't be defined in test.py if that file is run as __main__,
due to the fact that multiprocessing can't pickle functions from __main__.
"""
from itertools import chain
from functools import partial
from multiprocessing.pool import Pool, ThreadPool

def perform_access(client, _):
    obj_id = id(client._get_client())
    with client.access_context( 'my-resource', True, 1, 1000 ):
        return obj_id

def threaded_access(client, _):
    with ThreadPool(2) as threadpool:
        obj_ids = threadpool.map(partial(perform_access, client), range(4))
    return obj_ids

def _test_multiprocess(client):
    """
    Verify that if we pickle a client,
    it can be used from multiple processes and multiple threads per process,
    and that each underlying _ResourceManagerClient from _get_client() is unique.
    """
    with Pool(4) as pool:
        obj_id_groups = pool.map(partial(threaded_access, client), range(4))

    # 4 processes * 2 threads
    assert len(set(chain(*obj_id_groups))) == 4*2
