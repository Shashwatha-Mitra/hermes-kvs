from hermes_pb2_grpc import Hermes, HermesStub
from hermes_pb2 import *
import grpc
import random
from logging import *
import time

class HermesClient(Hermes):
    def __init__(self, server_list: list, id=0):
        self._server_list = server_list
        self._stubs = list()
        self._id = id
        for server in self._server_list:
            channel = grpc.insecure_channel(server)
            self._stubs.append(HermesStub(channel))

        self.RETRY_TIMEOUT = 2 # 2 seconds
        self.NUM_RETRIES = 7

    def access_service(self, op, key, value, num_retries, retry_timeout, failure):
        assert(op=="get" or op=="put")

        if num_retries:
            retries = num_retries
        else:
            retries = self.NUM_RETRIES

        if retry_timeout:
            timeout = retry_timeout
        else:
            timeout = self.RETRY_TIMEOUT

        if op == "put":
            assert(value)

        # Pick a random server to ping
        random_server = random.randint(0,len(self._server_list)-1)
        while (retries > 0):
            # Pick a random server first
            # random_server = random.randint(0, len(self._server_list)-1)
            debug(f'''[{self._id}]: 
                Querying server: {self._server_list[random_server]}
                op: {op}
                key: {key}
            ''')
            try:
                if (op == "get"):
                    response = self._stubs[random_server].Read(ReadRequest(key=key), timeout=timeout)
                    debug(f"[{self._id}]: Value: {response.value}")
                    return response.value
                else:
                    response = self._stubs[random_server].Write(WriteRequest(key=key, value=value), timeout=timeout)
                    debug(f"[{self._id}]: Put returned")
                    return
            except Exception as e:
                retries -= 1

                if (failure):
                    random_server = ((random_server + 1) % len(self._server_list))
                else: 
                    random_server = random.randint(0,len(self._server_list)-1)
                if retries == 0:
                    raise e

                # wait for sometime before retrying
                time.sleep(1)

    def get(self, key, failure=False, num_retries=None, retry_timeout=None):
        return self.access_service("get", key, "", num_retries, retry_timeout, failure)

    def put(self, key, value, num_retries=None, retry_timeout=None, failure=False):
        self.access_service("put", key, value, num_retries, retry_timeout, failure)

    def terminate(self, server_id, graceful=True, timeout=10):
        info(f"[{self._id}]: terminating server: {self._server_list[server_id]}")
        try:
            response = self._stubs[server_id].Terminate(TerminateRequest(graceful=graceful), timeout=timeout)
            debug(f"[{self._id}]: Terminate returned")
        except Exception as e:
            print(e.code())
            raise e
