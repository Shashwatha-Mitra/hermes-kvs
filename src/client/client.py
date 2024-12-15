from hermes_pb2_grpc import Hermes, HermesStub
from hermes_pb2 import *
import grpc
import random
from logging import *
import time

class HermesClient(Hermes):
    def __init__(self, server_list: list, id, logger):
        self._server_list = server_list
        self._stubs = {}
        self._id = id
        for server in self._server_list:
            channel = grpc.insecure_channel(server)
            self._stubs[server] = HermesStub(channel)

        self.RETRY_TIMEOUT = 3 # 2 seconds
        self.NUM_RETRIES_PER_KEY = 7
        self.NUM_RETRIES_PER_SERVER = 1

        self.logger = logger

    def access_service(self, op, key, value, num_retries, retry_timeout):
        assert(op=="get" or op=="put")
        assert(len(self._server_list) > 0)

        if num_retries:
            retries = num_retries
        else:
            retries = self.NUM_RETRIES_PER_KEY

        if retry_timeout:
            timeout = retry_timeout
        else:
            timeout = self.RETRY_TIMEOUT

        if op == "put":
            assert(value)

        # Pick a random server to ping
        #server_list_idx = random.randint(0,len(self._server_list)-1)
        while (retries > 0):
            # Pick a random server first
            server_list_idx = random.randint(0, len(self._server_list)-1)
            server = self._server_list[server_list_idx]
            retries_per_server = self.NUM_RETRIES_PER_SERVER
            while (retries_per_server > 0):
                self.logger.info(f'''[{self._id}]: 
                    Querying server: {server}
                    op: {op}
                    key: {key}''')
                try:
                    if (op == "get"):
                        response = self._stubs[server].Read(ReadRequest(key=key), timeout=timeout)
                        self.logger.debug(f"[{self._id}]: Value: {response.value}")
                        return response.value
                    else:
                        response = self._stubs[server].Write(WriteRequest(key=key, value=value), timeout=timeout)
                        self.logger.debug(f"[{self._id}]: Put returned")
                        return
                except grpc.RpcError as e:
                    self.logger.info(f"gRPC call failed with status {e.code()}: {e.details()}")
                    retries_per_server -= 1
                    if (retries_per_server == 0):
                        self.logger.info(f"removing {self._server_list[server_list_idx]} from server list")
                        self._server_list.pop(server_list_idx) # remove the server from client's active server list
                        assert(len(self._server_list) > 0)
                        retries -= 1
                        print(f"remaining retries per key: {retries}")
                        #server_list_idx = ((server_list_idx + 1) % len(self._server_list))
                        if retries == 0:
                            self.logger.exception(f"Exception: {e.what()}")
                            raise e
                    else:
                        # wait for sometime before retrying if retrying the same server
                        time.sleep(1)
                except Exception as e:
                    self.logger.exception(f"Exception: {e.what()}")
                    raise e

    def get(self, key, num_retries=None, retry_timeout=None):
        return self.access_service("get", key, "", num_retries, retry_timeout)

    def put(self, key, value, num_retries=None, retry_timeout=None):
        self.access_service("put", key, value, num_retries, retry_timeout)

    def terminate(self, server_id, graceful=True, timeout=10):
        info(f"[{self._id}]: terminating server: {self._server_list[server_id]}")
        try:
            response = self._stubs[server_id].Terminate(TerminateRequest(graceful=graceful), timeout=timeout)
            debug(f"[{self._id}]: Terminate returned")
        except Exception as e:
            print(e.code())
            raise e
