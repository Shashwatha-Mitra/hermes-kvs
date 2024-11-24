from hermes_pb2_grpc import Hermes, HermesStub
from hermes_pb2 import *
import grpc
import random
from logging import *

class HermesClient(Hermes):
    RETRY_TIMEOUT = 10 # seconds

    def __init__(self, server_list: list, id=0):
        self._server_list = server_list
        self._stubs = list()
        self._id = id
        for server in self._server_list:
            channel = grpc.insecure_channel(server)
            self._stubs.append(HermesStub(channel))

    def get(self, key):
        retries = 5
        while (retries > 0):
            random_server = random.randint(0, len(self._server_list)-1)
            info(f"[{self._id}]: Querying server for get: {self._server_list[random_server]}")
            try:
                response = self._stubs[random_server].Read(ReadRequest(key=key), timeout=self.RETRY_TIMEOUT)
                debug(f"[{self._id}]: Value: {response.value}")
                return response.value
            except Exception as e:
                retries -= 1
                if retries == 0:
                    raise e

    def put(self, key, value):
        retries = 5
        while (retries > 0):
            random_server = random.randint(0, len(self._server_list)-1)
            info(f"[{self._id}]: Querying server for put: {self._server_list[random_server]}")
            try:
                response = self._stubs[random_server].Write(WriteRequest(key=key, value=value), timeout=self.RETRY_TIMEOUT)
                debug(f"[{self._id}]: Put returned")
                return
            except Exception as e:
                retries -= 1
                if retries == 0:
                    raise e
