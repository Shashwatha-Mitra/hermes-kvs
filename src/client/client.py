from hermes_pb2_grpc import Hermes, HermesStub
from hermes_pb2 import *
import grpc
import random

class HermesClient(Hermes):
    RETRY_TIMEOUT = 10 # seconds

    def __init__(self, server_list: list):
        self._server_list = server_list
        self._stubs = list()
        for server in self._server_list:
            channel = grpc.insecure_channel(server)
            self._stubs.append(HermesStub(channel))

    def get(self, key):
        retries = 5
        val = ''
        while (retries > 0):
            random_server = random.randint(0, len(self._server_list)-1)
            print(f"Querying server: {self._server_list[random_server]}")
            random_server = random.randint(0, len(self._server_list)-1)
            print(f"Querying server: {self._server_list[random_server]}")
            response = self._stubs[random_server].Read(ReadRequest(key=key))
            print(f"Value: {response.value}")
            val = response.value
            retries -= 1
        return val

    def put(self, key, value):
        retries = 5
        while (retries > 0):
            random_server = random.randint(0, len(self._server_list)-1)
            print(f"Querying server: {self._server_list[random_server]}")
            response = self._stubs[random_server].Write(WriteRequest(key=key, value=value))
            retries -= 1
