# Standalone test to check concurrent writes on servers

import threading, random, string
from client import HermesClient
import logging

def gen_random_value():
    return ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(10))

b1 = threading.Barrier(2, timeout=5)
b2 = threading.Barrier(2, timeout=5)


def run_writes(client_id):
    server_list = ['localhost:50050', 'localhost:50051', 'localhost:50052', 'localhost:50053', 'localhost:50054']
    client = HermesClient(server_list)
    key = gen_random_value()
    for i in range(100):
        val = gen_random_value()
        print(f"[{client_id}]: Putting value: {val}")
        client.put(key, val)
        # print(f"[{client_id}]: Getting value: ")
        val_returned = client.get(key)
        if (val != val_returned):
            print(f"[{client_id}]: Incorrect value returned")
        print(f"[{client_id}]: Got value: {val_returned}")

def run_writes_same_key(key, client_id):
    # global b1, b2
    server_list = ['localhost:50050', 'localhost:50051', 'localhost:50052', 'localhost:50053', 'localhost:50054']
    mock_server_list = ['localhost:50052'] if client_id == 1 else ['localhost:50050']
    client = HermesClient(mock_server_list, id=client_id)
    for i in range(500):
        val = gen_random_value()
        # print(f"[{client_id}]: Putting value: {val}")
        b1.wait()
        client.put(key, val)
        # print(f"[{client_id}]: Getting value: ")
        b2.wait()
        # if client_id == 0:
        #     b1.reset()
        # b2.reset()
        val_returned = client.get(key)
        # if (val != val_returned) and client_id == 1:
        #     print(f"[{client_id}]: Incorrect value returned")
        # print(f"[{client_id}]: Got value: {val_returned}")

if __name__ == '__main__':
    # logging.getLogger().setLevel(logging.DEBUG)
    # threads = [threading.Thread(group=None, target=run_writes, args=(i,)) for i in range(5)]
    key = gen_random_value()
    threads = [threading.Thread(group=None, target=run_writes_same_key, args=(key, i,)) for i in range(2)]
    [t.start() for t in threads]
    [t.join() for t in threads]
