# Standalone test to check concurrent writes on servers

import threading, random, string
import logging
import sys
import time

sys.path.append('../src/client/')
from client import HermesClient


def gen_random_value():
    return ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(10))

num_clients = 2
b1 = threading.Barrier(num_clients, timeout=5)
b2 = threading.Barrier(num_clients, timeout=5)

server_list = ['localhost:50050', 'localhost:50051', 'localhost:50052', 'localhost:50053', 'localhost:50054']

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

def run_writes_same_key(key, client_id, num_writes):
    # global b1, b2
    mock_server_list = ['localhost:50052'] #if client_id == 1 else ['localhost:50050']
    server_list = ['localhost:50050', 'localhost:50051', 'localhost:50052', 'localhost:50053', 'localhost:50054']
    client = HermesClient(server_list, id=client_id)
    num_retries = 1
    retry_timeout = 10
    for i in range(num_writes):
        val = gen_random_value()
        # print(f"[{client_id}]: Putting value: {val}")
        # b1.wait()
        # print(f"Write num: {i}")
        client.put(key, val, num_retries=num_retries, retry_timeout=retry_timeout)
        # print(f"[{client_id}]: Getting value: ")
        #b2.wait()
        # if client_id == 0:
        #     b1.reset()
        # b2.reset()
        #val_returned = client.get(key, num_retries=num_retries)

        # if (val != val_returned) and client_id == 1:
        #     print(f"[{client_id}]: Incorrect value returned")
        # print(f"[{client_id}]: Got value: {val_returned}")

def run_writes(key, client_id, num_writes, server_list):
    # global b1, b2
    client = HermesClient(server_list, id=client_id)
    #key = gen_random_value()
    num_retries = 10
    retry_timeout = 10
    for i in range(num_writes):
        val = gen_random_value()
        # print(f"[{client_id}]: Putting value: {val}")
        client.put(key, val, num_retries=num_retries, retry_timeout=retry_timeout)
        # print(f"[{client_id}]: Getting value: ")
        #b2.wait()
        # if client_id == 0:
        #     b1.reset()
        # b2.reset()
        #val_returned = client.get(key, num_retries=num_retries)

        # if (val != val_returned) and client_id == 1:
        #     print(f"[{client_id}]: Incorrect value returned")
        # print(f"[{client_id}]: Got value: {val_returned}")

def run_reads(key, client_id, num_ops, server_list):
    # global b1, b2
    client = HermesClient(server_list, id=client_id)
    #key = gen_random_value()
    num_retries = 1
    retry_timeout = 10
    for i in range(num_ops):
        val = gen_random_value()
        # print(f"[{client_id}]: Putting value: {val}")
        client.get(key, num_retries=num_retries, retry_timeout=retry_timeout)
        # print(f"[{client_id}]: Getting value: ")
        #b2.wait()
        # if client_id == 0:
        #     b1.reset()
        # b2.reset()
        #val_returned = client.get(key, num_retries=num_retries)

        # if (val != val_returned) and client_id == 1:
        #     print(f"[{client_id}]: Incorrect value returned")
        # print(f"[{client_id}]: Got value: {val_returned}")
        time.sleep(0.1)

def terminate_server(client_id, server_id):
    client = HermesClient(server_list, id=client_id)
    time.sleep(50/1000) # sleep for in ms
    client.terminate(server_id)

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.DEBUG)
    # threads = [threading.Thread(group=None, target=run_writes, args=(i,)) for i in range(5)]
    key = gen_random_value()
    num_writes = 100
    num_reads = 100
    op_threads = []
    # for i in range(num_clients):
    #     if i%2 == 0:
    #         op_threads.append(threading.Thread(group=None, target=run_writes, args=(key, i, num_writes, ['localhost:50052'])))
    #     else:
    #         op_threads.append(threading.Thread(group=None, target=run_reads, args=(key, i, num_reads, ['localhost:50050'])))
    op_threads = [threading.Thread(group=None, target=run_writes_same_key, args=(key, i, num_writes,)) for i in range(num_clients)]
    [t.start() for t in op_threads]

    #terminate_server_id = 2
    #terminator_id = num_clients
    #terminate_thread = threading.Thread(group=None, target=terminate_server, args=(terminator_id, terminate_server_id,))
    #terminate_thread.start()

    [t.join() for t in op_threads]
    #terminate_thread.join()