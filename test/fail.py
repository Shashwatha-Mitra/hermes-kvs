import sys
import time

sys.path.append('../src/client/')

def terminate(cl, graceful=False):
    terminate_server_id = 2
    time.sleep(2)
    cl.terminate(terminate_server_id, graceful)
