import argparse
import sys
import os
import subprocess
import time
import signal
import shutil
import random
import numpy as np
import threading

build_dir = ''
db_dir = ''
log_dir = ''

load_measurement_processes = []
master_processes = []
server_processes = []
client_processes = []

def getPartitionConfig(config_file):
    # initialize the partition dictionary
    partitions = {}
    with open(config_file, "r") as f:
        num_partitions  = int(f.readline())
        
        for i in range(num_partitions):
            partitions[i] = []

        for line in f:
            server = line.split(',')
            assert(len(server) == 3)
            partition_id = int(server[0].strip())
            server_id = server[1].strip()
            port = server[2].strip()
            partitions[partition_id].append((server_id,port))
    
    # shouldn't be empty
    assert(partitions) 
    
    return partitions

def get_servers(config_file):
    servers = list()
    with open(config_file, "r") as f:
        for line in f:
            server = line.split('\n')[0]
            servers.append(server)
    return servers

def getServerCmd(log_dir, config_file, port, db_dir):
    cmd = build_dir + 'server'
    cmd += ' ' + f'--id={port}'
    cmd += ' ' + f'--port={port}'
    cmd += ' ' + f'--log_dir={log_dir}'
    cmd += ' ' + f'--config_file={config_file}'
    if db_dir is not None:
        cmd += ' ' + f'--db_dir={db_dir}'
    
    return cmd

def launch_server(server_port, log_dir='', config_file='', db_dir=None):
    cmd = getServerCmd(log_dir, config_file, server_port, db_dir)
    print(f"Starting server {server_port}")
    print(cmd)
    log_file = log_dir + f'server_{server_port}.log'

    global server_processes

    with open(log_file, 'w') as f:
        process = subprocess.Popen(cmd, shell=True, stdout=f, stderr=f, preexec_fn=os.setsid)
        print(f"server {server_port}, pid {process.pid}")
        server_processes.append(process)

def launch_master(config_file, port, log_dir):
    cmd = build_dir + 'master'
    cmd += ' ' + f'--id={port}'
    cmd += ' ' + f'--port={port}'
    cmd += ' ' + f'--log_dir={log_dir}'
    cmd += ' ' + f'--config_file={config_file}'
    
    print(f"Starting master")
    print(cmd)
    log_file = log_dir + f'master.log'

    global master_processes

    with open(log_file, 'w') as f:
        process = subprocess.Popen(cmd, shell=True, stdout=f, stderr=f, preexec_fn=os.setsid)
        master_processes.append(process)
    
def createService(config_file, master_port, log_dir='', start_master=True, db_dir='db'):
    #TODO: start the manager before creating chains

    #time.sleep(5)
    servers = get_servers(config_file)
    #servers = servers[1:]
    print (log_dir)
    for server in servers:
        launch_server(server, log_dir, config_file, db_dir)

    if start_master:
        launch_master(config_file, master_port, log_dir)

    # partitions = getPartitionConfig(config_file)
    # for _, servers in partitions.items():
    #     createChain(servers, master_port, log_dir, db_dir)

def terminateProcess(process):
    pid = process.pid
    try:
        os.killpg(os.getpgid(pid), signal.SIGTERM)
        process.wait()
        print(f"Sent SIGTERM signal to process {pid}")
    except OSError:
        print(f"Failed to send SIGTERM signal to process {pid}")

def terminateMaster():
    global master_processes
    for process in master_processes:
        terminateProcess(process)

def terminateServers():
    global server_processes
    for process in server_processes:
        terminateProcess(process)

def terminateService():
    terminateMaster()
    terminateServers()


def startClients(args):
    for client_id in range(args.num_clients):
#        real_fname = args.real_fname + str(client_id) + '.csv'
#        fake_fname = args.fake_fname + str(client_id) + '.csv'
        
        cmd = 'python3 simple_client.py'
        cmd += ' ' + f'--id={client_id}'
        cmd += ' ' + f'--config-file={args.config_file}'
#        cmd += ' ' + f'--test-type={args.test_type}'
#        cmd += ' ' + f'--top-dir={args.top_dir}'
        cmd += ' ' + f'--log-dir={args.log_dir}'
#        cmd += ' ' + f'--num-keys={args.num_keys}'
        
        if (args.vk_ratio != 0):  
            cmd += ' ' + f'--vk_ratio={args.vk_ratio}'

        if (args.skew):
            cmd += ' ' + f'--skew'

        print(f"Starting client {client_id}")
        print(cmd)
        log_file = log_dir + f'client_{client_id}.log'

        with open(log_file, 'w') as f:
            process = subprocess.Popen(cmd, shell=True, stdout=f, stderr=f)
            client_processes.append(process)
    #manualKillServers()


def terminateClients():
    global client_processes
    for process in client_processes:
        terminateProcess(process)

def terminateTest():
    for process in load_measurement_processes:
        terminateProcess(process)
    terminateClients()
    terminateService()
    sys.exit(1)   

def waitToFinish():
    for process in client_processes:
        process.wait()

def checkAndMakeDir(path):
    if os.path.exists(path) and os.path.isdir(path):
        # If the directory exists, clear its contents
        print(f"Directory '{path}' exists. Clearing its contents.")
        shutil.rmtree(path)  # Remove the directory and all its contents
        os.makedirs(path)  # Recreate the directory
    else:
        # Create the directory
        print(f"Directory '{path}' does not exist. Creating it.")
        os.makedirs(path)

def startLoadMeasurement(log_dir, master_processes, server_processes):
    cmd = 'python3 measure_load.py'
    pid_str = ''
    for process in master_processes:
        pid_str += ' ' + str(process.pid)
    for process in server_processes:
        pid_str += ' ' + str(process.pid)
    cmd += ' ' + f'--pids' + pid_str
    cmd += ' ' + f'--snapshot-duration=2'
    cmd += ' ' + f'--log-dir={log_dir}'

    print(cmd)
    global load_measurement_processes 
    process = subprocess.Popen(cmd, shell=True)
    load_measurement_processes.append(process)


def startKiller(config_file, clean=1, strategy='random'):
    killer_process_cmd = f'python3 kill_servers.py --config-file={config_file} --clean={clean} --strategy={strategy}'
    log_file = log_dir + f'killer.log'
    print ('Starting Killer Process')
    print (killer_process_cmd)

    with open(log_file, 'w') as f:
        process = subprocess.Popen(killer_process_cmd, shell=True, stdout=f, stderr=f, preexec_fn=os.setsid)
        return process

def manualKillServers(choice, wait_time = 1):
    time.sleep(wait_time)
    global server_processes
    node = None
    assert(choice < len(server_processes))
    node = server_processes[choice]
    print (f'Killing server {node}')
    #if (choice == 0):
    #    print ('Killing tail server')
    #    node = server_processes[-1]
    #elif (choice == 1):
    #    print ('Killing head server')
    #    node = server_processes[0]
    #else:
    #    node = random.choice(server_processes[1:-1])
    #    node = server_processes[1]
    #    print (f'Killing server {node}')
        

    if (node != None):
        terminateProcess(node)
        server_processes.remove(node)
    else:
        print ('Cannot remove a non-existing process')

if __name__ == "__main__":

    parser = argparse.ArgumentParser()

    parser.add_argument('--config-file', type=str, default='test_config.txt', help='chain configuration file')
#    parser.add_argument('--eeal-fname', type=str, default='real')
#    parser.add_argument('--fake-fname', type=str, default='fake')
    parser.add_argument('--test-type', type=str, default='sanity', help='sanity, correctness, crash_consistency, perf, availability')
    parser.add_argument('--top-dir', type=str, default='../', help='path to top dir')
    parser.add_argument('--log-dir', type=str, default='out/', help='path to log dir')
    parser.add_argument('--num-clients', type=int, default=1, help='number of clients')
    parser.add_argument('--master-port', type=str, default='60060', help='master port')
    parser.add_argument('--skew', action='store_true')
    parser.add_argument('--vk_ratio', type=int, default=0, help='ratio of value to key lenght')
    parser.add_argument('--num-keys', type=int, default=1000, help='number of gets to put and get in sanity test')


    parser.add_argument('--only-clients', action='store_true')
    parser.add_argument('--only-service', action='store_true')

    args = parser.parse_args()
    
    top_dir = args.top_dir
    test_type = args.test_type
    config_file = top_dir + args.config_file
    
    build_dir = top_dir + 'build/src/'
    db_dir = top_dir + 'db/'
    log_dir = top_dir + args.log_dir
    
    assert (not (args.only_clients and args.only_service))

    checkAndMakeDir(log_dir)
    checkAndMakeDir(db_dir)

    graceful_failure = False

    if (not args.only_clients):
        try:
            createService(config_file, args.master_port, log_dir=log_dir, start_master=(not graceful_failure), db_dir=db_dir)
        except Exception as e:
            print(f"An unexpected exception occured while starting service: {e}")
            terminateTest()
            
 #       time.sleep(30)

    # startLoadMeasurement(log_dir, master_processes, server_processes)

    if (not args.only_service):
        try:
            startClients(args)
        except Exception as e:
            print(f"An unexpected exception occured while starting clients: {e}")
            terminateTest()

    # if args.test_type == 'availability':
    #manualKillServers(0)
    # if not graceful_failure:
    #     threading.Thread(target=manualKillServers, args=(2,6,)).start()

    # time.sleep(5)
    # startServer([10, 5450], 50000, log_dir, db_dir)
    # time.sleep(30)

    if (not args.only_service):
        waitToFinish()
    else:
        kill = input("Press <enter> when you want to terminate the service: ")


    print("Test finished. Terminating service")
    # wait for sometime to flush the stdout buffers to the log file
    time.sleep(2)
    terminateService()
    # for process in load_measurement_processes:
    #    terminateProcess(process)
    
    print("Test End")
