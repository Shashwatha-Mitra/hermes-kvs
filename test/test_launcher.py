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
import psutil
import csv
from datetime import datetime

build_dir = ''
db_dir = ''
log_dir = ''

load_measurement_processes = []
master_processes = {}
server_processes = {}
client_processes = []

monitor_stop_event = threading.Event()

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

def getServerCmd(log_dir, config_file, port):
    cmd = build_dir + '/server'
    cmd += ' ' + f'--id={port}'
    cmd += ' ' + f'--port={port}'
    cmd += ' ' + f'--log_dir={log_dir}'
    cmd += ' ' + f'--config_file={config_file}'
    
    return cmd

def launch_server(server_port, log_dir='', config_file=''):
    cmd = getServerCmd(log_dir, config_file, server_port)
    print(f"Starting server {server_port}")
    print(cmd)
    log_file = log_dir + f'/server_{server_port}.log'

    global server_processes

    with open(log_file, 'w') as f:
        process = subprocess.Popen(cmd, shell=True, stdout=f, stderr=f, preexec_fn=os.setsid)
        print(f"server {server_port}, pid {process.pid}")
        server_processes[server_port] = process

def launch_master(config_file, port, log_dir):
    cmd = build_dir + '/master'
    cmd += ' ' + f'--id={port}'
    cmd += ' ' + f'--port={port}'
    cmd += ' ' + f'--log_dir={log_dir}'
    cmd += ' ' + f'--config_file={config_file}'
    
    print(f"Starting master")
    print(cmd)
    log_file = log_dir + f'/master.log'

    global master_processes

    with open(log_file, 'w') as f:
        process = subprocess.Popen(cmd, shell=True, stdout=f, stderr=f, preexec_fn=os.setsid)
        master_processes[port] = process
    
def createService(config_file, master_port, log_dir='', start_master=True):
    #TODO: start the manager before creating chains

    #time.sleep(5)
    servers = get_servers(config_file)
    #servers = servers[1:]
    print (log_dir)
    for server in servers:
        launch_server(server, log_dir, config_file)

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
    for _, process in master_processes.items():
        terminateProcess(process)

def terminateServers():
    global server_processes
    for _, process in server_processes.items():
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
    for _, process in master_processes.items():
        pid_str += ' ' + str(process.pid)
    for _, process in server_processes.items():
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

def manualKillServers(server_id, wait_time = 1):
    time.sleep(wait_time)
    global server_processes
    assert(server_id in server_processes.keys())
    node = server_processes[server_id]
    print (f'Killing server {server_id}')

    if (node != None):
        terminateProcess(node)
        del server_processes[server_id]
    else:
        print ('Cannot remove a non-existing process')

def monitor_cpu_utilization(processes, output_csv, sampling_rate=1, duration=30, stop_event=None):
    """
    Launches the commands as subprocesses and monitors their CPU utilization, dumping results into a CSV.
    
    Args:
        output_csv (str): Path to the output CSV file.
        sampling_rate (int): Sampling rate in seconds.
        duration (int): Total duration for monitoring in seconds.
    """
    cpu_count = psutil.cpu_count(logical=True)
    max_cpu_utilization = cpu_count * 100 # each CPU can contribute 100%

    parent_psutil_processes = {}
    child_psutil_processes = {}
    processes_list = list(processes.values())
    # Monitor CPU utilization
    for server_id, process in processes.items():
        parent_psutil_processes[server_id] = psutil.Process(process.pid)
        child_processes = parent_psutil_processes[server_id].children(recursive=False)
        # Each parent server process should fork only 1 child process
        assert(len(child_processes) == 1)
        child_psutil_processes[server_id] = psutil.Process(child_processes[0].pid)

    # Use child process to monitor CPU utilization
    psutil_processes = child_psutil_processes

    # Prepare CSV
    with open(output_csv, mode='w', newline='') as file:
        writer = csv.writer(file)
        
        # Write header: Timestamp + PIDs
        header = ["Timestamp"] + [f"{server_id}_{proc.pid}" for server_id, proc in psutil_processes.items()]
        writer.writerow(header)
        
        print(f"Monitoring CPU utilization for {duration} seconds... (sampling rate: {sampling_rate}s)")
        start_time = time.time()

        while not stop_event.is_set():
            row = [datetime.now().strftime('%Y-%m-%d %H:%M:%S')]  # Add timestamp
            
            for proc in psutil_processes.values():
                if proc.is_running():
                    cpu_usage = (proc.cpu_percent(interval=0.8*sampling_rate)/max_cpu_utilization) * 100
                    row.append(cpu_usage)
                else:
                    row.append("N/A")  # Process not running
            
            writer.writerow(row)  # Write the row to the CSV
            #print(f"Sampled CPU usage: {row}")
            time.sleep(sampling_rate)
    

if __name__ == "__main__":

    parser = argparse.ArgumentParser()

    parser.add_argument('--config-file', type=str, default='test_config.txt', help='chain configuration file')
#    parser.add_argument('--eeal-fname', type=str, default='real')
#    parser.add_argument('--fake-fname', type=str, default='fake')
    parser.add_argument('--test-type', type=str, default='sanity', help='sanity, correctness, crash_consistency, perf, availability')
    parser.add_argument('--top-dir', type=str, default='..', help='path to top dir')
    parser.add_argument('--log-dir', type=str, default='out', help='path to log dir')
    parser.add_argument('--num-clients', type=int, default=1, help='number of clients')
    parser.add_argument('--master-port', type=str, default='60060', help='master port')
    parser.add_argument('--skew', action='store_true')
    parser.add_argument('--vk_ratio', type=int, default=0, help='ratio of value to key lenght')
    parser.add_argument('--num-keys', type=int, default=1000, help='number of gets to put and get in sanity test')
    parser.add_argument('--protocol', type=str, default='hermes', help="replication protocol - hermes or cr")


    parser.add_argument('--only-clients', action='store_true')
    parser.add_argument('--only-service', action='store_true')

    args = parser.parse_args()
    
    top_dir = args.top_dir

    if (args.protocol == 'hermes'):
        build_dir = top_dir + '/build/src'
        log_dir = top_dir + '/' + args.log_dir + '/hermes'
    elif (args.protocol == 'cr'):
        build_dir = top_dir + '/../kv_store/bin'
        log_dir = top_dir + '/' + args.log_dir + '/cr'
    else:
        assert(0)

    test_type = args.test_type
    config_file = top_dir + '/' + args.config_file
    
    db_dir = top_dir + '/db/'
    
    assert (not (args.only_clients and args.only_service))

    checkAndMakeDir(log_dir)

    graceful_failure = False

    if (not args.only_clients):
        try:
            createService(config_file, args.master_port, log_dir=log_dir, start_master=(not graceful_failure))
        except Exception as e:
            print(f"An unexpected exception occured while starting service: {e}")
            terminateTest()
            
 #       time.sleep(30)

    # startLoadMeasurement(log_dir, master_processes, server_processes)
    cpu_utilization_csv = log_dir + '/cpu_utilization.csv'
    monitor_cpu_utilization_thread = threading.Thread(target=monitor_cpu_utilization, args=(server_processes, cpu_utilization_csv, 0.01, 5, monitor_stop_event,))
    monitor_cpu_utilization_thread.start()

    if (not args.only_service):
        try:
            startClients(args)
        except Exception as e:
            print(f"An unexpected exception occured while starting clients: {e}")
            terminateTest()

    # if args.test_type == 'availability':
    #manualKillServers(0)
    #if not graceful_failure:
    #    kill_server_thread = threading.Thread(target=manualKillServers, args=('50052',6,))
    #    kill_server_thread.start()

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
    monitor_stop_event.set()
    monitor_cpu_utilization_thread.join()
    #kill_server_thread.join()
    terminateService()
    # for process in load_measurement_processes:
    #    terminateProcess(process)
    
    print("Test End")
