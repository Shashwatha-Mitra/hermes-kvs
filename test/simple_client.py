import argparse
import random
import sys 
import os

sys.path.append('../src/client/')
from client import HermesClient

def parseConfigFile(path_to_file):
    server_list = []
    with open(path_to_file, 'r') as file:
        for line in file:
            port_no = line.strip()
            server_name = 'localhost:'+port_no
            server_list.append(server_name)
    return server_list
        

if __name__ == "__main__":

    parser = argparse.ArgumentParser()

    parser.add_argument('--id', type=int, default=1, help='Client id')
    parser.add_argument('--config-file', type=str, default='test_config.txt', help='chain configuration file')
    parser.add_argument('--test-type', type=str, default='sanity', help='sanity, correctness, crash_consistency, perf, kill')
    parser.add_argument('--top-dir', type=str, default='', help='path to top dir')
    parser.add_argument('--log-dir', type=str, default='out/', help='path to log dir')
    parser.add_argument('--num-keys', type=int, default=5, help='number of gets to put and get in sanity test')
    parser.add_argument('--vk_ratio', type=int, default=0, help='ratio of value length to key length')
    parser.add_argument('--skew', action='store_true')

    args = parser.parse_args()

    if (args.top_dir):
        top_dir = args.top_dir
    else:
        top_dir = '../'

    client_id = args.id
    if (client_id == -1):
        print ('Invalid Client ID. Please provide a positive integer as the client id')
        sys.exit(-1)

    test_type = args.test_type
    config_file = top_dir + args.config_file
   
    server_list = parseConfigFile(config_file)
    
    cl = HermesClient(server_list)
    cl.get('Hey') 
