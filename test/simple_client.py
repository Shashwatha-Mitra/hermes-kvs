import argparse
import random
import sys 
import os
import sanity
import logging
import correctness, populate, performance_test

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
    parser.add_argument('--num-keys', type=int, default=10, help='number of gets to put and get in sanity test')
    parser.add_argument('--vk_ratio', type=int, default=0, help='ratio of value length to key length')
    parser.add_argument('--skew', action='store_true')
    parser.add_argument('--write-percentage', type=int, default=0, help='write percentage for performance tests')

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
    num_keys = args.num_keys
    write_per = args.write_percentage
   
    server_list = parseConfigFile(config_file)
    
    logging.basicConfig(level = logging.DEBUG, format='%(asctime)s: %(message)s')
   
    logging.warning(f"Client {client_id}: Started") 
    cl = HermesClient(server_list)

    if (test_type == 'sanity'):
        sanity.test(cl)
    elif (test_type == 'correctness'):
        db_keys = populate.populateDB(cl, num_keys)
        correctness.correctnessTest(cl, db_keys)
    elif (test_type == 'perf'):
        keys = []
        values = []
        if (client_id == 0):
            keys, values = performance_test.populateDB(cl, num_keys, args.vk_ratio)
            logging.warning("Populate done!")
        logging.warning("Starting perf tests")
        performance_test.performanceTest(cl, num_keys, keys, values, write_per, args.skew, args.vk_ratio)
        logging.warning("Client {client_id} ended perf tests")
