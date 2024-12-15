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

def getLogger(log_file): 
    # create logger
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)

    # create console handler and set level to debug
    ch = logging.FileHandler(log_file, mode = 'w')
    ch.setLevel(logging.INFO)

    # create formatter
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # add formatter to ch
    ch.setFormatter(formatter)

    # add ch to logger
    logger.addHandler(ch)

    return logger


if __name__ == "__main__":

    parser = argparse.ArgumentParser()

    parser.add_argument('--id', type=int, default=1, help='Client id')
    parser.add_argument('--config-file', type=str, default='test_config.txt', help='chain configuration file')
    parser.add_argument('--test-type', type=str, default='sanity', help='sanity, correctness, crash_consistency, perf, failure')
    parser.add_argument('--top-dir', type=str, default='', help='path to top dir')
    parser.add_argument('--log-dir', type=str, default='out/', help='path to log dir')
    parser.add_argument('--num-keys', type=int, default=10, help='number of gets to put and get in sanity test')
    parser.add_argument('--vk_ratio', type=int, default=0, help='ratio of value length to key length')
    parser.add_argument('--distribution', type=str, default='uniform_random', help='key distribution: uniform_random (default), linear, skew')
    parser.add_argument('--write-percentage', type=int, default=0, help='write percentage for performance tests')
    
    parser.add_argument('--populate-db', action='store_true')

    args = parser.parse_args()

    if (args.top_dir):
        top_dir = args.top_dir + '/'
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

    stats_file = args.log_dir + '/' + f'stats_{client_id}.json'
    log_file = args.log_dir + '/' + f'logger_client_{client_id}.log'
    # print (test_type + ' ' + args.config_file + ' ' + config_file)
   
    server_list = parseConfigFile(config_file)
    
    logger = getLogger(log_file)

    #logging.basicConfig(filename=f'logger_client_{client_id}.log', encoding='utf-8', level=logging.DEBUG, format='%(asctime)s: %(message)s')
    #logging.basicConfig(level = logging.WARN, format='%(asctime)s: %(message)s')
   
    logging.warning(f"client {client_id}: started")
    logger.info(f"client {client_id}: started")
    cl = HermesClient(server_list, client_id, logger)

    if args.populate_db:
        performance_test.populateDB(cl, num_keys, args.vk_ratio)
        logging.warning("Populate done!")
        logger.info("Populate done!")
    else:
        if (test_type == 'sanity'):
            sanity.test(cl)
        elif (test_type == 'correctness'):
            db_keys = populate.populateDB(cl, num_keys)
            correctness.correctnessTest(cl, db_keys)
        elif (test_type == 'perf'):
            keys = []
            values = []
            logging.warning("Starting perf tests")
            logger.info("Starting perf tests")
            performance_test.performanceTest(cl, num_keys, stats_file, keys, values, write_per, args.distribution, args.vk_ratio)
            logging.warning("Client {client_id} ended perf tests")
            logger.info("Client {client_id} ended perf tests")
        elif (test_type == 'failure'):
            keys = []
            values = []
            logging.warning("Starting Failure Perf tests")
            logger.info("Starting Failure Perf tests")
            performance_test.performanceTest(cl, num_keys, stats_file, keys, values, write_per, args.distribution, args.vk_ratio, True)
            logging.warning("Client {client_id} ended Failure Perf tests")
            logger.info("Client {client_id} ended Failure Perf tests")
