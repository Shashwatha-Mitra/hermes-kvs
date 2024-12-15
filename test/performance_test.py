import numpy as np
import random
import string
import sys
import time
import math
import json

max_key_length = 15
time_interval = 3 # in seconds
def genRandomString (length):
    letters = string.ascii_lowercase
    return ''. join(random.choice(letters) for _ in range (length))

def getKeyNumber(skewFactor, num_keys_populated):
    assert (skewFactor >= 0) and (skewFactor <= 1), "Invalid skew factor"
    freq_gp_size = max(1, int(skewFactor * num_keys_populated))

    random_value = np.random.uniform(0,1)
    if (random_value < (1-skewFactor)):
        return np.random.randint(freq_gp_size)
    return np.random.randint(freq_gp_size, num_keys_populated)

def getKVPairs(num_keys):
    length = math.ceil(math.log10(num_keys))
    keys = [f"K{str(i).zfill(length)}" for i in range (num_keys)]
    values = [f"VAL{str(i).zfill(length)}" for i in range (num_keys)]

    return keys, values

def populateDB(client, num_keys, vk_ratio = 0, failure=False):
    print("Populating DB..........")
    total_duration = 0
    num_write_failures = 0
    keys = []
    values = []
    if (vk_ratio == 0):
        keys, values = getKVPairs(num_keys)

    for i in range(num_keys):
        key = ""
        value = ""
        
        if (vk_ratio != 0):
            key_len = np.random.randint(7,max_key_length)
            key = genRandomString(key_len)
            value_len = key_len * vk_ratio
            value = genRandomString(value_len)
            keys.append(key)
            values.append(value)

        else:
            key = keys[i]
            value = values[i]
        
        try:
            start = time.time_ns()
            client.put(key, value, failure)
            end = time.time_ns()
            time_in_us = (end - start)//1000
            total_duration += time_in_us

        except Exception as e:
            num_write_failures += 1

    print (f"Num put() failures: {num_write_failures}")
    print (f"Total duration: {total_duration}")
    print ("Populating DB Completed.........")
    
def performanceTest(client, num_keys, stats_file, keys=[], values=[], write_percentage = 10, distribution = 'uniform_random', vk_ratio = 0, failure = False):
    assert ('.json' in stats_file)

    print ("\nRunning Performance Tests...........\n")
    print(f"num_keys: {num_keys}")
    percentiles = [50, 70, 90, 99]

    #length = math.ceil(math.log10(num_keys))
    #if len(keys) == 0:
    #    keys = [f"K{str(i).zfill(length)}" for i in range (num_keys)]
    #if len(values) == 0:
    #    values = [f"VAL{str(i).zfill(length)}" for i in range (num_keys)]
    keys, values = getKVPairs(num_keys)

    #num_ops = 10*len(keys)
    num_ops = len(keys)
    num_keys_populated = len(keys)
    num_read_failures = 0
    num_write_failures = 0
    read_times = []
    write_times = []
    read_failure_keys = []
    write_failure_keys = []
    op_times = []
    op_start_times = []

    stats= {}
    stats["read"] = {}
    stats["write"] = {}

    print (f"Num_ops = {num_ops}, num_keys_populated = {num_keys_populated}")
    expt_start = time.time_ns()
    num_read_failures = 0
    num_write_failures = 0
    total_reads = 0
    total_writes = 0
    key_number = -1

    test_start_time = time.time_ns()

    for i in range (num_ops):
        random_value = np.random.randint(0, high=100)
    
        if (distribution == "skew"):
            key_number = getKeyNumber(0.1, num_keys_populated)
        elif (distribution == "linear"):
            key_number = (key_number + 1) % num_keys_populated 
        else:
            key_number = np.random.randint(num_keys_populated)

        key = keys[key_number]
        max_len = 6*len(key)
        value_len = np.random.randint(len(key), max_len)
        if (vk_ratio != 0):
            value_len = len(key) * vk_ratio
        start = 0
        end = 0
        value = ''

        try:
            if (random_value < write_percentage):
                # Perform put operation on a random value generated on the fly
                value = genRandomString (value_len)
                start = time.time_ns()
                client.put(key, value)
                end = time.time_ns()
                time_in_us = int((end - start)/1000)
                write_times.append(time_in_us)
                op_times.append(time_in_us)
                op_start_times.append((start-test_start_time)/(1000*1000))
                total_writes += 1

            else:
                # Get the value for key
                start = time.time_ns()
                value = client.get(key)
                end = time.time_ns()
                time_in_us = int((end - start)/1000)
                op_times.append(time_in_us)
                op_start_times.append((start-test_start_time)/(1000*1000))
                assert (value != '')
                #if (value != ''): # and value != 'Key not found'): # key not found is not a read failure
                read_times.append(time_in_us)
                #elif (value == 'Key not found'):
                #    num_read_failures += 1
                total_reads += 1

        except Exception as e:
            if (random_value < write_percentage):
                num_write_failures += 1
                write_failure_keys.append(key)
                #op_start_times.append((start-test_start_time)/(1000*1000))
                # print (f'Write Failure for key: {key}, value: {value}.\nException {e}\n')
                total_writes += 1
            elif (value == ''):
                num_read_failures += 1 
                read_failure_keys.append(key)
                #op_start_times.append((start-test_start_time)/(1000*1000))
                # print (f'Read Failure for key: {key}.\nException {e}\n')
                total_reads += 1

    expt_end = time.time_ns()
    expt_duration_in_us = (expt_end - expt_start)//1000
    throughput = (num_ops * 1000 * 1000)/(expt_duration_in_us)
    avg_write_duration = 0
    if (len(write_times) > 0):
        avg_write_duration = sum(write_times)//len(write_times)

    avg_read_duration = 0
    if (len(read_times) > 0):
        avg_read_duration = sum(read_times)//len(read_times)

    print ('Finished Performance Tests......')
    print (f"No. of read failures: {num_read_failures}")
    print (f"No. of write failures: {num_write_failures}")
    print (f"Percentage of writes: {write_percentage}")

    stats["write_percentage"] = write_percentage
    stats["read"]["total_ops"] = total_reads
    stats["write"]["total_ops"] = total_writes

    stats["read"]["num_failures"] = num_read_failures
    stats["write"]["num_failures"] = num_write_failures
    
    write_times.sort()
    read_times.sort()

    print ('Read times..')
    print (f"Average read latency: {avg_read_duration}")
    stats["read"]["avg_latency"] = avg_read_duration
    #stats["read"]["latency_per_op"] = read_times
    for i in percentiles:
        percentile_stats = {}
        if len(read_times) > 0:
            read_idx = int ((i * (len(read_times)-1))/100)
            read_thruput = (1000 * 1000)/read_times[read_idx]
            print (f"{i}th percentile read latency: {read_times[read_idx]}, throughput: {read_thruput:.2f}")
            percentile_stats[i] = (read_times[read_idx], round(read_thruput, 2))
    stats["read"]["percentile_stats"] = percentile_stats
    
    print ('................................')    
    
    print ('Write times..')
    print (f"Average write latency: {avg_write_duration}")
    stats["write"]["avg_latency"] = avg_write_duration
    #stats["write"]["latency_per_op"] = write_times
    for i in percentiles:
        percentile_stats = {}
        if len(write_times) > 0:
            write_idx = int ((i * (len(write_times) - 1))/100)
            write_thruput = (1000 * 1000)/write_times[write_idx]
            print (f"{i}th percentile write latency: {write_times[write_idx]}, throughput: {write_thruput:.2f}")
            percentile_stats[i] = (write_times[write_idx], round(write_thruput, 2))
    stats["write"]["percentile_stats"] = percentile_stats
    
    print ('................................')
    print (f"Throughput = {throughput:.2f}")
    print ('................................')
    print ('Failed Keys')
    print (f'Read Failures: {read_failure_keys}')
    print (f'Write Failures: {write_failure_keys}\n')

    stats["throughput"] = round(throughput, 2)
    #stats["read"]["failure_keys"] = read_failure_keys
    #stats["write"]["failure_keys"] = write_failure_keys


    # Time series throughput
    time_v_throughput = {
        "time": [],
        "throughput": []
    }
    print(f"len(op_start_times): {len(op_start_times)}")
    #assert(len(op_start_times) == num_ops)
    first_op_start_time = op_start_times[0]
    last_op_start_time = op_start_times[-1]
    bucket_size = time_interval * 1000 # bucket size in ms
    time_range = np.arange(first_op_start_time, last_op_start_time + bucket_size, bucket_size)
    throughput = np.array([
        sum(start <= t < start+bucket_size for t in op_start_times)
        for start in time_range
    ])
    
    time_range = (time_range / float(1000)).astype(int) # convert to seconds
    throughput = throughput / float(time_interval)  # convert to ops/second
    
    time_v_throughput["time"] = time_range.tolist()
    time_v_throughput["throughput"] = throughput.tolist()
    stats["time_v_throughput"] = time_v_throughput

    print ("\nEnding Performance Tests.................")

    with open(stats_file, 'w') as json_file:
        json.dump(stats, json_file, indent=4)

