import time
import sys
import math

max_retries = 5
wait_before_retry = 1

def generateKeys(num_keys):
    length = math.ceil(math.log10(num_keys))
    keys = [f"K{str(i).zfill(length)}" for i in range (num_keys)]
    return keys

def generateValues(num_values):
    length = math.ceil(math.log10(num_values))
    values = [f"VAL{str(i).zfill(length)}" for i in range (num_values)]
    return values

def populateDB(client, num_keys=10):
    print ("----------- [test] Start populateDB ------------")
    db_keys = {}
    num_keys_populated = 0
    total_duration = 0
    avg_duration = 0
    times = []
    percentiles = [50, 70, 90, 99]
    
    keys = generateKeys(num_keys)
    values = generateValues(num_keys)

    for i in range (num_keys):
        key = keys[i]
        value = values[i]

        # Try inserting key into the database
        try:
            start = time.time_ns()        # Record start time
            client.put(key, value, False)
            end = time.time_ns()          # Record end time

            # if (status == -1):
            #    print (f"Error: Could\'nt put() key {key} into database")
            #    if num_retries == max_retries:
            #        if crash_consistency_test:
            #            print (f"Error: reached retry limit {max_retries}. Aborting populateDB and moving to get()." )
            #            abort = True
            #            break
            #        else:
            #            print (f"Error: reached retry limit {max_retries}. Aborting client.")
            #            sys.exit(1)
            #    num_retries += 1
            #    time.sleep(wait_before_retry)
                    
            db_keys[key] = value
            num_keys_populated += 1
            duration = (end - start)
            total_duration += duration
            times.append((duration)//1000)

        except Exception as e:
            print (f"Error: Couldn\'t put() key {key} into the in-memory store")
            print (f"Error: Reached retry limit {client.MAX_RETRIES}")
            print (f"Error: Encounted exception {e}")
        
    # Print stats
    if num_keys_populated != 0:
        avg_duration = int(total_duration/(1000*num_keys_populated))
    print (f"Populated {num_keys_populated} keys")
    print (f"Average duration for populate {avg_duration} us")
    times.sort()
    print (f"Write times")
    for i in percentiles:
        if len(times) > 0:
            write_idx = int ((i * (len(times) - 1))/100)
            print (f"{i}th percentile write latency: {times[write_idx]}")
    throughput = (num_keys_populated * 1000 * 1000 * 1000)/(total_duration)
    print (f"Throughput for Only Writes (ops/s): {throughput:.2f}")
    print ("----------- [test] End populateDB --------------")

    return db_keys
