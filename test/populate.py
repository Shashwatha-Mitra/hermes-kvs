import time
import sys
import math

max_retries = 5
wait_before_retry = 1
db_keys = {}

def generateKeys(num_keys):
    length = math.ceil(math.log10(num_keys))
    keys = [f"K{str(i).zfill(length)}" for i in range (num_keys)]
    return keys

def generateValues(num_values):
    length = math.ceil(math.log10(num_keys))
    values = [f"VAL{str(i).zfill(length)}" for i in range (num_keys)]
    return values

def populateDB(client, num_keys=10, crash_consistency_test=False):
    print ("----------- [test] Start populateDB ------------")
    
    abort = False
    lines = []
    num_keys_populated = 0
    total_duration = 0
    avg_duration = 0
    
    keys = generateKeys(num_keys)
    values = generateValues(num_values)

    for i in range (num_keys):
        key = keys[i]
        value = values[i]
        status = -1

        # Try inserting key into the database
        while (status == -1):
            start = time.time_ns()        # Record start time
            status = client.put(key, value)
            end = time.time_ns()          # Record end time

            if (status == -1):
                print (f"Error: Could\'nt put() key {key} into database")
                if num_retries == max_retries:
                    if crash_consistency_test:
                        print (f"Error: reached retry limit {max_retries}. Aborting populateDB and moving to get()." )
                        abort = True
                        break
                    else:
                        print (f"Error: reached retry limit {max_retries}. Aborting client.")
                        sys.exit(1)
                num_retries += 1
                time.sleep(wait_before_retry)
                    
        # If it didn't fail, update the keys
        if not abort:
            db_keys[key] = value
            num_keys_populated += 1
            #if status == 0:
            #    overwritten_keys[key] = value
            total_duration += (end - start)
            
    # Print stats
    if num_keys_populated != 0:
        avg_duration = int(total_duration/(1000*num_keys_populated))
    print (f"Populated {num_keys_populated} keys")
    print (f"Average duration for populate {avg_duration} us")
    print ("----------- [test] End populateDB --------------")

    return db_keys, overwritten_keys
