import numpy as np
import time
import sys
from logging import *

def performRead(client, key, correct_value, fake):
    num_retries = 0
    value = ''
    success = True
    duration = 0
                
    try: 
        start = time.time_ns()
        value = client.get(key)
        end = time.time_ns()
        duration = (end - start)

        if value != 'Key not found' and fake:
            print ('Correctness Test Failed!!')
            print (f"key: {key}")
            print ('Database shouldn\'t contain this value')        
            success = False

        # With competing writes, this is not necessarily correct
        elif value != '' and value != correct_value and not fake:
            print ('Correctness Test Failed!!')
            print (f"key: {key}")
            print (f"correct_value: {correct_value}")
            print (f"retrieved value: {value}") 
            success = False

    except Exception as e:
        print (f"Client reached maximum retries: {client.MAX_RETRIES}")
        print (f"Encounted exception: {e}")
        
    return success, duration

def correctnessTest(client, db_keys):
    if (len(db_keys) == 0):
        print ('db_keys is not populated. Populated the db first')
        return None
    
    print ("----------- [test] Start Correctness Test ------------") 
    num_keys_populated = len(db_keys) 
    num_reads = 2 * num_keys_populated 
    num_fake_reads = int(0.1 * num_keys_populated)
    keys_populated = list(db_keys.keys())
    test_passed = True
    total_duration = 0
    avg_duration = 0
    times = []
    percentiles = [50, 70, 90, 99]
        
    print ('Testing real keys')    
    for num_reads_performed in range (0, num_reads):
        key_number = np.random.randint(num_keys_populated)
        key = keys_populated[key_number]
        correct_value = db_keys[key]
        
        test_passed, duration = performRead(client, key, correct_value, False) 
        total_duration += (duration)
        times.append(duration//1000)

    print ('Done testing real keys')
    print ('Testing fake keys')
    for num_reads_performed in range (0, num_fake_reads):
        # Keys inserted are assumed to be of the form 'K123' so these keys cannot
        # be inserted into the database
        key = 'Key' + str(num_reads_performed)
        test_passed = performRead(client, key, '', True)
    print ('Done testing fake keys')   
    
    if (test_passed):
        print ('Test Passed!')
        total_duration //= 1000
        avg_duration = int(total_duration/(num_reads))
        print (f"Total number of reads: {num_reads}")
        print (f"Total duration (in us): {total_duration}")
        print (f"Avg duration (in us): {avg_duration}")
        throughput = (num_reads * 1000 * 1000)/(total_duration)
        print (f"Throughput (in ops/s): {throughput:.2f}")
        times.sort()
        print ('Read times')
        for i in percentiles:
            if len(times) > 0:
                read_idx = int ((i * (len(times)-1))/100)
                print (f"{i}th percentile read latency: {times[read_idx]}")    
    else:
        print ('Test Failed!')
    print ("----------- [test] End Correctness Test ------------") 
