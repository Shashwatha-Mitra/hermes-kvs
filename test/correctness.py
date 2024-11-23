import numpy as np
import time
import sys

max_retries = 5
wait_before_retry = 1

def performRead(client, key, correct_value, fake):
    status = -1
    num_retries = 0
    value = ''
                
    while (status == -1):
        status, value = client.get(key)
            
        if status == -1:
            print (f"Error: Couldn\'t get() key {key} from database")
            if num_retries == max_retries:
                print (f"Error: reached retry limit {max_retries}. Aborting client.")
                sys.exit(1)                    
            num_retries += 1
            time.sleep(wait_before_retry)
        
        elif status == 0 and fake:
            print ('Correctness Test Failed!!')
            print (f"key: {key}")
            print ('Database shouldn\'t contain this value')            
            return False

        elif status == 0:
            if value != correct_value:
                print ('Correctness Test Failed!!')
                print (f"key: {key}")
                print (f"correct_value: {correct_value}")
                print (f"retrieved value: {value}") 
                return False
    return True

def correctnessTest(client, db_keys):
    if (len(db_keys) == 0):
        print ('db_keys is not populated. Populated the db first')
        return None
    
    print ("----------- [test] Start Correctness Test ------------") 
    num_keys_populated = len(db_keys) 
    num_reads = 10 * num_keys_populated 
    num_fake_reads = int(0.1 * num_keys_populated)
    keys_populated = list(db_keys.keys())
    test_passed = True
    total_duration = 0
    avg_duration = 0
        
    print ('Testing Real Keys')    
    for num_reads_performed in range (0, num_reads):
        key_number = np.random.randint(num_keys_populated)
        key = keys_populated[key_number]
        correct_value = db_keys[key]
        test_passed = performRead(client, key, correct_value, False)                        

    print ('Testing Fake Keys')
    for num_reads_performed in range (0, num_fake_reads):        

        # Keys inserted are assumed to be of the form 'K123' so these keys cannot
        # be inserted into the database
        key = 'Key' + str(num_reads_performed)
        test_passed = performRead(client, key, '', True)
       
    if (test_passed):
        print ('Test Passed!')
    else:
        print ('Test Failed!')
    print ("----------- [test] End Correctness Test ------------") 
