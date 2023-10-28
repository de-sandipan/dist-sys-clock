from multiprocessing import Process
from Customer import Customer
import os
import json
import time
import signal


def startCustomerProcess(customer):
    customer.createCustomerStub()
    customer.executeEvents()
    event_logs = json.dumps(customer.logEvents())
    print(event_logs)

    with open('output_logs.txt', 'a') as file_object:
        file_object.write(event_logs)
        file_object.write('\n')


if __name__ == "__main__":

    with open('input.json') as f:
        input_data = f.read()
    
    parsed_input_data = json.loads(input_data)

    customer_list = []
    customer_processes = []

    for record in parsed_input_data:
        if record['type'] == 'customer':
            customer = Customer(record['id'],record['events'])
            customer_list.append(customer)

    # Delete any previously present log files
    try:
        os.remove('output_logs.txt')
    except OSError:
        pass

    for customer in customer_list:
        proc = Process(target=startCustomerProcess, args=(customer,))
        customer_processes.append(proc)
        proc.start()
        time.sleep(0.25)

    for proc in customer_processes:
        proc.join()

    # Once processing for customers are completed, read
    # p-ids form the file to terminate the branch processes
    with open('branch_process_ids.txt', 'r') as f:
        branch_processes = f.readlines()

    for pid in branch_processes:
        os.kill(int(pid), signal.SIGTERM)
    