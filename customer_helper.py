from multiprocessing import Process, Queue
from Customer import Customer
import os
import json
import signal
import platform
import distro


def startCustomerProcess(customer, output):
    customer.createCustomerStub()
    customer.executeEvents()
    event_logs = customer.logEvents()
    output.put(event_logs)

if __name__ == "__main__":

    if platform.system() == 'Windows':
        print('''
              This application is not cross platform and can not be executed on
              Windows. Please use a Linux based platform, preferably Ubuntu 22.04.
              ''')
        exit(0)

    if distro.name() != 'Ubuntu' and distro.version() != '22.04':
        print('''
              This application is tested on Ubuntu 22.04.
              Execution in other platforms may produce unintended outputs.
              ''')

    # This data structure will capture events generated for all customers
    output = Queue()

    with open('input.json') as f:
        input_data = f.read()
    
    parsed_input_data = json.loads(input_data)

    customer_list = []
    customer_processes = []

    for record in parsed_input_data:
        if record['type'] == 'customer':
            customer = Customer(record['id'],record['customer-requests'])
            customer_list.append(customer)

    # Delete any files generated in previous execution
    try:
        os.remove('customer_event_logs.json')
    except OSError:
        pass

    for customer in customer_list:
        proc = Process(target=startCustomerProcess, args=(customer, output, ))
        customer_processes.append(proc)
        proc.start()

    for proc in customer_processes:
        proc.join()

    customer_processes_log = [output.get() for p in customer_processes]

    json_object = json.dumps(customer_processes_log, indent=2)
    print(json_object)

    with open('customer_event_logs.json', 'w') as file_object:
        file_object.write(json_object)

    # Once processing for customers are completed, read
    # p-ids form the file to terminate the branch processes
    with open('branch_process_ids.txt', 'r') as f:
        branch_processes = f.readlines()

    for pid in branch_processes:
        os.kill(int(pid), signal.SIGTERM)
    