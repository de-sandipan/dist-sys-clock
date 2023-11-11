import json
import grpc
from concurrent import futures
from Branch import Branch
from multiprocessing import Process, Queue
import os
import comm_service_pb2
import comm_service_pb2_grpc
import signal
import distro
import platform
import sys


class ExitHandler:
    def __init__(self, branch, output):
        self.branch = branch
        self.output = output
    
    def __call__(self, signum, frame):
        self.output.put(branch.logEvents())
        exit(0)

def startBranchProcess(branch, output):

    # This registers the exit handler. When a branch process is terminated
    # the handler will ensure graceful termination of the process and 
    # capture all the events for the branch in the 'output' queue
    signal.signal(signal.SIGTERM, ExitHandler(branch, output))


    # Each branch process will write the process id in the file. This file will be
    # referred to terminate branch processes when all customer events are processed.
    with open('branch_process_ids.txt', 'a') as file_object:
        file_object.write(str(os.getpid()))
        file_object.write('\n')

    # Create the branch processes and wait for customer events.
    branch.createBranchStub()
    port = str(50000 + branch.id)
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    comm_service_pb2_grpc.add_CommunicationsServicer_to_server(branch, server)
    server.add_insecure_port("[::]:" + port)
    server.start()
    print("Process started for branch id: " +  str(branch.id) + "; Server listening on port: " + port)
    server.wait_for_termination()





if __name__ == "__main__":

    if platform.system() == 'Windows':
        print('''
              This application is not cross platform and can not be executed on
              Windows. Please use a Linux based platform, preferably Ubuntu 22.04.
              ''')
        exit(0)

    if distro.name() != 'Ubuntu' or distro.version() != '22.04':
        print('''
              This application is tested on Ubuntu 22.04.
              Execution in other platforms may produce unintended outputs.
              ''')

    if len(sys.argv) < 2:
        print("Pease provide input file name")
        exit(0)
    
    file_name = sys.argv[1]


    # This data structure will capture events generated for all branches
    output = Queue()

    # Delete any files generated in previous execution
    try:
        os.remove('branch_process_ids.txt')
        os.remove('branch_event_logs.json')
        os.remove('all_event_logs.json')
    except OSError:
        pass
    
    # Create a clean file for all events as both branch and customer
    # processes first read the file and then writes into it
    with open('all_event_logs.json', 'w') as f:
        pass

    with open(file_name, 'r') as f:
        input_data = f.read()

    parsed_input_data = json.loads(input_data)

    branches = []
    branch_list = []
    branch_processes = []

    for record in parsed_input_data:
        if record['type'] == 'branch':
            branch = Branch(record['id'], record['balance'], branches)
            branches.append(branch.id)
            branch_list.append(branch)


    
    for branch in branch_list:
        proc = Process(target=startBranchProcess, args=(branch, output, ))
        branch_processes.append(proc)
        proc.start()
        # time.sleep(0.1)

    for proc in branch_processes:
        proc.join()


    # Capture all the events for all branches and generate branch process log 
    branch_processes_log = [output.get() for p in branch_processes]

    json_object = json.dumps(branch_processes_log, indent=2)
    print(json_object)

    with open('branch_event_logs.json', 'w') as file_object:
        file_object.write(json_object)
    