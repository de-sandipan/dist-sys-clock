import json
import grpc
from concurrent import futures
from Branch import Branch
from multiprocessing import Process, Queue
import os
import comm_service_pb2
import comm_service_pb2_grpc
import time


def startBranchProcess(branch, output):

    # Each process writes the process id in the file. This 
    # file will be fetched when processing for all customers
    # are completed and the processes will be terminated
    with open('branch_process_ids.txt', 'a') as file_object:
        file_object.write(str(os.getpid()))
        file_object.write('\n')

    branch.createBranchStub()
    port = str(50000 + branch.id)
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    comm_service_pb2_grpc.add_CommunicationsServicer_to_server(branch, server)
    server.add_insecure_port("[::]:" + port)
    server.start()
    print("Process started for branch id: " +  str(branch.id) + "; Server listening on port: " + port)
    server.wait_for_termination()

    # print("End of some branch process")
    # event_logs = branch.logEvents()
    # print(event_logs)
    # output.put(event_logs)




if __name__ == "__main__":

    output = Queue()

    with open('input.json') as f:
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

    # Delete any previously present exection record
    try:
        os.remove('branch_process_ids.txt')
    except OSError:
        pass
    
    for branch in branch_list:
        proc = Process(target=startBranchProcess, args=(branch, output, ))
        branch_processes.append(proc)
        proc.start()
        time.sleep(0.1)

    for proc in branch_processes:
        proc.join()

    
    print("I AM HERE")

    for branch in branch_list:
        l = branch.logEvents()
        print(l)
    # branch_processes_log = [output.get() for p in branch_processes]

    # json_object = json.dumps(branch_processes_log)

    # with open('branch_event_logs.json', 'w') as file_object:
    #     file_object.write(json_object)