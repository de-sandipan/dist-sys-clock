import grpc
import comm_service_pb2
import comm_service_pb2_grpc
from multiprocessing import Lock

class Branch(comm_service_pb2_grpc.CommunicationsServicer):

    def __init__(self, id, balance, branches):
        # unique ID of the Branch
        self.id = id
        # replica of the Branch's balance
        self.balance = balance
        # the list of process IDs of the branches
        self.branches = branches
        # the list of Client stubs to communicate with the branches
        # self.stubList = list()
        self.stubList = {}

        # a list of received messages used for debugging purpose
        self.recvMsg = list()

        self.clock = 0
        self.eventLogs = list()

        self.lock = Lock()


    def createBranchStub(self):
        for br_id in self.branches:
            if self.id != br_id:
                port = str(50000 + br_id)
                # print(str(self.id) + "    "  + port)
                channel = grpc.insecure_channel("localhost:"+port)
                stub = comm_service_pb2_grpc.CommunicationsStub(channel)
                # self.stubList.append(stub)
                self.stubList[br_id] = stub

    def increment_clock(self):
        try:
            self.lock.acquire()
            self.clock += 1
            current_clock = self.clock
        finally:
            self.lock.release()
        return current_clock

    def update_clock(self, remote_clock):
        try:
            self.lock.acquire()
            self.clock = max(self.clock, remote_clock)
            self.clock += 1
            current_clock = self.clock
        finally:
            self.lock.release()
        return current_clock

    def queryBalance(self, request, context):
        # Log incomming requests
        recvReq = {'customer_request_id': request.cusreqid, 'interface': request.interface, 'money': request.money}
        self.recvMsg.append(recvReq)

        current_clock = self.update_clock(request.clock)

        eventLog = {'customer_request_id': request.cusreqid, 'logical_clock': current_clock, 
                    'interface': request.interface, 'comment': f'event_recv from customer {self.id}'}
        
        self.eventLogs.append(eventLog)

        return comm_service_pb2.ResponseMessage(interface=request.interface, balance=self.balance, status='success', clock=current_clock)


    def depositMoney(self, request, context):
        # Log incomming requests
        recvReq = {'customer_request_id': request.cusreqid, 'interface': request.interface, 'money': request.money}
        self.recvMsg.append(recvReq)

        current_clock = self.update_clock(request.clock)

        eventLog = {'customer_request_id': request.cusreqid, 'logical_clock': current_clock, 
                    'interface': request.interface, 'comment': f'event_recv from customer {self.id}'}
        
        self.eventLogs.append(eventLog)

        # print(self.eventLogs)

        stat = 'success'

        # Validate transaction amount, balance, etc.
        if request.money < 0:
            stat = 'error'
        else:
            # If validation is successful, update balance and propagate to other branches
            self.balance += request.money

            # for stub in self.stubList:
            for (remote_br_id, stub) in self.stubList.items():
                
                incr_current_clock = self.increment_clock()

                eventLog = {'customer_request_id': request.cusreqid, 'logical_clock': incr_current_clock, 
                            'interface': 'propage_deposit', 'comment': f'event_sent to {remote_br_id}'}
        
                self.eventLogs.append(eventLog)

                stub.propagateDeposit(comm_service_pb2.RequestMessage(brcustid=self.id, cusreqid=request.cusreqid, interface=request.interface, money=request.money, clock=incr_current_clock))
        
        return comm_service_pb2.ResponseMessage(interface=request.interface, balance=self.balance, status=stat, clock=current_clock)


    def withdrawMoney(self, request, context):
        # Log incomming requests
        recvReq = {'customer_request_id': request.cusreqid, 'interface': request.interface, 'money': request.money}
        self.recvMsg.append(recvReq)

        current_clock = self.update_clock(request.clock)

        eventLog = {'customer_request_id': request.cusreqid, 'logical_clock': current_clock, 
                    'interface': request.interface, 'comment': f'event_recv from customer {self.id}'}

        self.eventLogs.append(eventLog)

        stat = 'success'

        # Validate transaction amount, balance, etc.
        if request.money < 0:
            stat = 'error'
        elif request.money > self.balance:
            stat = 'error'
        else:
            # If validation is successful, update balance and propagate to other branches
            self.balance -= request.money

            # for stub in self.stubList:
            for (remote_br_id, stub) in self.stubList.items():
                
                incr_current_clock = self.increment_clock()

                eventLog = {'customer_request_id': request.cusreqid, 'logical_clock': incr_current_clock, 
                            'interface': 'propagate_withdraw', 'comment': f'event_sent to {remote_br_id}'}
        
                self.eventLogs.append(eventLog)

                stub.porpagateWithdraw(comm_service_pb2.RequestMessage(brcustid=self.id, cusreqid=request.cusreqid, interface=request.interface, money=request.money, clock=incr_current_clock))

        return comm_service_pb2.ResponseMessage(interface=request.interface, balance=self.balance, status=stat, clock=current_clock)
            

    def propagateDeposit(self, request, context):

        self.balance += request.money

        current_clock = self.update_clock(request.clock)

        eventLog = {'customer_request_id': request.cusreqid, 'logical_clock': current_clock, 
                    'interface': 'propage_deposit', 'comment': f'event_recv from branch {request.brcustid}'}

        self.eventLogs.append(eventLog)

        return comm_service_pb2.ResponseMessage(interface=request.interface, balance=self.balance, status='success', clock=current_clock)


    def porpagateWithdraw(self, request, context):

        self.balance -= request.money

        current_clock = self.update_clock(request.clock)

        eventLog = {'customer_request_id': request.cusreqid, 'logical_clock': current_clock, 
                    'interface': 'propagate_withdraw', 'comment': f'event_recv from branch {request.brcustid}'}

        self.eventLogs.append(eventLog)

        return comm_service_pb2.ResponseMessage(interface=request.interface, balance=self.balance, status='success', clock=current_clock)
    
    def logEvents(self):

        log = {'id': self.id, 'type': 'branch', 'events': self.eventLogs}
        return log