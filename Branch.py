import grpc
import comm_service_pb2
import comm_service_pb2_grpc

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
        self.clock += 1

    def update_clock(self, remote_clock):
        self.clock = max(self.clock, remote_clock)
        self.clock += 1

    def queryBalance(self, request, context):
        # Log incomming requests
        recvReq = {'customer_request_id': request.cusreqid, 'interface': request.interface, 'money': request.money}
        self.recvMsg.append(recvReq)

        self.update_clock(request.clock)

        msg1 = f'branch_id: {self.id}, customer_request_id: {request.cusreqid}, logical clock: {self.clock}, '
        msg2 = f'interface: {request.interface}, comment: event_recv from customer {self.id}'
        print(msg1 + msg2)

        return comm_service_pb2.ResponseMessage(interface=request.interface, balance=self.balance, status='success', clock=self.clock)


    def depositMoney(self, request, context):
        # Log incomming requests
        recvReq = {'customer_request_id': request.cusreqid, 'interface': request.interface, 'money': request.money}
        self.recvMsg.append(recvReq)

        self.update_clock(request.clock)
        current_clock = self.clock

        msg1 = f'branch_id: {self.id}, customer_request_id: {request.cusreqid}, logical clock: {self.clock}, '
        msg2 = f'interface: {request.interface}, comment: event_recv from customer {self.id}'
        print(msg1 + msg2)

        stat = 'success'

        # Validate transaction amount, balance, etc.
        if request.money < 0:
            stat = 'error'
        else:
            # If validation is successful, update balance and propagate to other branches
            self.balance += request.money

            # for stub in self.stubList:
            for (remote_br_id, stub) in self.stubList.items():
                
                self.increment_clock()

                msg1 = f'branch_id: {self.id}, customer_request_id: {request.cusreqid}, logical clock: {self.clock}, '
                msg2 = f'interface: propage_deposit, comment: event_sent to {remote_br_id}'
                print(msg1 + msg2)

                stub.propagateDeposit(comm_service_pb2.RequestMessage(brcustid=self.id, cusreqid=request.cusreqid, interface=request.interface, money=request.money, clock=self.clock))
        
        return comm_service_pb2.ResponseMessage(interface=request.interface, balance=self.balance, status=stat, clock=current_clock)


    def withdrawMoney(self, request, context):
        # Log incomming requests
        recvReq = {'customer_request_id': request.cusreqid, 'interface': request.interface, 'money': request.money}
        self.recvMsg.append(recvReq)

        self.update_clock(request.clock)
        current_clock = self.clock

        msg1 = f'branch_id: {self.id}, customer_request_id: {request.cusreqid}, logical clock: {self.clock}, '
        msg2 = f'interface: {request.interface}, comment: event_recv from customer {self.id}'
        print(msg1 + msg2)

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
                
                self.increment_clock()
                msg1 = f'branch_id: {self.id}, customer_request_id: {request.cusreqid}, logical clock: {self.clock}, '
                msg2 = f'interface: propagate_withdraw, comment: event_sent to {remote_br_id}'
                print(msg1 + msg2)
                stub.porpagateWithdraw(comm_service_pb2.RequestMessage(brcustid=self.id, cusreqid=request.cusreqid, interface=request.interface, money=request.money, clock=self.clock))

        return comm_service_pb2.ResponseMessage(interface=request.interface, balance=self.balance, status=stat, clock=current_clock)
            

    def propagateDeposit(self, request, context):
        self.balance += request.money

        self.update_clock(request.clock)

        msg1 = f'branch_id: {self.id}, customer_request_id: {request.cusreqid}, logical clock: {self.clock}, '
        msg2 = f'interface: propage_deposit, comment: event_recv from branch {request.brcustid}'
        print(msg1 + msg2)

        return comm_service_pb2.ResponseMessage(interface=request.interface, balance=self.balance, status='success', clock=self.clock)


    def porpagateWithdraw(self, request, context):
        self.balance -= request.money

        self.update_clock(request.clock)

        msg1 = f'branch_id: {self.id}, customer_request_id: {request.cusreqid}, logical clock: {self.clock}, '
        msg2 = f'interface: propagate_withdraw, comment: event_recv from branch {request.brcustid}'
        print(msg1 + msg2)

        return comm_service_pb2.ResponseMessage(interface=request.interface, balance=self.balance, status='success', clock=self.clock)