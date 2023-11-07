import grpc
import comm_service_pb2
import comm_service_pb2_grpc

class Customer:
    def __init__(self, id, events):
        # unique ID of the Customer
        self.id = id
        # events from the input
        self.events = events
        # a list of received messages used for debugging purpose
        self.recvMsg = list()
        # pointer for the stub
        self.stub = None

        self.clock = 0
        self.eventLogs = list()

    def createCustomerStub(self):
        port = str(50000 + self.id)
        channel = grpc.insecure_channel("localhost:"+port)
        self.stub = comm_service_pb2_grpc.CommunicationsStub(channel)

    def increment_clock(self):
        self.clock += 1

    def update_clock(self, remote_clock):
        self.clock = max(self.clock, remote_clock)
        self.clock += 1

    def executeEvents(self):
        for event in self.events:

            self.increment_clock()

            eventLog = {'customer-request-id': event['id'], 'logical-clock': self.clock, 
                          'interface': event['interface'], 'comment': f'event sent from customer {self.id}'}
            
            self.eventLogs.append(eventLog)

            if event['interface'] == 'query':
                response = self.stub.queryBalance(comm_service_pb2.RequestMessage(
                    brcustid = self.id, cusreqid=event['id'], interface=event['interface'], money=0, clock=self.clock))
                
            elif event['interface'] == 'deposit':
                response = self.stub.depositMoney(comm_service_pb2.RequestMessage(
                    brcustid = self.id, cusreqid=event['id'], interface=event['interface'], money=0, clock=self.clock))
                
            elif event['interface'] == 'withdraw':
                response = self.stub.withdrawMoney(comm_service_pb2.RequestMessage(
                    brcustid = self.id, cusreqid=event['id'], interface=event['interface'], money=0, clock=self.clock))
            
            else:
                pass
            
            # self.update_clock(response.clock)

            # Format the response from the server to display/log into file
            if event['interface'] == 'query':
                recvRes = {'interface': response.interface, 'balance': response.balance}
            elif event['interface'] =='deposit' or event['interface'] == 'withdraw':
                recvRes = {'interface': response.interface, 'result': response.status}
            else:
                recvRes = {'interface': event['interface'], 'result': 'Invalid Interface'}
            
            # Group responses corresponding to events for a customer
            self.recvMsg.append(recvRes)

        self.logEvents()
    
    def logEvents(self):
        
        log = {'id': self.id, 'type': 'customer', 'events': self.eventLogs}

        return log
