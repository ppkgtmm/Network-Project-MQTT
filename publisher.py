from socket import * 
import sys
import json
class Publisher:
    ip = 'localhost'
    server_port = 12000
    time_out = 20
    size = 2048
    def __init__(self):
        self.address = (Publisher.ip,Publisher.server_port)
        self.s = socket(AF_INET,SOCK_DGRAM)
        self.topic = ""
        self.data = ""

    def getTopic(self):
        self.topic = input('Enter topic to publish or exit >> ')

    def getData(self):
        self.data = input('Enter data to publish : ')

    def pack(self):
        message = json.dumps({'type':'publish','topic':self.topic,'data':self.data})
        return message

    def execute(self):
        self.s.connect(self.address)
        while True:
            self.getTopic()
            if self.topic == 'exit':
                sys.exit()
            else:
                self.getData()
                message = self.pack()
                self.s.sendto(message.encode('utf-8'),self.address)
                self.s.settimeout(Publisher.time_out)
                try:
                    reply,serverAddr = self.s.recvfrom(Publisher.size)
                    print(reply.decode('utf-8'))
                except TimeoutError:
                    print('Broker is taking too much time for reply')
                
if __name__ == "__main__":
    publisher = Publisher()
    publisher.execute()

    
