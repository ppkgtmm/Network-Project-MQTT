from socket import * 
import sys
import json
class Subscriber:
    ip = 'localhost'
    server_port = 12000
    time_out = 60
    size = 2048
    def __init__(self):
        self.address = (Subscriber.ip,Subscriber.server_port)
        self.s = socket(AF_INET,SOCK_DGRAM)
        self.topic = ""

    def getTopic(self):
        self.topic = input('Enter topic to subscribe or exit >> ')

    def pack(self):
        message = json.dumps({'type':'subscribe','topic':self.topic})
        return message

    def execute(self):
        while True:
            self.getTopic()
            if self.topic == 'exit':
                sys.exit()
            else:
                message = self.pack()
                self.s.sendto(message.encode('utf-8'),self.address)
                reply,serverAddr = self.s.recvfrom(Subscriber.size)
                print("Message is >> ",reply.decode('utf-8'))
                
if __name__ == "__main__":
    subscriber = Subscriber()
    subscriber.execute()

