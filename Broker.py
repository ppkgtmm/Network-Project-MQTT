from socket import *
import sys
import json
class Broker:
    ip = 'localhost'
    server_port = 12000
    size = 2048
    publishedData = list(dict())
    subscriberData = list(dict())
    
    def __init__(self):
        self.address = (Broker.ip,Broker.server_port)
        self.s = socket(AF_INET,SOCK_DGRAM)
        self.s.bind(self.address)
        print('Broker started ...')

    def printConnector(self,who,addr):
        if who == 'publish':
            print('Publisher has published from ',addr)
        elif who == 'subscribe':
            print('Subscriber has subscribed from ',addr)
    
    def handlePublisher(self,data,addr):
        if ('topic' in data) & ('data' in data):
            found = False
            for item in Broker.publishedData:
                if item['topic'] == data['topic']:
                    item['data'] = data['data']
                    found = True
                    break
            if found == False:
                Broker.publishedData.append({'topic': data['topic'],'data': data['data']})
            self.s.sendto(str('Broker received message').encode('utf-8'),addr)
        else:
            print('Publisher sent message with incorrect format')

    def getData(self,topic):
        for item in Broker.publishedData:
                if item['topic'] == topic:
                    return item['data']

    def pushData(self):
        index = 0
        while index < len(Broker.subscriberData):
            data = self.getData(Broker.subscriberData[index]['topic'])
            if data is not None:
                self.s.sendto(str(data).encode('utf-8'),Broker.subscriberData[index]['socket'])
                Broker.subscriberData.remove(Broker.subscriberData[index])
                index = index - 1
            index = index + 1

    def handleSubscriber(self,data,addr):
        if 'topic' in data:
            Broker.subscriberData.append({'socket': addr,'topic':data['topic']})
        else:
            print('Subscriber sent message with incorrect format')

    def handleSender(self,data,addr):
        if data['type'] == 'publish':
            self.handlePublisher(data,addr)
        elif data['type'] == 'subscribe':
            self.handleSubscriber(data,addr)

    def execute(self):
        while True:
            msgIn,addr = self.s.recvfrom(Broker.size)
            data = json.loads(msgIn.decode('utf-8'))
            if 'type' in data:
                self.printConnector(data['type'],addr)
                self.handleSender(data,addr)
                self.pushData()
            else:
                print('Broker had received message with invaid format')
        self.s.close()

if __name__ == "__main__":
   broker = Broker()
   broker.execute()

                   

    

