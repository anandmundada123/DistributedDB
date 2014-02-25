import os, sys, json, argparse, threading, signal, random
from sys import stdout

from twisted.internet import reactor
from twisted.internet.protocol import Protocol, Factory, ClientFactory
from twisted.internet.task import LoopingCall

class SenderProtocol(Protocol):
    """
        This class is in charge of keeping the routers up to date with config data.
    """
    def __init__(self, addr, f):
        self.addr = addr
        self.factory = f
    
    def connectionMade(self):
        self.factory.sendMsg('OK')

    def logPrefix(self):
        return "[WRAPPER]"

    def dataReceived(self, data):
        data = data.rstrip()
        data = json.loads(data)

        #Look for the token
        if('msgType' not in data.keys()):
            print("%s Missing msgType: %s" % (self.logPrefix(), data))
            return
        
        if('stdout' not in data.keys()):
            print("%s Missing stdout: %s" % (self.logPrefix(), data))
            return

        if(data['msgType'] == "result"):
            print(data['stdout'])

    def connectionLost(self, reason):
        pass
        
class SenderFactory(ClientFactory):
    """
        This class is in charge of taking the memcache data about Status and
        sending it off to the status server.
    """
    def __init__(self):
        self.proto = None
        self.token = "PERSIST-%d" % random.randint(0, 9999)
    
    def buildProtocol(self, addr):
        self.proto = SenderProtocol(addr, self)
        return self.proto
    
    def startedConnecting(self, connector):
        pass

    def clientConnectionLost(self, connector, reason):
        self.proto = None

    def clientConnectionFailed(self, connector, reason):
        pass

    def sendMsg(self, msg):
        output = {'token':self.token, 'query':msg}
        output = json.dumps(output)
        if(self.proto):
            self.proto.transport.write(output)

class Test:
    def __init__(self):
        self.data = []
        self.thd = threading.Thread(target=self.captureStdin)
        self.mutex = threading.Semaphore()
        self._stop = threading.Event()
        self.thd.start()

    def captureStdin(self):
        while(True):
            a = sys.stdin.readline()
            self.mutex.acquire()
            self.data.append(a)
            self.mutex.release()
    
    def stop(self):
        print('-- Calling stop')
        self._stop.set()
    
    def stopped(self):
        return self._stop.isSet()

def checkStdin(lf, t):
    if(len(t.data) > 0):
        if(lf.proto):
            if(len(t.data) > 0):
                t.mutex.acquire()
                d = t.data[0]
                del(t.data[0])
                t.mutex.release()
                lf.sendMsg(d)

addr = "127.0.0.1"
port = 50000

f = SenderFactory()
t = Test()

reactor.connectTCP(addr, port, f)
#Setup repeating function call
repeater = LoopingCall(checkStdin, f, t)
repeater.start(2)

reactor.run()
os.killpg(0, signal.SIGKILL)
