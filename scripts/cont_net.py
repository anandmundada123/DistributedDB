import os, sys, json, subprocess, socket, time
from sys import stdout

from twisted.internet import reactor
from twisted.internet.protocol import Protocol, Factory, ClientFactory
LOGFILE = "/home/hduser/cont_net.log"
log = None

def output(arg):
    sys.stdout.write(arg)
    sys.stdout.flush()
    log.write('%d %s' % (time.time(), arg))
    log.flush()

class SenderProtocol(Protocol):
    """
        This class is in charge of keeping the routers up to date with config data.
    """
    def __init__(self, addr, f):
        self.addr = addr
        self.factory = f
    
    def connectionMade(self):
        output('-- [STATUSSENDER] Connection made.\n')
        # Note it doesn't matter what the connect sting says because we don't use it
        s = "connect %s 9999" % self.factory.myhostname
        self.transport.write(s)

    def dataReceived(self, data):
        data = data.rstrip()
        output('-- [STATUSSENDER] Received: %s\n' % data)
        # On getting a query, send it to subproc
        output('-- [CONT_NET] Calling exec_cmd python\n')
        outBlk = "out-%d-%s-%04d" % (self.factory.appid, self.factory.myhostname, self.factory.queryNum)
        self.factory.queryNum += 1
        cmd = "python exec_cmd.py %s '%s'" % (outBlk, data)
        output('-- CMD: "%s"\n' % cmd)
        proc = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdoutput, errors = proc.communicate()
        output('-- [CONT_NET] exec_cmd communicate finished\n')

        if(proc.returncode):
            output("!! ERROR: exec_cmd returned error (%d) %s %s\n" % (proc.returncode, errors, stdoutput))
            self.transport.write('ERROR: exec_cmd returned error (%d) %s %s\n' % (proc.returncode, errors, stdoutput))

        else:
            # Got good data, forward whatever output is
            output('-- exec_cmd returned: %s' % stdoutput)
            
            #Output from subproc is either going to start with "ERROR" or "SUCCESS", use this 
            # to determine what we should send back to the Client
            if("SUCCESS" in stdoutput):
                # Here output should either be SELECT or NOTSELECT
                # if NOTSELECT we don't have a output block to send back so don't
                if('NOTSELECT' == stdoutput.rstrip().split(':')[1]):
                    self.transport.write('SUCCESS')
                else:
                    self.transport.write('OUTPUT %s' % outBlk)
            else:
                # Should start with ERROR, check for this
                self.transport.write(stdoutput)

    def connectionLost(self, reason):
        output('!! [STATUSSENDER] Connection lost\n')

class SenderFactory(Factory):
    def __init__(self, host, port, appid):
        self.host = host
        self.myhostname = socket.gethostname()
        self.port = port
        self.appid = appid
        self.queryNum = 0
    
    def buildProtocol(self, addr):
        return SenderProtocol(addr, self)
    
    def startedConnecting(self, connector):
        output('Started to connect.\n')

    def clientConnectionLost(self, connector, reason):
        print 'Lost connection.  Reason:', reason

    def clientConnectionFailed(self, connector, reason):
        print 'Connection failed. Reason:', reason


# Get args
try:
    host = sys.argv[1]
    port = int(sys.argv[2])
    appID = int(sys.argv[3])
except:
    print("Usage: $0 <host> <port> <appID>")
    exit()
log = open(LOGFILE, 'a')

f = SenderFactory(host, port, appID)
reactor.connectTCP(host, port, f)
reactor.run()
