import os, sys, json, time, subprocess

from twisted.internet import reactor
from twisted.internet.protocol import Protocol, Factory

timeflt = lambda: time.time()



class StatusProtocol(Protocol):
    """
        Protocol that gets the data from each of the APs.
        The data is Status information in JSON form.
        The data is expected to be in the form:
    """
    def __init__(self, addr, f):
        self.factory = f
        self.myhost = addr.host
        self.myport = addr.port
        self.myid = '%s:%d' % (self.myhost, self.myport)

    def logPrefix(self):
        return '[CONSOLE-%s @ %f]' % (self.myid, timeflt())
    
    def dataReceived(self, data):
        """@override:
            Main function which receives data from outside world"""
        data = data.rstrip()
        
        if(data == "verbose on"):
            self.factory.verbose = True
            self.transport.write("ok")
            self.transport.loseConnection()
            return
        elif(data == "verbose off"):
            self.factory.verbose = False
            self.transport.write("ok")
            self.transport.loseConnection()
            return
        print("%s data: %s" % (self.logPrefix(), data))
        
        #Call the subproc
        proc = subprocess.Popen("hadoop jar distributedDB.jar distributeddb.Client -jar distributedDB.jar -query '%s' -node wah" % data, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        output, errors = proc.communicate()
        
        #An error means we still need to push stdout to the client anyway
        if(proc.returncode):
            print('%s Error in query "%s"' % (self.logPrefix(), data))
            self.transport.write("Error in query")
            if(self.factory.verbose):
                self.transport.write(errors)
        
        #Send back the response
        self.transport.write(output)

        #Close the connection so they know they got everything
        self.transport.loseConnection()

class Listener(Factory):
    """
        Factory that listens to Status blocks coming in from the APs and writes them to the database.
    """
    def __init__(self):
        self.verbose = False

    def buildProtocol(self, addr):
        return StatusProtocol(addr, self)

if(__name__ == "__main__"):
    while(True):
        try:
            l = Listener()
            reactor.listenTCP(50000, l)
            reactor.run()
        
        except Exception as e:
            print "Exception caught"
            print str(e)
            reactor.stop()

