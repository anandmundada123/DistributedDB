import os, sys, json, time, subprocess, random

from twisted.internet import reactor
from twisted.internet.protocol import Protocol, Factory, ClientFactory
from twisted.internet.task import LoopingCall

MY_PORT = 50000
PRUNE_TIMEOUT = 300
PRUNE_PERIOD = 60

timeflt = lambda: time.time()

class HadoopConnProtocol(Protocol):
    """
        This class is in charge of keeping the routers up to date with config data.
    """
    def __init__(self, addr, f, q):
        self.addr = addr
        self.myhost = addr.host
        self.myport = addr.port
        self.myid = '%s:%d' % (self.myhost, self.myport)
        self.factory = f
        self.query = q
    
    def connectionMade(self):
        print('-- [HADOOPSENDER] Connection made. %s' % self.myid)
        #Initial connection made, send the query we are supposed to
        self.factory.sendQuery(self.query)
        del(self.query)

    def dataReceived(self, data):
        print('-- [HADOOPSENDER] Received: ', data)
        self.factory.hadoop.sendResults(data.rstrip())

    def connectionLost(self, reason):
        print '!! [HADOOPSENDER] Connection lost:', reason
    
        
class HadoopConnFactory(ClientFactory):
    """
        This class connects to the query acceptor side of the Client code for the hadoop program.
    """
    def __init__(self, h, q):
        self.proto = None
        self.hadoop = h
        self.query = q
    
    def buildProtocol(self, addr):
        self.proto = HadoopConnProtocol(addr, self, self.query)
        #Forget the query from init since it shouldn't be kept in the persistent factory
        del(self.query)
        return self.proto
    
    def startedConnecting(self, connector):
        print 'Started to connect.'

    def clientConnectionLost(self, connector, reason):
        print 'Lost connection.  Reason:', reason

    def clientConnectionFailed(self, connector, reason):
        print 'Hadoop: Connection failed. Reason:', reason

    def sendQuery(self, query):
        """@deferred
            Sends the query, expected to be called as first member of deferred chain."""
        if(self.proto):
            print("-- Sending '%s'" % query)
            self.proto.transport.write(query)
            return True
        else:
            print("!! Cannot send query, no connection, trying again in 5 seconds")
            return False

class Hadoop:
    """
        This protocol just holds stuff for the hadoop connection like the subprocess and the token.
    """
    def __init__(self, r, token, p):
        self.reactor = r
        self.closed = False
        self.token = token
        self.proto = p
        self.ts = timeflt()
        self.port = 51000
        #The client is instructed to open the arg port to listen to
        self.proc = subprocess.Popen(["yarn", "jar", "distributedDB.jar", "distributeddb.Client", "-node", "wah", "-port", str(self.port)], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        print('** New process(%d) for token %s' % (self.proc.pid, token))

        #Some time needs to pass before we can try to establish a connection to the yarn server, so don't do the conn here
        time.sleep(1)
        self.conn = None

    def logPrefix(self):
        return "[%d @ %.3f]" % (self.proc.pid, timeflt())

    def kill(self):
        self.proc.kill()
        #TODO: kill client connections
    
    def performQueryAction(self, query, numTries=5):
        """Sends the query, since the response will arrive in the Protocol.dataReceived object, we just hold onto the consoleProtocol and return."""
        if(numTries == 0):
            print('!! %s ERROR: Unable to send query "%s", num tries up.' % (self.logPrefix(), query))
            return
        if(query.rstrip() == "OK"):
            print('-- OK, %s' % (self.logPrefix()))
            return
        
        #Check if we need a connection here
        if(not self.conn):
            #If we do need to connect, pass the query off to the Factory, when the Protocol connects it will send it for us
            self.conn = HadoopConnFactory(self, query)
            self.reactor.connectTCP('127.0.0.1', self.port, self.conn)
            #So now we can just return
            return
        
        r = self.conn.sendQuery(query)
        #if r is false we couldn't send the query so do a call later
        if(r == False):
            self.reactor.callLater(5, self.performQueryAction, query, numTries-1)

    def sendResults(self, data):
        """Called by the dataReceived protocol function."""
        #The data contains | chars in place of newlines, fix this then send the data
        data = data.replace('|', '\n')
        self.proto.sendMsg(data)


class StatusProtocol(Protocol):
    """
        Protocol that gets the data from each of the APs.
        The data is Status information in JSON form.
        The data is expected to be in the form:
    """
    def __init__(self, addr, f):
        self.factory = f
        self.persist = False
        self.myhost = addr.host
        self.myport = addr.port
        self.myid = '%s:%d' % (self.myhost, self.myport)

    def logPrefix(self):
        return '[CONSOLE-%s @ %f]' % (self.myid, timeflt())
    
    def sendMsg(self, msg):
        """Wraps message in JSON and sends it"""
        output = {'msgType':'result', 'stdout':msg}
        output = json.dumps(output)
        print("-- %s Sending: '%s'" % (self.logPrefix(), output))
        self.transport.write(output)
        if(not self.persist):
            print("** %s Dropping connection" % (self.logPrefix()))
            self.transport.loseConnection()

    def dataReceived(self, data):
        """@override:
            Main function which receives data from outside world"""
        data = data.rstrip()
        print("%s Got: '%s'" % (self.logPrefix(), data))
        self.factory.parseData(self, data)

class Listener(Factory):
    """
        Factory that listens to Status blocks coming in from the APs and writes them to the database.
    """
    def __init__(self, reactor):
        self.reactor = reactor
        self.hadoops = {}

    def buildProtocol(self, addr):
        return StatusProtocol(addr, self)
    
    def pruneConnections(self):
        """Periodically called, it prunes connections that aren't being used"""
        for k,v in self.hadoops.iteritems():
            if(not v.closed):
                #check staleness
                if((timeflt() - v.ts) > PRUNE_TIMEOUT):
                    print('%s Pruning %s' % (self.logPrefix(), k))
                    v.closed = True
                    v.kill()

    def parseData(self, proto, data):
        """Parse the data, maintain persistent state, also send results"""
        try:
            data = json.loads(data)
        except Exception as e:
            print("!! %s JSON loads failed: %s" % (self.logPrefix(), data))
            return

        #Look for the token
        if('token' not in data.keys()):
            print("%s Missing token: %s" % (self.logPrefix(), data))
            proto.sendMsg('missing token')
            return
        
        if('query' not in data.keys()):
            print("%s Missing query: %s" % (self.logPrefix(), data))
            proto.sendMsg('missing query')
            return

        token = data['token']
        #We need to make sure to convert this to a string, otherwise it shows up as a 'unicode' and fails later
        query = str(data['query'])

        #See if there is a match for the token
        if(token not in self.hadoops.keys()):
            #Special case if the token starts with "PERSIST"
            if("PERSIST" in token):
                #Set persistent mode on
                proto.persist = True
                print('%s Persist connection added for %s, %s' % (self.logPrefix(), token, proto.myid))
                self.hadoops[token] = Hadoop(self.reactor, token, proto)
            else:
                proto.persist = False
                print("%s New token: %s" % (self.logPrefix(), token))
                self.hadoops[token] = Hadoop(self.reactor, token, proto)

        if('ASDF' in query):
            for k, v in self.hadoops.iteritems():
                print('%s: %s' % (k, v.proto.myid))
            return
        
        #Found a connection
        hd = self.hadoops[token]
        
        #See if the connection timed out
        if(hd.closed):
            print("%s Timed out connection used: %s" % (self.logPrefix(), token))
            proto.sendMsg("connection timed out")
            return
        
        # NOTE: *** BIG DEAL HERE ***
        # Since the Protocol used to CREATE the Hadoop class might be different then the current one
        # ie. not persistent, we need to point the Hadoop object to the current protocol so it knows where to send results
        hd.proto = proto

        #Update the timestamp so we maintain the connection
        hd.ts = timeflt()
        
        #send query
        hd.performQueryAction(query)
    

if(__name__ == "__main__"):
    while(True):
        try:
            l = Listener(reactor)
            
            #Listen for queries
            reactor.listenTCP(MY_PORT, l)
            
            #Prune dormant queries
            repeater = LoopingCall(l.pruneConnections)
            repeater.start(PRUNE_PERIOD)
            reactor.run()
        except KeyboardInterrupt:
            reactor.stop()
            print("done")
            break
        except Exception as e:
            print "Exception caught"
            print str(e)
            reactor.stop()

