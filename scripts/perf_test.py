import os, sys, json, subprocess, socket, time
from sys import stdout

from twisted.internet import reactor
from twisted.internet.protocol import Protocol, Factory, ClientFactory

# ANSI forground green
C_REQ = '\033[34m'
C_GOOD = '\033[32m'
C_BAD = '\033[31m'
C_END = '\033[0m'

def output(arg):
    sys.stdout.write(arg)
    sys.stdout.flush()

def getChunk(a, i):
    """Returns a portion of a string, being mindful of its size."""
    if(i < 10):
        start = 0
    else:
        start = i - 10
    if(i + 10 > len(a)):
        end = len(a)
    else:
        end = i + 10
    return a[start:end]

class SenderProtocol(Protocol):
    """
        This class is in charge of keeping the routers up to date with config data.
    """
    def __init__(self, addr, f):
        self.addr = addr
        self.factory = f
    
    def connectionMade(self):
        print('=============== Starting testing ================')
        # Get the first data and send it
        req = self.factory.tester.getRequest()
        output(C_REQ + req + C_END)
        self.transport.write(req)

    def dataReceived(self, data):
        # Pass it to the tester for validation
        resp = self.factory.tester.validateResponse(data)
        # If ok do next one
        if(resp):
            try:
                self.factory.tester.next()
            except:
                print('=============== All done ================')
                self.transport.loseConnection()
                reactor.stop()
                return
            
            # Send next test
            req = self.factory.tester.getRequest()
            output(C_REQ + req + C_END)
            self.transport.write(req)
        else:
            print('-- not a match')
            

    def connectionLost(self, reason):
        pass

class SenderFactory(Factory):
    def __init__(self, host, port, tester):
        self.host = host
        self.port = port
        self.tester = tester
    
    def buildProtocol(self, addr):
        return SenderProtocol(addr, self)
    
    def startedConnecting(self, connector):
        pass

    def clientConnectionLost(self, connector, reason):
        pass

    def clientConnectionFailed(self, connector, reason):
        print('Cannot connect')

class Test(object):
    """All this does is hold 2 objects "request" a string to send to the node
       and "response" which is a list of strings that the response should be."""
    def __init__(self, req=None, resp=None):
        self.request = req
        self.response = resp

    def __str__(self):
        return "%s:\n%s" % (self.request, self.response)

class Tester:
    """Class to hold all the Test objects."""
    def __init__(self, tests=[]):
        self.ptr = 0
        self.tests = []

    def clearTests(self):
        self.ptr = 0
        self.tests = []

    def loadFromFile(self, filename):
        """Load a file in a specific format for test cases"""
        with open(filename, 'r') as fd:
            test = Test("", "")
            FSM = "INIT"
            while(True):
                line = fd.readline()
                if(not line):
                    break
                if(line.rstrip() == "__DONE__"):
                    break
                
                if(FSM == "INIT"):
                    # First line is request
                    if(line.startswith("__REQUEST__")):
                        test.request = line.replace("__REQUEST__ ", "")
                        #print('-- New Request "%s"' % test.request.rstrip())
                        FSM = "RESPINIT"
                
                # Search for response start
                elif(FSM == "RESPINIT"):
                    if(line.startswith("__RESPONSE__")):
                        # Check if a line equals __IGNORE__, if so we don't care what the response is
                        if("__IGNORE__" in line):
                            test.response = "__IGNORE__"
                            FSM = "INIT"
                            self.tests.append(test)
                            test = Test("", "")
                        else:
                            FSM = "RESPBODY"
                
                # Add everything as the response body
                elif(FSM == "RESPBODY"):
                    # Search for ending of body
                    if(line.startswith("__RESPONSE__")):
                        FSM = "INIT"
                        self.tests.append(test)
                        test = Test("", "")
                    else:
                        #print('-- Response: "%s"' % line.rstrip())
                        test.response += line

    def registerTests(self, tests):
        """Takes in a data object of request->response objects and sets up the internal structure."""
        self.tests = tests
        # Reset the pointer
        self.ptr = 0
    
    def getRequest(self):
        """Returns a request string to be sent out."""
        return self.tests[self.ptr].request
    
    def getResponse(self):
        """Returns a request string to be sent out."""
        return self.tests[self.ptr].response

    def validateResponse(self, resp):
        """Compares the response string provided against the expected response
            for this specific test."""
        #Don't process if ignore, return valid
        if(self.getResponse() == "__IGNORE__"):
            output(C_GOOD + "Query %d ignored\n" % self.ptr + C_END)
            return True

        if(resp == self.getResponse()):
            output(C_GOOD + "Query %d passed\n" % self.ptr + C_END)
            return True
        else:
            # Print out some data about where the difference started
            diff = self.calcDiff(resp, self.getResponse())

            output(C_BAD + "Query %d failed, difference:\n" % self.ptr + C_END)
            for k, v in diff.iteritems():
                output(C_BAD + "  %d: %s != %s\n" % (k, v[0], v[1]) + C_END)
            return False
    
    def calcDiff(self, a, b):
        # Make sure both strings are the same length
        if(len(a) > len(b)):
            b += "*" * (len(a) - len(b))
        elif(len(a) < len(b)):
            a += "*" * (len(b) - len(a))
        
        x = {}
        p = 0
        for i, j in zip(a, b):
            if(i != j):
                x[p] = (i, j)
            p += 1
        return x

    def next(self):
        """Move to next test case, raises Exception when done."""
        self.ptr += 1
        if(self.ptr >= len(self.tests)):
            raise Exception("AllDone")
    
# Get args
try:
    host = sys.argv[1]
    port = int(sys.argv[2])
    testFile = sys.argv[3]
except:
    print("Usage: $0 <host> <port> <testfile>")
    exit()
    
test = Tester()
test.loadFromFile(testFile)

# A test case
if(True):
    while(True):
        req = test.getRequest()
        resp = test.getResponse()
        testresp = resp + "x"
        
        ans1 = test.validateResponse(testresp)
        print(ans1)
        ans2 = test.validateResponse(resp)
        print(ans2)

        try:
            test.next()
        except:
            break

#f = SenderFactory(host, port, test)
#reactor.connectTCP(host, port, f)
#reactor.run()
