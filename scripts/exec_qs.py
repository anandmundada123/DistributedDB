import os, sys, json, subprocess, time, signal, threading, socket, shutil

from twisted.internet import reactor
from twisted.internet.protocol import Protocol, Factory, ClientFactory

DEFAULT_CATALOG = '{"databases": [{"name": "default", "relations": []}]}'

def outFunc(args):
    sys.stdout.write('[%.2f] ' % time.time() + args)
    sys.stdout.flush()

class QuickstepHandler:
    """This class is a thread enabled wrapper around the quickstep binary"""
    def __init__(self, out):
        self.blocks = {}
        self.out = out
        # We need to setup the subproc to run in a new thread and keep track of it
        self._qsThread = threading.Thread(target=self.runQS)

        # setup a flag so other classes can see if QS is active
        self._qsRunning = threading.Event()

        # Finally, before launching the quickstep binary, setup the environment for it
        self.setupEnvironment()

        self._qsThread.start()

    def isRunning(self):
        """Returns true/false if QS is known to be running/not"""
        return self._qsRunning.isSet()

    def setupEnvironment(self):
        """Sets up the initial environment for quickstep, this includes making sure
        there is a "qsstor" directory for the qsblks and a default catalog.json file."""
        if(not os.path.exists("/home/hduser/qsstor")):
            self.out("-- Making qsstor\n")
            try:
                os.mkdir("/home/hduser/qsstor")
            except Exception as e:
                self.out("!! Error making qsstor: %s\n" % str(e))
        
        # Setup the default catalog.json
        if(not os.path.exists("/home/hduser/catalog.json")):
            try:
                fd = open("/home/hduser/catalog.json", "w")
                fd.write(DEFAULT_CATALOG)
                fd.flush()
                fd.close()
            except Exception as e:
                self.out("!! Error writing catalog: %s\n" % str(e))

    def runQS(self):
        """Takes charge of starting up the quickstep binary, then watches for any errors.
        Sets _qsRunning if errors are found."""
        
        # NOTE we need to add the /home/hduser path to LDLIB so the proper .SO's can be found
        # Also we change the CWD to /home/hduser too so this is where all the quickstep data is expected to be stored
        self.proc = subprocess.Popen('LD_LIBRARY_PATH="/home/hduser/:$LD_LIBRARY_PATH" /home/hduser/quickstep_cli_net 1>./qs_stdout 2>./qs_stderr', shell=True, cwd="/home/hduser/")#, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        while(True):
            # Poll to see its status
            retcode = self.proc.poll()
            if(retcode):
                self.out("!! quickstep process returned: %d\n" % retcode)
                # KILL EVERYTHING!
                os.killpg(0, signal.SIGKILL)
            time.sleep(1)
    
    def getBlocks(self):
        """Read the qsblocks file, return an array of lines from the file.
        If there was no qsblocks file, returns an empty array rather than None."""
        try:
            if(not os.path.exists('/home/hduser/qsblocks')):
                return []
            
            with open("/home/hduser/qsblocks", "r") as fd:
                return fd.readlines()
        except Exception as e:
            self.out("!! Error getting qsblock data: %s\n" % str(e))

    def takeBlocksSnapshot(self, query):
        """This function reads in the 'qsblocks' file if it exists and stores it internally."""
        try:
            k = hash(query)
            self.blocks[k] = self.getBlocks()
        except Exception as e:
            self.out("!! Error getting qsblock data: %s\n" % str(e))
    
    def getBlocksSnapshot(self, query):
        """This function returns the snapshot of 'qsblocks' based on the query input."""
        k = hash(query)
        # Remove item from blocks and return it, or None if it doesn't exist
        return self.blocks.pop(k, None)

    def getCatalog(self):
        """Return the json parsed catalog data."""
        try:
            if(not os.path.exists('/home/hduser/catalog.json')):
                self.out('!![QS] catalog doesnt exist\n')
                return None
            lines = ""
            with open("/home/hduser/catalog.json", "r") as fd:
                while(True):
                    line = fd.readline().rstrip()
                    if(not line):
                        break
                    lines += line
            
            data = json.loads(lines)
            return data
        except Exception as e:
            self.out("!! Error getting catalog: %s\n" % str(e))
            return None

class QuickstepClientProtocol(Protocol):
    """
        This class communicates to the YARN client
    """
    def __init__(self, addr, f, out):
        self.addr = addr
        self.factory = f
        self.out = out
    
    def connectionMade(self):
        self.out('--[QSCLIENT] Connection made.\n')

    def dataReceived(self, data):
        # As quickly as possible, read the qsblocks file so we can compare to the snapshot
        afterBlocks = self.factory.qsProc.getBlocks()
        
        data = data.rstrip()
        #self.out('--[QSCLIENT] Received: %s\n' % data)

        # We have to interpret what the data is that we received:
        # Here are the choices:
        # "quickstep> "      <---- this is ok, throw it out
        # "      ...> "      <---- This is not good, the last query wasn't interpretted properly
        # "Query Complete"   <---- we got a resonse back for our query
        #NOTE: the response might look like "Query Complete\nquickstep>"
        # This is why the first check should be "Query Complete"
        if("Query Complete" in data):
            self.out('-- [QSCLIENT] Query completed\n')
            # Now check the queue to see how many queries we have to process
            if(len(self.factory.queue) > 1):
                self.out('!! [QSCLIENT] we have more then 1 query to process! (%d)\n' % self.factory.queue.size())
            elif(len(self.factory.queue) == 0):
                self.out('--[QSCLIENT] Received query complete but no queries sent\n')
                # In this case they are just sending an initial "quickstep>" prompt, ignore it and return
                return
            
            # Get the query from our queue
            outBlock, theQuery = self.factory.queue.pop()
            
            #Use the hash of the query to get the blocks snapshot data
            beforeBlocks = self.factory.qsProc.getBlocksSnapshot(theQuery)

            # Now we compare the blocks
            diffBlocks = list(set(afterBlocks) - set(beforeBlocks))
            
            # Even if there are diffBlocks this can happen during an insert call
            if("select" not in theQuery.lower()):
                selectStmt = False
            else:
                selectStmt = True
            
            print(diffBlocks)
            # If there is no difference then it wasn't a select query, return success
            if(not selectStmt or len(diffBlocks) == 0):
                self.out('-- [QSCLIENT] Identified response to NONSELECT query success\n')
                print(beforeBlocks)
                print(afterBlocks)
                self.factory.yarnConn.sendData("SUCCESS")
            else:
                # Get the catalog so we can parse out the schema
                catalog = self.factory.qsProc.getCatalog()
                
                tableName = ""
                qtmp = theQuery.split(' ')
                for i in range(0, len(qtmp)):
                    if(qtmp[i] == "from"):
                        tableName = qtmp[i + 1].replace(';', '')
                        break
                else:
                    self.out('!! [QSCLIENT] Unable to find table name in select query "%s"\n' % theQuery)
                    self.factory.yarnConn.sendData("ERROR: cannot parse table name from select stmt\n")
                    return
                
                # Parse through the catalog and find the table name
                try:
                    relationData = ""
                    rel = catalog['databases'][0]['relations']
                    
                    for r in rel:
                        # NOTE: I think select statements are dropping "NULL" entries into catalog so check for that
                        if(r):
                            if(r['name'] == tableName):
                                relationData = json.dumps(r)
                                break
                    
                    # If we couldn't find the match and so didn't break
                    else:
                        self.out('!! [QSCLIENT] Unable to find match to table name in catalog "%s"\n' % tableName)
                        self.out(str(catalog) + "\n")
                        self.factory.yarnConn.sendData("ERROR: Unable to find match to table name in catalog '%s'\n" % tableName)
                        return
                        
                except:
                    self.out('!! [QSCLIENT] Unable to parse catalog table: "%s"\n' % tableName)
                    self.factory.yarnConn.sendData("ERROR: cannot parse catalog for table %s\n" % tableName)
                    return
                    

                # Push the identified data blocks to HDFS, then spit out OUTPUT <stuff>
                blkNum = 0
                blkOutput = []
                for b in diffBlocks:
                    # The b string should look like: "SAVED: qsblk_%d.qsb\n", fix this
                    b = b.replace("SAVED: ", "").rstrip()
                    tmpBlkName = "%s-%d" % (outBlock, blkNum)
                    # First rename the file and move it out of the qsstor dir
                    shutil.move("/home/hduser/qsstor/%s" % b, "/home/hduser/%s" % tmpBlkName)

                    # cmd to send blocks to HDFS
                    cmd = "hdfs dfs -copyFromLocal /home/hduser/%s" % tmpBlkName
                    self.out('--[QSCLIENT] Performing command: %s' % cmd)
                    proc = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                    stdoutput, errors = proc.communicate()

                    if(proc.returncode):
                        self.out('ERROR Unable to perform hdfs operation %s' % errors)
                        self.factory.yarnConn.sendData("ERROR: Unable to send file to HDFS: %s\n" % stdoutput)
                        return
                    
                    # increment the block number
                    blkNum += 1
                    
                    # Store the string to send back to the user
                    blkOutput.append(tmpBlkName)
                
                # Send the Yarn Client the output so they know what to do
                outputStr = ",".join(blkOutput)
                # Fix the JSON string to remove special chars
                relationData = relationData.replace('\n', '').replace('\t', '')
                self.factory.yarnConn.sendData("OUTPUT %s %s" % (outputStr, relationData))

        elif("quickstep>" in data):
            self.out('-- [QSCLIENT] Got a good response\n')
            # If its the very first prompt then don't spit it out, it interfers with the Client handshake
            if(not self.factory.firstMsg):
                self.factory.yarnConn.sendData("SUCCESS")
            self.factory.firstMsg = False
        elif("      ...>" in data):
            self.out('-- [QSCLIENT] Got a bad response\n')
            #TODO, access the stdout file to pull out what the error was?
            self.factory.yarnConn.sendData("ERROR: malformed statement\n")
        else:
            #ERROR CASE
            self.out('!! [QSCLIENT] Got an error: "%s"\n' % data)
            #TODO, access the stdout file to pull out what the error was?
            self.factory.yarnConn.sendData("ERROR: malformed statement\n")

    def connectionLost(self, reason):
        self.out('!![QSCLIENT] Connection lost\n')

class QuickstepClientFactory(Factory):
    def __init__(self, qsProc, out):
        self.myhostname = socket.gethostname()
        self.qsProc = qsProc
        self.out = out
        self.proto = None
        self.yarnConn = None
        self.queue = []
        self.firstMsg = True
    
    def registerYarnClient(self, yarnConn):
        """Hold onto the yarnConn here so we can send data directly to it."""
        self.yarnConn = yarnConn
    
    def buildProtocol(self, addr):
        """Store the proto internally so we can call its transport object."""
        self.proto = QuickstepClientProtocol(addr, self, self.out)
        return self.proto
    
    def sendQuery(self, outBlock, query):
        """Sends a query string to the protocol connection we have with quickstep."""
        if(not self.proto):
            self.out('!! [QSCLIENT] Attempting to send query "%s" but we dont have a connection\n' % query)
            return
        
        # We got a new query, but before we send it out we should take a snapshot of what the qsblocks file looks like
        # we will use this to figure out if our query wrote out any blocks that we need to capture
        self.qsProc.takeBlocksSnapshot(query)
        # Hold onto this query so we can track if too many are called together
        self.queue.append((outBlock, query))
        
        self.out('[QSCCLIENT] Sending query to QS: "%s"\n' % query)
        self.proto.transport.write(query + "\n")
            
    def startedConnecting(self, connector):
        self.out('--[QSCLIENT] Started to connect.\n')

    def clientConnectionLost(self, connector, reason):
        self.out('**[QSCLIENT] Lost connection.  Reason: %s\n' % str(reason))

    def clientConnectionFailed(self, connector, reason):
        self.out('!![QSCLIENT] Connection failed. Reason: %s\n' % reason)

class YarnClientProtocol(Protocol):
    """
        This class communicates to the YARN client
    """
    def __init__(self, addr, f, out):
        self.addr = addr
        self.factory = f
        self.out = out
    
    def connectionMade(self):
        self.out('-- [STATUSSENDER] Connection made.\n')
        # Note it doesn't matter what the port says because we don't use it
        # The host is important though
        s = "connect %s 9999" % self.factory.myhostname
        self.transport.write(s)

    def dataReceived(self, data):
        data = data.rstrip()
        self.out('-- [YARNCLIENT] Received: %s\n' % data)
        # Get a good unique name for the output blocks we might generate
        outBlk = "out-%d-%s-%04d" % (self.factory.appid, self.factory.myhostname, self.factory.queryNum)
        self.factory.queryNum += 1
        
        # Make sure the query ends in a ';'
        if(data[-1] != ';'):
            data += ';'
        
        # Send the output data to the Quickstep connection we have
        self.factory.qsConn.sendQuery(outBlk, data)

    def connectionLost(self, reason):
        self.out('!! [STATUSSENDER] Connection lost\n')

class YarnClientFactory(Factory):
    def __init__(self, appid, qsConn, qsProc, out):
        self.myhostname = socket.gethostname()
        self.appid = appid
        self.qsConn = qsConn
        self.qsProc = qsProc
        self.out = out
        self.queryNum = 0
        self.proto = None
    
    def buildProtocol(self, addr):
        self.proto = YarnClientProtocol(addr, self, self.out)
        return self.proto

    def sendData(self, data):
        if(self.proto):
            self.out('**[YARNCLIENT] Sending data: "%s"\n' % data.rstrip())
            self.proto.transport.write(data)
        else:
            self.out('!![YARNCLIENT] NO proto to send data "%s"\n' % data.rstrip())
    
    def startedConnecting(self, connector):
        self.out('Started to connect.\n')

    def clientConnectionLost(self, connector, reason):
        self.out('Lost connection.  Reason: %s\n' % str(reason))

    def clientConnectionFailed(self, connector, reason):
        self.out('Connection failed. Reason: %s\n' % reason)




#############################################################################################################
# MAIN
try:
    host = sys.argv[1]
    port = int(sys.argv[2])
    appID = int(sys.argv[3])
except:
    print("Usage: $0 <host> <port> <appID>")
    exit()

# Start up quickstep
qsProc = QuickstepHandler(outFunc)
# Give quickstep a second to get setup
time.sleep(1)

# Now startup a client connection to quickstep
qsConn = QuickstepClientFactory(qsProc, outFunc)

# Startup a connection to the yarn client
yarnConn = YarnClientFactory(appID, qsConn, qsProc, outFunc)

# Tell the quickstep client about the yarn factory so they can communicate
qsConn.registerYarnClient(yarnConn)

reactor.connectTCP(host, port, yarnConn)
reactor.connectTCP('localhost', 10000, qsConn)
reactor.run()

# WE NEVER GET HERE
try:
    while(True):
        time.sleep(1)
except KeyboardInterrupt:
    print("Killing quickstep:")
    os.killpg(0, signal.SIGKILL)

    
