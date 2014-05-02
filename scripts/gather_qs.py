import os, sys, json, subprocess, time, signal, threading, socket, shutil

from twisted.internet import reactor
from twisted.internet.protocol import Protocol, Factory, ClientFactory

DEFAULT_CATALOG = {"databases": [{"name": "default", "relations": []}]}

def outFunc(args):
    sys.stdout.write('[%.2f] ' % time.time() + args)
    sys.stdout.flush()



#############################################################################################################
# MAIN
try:
    selectStmt = sys.argv[1]
    tableName = sys.argv[2]
    whereClause = sys.argv[3]
    blockList = sys.argv[4]
    jsonStr = sys.argv[5]
except:
    print("Usage: $0 <selectStmt> <tableName> <whereClause> <blockList> <jsonStr>")
    exit(1)

try:

    #
    # A few things need to happen before we can call quickstep
    #

    # Setup the vars properly
    blockList = blockList.split(",")
    resultSchema = json.loads(jsonStr)
    deleteMe = []

    # First setup the quickstep environment (qsstor, catalog.json)
    if(not os.path.exists('qsstor')):
        os.mkdir("qsstor")

    ## Fix the catalog.json to contain our schema data for this output
    catalog = DEFAULT_CATALOG

    ## Determine the block numbers we will need and fix that in the JSON
    ## For the number of blocks in the blockList, make an array [1, .., N] to use
    newBlockDefinition = [i for i in range(1, len(blockList) + 1)]

    ## Store the new block array into the schema
    resultSchema['blocks'] = newBlockDefinition

    ## Now shove the new def into the catalog
    catalog['databases'][0]['relations'].append(resultSchema)

    ## Write the catalog to disk
    cStr = json.dumps(catalog)
    fd = open('catalog.json', 'w')
    fd.write(cStr + "\n")
    fd.flush()
    fd.close()

    # Remove this at the end:
    deleteMe.append('catalog.json')

    # Find all blocks in /tmp/ based on the blockList provided, move these files into qsstor
    # also rename these files to match quickstep standards qsblk_%d.qsb
    for blkNum in range(1, len(blockList) + 1):
        blkName = blockList[blkNum-1]
        qsStor = "qsstor/qsblk_%d.qsb" % blkNum
        shutil.move("/tmp/%s" % blkName, qsStor)
        #print("/tmp/%s -> %s" % (blkName, qsStor))
        deleteMe.append(qsStor)
        
    # build up the commands we need to pass to quickstep, append a "quit;" at the end and store to disk
    cmdStr = "%s from %s %s;\nquit;\n" % (selectStmt, tableName, whereClause)
    cmdFile = 'qs-%d.cmd' % time.time()
    deleteMe.append(cmdFile)
    fd = open(cmdFile, 'w')
    fd.write(cmdStr)
    fd.flush()
    fd.close()
    
    cmd = 'LD_LIBRARY_PATH="/home/hduser/:$LD_LIBRARY_PATH" /home/hduser/quickstep_cli_shell <%s' % cmdFile

    # Call the command and pass stdin via a file
    proc = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdoutput, errors = proc.communicate()

    if(proc.returncode):
        print('ERROR: Unable to perform quickstep shell operation %s' % errors)
        exit(1)

    # Print the output
    print(stdoutput)

    # Finally, clean up all the files
    for d in deleteMe:
        os.unlink(d)

except Exception as e:
    print("ERROR: %s" % str(e))
    exit(1)

