import os, sys, json, subprocess, time, signal, threading, socket, shutil

from twisted.internet import reactor
from twisted.internet.protocol import Protocol, Factory, ClientFactory

DEFAULT_CATALOG = '{"databases": [{"name": "default", "relations": []}]}'

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
    exit()

print(selectStmt)
print(tableName)
print(whereClause)
print(blockList)
print(jsonStr)
