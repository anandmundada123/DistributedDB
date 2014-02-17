#!/usr/bin/python
import sqlite3 as lite
import sys

DBPATH = "/home/hduser/test.db"
FILEPATH = "/home/hduser/output"

def chech_i_startwith(txt, pattern):
    n = len(pattern);
    str = txt[0:n].lower()
    return str == pattern
    

def fixQuery(query):
    """The version of sqlite to support a multi-value insert statement is 3.7.11, assuming we don't have this
        we need to fix the statement."""
    
    newQueryList = []
    
    #Make sure we compare against the proper string
    regq = query.split(" ")
    lwrq = query.lower().split(" ")
    if("insert" in lwrq[0]):
        #We are only concerned with what comes after a 'values' statement
        if('values' not in lwrq):
            print "ERROR: INSERT statement doesn't contain 'VALUES'"
            return query
        
        valPtr = lwrq.index('values')
        
        #Pull out the initial query =  "INSERT INTO tbl (stuff) VALUES"
        initialQuery = " ".join(regq[0:valPtr+1]) + " "

        #Now for the rest of the query we have to pair parens
        predList = []
        FSM = "INIT"
        for p in regq[valPtr+1:]:
            #Looking for first "("
            if(FSM == "INIT"):
                if('(' in p):
                    predList.append(p)
                    FSM = "LOOKFOREND"
                
            elif(FSM == "LOOKFOREND"):
                #look for end of list
                if(')' in p):
                    FSM = "INIT"
                    #If there is a comma at the end we need to remove it
                    p = p.rstrip(',')
                    predList.append(p)
                    #Found a predicate so generate a new query
                    newQueryList.append(initialQuery + " ".join(predList))
                    predList = []
                
                else:
                    #Append all of these in the middle
                    predList.append(p)
    else:
        newQueryList.append(query)

    return newQueryList
            

if __name__ == "__main__":
    query = sys.argv[1:][0]
    query = query.strip()
    #Fix the query if required depending on type
    query = fixQuery(query)
    con = lite.connect(DBPATH)
    with con:
        cur = con.cursor()
        f = open(FILEPATH, 'w')
        #the fixQuery function returns a list of queries to run (even if its just one)
        for q in query:
            #parse over each row returned
            for row in cur.execute(q):
                #convert the data into a string
                row = [str(r) for r in row]
                #Convert the list returned into a string to print to file
                s = '\t'.join(row)
                f.write(s+ '\n')
        f.close()
        con.commit() 
        cur.close()
