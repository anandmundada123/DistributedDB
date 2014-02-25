#!/usr/bin/python
import sqlite3 as lite
import sys

DBPATH = "/home/hduser/test.db"
FILEPATH = "/home/hduser/output"
queryType = ''

def fixInsertVals(pred):
    """This function takes a predicate segment like "(7," and it surrounds the actual value with quotes.
        If we do this to all predicates then it won't ever fail (numbers, text, etc..)."""
    #Strip out anything unimportant:
    p = pred.strip('(,) ')
    q = "'%s'" % p
    pred = pred.replace(p, q)
    return pred

def fixQuery(query):
    """The version of sqlite to support a multi-value insert statement is 3.7.11, assuming we don't have this
        we need to fix the statement."""
    global queryType
    newQueryList = []
    
    #Make sure we compare against the proper string
    regq = query.split(" ")
    lwrq = query.lower().split(" ")
    if("insert" in lwrq[0]):
        queryType = 'insert'
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
                    predList.append(fixInsertVals(p))
                    FSM = "LOOKFOREND"
                
            elif(FSM == "LOOKFOREND"):
                #look for end of list
                if(')' in p):
                    FSM = "INIT"
                    p = fixInsertVals(p)
                    #If there is a comma at the end we need to remove it
                    p = p.rstrip(',')
                    predList.append(p)
                    #Found a predicate so generate a new query
                    newQueryList.append(initialQuery + " ".join(predList))
                    predList = []
                
                else:
                    #Append all of these in the middle
                    predList.append(p)
    
    elif("select" in lwrq[0]):
        queryType = 'select'
        newQueryList.append(query)
    
    elif("create" in lwrq[0]):
        queryType = 'create'
        newQueryList.append(query)
    
    else:
        queryType = 'other'
        newQueryList.append(query)

    return newQueryList
            

if __name__ == "__main__":
    query = sys.argv[1:][0]
    query = query.strip()
    #Fix the query if required depending on type
    query = fixQuery(query)
    con = lite.connect(DBPATH)
    try:
        cur = con.cursor()
        #the fixQuery function returns a list of queries to run (even if its just one)
        for q in query:
            #parse over each row returned
            results = cur.execute(q)
            for row in results:
                #convert the data into a string
                row = [str(r) for r in row]
                #Convert the list returned into a string to print to file
                s = '\t'.join(row)
                #DFW: switching to a | for a newline because otherwise the newlines get filtered out as the data makes it back to the Client
                sys.stdout.write(s + "|")
        con.commit()
        if(queryType != 'select'):
            print('success')
        cur.close()
    except Exception as e:
        print(str(e))
    finally:
        con.close()
