#!/usr/bin/python
import re, os, subprocess
import sqlite3 as lite
import sys

DBPATH = "/home/hduser/test.db"
queryType = ''
queryTable = ''

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
    global queryTable
    newQueryList = []
    
    #Make sure we compare against the proper string
    regq = query.split(" ")
    lwrq = query.lower().split(" ")
    if("insert" in lwrq[0]):
        queryType = 'insert'
        #We are only concerned with what comes after a 'values' statement
        if('values' not in lwrq):
            print "ERROR: INSERT statement doesn't contain 'VALUES', we expect exactly: 'VALUES (stuff)'"
            return None
        
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
        # Find the table name to get the schema
        regex = re.match('.*from (.*)', query)
        if(regex):
            # easiest way to do this: if the table isn't the end, check for spaces and pull out first element
            res = regex.group(1)
            if(' ' in res):
                queryTable = res.split(' ')[0]
            else:
                queryTable = res
        else:
            print('ERROR no match ERROR1')
        newQueryList.append(query)
    
    elif("create" in lwrq[0]):
        queryType = 'create'
        newQueryList.append(query)
    
    else:
        queryType = 'other'
        newQueryList.append(query)

    return newQueryList
            

if __name__ == "__main__":
    outputdb = sys.argv[1]
    #print('-- DB file: %s' % outputdb)
    #print('-- OS CWD: %s' % os.getcwd())
    query = sys.argv[2:]
    query = ' '.join(query)
    query = query.strip()
    
    #Fix the query if required depending on type
    query = fixQuery(query)
    # Expect that someone else outputed an error message
    if(not query):
        exit(1)

    con = lite.connect(DBPATH)
    try:
        cur = con.cursor()
        # If SELECT get the table schema
        if(queryType == "select"):
            #If its a select query we need to generate an image to store the output to
            con_out = lite.connect(outputdb)
            #res = cur.execute('PRAGMA table_info(%s)' % queryTable)
            res = cur.execute("select sql from sqlite_master where tbl_name = '%s' and type = 'table'" % queryTable)
            schema = ""
            for row in res:
                schema = row[0]
            
            # Now generate the same table in this new database file
            cur_out = con_out.cursor()
            res = cur_out.execute(schema)
            #for row in res:
            #    print(row)

        #the fixQuery function returns a list of queries to run (even if its just one)
        for q in query:
            #parse over each row returned
            results = cur.execute(q)
            for row in results:
                #convert the data into a string
                row = [str(r) for r in row]
                
                # If SELECT push results into new database
                if(queryType == "select"):
                    q = 'insert into %s values %s' % (queryTable, tuple(row))
                    #print(q)
                    cur_out.execute(q)

                #Convert the list returned into a string to print to file
                #s = '\t'.join(row)
                #DFW: switching to a | for a newline because otherwise the newlines get filtered out as the data makes it back to the Client
                #sys.stdout.write(s + "|")
        
        con.commit()
        if(queryType != 'select'):
            print('SUCCESS:NOTSELECT')
        else:
            con_out.commit()
            cur_out.close()
            con_out.close()
            #print('-- DB file written %s' % outputdb)
            # Finally, send the output file to HDFS
            cmd = "hdfs dfs -copyFromLocal %s" % outputdb
            #print('-- Performing command: %s' % cmd)
            proc = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            stdoutput, errors = proc.communicate()

            if(proc.returncode):
                print('ERROR Unable to perform hdfs operation %s' % errors)
            else:
                print('SUCCESS:SELECT')
        cur.close()
    except Exception as e:
        print('ERROR: %s' % str(e))
    finally:
        con.close()
