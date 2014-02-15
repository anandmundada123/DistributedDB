#!/usr/bin/python
import sqlite3 as lite
import sys

DBPATH = "/home/hduser/test.db"
FILEPATH = "/home/hduser/output"

def chech_i_startwith(txt, pattern):
    n = len(pattern);
    str = txt[0:n].lower()
    return str == pattern
    

def reCreate_query(query):
    if(chech_i_startwith(query, "insert")):
        n1 = query.index('(')
        n2 = query.index(')')
        ql = query[:n1]
        qr = query[n1+1:n2]
        vArray = qr.split(',')
        n = len(vArray)
        newValue = '\'' + vArray[0] + '\''
        for i in range(1, n):
            newValue += ', \'' + vArray[i]  + '\''
        query = ql + '(' + newValue + ')'
    return query
            

if __name__ == "__main__":
    print sys.argv
    query = sys.argv[1:][0]
    print query
    query = query.strip()
    query = reCreate_query(query)
    print query
    con = lite.connect(DBPATH)
    with con:
        cur = con.cursor()
        f = open(FILEPATH, 'w')
        for row in cur.execute(query):
            n = len(row)
            row_str = row[0]
            for i in range(1, n):
                row_str += '\t' + member
            f.write(row_str+ '\n')
            #print row_str
        f.close()
        con.commit() 
        cur.close()
