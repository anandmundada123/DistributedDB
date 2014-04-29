#!/bin/bash

# Take a list of databases and a query segment and explode it so all portions
# from the different databases are combined and spit out properly

# Usage:
# $0 <select segment> <table name> <where clause> <list of database files>
#
# Example:
#   $0 "select * " "test" "" test1.db test2.db
#   Means: "select * from test"
LOG="/home/hduser/gather_sqlite.log"
echo "Args: $*" >> $LOG
if [ $# -lt 4 ]; then
    echo "Usage: $0 <select segment> <table name> <where clause> <list of database files>"
    exit 1
fi

echo "0: $0, 1: $1, 2: $2, 3: $3" >> $LOG

SQLSEL="$1"
shift
SQLTBL="$1"
shift
SQLWHERE="$1"
shift
NUMDBS=$#
DBNUM=1

echo "SQL: $SQLSEL, $SQLTBL, $SQLWHERE" >> $LOG

ATTACHCMD=""
SELECTCMD=""
# Loop through to generate sqlite command
# First we generate the ATTACH commands: "attach database 'test.db' as e1;"
# Next we loop through to generate the select command
# select * from e1.test union select * from e2.test;
while [[ $DBNUM -le $NUMDBS ]]; do
    TMP="attach database '$1' as e$DBNUM;"
    ATTACHCMD="$ATTACHCMD$TMP"
    
    TMP=" $SQLSEL FROM e$DBNUM.$SQLTBL $SQLWHERE union"
    SELECTCMD="$SELECTCMD$TMP"

    shift
    DBNUM=$[$DBNUM + 1]
done

# Last step: need to remove last Union from select statement
SELECTCMD=`echo "$SELECTCMD" | sed "s/union$//"`
# And add an sending semicolon
SELECTCMD="$SELECTCMD;"

CMDFILE="/tmp/cmd_${SQLTBL}_`date +%s`.cmd"
echo "The cmdfile: $CMDFILE" >> $LOG
echo "$ATTACHCMD$SELECTCMD" > $CMDFILE
echo "The cmd: $ATTACHCMD$SELECTCMD" >> $LOG

sqlite3 < $CMDFILE

rm -f $CMDFILE
