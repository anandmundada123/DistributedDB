#!/bin/bash

LOG="/home/hduser/cmd.log"
DBCMD="exec_cmd.py"


[ $# -ne 2 ] && echo "!! [`date`] Usage: $0 APPID QUERY" >> $LOG && exit 1

APPID=$1
QUERY="$2"
OUTPUTFILE="output_$APPID"

#Move to home dir
cd /home/hduser

echo "-- [`date`] Launched: $APPID, $QUERY" >> $LOG

#Run command:
eval "python $DBCMD '$QUERY' 2>&1 1>$OUTPUTFILE"

#Make sure the output was generated
if [ ! -s $OUTPUTFILE ]; then
    echo "** [`date`] Output file wasn't created: $OUTPUTFILE" >> $LOG
    exit 1
fi

#Now push output file into HDFS
#First check if it is there
if [ `hdfs dfs -ls $OUTPUTFILE 2>/dev/null | head -n 1 | grep -o "Found 1 items" | wc -l` -eq 1 ]; then
    #Delete the file first
    echo "** [`date`] Output file exists, removing $OUTPUTFILE" >> $LOG
    CMD="hdfs dfs -rm -f $OUTPUTFILE"
    $CMD 2>/dev/null
fi

#Now push file to HDFS
CMD="hdfs dfs -copyFromLocal $OUTPUTFILE"
$CMD 2>/dev/null

#Delete the temp output file
rm -f $OUTPUTFILE
