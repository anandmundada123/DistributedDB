#!/bin/bash

LOG=/home/hduser/cmd.log

#Move to home dir
cd /home/hduser

#Run command:
echo "[`date`] Capturing output" >> $LOG

CMD="hdfs dfs -copyFromLocal /home/hduser/output"
$CMD
