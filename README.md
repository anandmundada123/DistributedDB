DistributedDB
=============
setup:
1) Copy exec_cmd.py to /home/hduser/exec_cmd.py
2) Copy jar file at /home/hduser/distributeddb.jar
3) You should have test.db at /home/hduser/test.db 

============
how to run?
============

1) run startDB.py

$ python startDB.py
--> This will prompt you command prompt

2) Type query and press enter. Here you have to mention where you have to execute this query like master or slave. Note that this is just a request to DB Manager but DB Manager can decide anyhthing else.

distributedDB> master.CREATE TABLE abc(name TEXT)
distributedDB> slave.CREATE TABLE pqr(test TEXT)
distributedDB> slave.SELECT * FROM pqr
distributedDB> master.SELECT * FROM abc
distributedDB> clear   
--> Clear Screen
distributedDB> quit
--> Quit from DB script



