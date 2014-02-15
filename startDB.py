#!/usr/bin/python

import sys
import os
import subprocess
OUTPATH = "/home/hduser/results"

if __name__ == "__main__":
    cmd = "hadoop jar distributeddb.jar -jar distributeddb.jar "
    query = " "
    node = " "
    os.system("clear")
    try:
        while True:
            user_cmd = raw_input("distributedDB>")
            commands = user_cmd.split('.')
            node = commands[0].strip()
            if node == "clear":
                os.system("clear")
            if node == "quit":
                sys.exit(0)
            if len(commands) == 2:
                query = commands[1].strip()
                final_cmd = cmd + '-query "' + query + '" -node ' + node
                #print final_cmd
                #os.system(final_cmd)
                p = subprocess.Popen(final_cmd, stdout=subprocess.PIPE, shell=True)
                (output, err) = p.communicate()
                p_status = p.wait()
                f = open(OUTPATH, 'w')
                f.write(output)
                f.close()
                #if p_status == 0:
                #    print 'done!!'
                #else:
                #    print 'failed!!'
    except KeyboardInterrupt:
        sys.exit(0)
    # except Exception:
    #     traceback.print_exc(file=sys.stdout)
    sys.exit(0)
