#!/usr/bin/env python

import sys
import subprocess
import time
# tempo maximo de simulacao / limite de 50 ciclos
#
TIME_TO_STOP = [10, 30, 60]  # Seconds
TIME_TILL_REQUEST = [10, 30, 60]  # Seconds
NUMBER_POINTS_PER_CYCLE = 2500
NUMBER_ITERATIONS_TILL_WRITE = 5
NUMBER_CLIENTS = 10
# MAX_SIMULATION_TIME
CYCLE_LIMIT = 50



# --------
# Is this a Interesting Variable ?

# number of points to write to client's DB -> 5000 after each NUMBER_ITERATIONS_TILL_WRITE iteration
# db.write_points(GLOBAL_DATA, database=client_db_name, time_precision='n', batch_size=5000, protocol="line")

# try to test more scenarios for a pool of variables -> save data ?

if __name__ == "__main__":
    for i in range(NUMBER_CLIENTS):
        proc = subprocess.Popen(["python", "client.py", str(i+1), str(NUMBER_ITERATIONS_TILL_WRITE), str(NUMBER_POINTS_PER_CYCLE)], shell=True)
    # time.sleep(TIME_TO_STOP)
    time.sleep(TIME_TILL_REQUEST)
    subprocess.call([sys.executable, 'master.py', str(NUMBER_CLIENTS)], shell=True)
    quit()

