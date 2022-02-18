#!/usr/bin/env python
import logging
import sys
import subprocess
import time
import os
from playsound import playsound

"""
NUMBER_CLIENTS = [15]
NUMBER_ITERATIONS_TILL_WRITE = [15]
NUMBER_GENERATED_POINTS_PER_CYCLE = [5000]
TIME_TILL_REQUEST = [30]
"""

NUMBER_CLIENTS = [5, 10, 15]
NUMBER_ITERATIONS_TILL_WRITE = [5]
NUMBER_GENERATED_POINTS_PER_CYCLE = [5000]
TIME_TILL_REQUEST = [20, 40, 60]

"""
NUMBER_CLIENTS = [5, 10, 15]
NUMBER_ITERATIONS_TILL_WRITE = [5, 10, 15]
NUMBER_GENERATED_POINTS_PER_CYCLE = [2500, 5000]
TIME_TILL_REQUEST = [5, 10, 15]
"""


def init_logging_config():
    logging.basicConfig(level=logging.INFO)
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)

    # set a format which is simpler for console use
    formatter = logging.Formatter('%(asctime)s : %(levelname)s : %(message)s')
    console.setFormatter(formatter)
    logging.getLogger("").addHandler(console)


def create_solution_directory(clients, number_iterations, number_generated, time_till_req):
    sol_path = 'logs\\' + str(clients) + '_' + str(number_iterations) + '_' + str(number_generated) + '_' + \
               str(time_till_req)

    if not os.path.exists(sol_path):
        directory_path = os.getcwd()
        path = os.path.join(directory_path, sol_path)
        os.mkdir(path, mode=0o666)

    return sol_path


def alertSimulationFinished():
    playsound('C:/Users/work/Documents/FEUP/MQTT_Project/MQTT_python/alert.wav')


if __name__ == "__main__":

    proc2 = subprocess.call([sys.executable, 'cleanInfluxDB.py', str(30)], shell=True)

    init_logging_config()
    for i in range(5):
        for number_cli in NUMBER_CLIENTS:
            for number_iter in NUMBER_ITERATIONS_TILL_WRITE:
                for number_gen in NUMBER_GENERATED_POINTS_PER_CYCLE:
                    for time_req in TIME_TILL_REQUEST:

                        solutionPath = create_solution_directory(number_cli, number_iter, number_gen, time_req)
                        processID = []
                        for clientNumber in range(number_cli):
                            proc = subprocess.Popen(["python", "client.py",
                                                     str(clientNumber + 1),
                                                     str(number_iter),
                                                     str(number_gen),
                                                     solutionPath,
                                                     "INFO_MODE"],
                                                    shell=True)
                            processID.append(proc)

                        time.sleep(time_req)
                        subprocess.call([sys.executable, 'master.py', str(number_cli), solutionPath, "INFO_MODE"],
                                        shell=True)
                        exit_codes = [p.wait() for p in processID]

                        # clean influxDBs
                        proc2 = subprocess.call([sys.executable, 'cleanInfluxDB.py', str(number_cli)], shell=True)
                        processID.clear()
    alertSimulationFinished()
