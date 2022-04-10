#!/usr/bin/env python

"""
File to run whole framework given different arguments <N_CLIENTS>, <N_ITERATIONS_TILL_WRITE>,
<N_GEN_POINTS_PER_CYCLE>, <TIME_TILL_REQ>

"""

# ------------------------------------------------------------------------------

# Standard Library
import logging
import sys
import subprocess
import time
import os

__author__ = "António Pereira"
__email__ = "antonio_m_sp@hotmail.com"
__status__ = "Development"

# ------------------------------------------------------------------------------

NUMBER_CLIENTS = [10]
NUMBER_ITERATIONS_TILL_WRITE = [5]
NUMBER_GENERATED_POINTS_PER_CYCLE = [5000]
TIME_TILL_REQUEST = [10]
MAX_ITERATIONS_SIMULATION = 5


def init_logging_config():
    """
    Initiates the logging configuration
    """
    logging.basicConfig(level=logging.INFO)
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)

    formatter = logging.Formatter('%(asctime)s : %(levelname)s : %(message)s')
    console.setFormatter(formatter)
    logging.getLogger("").addHandler(console)


def create_solution_directory(clients, number_iterations, number_generated, time_till_req):
    """
    Creates the folder for the current solution "e.g., 10_5_5000_10" inside /logs/ folder.
    """
    sol_path = 'logs\\' + str(clients) + '_' + str(number_iterations) + '_' + str(number_generated) + '_' + \
               str(time_till_req)

    if not os.path.exists(sol_path):
        directory_path = os.getcwd()
        path = os.path.join(directory_path, sol_path)
        os.mkdir(path, mode=0o666)

    return sol_path


if __name__ == "__main__":
    clean_proc = subprocess.call([sys.executable, 'clean_influxDB.py', str(30)], shell=True)

    init_logging_config()
    for i in range(MAX_ITERATIONS_SIMULATION):
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
                        exit_codes = [p.wait() for p in processID] # Waits for client processes to finish before proceeding
                        clean_proc = subprocess.call([sys.executable, 'clean_influxDB.py', str(number_cli)], shell=True)
                        processID.clear()
