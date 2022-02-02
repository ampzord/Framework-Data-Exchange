import os
import re
import math
import numpy as np
import npm as npm
import pandas as pd
import csv
import numpy as np


def average(lst):
    avg = sum(lst) / len(lst)
    avg_float_decimal = float("{0:.3f}".format(avg))
    return avg_float_decimal


if __name__ == "__main__":

    cwd_logs = os.getcwd() + "/logs/"
    WELDING_VALUE_LIST = []
    MASTER_SOLUTION_LIST = []
    CLIENT_SOLUTION_LIST = []
    TIME_ELAPSED_THREAD = []  # 5 valores (cliente1), (cliente2)
    TIME_TO_FINISH = []  # 5 valores (master)
    file_ending_client = ("1.log", "2.log", "3.log", "4.log", "5.log", "6.log", "7.log", "8.log", "9.log", "0.log")

    with open('solution.csv', 'w', encoding='UTF8', newline='') as f_csv:
        writer = csv.writer(f_csv)
        header1 = ["PARAMETERS", "", "", "", "METRICS"]
        header2 = ["", "", "", "", "Solution_Time", "", "Number_Welding_Values", "", "TIME_OF_THREAD_CLIENT1", "",
                   "TIME_OF_THREAD_CLIENT2", "", "TIME_OF_THREAD_CLIENT3", "", "TIME_OF_THREAD_CLIENT4", "", "TIME_OF_THREAD_CLIENT5", "",
                   "TIME_OF_THREAD_CLIENT6", "", "TIME_OF_THREAD_CLIENT7", "", "TIME_OF_THREAD_CLIENT8", "", "TIME_OF_THREAD_CLIENT9", "",
                   "TIME_OF_THREAD_CLIENT10", "", "TIME_OF_THREAD_CLIENT11", "", "TIME_OF_THREAD_CLIENT12", "", "TIME_OF_THREAD_CLIENT13", "",
                   "TIME_OF_THREAD_CLIENT14", "", "TIME_OF_THREAD_CLIENT15", "", "TIME_OF_THREAD_CLIENT16", "", "TIME_OF_THREAD_CLIENT17", "",
                   "TIME_OF_THREAD_CLIENT18", "", "TIME_OF_THREAD_CLIENT19", "","TIME_OF_THREAD_CLIENT20"]
        header3 = ["NUMBER_OF_CLIENTS", "NUMBER_ITERATIONS_TILL_WRITE", "NUMBER_GENERATED_POINTS_PER_CYCLE", "TIME_TILL_REQUEST_OF_INFORMATION", "Average", "Std_Deviation", "Average", "Std_Deviation", "Average", "Std_Deviation",
                   "Average", "Std_Deviation", "Average", "Std_Deviation", "Average", "Std_Deviation", "Average", "Std_Deviation", "Average", "Std_Deviation", "Average", "Std_Deviation", "Average", "Std_Deviation", "Average", "Std_Deviation",
                   "Average", "Std_Deviation", "Average", "Std_Deviation", "Average", "Std_Deviation", "Average", "Std_Deviation", "Average", "Std_Deviation", "Average", "Std_Deviation", "Average", "Std_Deviation", "Average", "Std_Deviation",
                   "Average", "Std_Deviation"]
        writer.writerow(header1)
        writer.writerow(header2)
        writer.writerow(header3)

    MASTER_DONE = False

    for root, dirs, files in os.walk(cwd_logs):
        for dir in dirs:
            print(dir)
            solutionPath2 = os.path.join(cwd_logs, dir)
            print("solution of:" + solutionPath2)

            # MASTER
            if os.path.isfile(solutionPath2 + '\\master.log') and not MASTER_DONE:
                correct_filename = solutionPath2 + "\\master.log"
                print("WORKING ON:", correct_filename)

                f_master = open(correct_filename, 'r')
                for line in f_master:
                    if "Since Master did Request till all data arrived" in line:
                        # TIME_TAKEN_BY_SIM
                        time_to_finish_value = re.findall('{(.+?)}', line)
                        TIME_TO_FINISH.append(time_to_finish_value)
                    if "Number of Welding Values in MasterDB" in line:
                        # NUMBER OF WELDING_VALUES
                        welding_value = re.findall('{(.+?)}', line)
                        WELDING_VALUE_LIST.append(welding_value)
                f_master.close()

                TIME_TO_FINISH_FLOAT = []
                for item in TIME_TO_FINISH:
                    f = float(item[0])
                    TIME_TO_FINISH_FLOAT.append(f)

                WELDING_VALUE_LIST_INT = []
                for item in WELDING_VALUE_LIST:
                    f = int(item[0])
                    WELDING_VALUE_LIST_INT.append(f)

                time_to_finish_value_avg = average(TIME_TO_FINISH_FLOAT)
                time_to_finish_value_std = np.std(TIME_TO_FINISH_FLOAT)
                time_to_finish_value_std_decimal = float("{0:.3f}".format(time_to_finish_value_std))

                welding_value_avg = average(WELDING_VALUE_LIST_INT)
                welding_value_std = np.std(WELDING_VALUE_LIST_INT)
                welding_value_std_decimal = float("{0:.3f}".format(welding_value_std))

                MASTER_SOLUTION_LIST.append(time_to_finish_value_avg)
                MASTER_SOLUTION_LIST.append(time_to_finish_value_std_decimal)

                MASTER_SOLUTION_LIST.append(welding_value_avg)
                MASTER_SOLUTION_LIST.append(welding_value_std_decimal)
                MASTER_DONE = True

            # ALL CLIENTS
            for i in range(20):
                if os.path.isfile(solutionPath2 + '\\client' + str(i+1) + ".log"):
                    correct_filename = solutionPath2 + '\\client' + str(i+1) + ".log"
                    print("WORKING ON:", correct_filename)
                    f = open(correct_filename, 'r')
                    for line in f:
                        if "Solution time adding all time_elapsed of thread" in line:
                            # TIME_ELAPSED
                            time_elapsed_value = re.findall('{(.+?)}', line)
                            TIME_ELAPSED_THREAD.append(time_elapsed_value)

                            # CLIENT_ID
                            regex = re.compile(r"\bclient\d+")
                            CLIENT_ID = regex.findall(line)
                    f.close()

                    TIME_ELAPSED_THREAD_FLOAT = []
                    for item in TIME_ELAPSED_THREAD:
                        f = float(item[0])
                        TIME_ELAPSED_THREAD_FLOAT.append(f)

                    time_elapsed_value_avg = average(TIME_ELAPSED_THREAD_FLOAT)
                    CLIENT_SOLUTION_LIST.append(time_elapsed_value_avg)

                    time_elapsed_value_std = np.std(TIME_ELAPSED_THREAD_FLOAT)
                    time_elapsed_value_std_decimal = float("{0:.3f}".format(time_elapsed_value_std))

                    CLIENT_SOLUTION_LIST.append(time_elapsed_value_std_decimal)

            # INFORMATION TO SEND TO .CSV
            parameters_divided = dir.split('_')  # PARAMETERS
            print(parameters_divided)
            print(MASTER_SOLUTION_LIST)
            SOLUTION_ROW_LIST = parameters_divided + MASTER_SOLUTION_LIST + CLIENT_SOLUTION_LIST

            with open('solution.csv', 'a', encoding='UTF8', newline='') as f_csv:
                writer = csv.writer(f_csv)
                writer.writerow(SOLUTION_ROW_LIST)

            # RESET VARIABLES
            SOLUTION_ROW_LIST = []
            TIME_TO_FINISH = []
            WELDING_VALUE_LIST = []
            CLIENT_SOLUTION_LIST = []
            MASTER_SOLUTION_LIST = []
            TIME_ELAPSED_THREAD = []
            TIME_ELAPSED_THREAD_FLOAT = []
            new_list_float_avg = None
            CLIENT_ID = None
            SOLUTION_ROW_LIST = []
            MASTER_DONE = False

    f_csv.close()
