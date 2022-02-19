#!/usr/bin/env python

"""
Creates a Client on the MQTT Protocol to communicate with Master

Client generates information to be sent to the Master when it's to be requested.
The communication is done through the MQTT Protocol with InfluxDB.
"""

# ------------------------------------------------------------------------------

# Standard Library
import random
import time
import sys
import threading
import os
from datetime import datetime

# 3rd Party Packages
import paho.mqtt.client as mqtt
from influxdb import InfluxDBClient
import pandas as pd

# Local Source
from utils import *

__author__ = "António Pereira"
__email__ = "antonio_m_sp@hotmail.com"
__status__ = "Development"

# ------------------------------------------------------------------------------

# Global Variables

WELDING_GEN_THREAD_TIMESTAMP = None  # added later

GEN_THREAD_TIME_DATA = []  # time taken by thread
GEN_THREAD_ITERATION_DATA = []  # thread iterator
GEN_THREAD_REQUEST = []  # IDLE / REQUEST
GEN_AUXILIAR_LIST = []
GEN_THREAD_ITERATION_AUX = 1


# -------------

itera = 0
prev_ts = 0

# Util function


def now() -> int:
    global itera, prev_ts
    ts = time.time_ns()
    if ts == prev_ts:
        itera += 1
    else:
        itera = 0
        prev_ts = ts
    return ts + itera


def mathematical_calculation():
    k = 1 # Initialize denominator
    s = 0 # Initialize sum

    for i in range(1000000):
        # even index elements are positive
        if i % 2 == 0:
            s += 4 / k
        else:
            # odd index elements are negative
            s -= 4 / k

        # denominator is odd
        k += 2
    return s
    # print(s)

# ---------


def save_thread_timestamp_intervals_csv():
    """
    Writes the saved elapsed timestamp from generating_data() thread to clientID_thread_time_elapsed.csv
    """

    tmp_dict = {'Thread_Iteration': GEN_THREAD_ITERATION_DATA,
                'Time_Elapsed': GEN_THREAD_TIME_DATA,
                'MASTER_REQUEST': GEN_THREAD_REQUEST}

    df = pd.DataFrame(tmp_dict)
    sum_timestamps = sum(GEN_THREAD_TIME_DATA) * pow(10, -9)
    logging.info(CLIENT_ID + " - Solution time adding all time_elapsed of thread: {%.5f} seconds", sum_timestamps)
    timestr = time.strftime("%Y%m%d-%H%M%S")
    df.to_csv(SOLUTION_PATH + '\\' + CLIENT_ID + '_thread_time_elapsed_' + timestr + '.csv', index=False)


def welding_data_generation_simulation():
    """
    Generates the welding values with its assigned timestamps to be inserted in the DB.
    """
    # thread_start_time = time.thread_time_ns()
    logging.debug("Welding Simulator Working...")
    global WELDING_DATA, GEN_THREAD_TIME_DATA, GEN_THREAD_ITERATION_AUX, GEN_THREAD_REQUEST
    measurement_name = "weldingEvents"
    mathematical_calculation()

    for i in range(NUMBER_GENERATED_POINTS_PER_CYCLE):
        # start_time = time.time()
        # logging.info("BEFORE: " + str(start_time))
        # mathematical_calculation()
        # logging.info("TIME TAKEN BY MATHEMATICAL CALCULATION: " + str(time.time() - start_time))
        welding_value = format(round(random.uniform(0, 30), 4))
        uniqueID = str(i+1)
        # time.clock_gettime_ns(time.CLOCK_REALTIME))
        # time_now_temp = int(time.time() * 1000)
        time_now_temp = time.time_ns()
        WELDING_DATA.append("{measurement},client={client},uniqueID={uniqueID} welding_value={welding_value} {timestamp}"
                            .format(measurement=measurement_name,
                                    client=CLIENT_ID,
                                    uniqueID=uniqueID,
                                    welding_value=welding_value,
                                    timestamp=time_now_temp))
    """    
    thread_end_time = time.thread_time_ns()
    GEN_THREAD_TIME_DATA.append(thread_end_time - thread_start_time)  # thread_time
    GEN_THREAD_ITERATION_DATA.append(GEN_THREAD_ITERATION_AUX)  # thread_iterator number

    if MASTER_REQ_INFO:
        GEN_THREAD_REQUEST.append("REQUEST")
    else:
        GEN_THREAD_REQUEST.append("IDLE")
    GEN_THREAD_ITERATION_AUX += 1
    """


def store_welding_generation_DB():
    """
    Writes the generated information to the client's DB.
    """

    global WELDING_DATA
    logging.debug("Storing generated welding data to DB...")

    client_write_start_time = time.perf_counter()
    db.write_points(WELDING_DATA, database=CLIENT_DB_NAME, time_precision='n', batch_size=10000,
                    protocol="line")  # try batch_size=10000
    client_write_end_time = time.perf_counter()
    welding_write_time_to_DB = client_write_end_time - client_write_start_time

    logging.debug(CLIENT_ID + " stored the welding generated data in: %.5f seconds", welding_write_time_to_DB)
    WELDING_DATA.clear()


def welding_workflow():
    logging.debug("Starting Welding Workflow Cycle Thread...")

    global MACHINE_WORKFLOW_CYCLE, ITERATOR_GENERATE_DATA, THREAD_START_TIME, THREAD_END_TIME, WELDING_ITERATOR_WORKFLOW, GEN_THREAD_ITERATION_AUX, GEN_THREAD_BOOLEAN_ACTIVE, THREAD_DB_START_TIME, THREAD_DB_END_TIME

    extra = 0
    while MACHINE_WORKFLOW_CYCLE:
        generate_data_thread = threading.Thread(target=welding_data_generation_simulation, args=(), daemon=True)

        THREAD_START_TIME = time.perf_counter_ns()
        generate_data_thread.start()
        generate_data_thread.join()
        THREAD_END_TIME = time.perf_counter_ns()
        GEN_AUXILIAR_LIST.append(THREAD_END_TIME - THREAD_START_TIME)

        ITERATOR_GENERATE_DATA += 1

        if ITERATOR_GENERATE_DATA >= NUMBER_ITERATIONS_TILL_WRITE:
            store_data_thread = threading.Thread(target=store_welding_generation_DB, args=(), daemon=True)
            # THREAD_DB_START_TIME = time.perf_counter_ns()
            store_data_thread.start()
            store_data_thread.join()
            # THREAD_DB_END_TIME = time.perf_counter_ns()
            ITERATOR_GENERATE_DATA = 0

            GEN_THREAD_TIME_DATA.append(sum(GEN_AUXILIAR_LIST))  # thread_time
            GEN_THREAD_ITERATION_DATA.append(GEN_THREAD_ITERATION_AUX)  # thread_iterator number
            GEN_AUXILIAR_LIST.clear()

            if MASTER_REQ_INFO:
                GEN_THREAD_REQUEST.append("REQUEST")
            else:
                GEN_THREAD_REQUEST.append("IDLE")

        if WELDING_ITERATOR_WORKFLOW >= MAX_CYCLE_LIMIT-1:  # Problem of generating data and it not being saved MAX_CYCLE Limit vs NUMBERS_ITER
            # logging.info("Welding iterator workflow passed MAX_CYCLE_LIMIT of: " + str(MAX_CYCLE_LIMIT))
            MACHINE_WORKFLOW_CYCLE = False

        GEN_THREAD_ITERATION_AUX += 1
        WELDING_ITERATOR_WORKFLOW += 1


def check_duplicate_timestamp_unused():
    """
    Checks if client's DB has any duplicate timestamp value
    """

    data = db.query("SELECT * FROM weldingEvents;")
    # print('Data raw: ', data.raw)
    points = data.get_points(tags={'client': CLIENT_ID})
    timestamp_list = []
    for point in points:
        # print("Time: {}, Welding value: {}".format(point['time'], point['welding_value']))
        timestamp_list.append(point['time'])
    """
    if has_duplicate_values(timestamp_list):
        logging.debug(CLIENT_ID + ' database contains duplicate timestamps.\n')
        mqtt_client.publish(CLIENT_TOPIC, "REPEATED_TIMESTAMP")
    else:
        pass
        logging.debug('No duplicate timestamp found in client\'s database.\n')
    """

def send_client_data_to_master():
    """
    Send Client's DB data to Master's DB through influxDB
    """
    logging.debug("Sending all data in %s to master_db", CLIENT_DB_NAME)
    global MASTER_REQ_INFO, CLIENT_SENT_ALL_DATA
    db.switch_database(CLIENT_DB_NAME)

    client_write_start_time = time.perf_counter()
    send_data = db.query('SELECT * INTO master_db..weldingEvents FROM weldingEvents GROUP BY *;')
    client_write_end_time = time.perf_counter()

    client_write_time_to_master = client_write_end_time - client_write_start_time
    logging.info(CLIENT_ID + " sent Data to Master in: {%.5f} seconds\n", client_write_time_to_master)

    # logging.info("Query Successful: ", str(send_data))

    mqtt_client.publish(CLIENT_TOPIC, "ALL_INFORMATION_SENT", qos=1, retain=False)

    CLIENT_SENT_ALL_DATA = True
    MASTER_REQ_INFO = False


def clear_clientDB_data():
    """
    Clears every value from client's DB.
    """

    db.switch_database(CLIENT_DB_NAME)
    # db.drop_database(CLIENT_DB_NAME)

    query = "DROP SERIES FROM weldingEvents WHERE client=$client;"
    bind_params = {'client': CLIENT_ID}
    # logging.info("Starting to clean client DB..")
    deleted_data = db.query(query, bind_params=bind_params)
    # logging.info("Deleted data query: ", deleted_data)
    # logging.info("Deleted client DB..")


def influxDB_terminate():
    db.close()


def init_logging_config():

    logging.root.handlers = []

    if 'DEBUG_MODE' in sys.argv:
        logging.basicConfig(filename=SOLUTION_PATH + '\\' + CLIENT_ID + '.log', format='%(asctime)s : %(levelname)s : %(message)s',
                            level=logging.DEBUG, filemode='a')
    else:
        logging.basicConfig(filename=SOLUTION_PATH + '\\' + CLIENT_ID + '.log', format='%(asctime)s : %(levelname)s : %(message)s',
                            level=logging.INFO, filemode='a')

    # logging.basicConfig(format=thread_format, level=logging.INFO, datefmt="%H:%M:%S")

    # set up logging to console
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)

    # set a format which is simpler for console use
    formatter = logging.Formatter('%(asctime)s : ' + SOLUTION_PATH + ' : %(levelname)s : %(message)s')
    console.setFormatter(formatter)
    logging.getLogger("").addHandler(console)


# MQTT Protocols


def on_connect(client, userdata, message, return_code):
    """
    MQTT connect protocol

    @param client:
        the client instance for this callback
    @param userdata:
        the private user data as set in Client() or userdata_set()
    @param message:
        response message sent by the broker
    @param return_code:
        the connection result
        0: Connection successful
        1: Connection refused – incorrect protocol version
        2: Connection refused – invalid client identifier
        3: Connection refused – server unavailable
        4: Connection refused – bad username or password
        5: Connection refused – not authorised
        6-255: Currently unused.
    """

    if return_code != 0:
        logging.info(CLIENT_ID + " - Error Connecting, Error Code: ", return_code)
        client.reconnect()
    else:
        logging.info(CLIENT_ID + " - Successfully Connected to Broker.\n")
        client.subscribe("topic/simulation/clients/#")  # subscribes to every topic of clients including itself
        client.subscribe("topic/simulation/master", qos=1)


def on_disconnect(client, userdata, rc):
    logging.info("%s is disconnecting with reason: %s ", CLIENT_ID, str(rc))


def information_requested(decoded_message):
    if "GET_INFORMATION_" in decoded_message:
        # parse decoded message
        split_string = decoded_message.split("_")  # 1,4,7,10,13,
        # print("SplitString: ", split_string)
        if "," in split_string[2]:
            split_of_comma_string = split_string[2].split(",")  # 1 4 7 10 13
            # print("split_of_comma_string: ", split_of_comma_string)
            for i in range(len(split_of_comma_string)):
                # print("split_of_comma_string[i]: ", split_of_comma_string[i])
                # print("MACHINE_NUMBER: ", MACHINE_NUMBER)
                if MACHINE_NUMBER == split_of_comma_string[i]:
                    # print("IM IN - MACHINE NUMBER: ", MACHINE_NUMBER)
                    return True
    return False


def on_message(client, userdata, message):
    """
    Receives the messages that are published through the broker
    """

    if message.topic == "topic/simulation/master":
        decoded_message = str(message.payload.decode("utf-8"))
        global MACHINE_WORKFLOW_CYCLE, MASTER_REQ_INFO

        if decoded_message == "GET_INFORMATION_FROM_" + CLIENT_ID or information_requested(decoded_message):
            mqtt_protocol_print(message)
            MASTER_REQ_INFO = True

        elif decoded_message == "DATA_RECEIVED_FROM_" + CLIENT_ID:
            mqtt_protocol_print(message)
            pass

        elif decoded_message == "REQUEST_FINISHED":
            mqtt_protocol_print(message)
            # MACHINE_WORKFLOW_CYCLE = False


def mqtt_init(client):
    """
    Connects to Broker and initializes protocols
    """

    # broker_address = "broker.hivemq.com"
    broker_address = "localhost"
    broker_port = 1883

    client.on_connect = on_connect
    client.on_message = on_message

    try:
        client.connect(broker_address, port=broker_port)
    except ConnectionError:
        logging.info("Error connecting to Broker: %s, on Port: %s" % (broker_address, broker_port))
        client.reconnect()


def mqtt_terminate(client):
    client.disconnect()
    client.loop_stop()


if __name__ == "__main__":

    # Arguments
    MACHINE_NUMBER = sys.argv[1]
    NUMBER_ITERATIONS_TILL_WRITE = int(sys.argv[2])
    NUMBER_GENERATED_POINTS_PER_CYCLE = int(sys.argv[3])
    SOLUTION_PATH = sys.argv[4]
    MAX_CYCLE_LIMIT = 250

    # Const Global Variables
    CLIENT_DB_NAME = 'client' + MACHINE_NUMBER + '_db'
    CLIENT_ID = 'client' + MACHINE_NUMBER
    CLIENT_TOPIC = "topic/simulation/clients/" + CLIENT_ID

    # Boolean Global Variables
    MACHINE_WORKFLOW_CYCLE = True
    CLIENT_SENT_ALL_DATA = False
    MASTER_REQ_INFO = False

    # Global Variables
    ITERATOR_GENERATE_DATA = 0
    WELDING_ITERATOR_WORKFLOW = 0
    THREAD_START_TIME = None
    THREAD_END_TIME = None
    THREAD_DB_START_TIME = None
    THREAD_DB_END_TIME = None
    WELDING_DATA = []

    # ------------------------------------

    # data_end_time = int(time.time() * 1000)  # milliseconds

    # Logging Configuration
    init_logging_config()

    # InfluxDB
    db = InfluxDBClient('localhost', 8086, 'root', 'root', CLIENT_DB_NAME)
    db.create_database(CLIENT_DB_NAME)
    # clear_clientDB_data()

    # MQTT Protocol
    mqtt_client = mqtt.Client()
    mqtt_init(mqtt_client)

    welding_workflow_thread = threading.Thread(target=welding_workflow, args=(), daemon=True)
    welding_workflow_thread.start()

    mqtt_client.loop_start()

    mqtt_client.publish(CLIENT_TOPIC, "WORKING", qos=0, retain=True)

    while not MASTER_REQ_INFO:
        pass

    mqtt_client.publish(CLIENT_TOPIC, "SENDING_DATA", qos=0, retain=False)

    # check_duplicate_timestamp()

    send_data_thread = threading.Thread(target=send_client_data_to_master, args=(), daemon=True)
    send_data_thread.start()
    # logging.debug("Thread ID, send_data_thread: ", send_data_thread.get_ident())

    while not CLIENT_SENT_ALL_DATA:
        pass

    while MACHINE_WORKFLOW_CYCLE:
        pass

    # Exit functions
    save_thread_timestamp_intervals_csv()
    influxDB_terminate()
    mqtt_terminate(mqtt_client)
