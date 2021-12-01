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
import logging
import threading

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

ELAPSED_TIME_DATA = []
GEN_THREAD_TIME_DATA = []
GEN_THREAD_ITERATION_DATA = []
GEN_THREAD_ITERATION_AUX = 1
GEN_THREAD_REQUEST = []

iter = 0
prev_ts = 0


def now() -> int:
    global iter, prev_ts
    ts = time.time_ns()
    if ts == prev_ts:
        iter += 1
    else:
        iter = 0
        prev_ts = ts
    return ts + iter


def save_thread_time():
    measurement_name = "thread_timestamp_events"
    global GEN_THREAD_TIME_DATA, GEN_THREAD_ITERATION_DATA
    for i in range(len(GEN_THREAD_ITERATION_DATA)):
        ELAPSED_TIME_DATA.append(
            "{measurement},client={client} thread_time={thread_time},thread_iteration={thread_iteration}i"
                .format(measurement=measurement_name,
                        client=CLIENT_ID,
                        thread_time=GEN_THREAD_TIME_DATA[i],
                        thread_iteration=GEN_THREAD_ITERATION_DATA[i]))


def store_thread_time():
    """
    Writes the saved timestamp data to aux_master_db
    """

    global ELAPSED_TIME_DATA
    # db.write_points(ELAPSED_TIME_DATA, database='aux_master_db', time_precision='n', batch_size=5000,
    #                protocol="line")

    # SAVE TO .CSV FILE

    # GEN_THREAD_TIME_DATA
    # GEN_THREAD_ITERATION_DATA
    # print("GEN_THREAD_TIME_DATA: ", GEN_THREAD_TIME_DATA)
    # print("GEN_THREAD_ITERATION_DATA: ", GEN_THREAD_ITERATION_DATA)
    dict = {'Thread_Iteration': GEN_THREAD_ITERATION_DATA, 'Time_Elapsed': GEN_THREAD_TIME_DATA,
            'MASTER_REQUEST': GEN_THREAD_REQUEST}
    df = pd.DataFrame(dict)

    df.to_csv(CLIENT_ID + '_thread_time_elapsed.csv')


def generate_data(thread_name):
    """
    Generates the welding values with its assigned timestamps to be inserted in the DB.
    """
    global GLOBAL_DATA, GLOBAL_THREAD_START_TIME, GLOBAL_THREAD_END_TIME, GEN_THREAD_TIME_DATA, GEN_THREAD_ITERATION_AUX, GEN_THREAD_REQUEST
    # GLOBAL_THREAD_START_TIME = time.thread_time()
    thread_start_time = time.thread_time_ns()
    # logging.info("%s: Starting", thread_name)
    measurement_name = "weldingEvents"
    number_of_points = NUMBER_POINTS_PER_CYCLE
    curr_time = tmp_time
    for i in range(number_of_points):
        welding_value = format(round(random.uniform(0, 30), 4))
        GLOBAL_DATA.append("{measurement},client={client} welding_value={welding_value} {timestamp}"
                           .format(measurement=measurement_name,
                                   client=CLIENT_ID,
                                   welding_value=welding_value,
                                   timestamp=now()))

    # GLOBAL_THREAD_END_TIME = time.thread_time()
    thread_end_time = time.thread_time_ns()
    GEN_THREAD_TIME_DATA.append(thread_end_time - thread_start_time)  # thread_time
    GEN_THREAD_ITERATION_DATA.append(GEN_THREAD_ITERATION_AUX)  # thread_iterator number
    if MASTER_REQ_INFO:
        GEN_THREAD_REQUEST.append("REQUEST")
    else:
        GEN_THREAD_REQUEST.append("IDLE")
    GEN_THREAD_ITERATION_AUX += 1
    # print(GEN_THREAD_TIME_DATA)
    # print("GEN_THREAD_TIME_DATA: ", GEN_THREAD_TIME_DATA)
    # print("GEN_THREAD_ITERATION_DATA: ", GEN_THREAD_ITERATION_DATA)
    # print("GEN_THREAD_ITERATION_AUX: ", GEN_THREAD_ITERATION_AUX)
    # print("The time spent is {}".format(thread_end_time - thread_start_time))


def store_data(thread_name):
    """
    Writes the generated information to the client's DB.
    """

    logging.info("%s: Starting", thread_name)
    global GLOBAL_DATA

    client_write_start_time = time.perf_counter()
    db.write_points(GLOBAL_DATA, database=CLIENT_DB_NAME, time_precision='n', batch_size=5000,
                    protocol="line")
    client_write_end_time = time.perf_counter()
    print(CLIENT_ID + " wrote the Generated Data in: {time}s".format(
        time=client_write_end_time - client_write_start_time))
    GLOBAL_DATA.clear()


def machine_workflow(thread_name):
    # logging.info("%s: Starting", thread_name)
    global GLOBAL_MACHINE_WORKFLOW_CYCLE, GLOBAL_DATA, GLOBAL_ITERATOR_GENERATE_DATA, GLOBAL_THREAD_START_TIME, \
        GLOBAL_THREAD_END_TIME

    while GLOBAL_MACHINE_WORKFLOW_CYCLE:

        # thread_start_time = time.thread_time()
        generate_data_thread = threading.Thread(target=generate_data, args=("Generate Data Thread",), daemon=True)
        generate_data_thread.start()
        # generate_data_thread.join()
        # thread_end_time = time.thread_time()
        # GEN_THREAD_TIME_DATA.append(thread_end_time - thread_start_time)  # thread_time
        # GEN_THREAD_ITERATION_DATA.append(GEN_THREAD_ITERATION_AUX)  # thread_iterator number
        # GEN_THREAD_ITERATION_AUX += 1

        # print("The time spent is {}".format(GLOBAL_THREAD_END_TIME - GLOBAL_THREAD_START_TIME))

        GLOBAL_ITERATOR_GENERATE_DATA += 1

        if GLOBAL_ITERATOR_GENERATE_DATA >= NUMBER_ITERATIONS_TILL_WRITE:
            store_data_thread = threading.Thread(target=store_data, args=("Store Data Thread",), daemon=True)
            store_data_thread.start()
            GLOBAL_ITERATOR_GENERATE_DATA = 0
            store_data_thread.join()


def has_duplicate(tmp_list):
    """
    Checks to see if a given list has any duplicate value.

    :returns: False if list has no duplicate.
    """

    set_of_elems = set()
    for elem in tmp_list:
        if elem in set_of_elems:
            return True
        else:
            set_of_elems.add(elem)
    return False


def get_db_data():
    """
    Checks if client's DB has any duplicate value timestamp
    """

    data = db.query("SELECT * FROM weldingEvents;")
    # print('Data raw: ', data.raw)
    points = data.get_points(tags={'client': CLIENT_ID})
    timestamp_list = []
    for point in points:
        # print("Time: {}, Welding value: {}".format(point['time'], point['welding_value']))
        timestamp_list.append(point['time'])

    if has_duplicate(timestamp_list):
        pass
        # print(client_id + ' database contains duplicate timestamps.\n')
        # client.publish(topic_name, "REPEATED_TIMESTAMP")
    else:
        print('No duplicate timestamp found in client\'s database.\n')


def send_client_data(thread_name):
    """
    Send client's DB to master's DB
    """
    global MASTER_REQ_INFO

    logging.info("%s: Starting", thread_name)
    db.switch_database(CLIENT_DB_NAME)
    # global DB_LOCK
    # DB_LOCK.acquire()
    client_write_start_time = time.perf_counter()
    # machine_workflow_thread.do_run = False
    send_data = db.query('SELECT * INTO master_db..weldingEvents FROM weldingEvents GROUP BY *;')

    # machine_workflow_thread.do_run = True
    # bind_params={"$client_db_name": client_db_name}
    # )

    # params = {"client_db_name": client_db_name}
    # send_data = db.query('SELECT * INTO master_db..weldingEvents FROM client_db_name WHERE client_db_name=$client_db_name GROUP BY *;', bind_params=params)  # ,

    '''
    query('SELECT * FROM alerts '
          'WHERE time>=$start AND time<$stop '
          'AND client_id=$client AND rule_id=$rule',
          bind_params=params
          )
    '''

    client_write_end_time = time.perf_counter()
    # DB_LOCK.release()
    print(CLIENT_ID + " sent all its Data to Master\'s DB in: {time}s".format(
        time=client_write_end_time - client_write_start_time))
    # print("Query Successful: ", send_data)
    client.publish(CLIENT_TOPIC, "ALL_INFORMATION_SENT")
    global CLIENT_SENT_ALL_DATA
    CLIENT_SENT_ALL_DATA = True
    MASTER_REQ_INFO = False
    '''
     remove_data = db.query(
         query='DROP SERIES FROM weldingEvents WHERE client=$client;',
         params={"client": client_id}
     )
     '''


def on_connect(client, userdata, flags, rc):
    """
    MQTT connect protocol

    RC:
    0: Connection successful
    1: Connection refused – incorrect protocol version
    2: Connection refused – invalid client identifier
    3: Connection refused – server unavailable
    4: Connection refused – bad username or password
    5: Connection refused – not authorised
    6-255: Currently unused.
    """

    if rc != 0:
        print(CLIENT_ID + " - Error Connecting, RC: ", rc)
    else:
        print(CLIENT_ID + " - Successfully Connected to Broker.")
        client.subscribe("topic/simulation/clients/#")  # subscribes to every topic of clients including itself
        client.subscribe("topic/simulation/master")


def on_message(client, userdata, message):
    """
    Receives the messages that are published through the broker
    """
    global GLOBAL_MACHINE_WORKFLOW_CYCLE
    decoded_message = str(message.payload.decode("utf-8"))

    if message.topic == "topic/simulation/master":
        if decoded_message == "GET_INFORMATION":
            mqtt_protocol_print(message)
            global MASTER_REQ_INFO
            MASTER_REQ_INFO = True
        if decoded_message == "DATA_RECEIVED_FROM_" + CLIENT_ID:
            pass
            # mqtt_protocol_print(message)

        if decoded_message == "REQ_DONE":
            GLOBAL_MACHINE_WORKFLOW_CYCLE = False
    """
    if "topic/simulation/clients/" not in message.topic:
        print("message received: ", decoded_message)
        print("message topic: ", message.topic)
        print("message qos: ", message.qos)  # 0, 1 or 2.
        print("message retain flag: ", message.retain, "\n")
    """
    # if decoded_message == "GET_INFORMATION" and message.topic == "topic/simulation/master":
    #    global MASTER_REQ_INFO
    #    MASTER_REQ_INFO = True


def clear_data(client_db):
    """
    Clears every value from client's DB.
    """

    query = "DROP SERIES FROM weldingEvents WHERE client=$client;"
    bind_params = {'client': CLIENT_ID}
    remove_data = client_db.query(query, bind_params=bind_params)

    # logging.debug("Removed data after sending its data to Master's Database: ", remove_data)
    data = client_db.query("SELECT * FROM weldingEvents;")
    # logging.debug('AFTER DELETION: ', data.raw)


def mqtt_init(tmp_client):
    """
    Connects to Broker and initializes protocols
    """

    tmp_client.on_connect = on_connect
    tmp_client.on_message = on_message
    broker_address = "broker.hivemq.com"  # broker_address = "localhost"
    tmp_client.connect(broker_address, port=1883)  # connect to broker


def init_client_variables():
    pass


def init_logging_config():
    # thread_format = "%(asctime)s: %(message)s"
    if 'DEBUG_MODE' in sys.argv:
        logging.basicConfig(filename=CLIENT_ID + '.log', format='%(asctime)s - %(message)s', level=logging.DEBUG)
    else:
        logging.basicConfig(filename=CLIENT_ID + '.log', format='%(asctime)s - %(message)s', level=logging.INFO)


    # logging.basicConfig(format=thread_format, level=logging.INFO, datefmt="%H:%M:%S")


if __name__ == "__main__":

    # Arguments
    MACHINE_NUMBER = sys.argv[1]
    NUMBER_ITERATIONS_TILL_WRITE = int(sys.argv[2])
    NUMBER_POINTS_PER_CYCLE = int(sys.argv[3])

    # CONST GLOBAL VARIABLES

    CLIENT_DB_NAME = 'client' + MACHINE_NUMBER + '_db'
    CLIENT_ID = 'client' + MACHINE_NUMBER
    CLIENT_TOPIC = "topic/simulation/clients/" + CLIENT_ID

    # ------------------------------------

    GLOBAL_ITERATOR_GENERATE_DATA = 0
    GLOBAL_THREAD_START_TIME = None
    GLOBAL_THREAD_END_TIME = None
    GLOBAL_MACHINE_WORKFLOW_CYCLE = True
    GLOBAL_DATA = []
    CLIENT_SENT_ALL_DATA = False
    MASTER_REQ_INFO = False

    # ------------------------------------

    init_logging_config()

    db = InfluxDBClient('localhost', 8086, 'root', 'root', CLIENT_DB_NAME)
    db.create_database(CLIENT_DB_NAME)

    clear_data(db)
    tmp_time = int(time.time() * 1000)  # milliseconds

    # Threads

    # Threads init

    machine_workflow_thread = threading.Thread(target=machine_workflow, args=("Machine Workflow Thread",), daemon=True)
    machine_workflow_thread.start()

    client = mqtt.Client()
    mqtt_init(client)
    client.loop_start()

    while not MASTER_REQ_INFO:
        pass

    client.publish(CLIENT_TOPIC, "SENDING_DATA")
    get_db_data()  # used to confirm if data is valid (no repeated timestamps)

    send_data_thread = threading.Thread(target=send_client_data, args=("Send Data Thread",), daemon=True)
    send_data_thread.start()

    while not CLIENT_SENT_ALL_DATA:
        pass

    while GLOBAL_MACHINE_WORKFLOW_CYCLE:
        pass

    save_thread_time()
    store_thread_time()
    # input("Press the <ENTER> key to continue...")
