import paho.mqtt.client as mqtt
from influxdb import InfluxDBClient
import random
import time
import sys
import logging
import threading
from time import sleep
from threading import Thread, Lock
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor

elapsed_time_data = []

def save_timestamps_db():
    measurement_name = "thread_timestamp_events"
    average_time = None
    thread_iteration = None
    for i in range(10000):
        elapsed_time_data.append("{measurement},client={client} average_time={average_time},thread_iteration={"
                                 "thread_iteration} "
                                 .format(measurement=measurement_name,
                                         client=client_id,
                                         average_time=average_time,
                                         thread_iteration=thread_iteration))


def store_timestamp_data(thread_name):
    """
    Writes the saved timestamp data to aux_master_db
    """
    # TODO
    global elapsed_time_data
    db.write_points(elapsed_time_data, database='aux_master_db', time_precision='ms', batch_size=5000,
                    protocol="line")
    


def generate_data(thread_name):
    """
    Generates the welding values with its assigned timestamps to be inserted in the DB.
    """
    global GLOBAL_DATA, GLOBAL_THREAD_START_TIME, GLOBAL_THREAD_END_TIME
    # GLOBAL_THREAD_START_TIME = time.thread_time()
    thread_start_time = time.thread_time()
    logging.info("Thread %s: starting", thread_name)
    measurement_name = "weldingEvents"
    number_of_points = 1000
    # data = []
    curr_time = tmp_time
    for i in range(number_of_points):
        welding_value = format(round(random.uniform(0, 30), 4))
        curr_time = curr_time - random.randint(1,
                                               1000000)  # curr_time = int(time.time() * 1000), int(time.time() * 1000000000)

        # uniqueID = 'uniqueID' + str(i + 1)
        # data.append("{measurement},client={client},uniqueID={uniqueID} welding_value={welding_value} {timestamp}"
        #            .format(measurement=measurement_name,
        #                    client=client_name,
        #                    uniqueID=uniqueID,
        #                    welding_value=welding_value,
        #                    timestamp=curr_time))
        GLOBAL_DATA.append("{measurement},client={client} welding_value={welding_value} {timestamp}"
                           .format(measurement=measurement_name,
                                   client=client_id,
                                   welding_value=welding_value,
                                   timestamp=curr_time))

        # GLOBAL_DATA.append("{measurement},client={client} welding_value={welding_value}"
        #                   .format(measurement=measurement_name,
        #                           client=client_id,
        #                           welding_value=welding_value))
    # return data
    # GLOBAL_THREAD_END_TIME = time.thread_time()
    thread_end_time = time.thread_time()
    print("The time spent is {}".format(thread_end_time - thread_start_time))


def store_data(thread_name):
    """
    Writes the generated information to the client's DB.
    """

    logging.info("Thread %s: starting", thread_name)
    global GLOBAL_DATA

    client_write_start_time = time.perf_counter()
    db.write_points(GLOBAL_DATA, database=client_db_name, time_precision='ms', batch_size=5000,
                    protocol="line")  # previous time_precision='n'
    client_write_end_time = time.perf_counter()
    print(client_id + " wrote ALL generated Data to Client DB in: {time}s".format(
        time=client_write_end_time - client_write_start_time))
    GLOBAL_DATA.clear()


def machine_workflow(thread_name):
    logging.info("Thread %s: starting", thread_name)
    global GLOBAL_MACHINE_WORKFLOW_CYCLE, GLOBAL_DATA, GLOBAL_ITERATOR_GENERATE_DATA, GLOBAL_THREAD_START_TIME, \
        GLOBAL_THREAD_END_TIME

    while GLOBAL_MACHINE_WORKFLOW_CYCLE:

        # TODO run generating_data thread for X cycles and after save to clientDB

        generate_data_thread = threading.Thread(target=generate_data, args=("Generate Data Thread",))
        generate_data_thread.start()
        # generate_data_thread.join()
        # print("The time spent is {}".format(GLOBAL_THREAD_END_TIME - GLOBAL_THREAD_START_TIME))

        GLOBAL_ITERATOR_GENERATE_DATA += 1

        if GLOBAL_ITERATOR_GENERATE_DATA >= 10:
            store_data_thread = threading.Thread(target=store_data, args=("Store Data Thread",))
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
    points = data.get_points(tags={'client': client_id})
    timestamp_list = []
    for point in points:
        # print("Time: {}, Welding value: {}".format(point['time'], point['welding_value']))
        timestamp_list.append(point['time'])

    if has_duplicate(timestamp_list):
        print('Yes, client\'s database contains duplicate timestamps.\n')
        client.publish(topic_name, "REPEATED_TIMESTAMP")
    else:
        print('No duplicate timestamp found in client\'s database.\n')


def send_client_data(thread_name):
    """
    Send client's DB to master's DB
    """

    logging.info("Thread %s: starting", thread_name)
    db.switch_database(client_db_name)
    # global DB_LOCK
    # DB_LOCK.acquire()
    client_write_start_time = time.perf_counter()
    # machine_workflow_thread.do_run = False
    # print("Hello before select")
    send_data = db.query('SELECT * INTO master_db..weldingEvents FROM weldingEvents GROUP BY *;')
    # print("Hello after select")

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
    print(client_id + " sent ALL Data to Master\'s DB in: {time}s".format(
        time=client_write_end_time - client_write_start_time))
    print("Query Successful: ", send_data)
    client.publish(topic_name, "ALL_INFORMATION_SENT")
    global CLIENT_SENT_ALL_DATA
    CLIENT_SENT_ALL_DATA = True
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
        print(client_id + " - error connecting, rc: ", rc)
    else:
        print(client_id + " - successfully connected.")
        client.subscribe("topic/simulation/clients/#")  # subscribes to every topic of clients including itself
        client.subscribe("topic/simulation/master")


def on_message(client, userdata, message):
    """
    Receives the messages that are published through the broker
    """

    decoded_message = str(message.payload.decode("utf-8"))
    if "topic/simulation/clients/" not in message.topic:
        print("message received: ", decoded_message)
        print("message topic: ", message.topic)
        print("message qos: ", message.qos)  # 0, 1 or 2.
        print("message retain flag: ", message.retain, "\n")

    if decoded_message == "GET_INFORMATION" and message.topic == "topic/simulation/master":
        global MASTER_REQ_INFO
        MASTER_REQ_INFO = True


def clear_data(client_db):
    """
    Clears every value from client's DB.
    """

    query = "DROP SERIES FROM weldingEvents WHERE client=$client;"
    bind_params = {'client': client_id}
    remove_data = client_db.query(query, bind_params=bind_params)
    # print("Removed data after sending its data to Master's Database: ", remove_data)
    # data = database.query("SELECT * FROM weldingEvents;")
    # print('AFTER DELETION: ', data.raw)


def mqtt_init(tmp_client):
    """
    Connects to Broker and initializes protocols
    """

    tmp_client.on_connect = on_connect
    tmp_client.on_message = on_message
    broker_address = "broker.hivemq.com"  # broker_address = "localhost"
    tmp_client.connect(broker_address, port=1883)  # connect to broker


if __name__ == "__main__":
    GLOBAL_ITERATOR_GENERATE_DATA = 0
    GLOBAL_THREAD_START_TIME = None
    GLOBAL_THREAD_END_TIME = None
    GLOBAL_MACHINE_WORKFLOW_CYCLE = True
    GLOBAL_DATA = []
    CLIENT_SENT_ALL_DATA = False
    MASTER_REQ_INFO = False
    machine_number = (sys.argv[1])
    client_db_name = 'client' + machine_number + '_db'  # client1_db
    client_id = 'client' + machine_number  # client1
    topic_name = "topic/simulation/clients/" + client_id

    # home_pc = '192.168.1.14'
    db = InfluxDBClient('localhost', 8086, 'root', 'root', client_db_name)  # localhost
    db.create_database(client_db_name)


    clear_data(db)

    tmp_time = int(time.time() * 1000)  # milliseconds

    # Threads

    # Threads init
    thread_format = "%(asctime)s: %(message)s"
    logging.basicConfig(format=thread_format, level=logging.INFO, datefmt="%H:%M:%S")

    machine_workflow_thread = threading.Thread(target=machine_workflow, args=("Machine Workflow Thread",))
    machine_workflow_thread.start()

    client = mqtt.Client()
    mqtt_init(client)
    client.loop_start()

    while not MASTER_REQ_INFO:
        pass

    client.publish(topic_name, "SENDING_DATA")
    # get_db_data()  # used to confirm if data is valid (no repeated timestamps)

    send_data_thread = threading.Thread(target=send_client_data, args=("Send Data Thread",))
    send_data_thread.start()

    # send_data_thread.join()

    # send_client_data("Send Data Thread")

    while not CLIENT_SENT_ALL_DATA:
        pass

    # clear_data(db)
    # db.drop_database(client_db_name)
    # client.loop_stop()
    # client.disconnect()
    GLOBAL_MACHINE_WORKFLOW_CYCLE = False
    input("Press the <ENTER> key to continue...")

    # dbs = db.get_list_database()
    # print('List of DBs: ', dbs)
