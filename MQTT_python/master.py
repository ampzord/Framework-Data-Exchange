import paho.mqtt.client as mqtt
import time
from influxdb import InfluxDBClient
import sys


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
        print("Master - Error connecting to Broker. Return Code: ", rc)
    else:
        print("Master - Successfully connected to Broker.\n")
        print("Master - Subscribing to every Client topic.\n")
        client.subscribe("topic/simulation/clients/#")


def on_message(client, userdata, message):
    """
    Receives the messages that are published through the broker
    """

    decoded_message = str(message.payload.decode("utf-8"))
    DEBUG = True
    if DEBUG:
        print("message received: ", decoded_message)
        print("message topic: ", message.topic)
        print("message qos: ", message.qos)  # 0, 1 or 2.
        print("message retain flag: ", message.retain, "\n")

    if decoded_message == "ALL_INFORMATION_SENT":
        global RECEIVED_DATA_FROM_CLIENT_COUNT
        RECEIVED_DATA_FROM_CLIENT_COUNT += 1

        if RECEIVED_DATA_FROM_CLIENT_COUNT == client_size:
            global ALL_DATA_RECEIVED
            ALL_DATA_RECEIVED = True
            req_end_time = time.perf_counter()
            print("Simulation took: {time} seconds".format(time=req_end_time - req_start_time))

        try:
            client_name = message.topic.split('/')
            client.publish("topic/simulation/master", "Master received all data from " + client_name[3])
        except ValueError:
            print("Error splitting topic string in " + client_name[3])


def mqtt_init(tmp_master):
    """
    Connects to Broker and initializes protocols
    """

    tmp_master.on_connect = on_connect
    tmp_master.on_message = on_message
    broker_address = "broker.hivemq.com"  # broker_address = "localhost"
    tmp_master.connect(broker_address, port=1883)  # connect to broker


def clear_data(master_db):
    """
    Clears every value from MasterDB.
    """
    db.switch_database('master_db')
    query = "DROP SERIES FROM weldingEvents;"
    remove_data = master_db.query(query)
    # print("Data before deleting: ", remove_data)
    data = db.query("SELECT * FROM weldingEvents;")
    # print('Data After deletion: ', data.raw)


def clear_data_aux(aux_master_db):
    """
    Clears every value from Master's auxiliary DB.
    """
    db.switch_database('aux_master_db')
    query = "DROP SERIES FROM thread_timestamp_events;"
    remove_data = aux_master_db.query(query)


def mqtt_terminate(tmp_master):
    """
    Stops MQTT loop and disconnects client
    """

    tmp_master.loop_stop()
    tmp_master.disconnect()


def get_db_data():
    """
    Queries master's DB to print the total number of
    welding_value that reached master's DB from its clients.
    """
    db.switch_database('master_db')
    data = db.query("SELECT * FROM weldingEvents;")
    # print("data: ", data)
    points = data.get_points(tags={'measurement': 'weldingEvents'})
    # print("Master DB: \n", data.raw)
    # for point in points:
    # print("Time: {}, Welding value: {}".format(point['time'], point['welding_value']))

    try:
        new_data = db.query("SELECT count(welding_value) FROM weldingEvents;")
        all_events = list(new_data.get_points(measurement='weldingEvents'))
        print('Total Welding value count: ', all_events[0]['count'])
    except "CountError":
        print("Error counting number of total welding values reached Master DB.")


def check_thread_data():

    db.switch_database('aux_master_db')
    data = db.query("SELECT * FROM thread_timestamp_events;")
    # print("AUX MASTER data: ", data)
    points = data.get_points(tags={'measurement': 'thread_timestamp_events'})
    print("AUX Master DB: \n", data.raw)
    # for point in points:
    # print("Time: {}, Welding value: {}".format(point['time'], point['welding_value']))


if __name__ == "__main__":

    # Arguments
    client_size = int(sys.argv[1])

    # Global Variables
    ALL_DATA_RECEIVED = False
    RECEIVED_DATA_FROM_CLIENT_COUNT = 0

    # InfluxDB
    db = InfluxDBClient('localhost', 8086, 'root', 'root', 'master_db')
    db.create_database('master_db')
    db.create_database('aux_master_db')

    clear_data(db)
    clear_data_aux(db)
    # Broker
    master = mqtt.Client("master")
    mqtt_init(master)

    # dbs = db.get_list_database()
    # print('List of DBs: ', dbs)

    req_start_time = time.perf_counter()
    master.publish("topic/simulation/master", "GET_INFORMATION")
    master.loop_start()

    while not ALL_DATA_RECEIVED:
        pass

    get_db_data()
    check_thread_data()

    # Terminate connection
    # db.drop_database('master_db')
    # mqtt_terminate(master)
    input("Press the <ENTER> key to continue...")

