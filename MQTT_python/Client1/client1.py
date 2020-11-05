import paho.mqtt.client as mqtt
from influxdb import InfluxDBClient
import random
import time
import sys


# TODO find every machine connected to broker and then subscribe it


def generate_data():
    measurement_name = "weldingEvents"
    number_of_points = 250000
    data = []
    curr_time = data_end_time
    for i in range(number_of_points):
        welding_value = format(round(random.uniform(0, 30), 4))
        # curr_time = int(time.time() * 1000)
        curr_time = curr_time - random.randint(1, 100)

        # curr_time = int(time.time() * 1000000000)
        # uniqueID = 'uniqueID' + str(i + 1)
        # data.append("{measurement},client={client},uniqueID={uniqueID} welding_value={welding_value} {timestamp}"
        #            .format(measurement=measurement_name,
        #                    client=client_name,
        #                    uniqueID=uniqueID,
        #                    welding_value=welding_value,
        #                    timestamp=curr_time))
        data.append("{measurement},client={client} welding_value={welding_value} {timestamp}"
                    .format(measurement=measurement_name,
                            client=client_id,
                            welding_value=welding_value,
                            timestamp=curr_time))

    client_write_start_time = time.perf_counter()
    db.write_points(data, database=client_db_name, time_precision='ms', batch_size=10000,
                    protocol="line")  # previous time_precision='n'
    client_write_end_time = time.perf_counter()
    print("Client " + client_id + " write ALL data generated to client's DB: {time}s".format(
        time=client_write_end_time - client_write_start_time))


def checkListDuplicates(listOfElems):
    # Check if given list contains any duplicates
    setOfElems = set()
    for elem in listOfElems:
        if elem in setOfElems:
            return True
        else:
            setOfElems.add(elem)
    return False


def get_db_data():
    data = db.query("SELECT * FROM weldingEvents;")
    # print('Data raw: ', data.raw)
    points = data.get_points(tags={'client': client_id})
    timestamp_list = []
    for point in points:
        # print("Time: {}, Welding value: {}".format(point['time'], point['welding_value']))
        timestamp_list.append(point['time'])

    if checkListDuplicates(timestamp_list):
        print('Yes, list contains duplicates.\n')
    else:
        print('No duplicates found in list.\n')

    client_write_start_time = time.perf_counter()


    params = {"client_db_name": client_db_name}
    send_data = db.query('SELECT * INTO master_db..weldingEvents FROM client_db_name WHERE client_db_name=$client_db_name GROUP BY *;', bind_params=params)  # ,

    '''
    query('SELECT * FROM alerts '
          'WHERE time>=$start AND time<$stop '
          'AND client_id=$client AND rule_id=$rule',
          bind_params=params
          )
    '''

    client_write_end_time = time.perf_counter()
    print("Client " + client_id + " send ALL data to Master time: {time}s".format(
        time=client_write_end_time - client_write_start_time))
    print("Query Successful: ", send_data)
    client.publish(topic_name, "ALL_INFORMATION_SENT")


def on_connect(client, userdata, flags, rc):
    if rc != 0:
        print(client_id + " - error connecting, rc: ", rc)
    else:
        print(client_id + " - successfully connected.")
        client.subscribe("topic/master")
        # client.subscribe("topic/client2")


def on_message(client, userdata, message):
    decoded_message = str(message.payload.decode("utf-8"))
    print("message received: ", decoded_message)
    print("message topic: ", message.topic)
    print("message qos: ", message.qos)  # 0, 1 or 2.
    print("message retain flag: ", message.retain, "\n")

    if decoded_message == "GET_INFORMATION":
        client.publish(topic_name, "Starting to send all data related to " + client_id)
        get_db_data()


if __name__ == "__main__":
    machine_number = (sys.argv[1])
    client_db_name = 'client' + machine_number + '_db'  # client1_db
    client_id = 'client' + machine_number  # client1
    topic_name = "topic/" + client_id

    db = InfluxDBClient('192.168.1.10', 8086, 'root', 'root', client_db_name)  # localhost
    db.create_database(client_db_name)

    data_end_time = int(time.time() * 1000)  # milliseconds
    broker_address = "broker.hivemq.com"  # use external broker
    # broker_address = "localhost"  # local broker

    client = mqtt.Client()  # create new
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(broker_address, port=1883)  # connect to broker

    generate_data()

    client.loop_start()

    print("Waiting 4 seconds...\n")
    time.sleep(10)

    dbs = db.get_list_database()
    print('List of DBs: ', dbs)

    #################################################

    time.sleep(10)  # wait
    db.drop_database(client_db_name)
    client.loop_stop()  # stop the loop
    client.disconnect()
