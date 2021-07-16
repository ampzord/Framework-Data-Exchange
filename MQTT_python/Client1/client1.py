import paho.mqtt.client as mqtt
from influxdb import InfluxDBClient
import random
import time
import sys


def generate_data():
    measurement_name = "weldingEvents"
    number_of_points = 20000
    data = []
    curr_time = data_end_time
    for i in range(number_of_points):
        welding_value = format(round(random.uniform(0, 30), 4))
        curr_time = curr_time - random.randint(1, 1000000)  # curr_time = int(time.time() * 1000), int(time.time() * 1000000000)

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
    return data


def store_data(data):
    client_write_start_time = time.perf_counter()
    db.write_points(data, database=client_db_name, time_precision='ms', batch_size=10000,
                    protocol="line")  # previous time_precision='n'
    client_write_end_time = time.perf_counter()
    print(client_id + " wrote ALL generated Data to Client DB in: {time}s".format(
        time=client_write_end_time - client_write_start_time))


def has_duplicate(tmp_list):
    set_of_elems = set()
    for elem in tmp_list:
        if elem in set_of_elems:
            return True
        else:
            set_of_elems.add(elem)
    return False


def get_db_data():
    data = db.query("SELECT * FROM weldingEvents;")
    # print('Data raw: ', data.raw)
    points = data.get_points(tags={'client': client_id})
    timestamp_list = []
    for point in points:
        # print("Time: {}, Welding value: {}".format(point['time'], point['welding_value']))
        timestamp_list.append(point['time'])

    if has_duplicate(timestamp_list):
        print('Yes, client\'s database contains duplicate timestamps.\n')
    else:
        print('No duplicate timestamp found in client\'s database.\n')


def send_client_data():
    db.switch_database(client_db_name)
    client_write_start_time = time.perf_counter()
    send_data = db.query('SELECT * INTO master_db..weldingEvents FROM weldingEvents GROUP BY *;')
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
    print(client_id + " sent ALL Data to Master\'s DB in: {time}s".format(
        time=client_write_end_time - client_write_start_time))
    print("Query Successful: ", send_data)
    client.publish(topic_name, "ALL_INFORMATION_SENT")
    '''
     remove_data = db.query(
         query='DROP SERIES FROM weldingEvents WHERE client=$client;',
         params={"client": client_id}
     )
     '''


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
        get_db_data() # used to confirm if data is valid (no repeated timestamps)
        send_client_data()


def clear_data(client_db):
    query = "DROP SERIES FROM weldingEvents WHERE client=$client;"
    bind_params = {'client': client_id}
    remove_data = client_db.query(query, bind_params=bind_params)
    print("Removed data after sending its data to Master's Database: ", remove_data)

    # data = database.query("SELECT * FROM weldingEvents;")
    # print('AFTER DELETION: ', data.raw)


def mqtt_init(tmp_client):
    tmp_client.on_connect = on_connect
    tmp_client.on_message = on_message
    broker_address = "broker.hivemq.com"  # broker_address = "localhost"
    tmp_client.connect(broker_address, port=1883)  # connect to broker


if __name__ == "__main__":
    machine_number = (sys.argv[1])
    client_db_name = 'client' + machine_number + '_db'  # client1_db
    client_id = 'client' + machine_number  # client1
    topic_name = "topic/" + client_id

    db = InfluxDBClient('192.168.1.8', 8086, 'root', 'root', client_db_name)  # localhost
    db.create_database(client_db_name)

    data_end_time = int(time.time() * 1000)  # milliseconds

    client = mqtt.Client()
    mqtt_init(client)

    machine_data = generate_data()
    store_data(machine_data)

    client.loop_start()

    print("Waiting 10 seconds...\n")
    time.sleep(10)

    dbs = db.get_list_database()
    print('List of DBs: ', dbs)

    #################################################

    print("Waiting 10 seconds...\n")
    time.sleep(10)

    clear_data(db)
    db.drop_database(client_db_name)
    client.loop_stop()
    client.disconnect()
