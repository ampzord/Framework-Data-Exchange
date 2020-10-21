import paho.mqtt.client as mqtt
from influxdb import InfluxDBClient
import random
import time

# db = InfluxDBClient('localhost', 8086, 'root', 'root', 'client2_db')
db = InfluxDBClient('192.168.1.10', 8086, 'root', 'root', 'client2_db')
db.create_database('client2_db')


def generate_data():
    measurement_name = "weldingEvents"
    client_name = "client2"
    number_of_points = 5000
    data = []
    for i in range(number_of_points):
        welding_value = format(round(random.uniform(0, 30), 4))
        curr_time = int(time.time() * 1000)  # previous int(time.time() * 1000000000)
        uniqueID = 'uniqueID' + str(i + 1)
        data.append("{measurement},client={client},uniqueID={uniqueID} welding_value={welding_value} {timestamp}"
                    .format(measurement=measurement_name,
                            client=client_name,
                            uniqueID=uniqueID,
                            welding_value=welding_value,
                            timestamp=curr_time))
    db.write_points(data, database='client2_db', time_precision='ms', batch_size=5000,
                    protocol="line")  # previous time_precision='n'


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
    points = data.get_points(tags={'client': 'client2'})
    timestamp_list = []
    for point in points:
        # print("Time: {}, Welding value: {}".format(point['time'], point['welding_value']))
        timestamp_list.append(point['time'])

    if checkListDuplicates(timestamp_list):
        print('Yes, list contains duplicates.\n')
    else:
        print('No duplicates found in list.\n')

    sendData = db.query("SELECT * INTO master_db..weldingEvents FROM client2_db..weldingEvents GROUP BY *;")
    print("Query Successful: ", sendData)
    client.publish("topic/client2", "ALL_INFORMATION_SENT")


def on_connect(client, userdata, flags, rc):
    if rc != 0:
        print("Client2 - error connecting, rc: ", rc)
    else:
        print("Client2 - successfully connected.")
        client.subscribe("topic/master")
        client.subscribe("topic/client1")


def on_message(client, userdata, message):
    decoded_message = str(message.payload.decode("utf-8"))
    print("message received: ", decoded_message)
    print("message topic: ", message.topic)
    print("message qos: ", message.qos)  # 0, 1 or 2.
    print("message retain flag: ", message.retain, "\n")

    if decoded_message == "GET_INFORMATION":
        client.publish("topic/client2", "Starting to send all data related to Client 2.")
        get_db_data()


broker_address = "broker.hivemq.com" # use external broker
# broker_address = "localhost"  # local broker

client = mqtt.Client()  # create new
client.on_connect = on_connect
client.on_message = on_message
client.connect(broker_address, port=1883)  # connect to broker

generate_data()

client.loop_start()

print("Waiting 4 seconds...\n")
time.sleep(4)

dbs = db.get_list_database()
print('List of DBs: ', dbs)

#################################################

time.sleep(4)  # wait
db.drop_database('client2_db')
client.loop_stop()  # stop the loop
client.disconnect()
