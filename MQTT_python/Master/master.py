import paho.mqtt.client as mqtt
import time
from influxdb import InfluxDBClient


def on_connect(client, userdata, flags, rc):
    if rc != 0:
        print("Master - error connecting, rc: ", rc)
    else:
        print("Master - successfully connected.\n")
        client.subscribe("topic/client1")
        client.subscribe("topic/client2")


def on_message(client, userdata, message):
    decoded_message = str(message.payload.decode("utf-8"))
    print("message received: ", decoded_message)
    print("message topic: ", message.topic)
    print("message qos: ", message.qos)  # 0, 1 or 2.
    print("message retain flag: ", message.retain, "\n")

    if decoded_message == "ALL_INFORMATION_SENT" and message.topic == "topic/client1":
        client.publish("topic/master", "Master received all data from Client 1.")
    elif decoded_message == "ALL_INFORMATION_SENT" and message.topic == "topic/client2":
        client.publish("topic/master", "Master received all data from Client 2.")


def mqtt_init(tmp_master):
    tmp_master.on_connect = on_connect
    tmp_master.on_message = on_message
    broker_address = "broker.hivemq.com"  # broker_address = "localhost"

    tmp_master.connect(broker_address, port=1883)  # connect to broker


def get_db_data():
    data = db.query("SELECT * FROM weldingEvents;")
    print("data: ", data)
    points = data.get_points(tags={'measurement': 'weldingEvents'})
    print("Master DB: \n", data.raw)
    for point in points:
        print("Time: {}, Welding value: {}".format(point['time'], point['welding_value']))

    try:
        new_data = db.query("SELECT count(welding_value) FROM weldingEvents;")
        all_events = list(new_data.get_points(measurement='weldingEvents'))
        print('Total Welding value count: ', all_events[0]['count'])
    except "CountError":
        print("Error counting number of total welding values reached Master DB.")


db = InfluxDBClient('localhost', 8086, 'root', 'root', 'master_db')
db.create_database('master_db')

master = mqtt.Client()  # create new instance
mqtt_init(master)
master.loop_start()

print("Waiting 10 seconds for client be able to produce data...\n")
time.sleep(10)

master.publish("topic/master", "GET_INFORMATION")

#################################################

print("Waiting 10 seconds...\n")
time.sleep(10)

get_db_data()
print('List of DBs: ', db.get_list_database())

# db.drop_database('master_db')
master.loop_stop()  # stop the loop
master.disconnect()
