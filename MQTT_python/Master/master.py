import paho.mqtt.client as mqtt
import time
from influxdb import InfluxDBClient

#################################################

#  Scenario where Master requests information from two different clients.

#################################################

db = InfluxDBClient('localhost', 8086, 'root', 'root', 'master_db')
db.create_database('master_db')


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


def get_db_data():
    data = db.query("SELECT * FROM weldingEvents;")
    print("data: ", data)
    points = data.get_points(tags={'measurement': 'weldingEvents'})
    #print("Master DB: \n", data.raw)
    #for point in points:
        #print("Time: {}, Welding value: {}".format(point['time'], point['welding_value']))

    new_data = db.query("SELECT count(welding_value) FROM weldingEvents;")
    all_events = list(new_data.get_points(measurement='weldingEvents'))
    #print('Total Welding value count: ', all_events[0]['count'])

# broker_address = "broker.hivemq.com" #use external broker
broker_address = "localhost"  # local broker

master = mqtt.Client()  # create new instance
master.on_connect = on_connect
master.on_message = on_message
master.connect(broker_address, port=1883)  # connect to broker
master.loop_start()

print("Waiting 4 seconds...\n")
time.sleep(4)

master.publish("topic/master", "GET_INFORMATION")


#################################################

time.sleep(4)  # wait
get_db_data()
#db.drop_database('master_db')
master.loop_stop()  # stop the loop
master.disconnect()
