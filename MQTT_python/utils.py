"""
Utilities file with several aiding functions to help
"""


def mqtt_protocol_print(message):
    decoded_message = str(message.payload.decode("utf-8"))

    print("Message Received: ", decoded_message)
    print("Message Topic: ", message.topic)
    print("Message QoS: ", message.qos)  # 0, 1 or 2. | at most once, at least once, exactly once
    print("Message Retain Flag: ", message.retain, "\n")
