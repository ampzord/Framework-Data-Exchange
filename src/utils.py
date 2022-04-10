"""
Utilities file with several aiding functions for the rest of the Project
"""

# ------------------------------------------------------------------------------

# Standard Library
import logging

__author__ = "António Pereira"
__email__ = "antonio_m_sp@hotmail.com"
__status__ = "Development"

# ------------------------------------------------------------------------------


def mqtt_protocol_print(message):
    """
    Prints message received from MQTT Protocol along with the topic, QoS and the Retain Flag.\n
    Prints Received Message decoded from UTF-8 Encoding\n
    Prints Topic where message was posted\n
    Prints Quality of Service of message received\n
        0 - At Most Once\n
        1 - At Least Once\n
        2 - Exactly Once\n
    Prints Message Retain Flag\n
        True - Keeps last updated message to topic to display to recent subscribers\n
        False - Doesn't keep track of last message received to topic\n
    """

    decoded_message = str(message.payload.decode("utf-8"))

    logging.info("Message Received: %s", decoded_message)
    logging.info("Message Topic: %s", message.topic)
    logging.info("Message QoS: %s", message.qos)
    logging.info("Message Retain Flag: %s", message.retain)
    logging.info("\n")


def has_duplicate_values(tmp_list):
    """
    Checks to see if a given list has any duplicate value.

    :returns: False if tmp_list has no duplicate values.
    :returns: True if tmp_list has duplicate values.
    """

    set_of_elements = set()
    for elem in tmp_list:
        if elem in set_of_elements:
            return True
        else:
            set_of_elements.add(elem)
    return False