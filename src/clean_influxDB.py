#!/usr/bin/env python

"""
Clears MasterDB and all ClientsDB

Clears the database inside master_db and inside all clients database from <client1> to <NUMBER_CLIENTS>.
"""

# ------------------------------------------------------------------------------

# Standard Library
import sys
import logging

# 3rd Party Package
import paho.mqtt.client as mqtt
from influxdb import InfluxDBClient

__author__ = "António Pereira"
__email__ = "antonio_m_sp@hotmail.com"
__status__ = "Development"

# ------------------------------------------------------------------------------


def init_logging_config():
    """
    Initiates the logging configuration
    """

    logging.basicConfig(level=logging.INFO)
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)

    formatter = logging.Formatter('%(asctime)s : %(levelname)s : %(message)s')
    console.setFormatter(formatter)
    logging.getLogger("").addHandler(console)


def clear_clientDB_data(CLIENT_DB_NAME, CLIENT_ID):
    """
    Clears every value from client's DB.
    """

    db.switch_database(CLIENT_DB_NAME)
    query = "DROP SERIES FROM weldingEvents WHERE client=$client;"
    bind_params = {'client': CLIENT_ID}
    deleted_data = db.query(query, bind_params=bind_params)


if __name__ == "__main__":
    NUMBER_CLIENTS = sys.argv[1]
    CLIENT_NAME = "client"
    cli_end = "_db"

    db = InfluxDBClient('localhost', 8086, 'root', 'root', "master_db")
    db.switch_database("master_db")

    for clientNumber in range(int(NUMBER_CLIENTS)):
        db.drop_database(CLIENT_NAME + str(clientNumber+1) + cli_end)

    db.drop_database("master_db")





