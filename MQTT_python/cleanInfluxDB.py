import paho.mqtt.client as mqtt
from influxdb import InfluxDBClient
import sys
import logging


def init_logging_config():
    logging.basicConfig(level=logging.INFO)
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)

    # set a format which is simpler for console use
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
    # logging.info("Starting to clean client DB..")
    deleted_data = db.query(query, bind_params=bind_params)
    # logging.info("Deleted data query: ", deleted_data)
    # logging.info("Deleted client DB..")


if __name__ == "__main__":
    # print("ENTEREDDDDDDD")
    # Arg
    NUMBER_CLIENTS = sys.argv[1]
    CLIENT_NAME = "client"
    cli_end = "_db"

    # Influx
    db = InfluxDBClient('localhost', 8086, 'root', 'root', "master_db")
    db.switch_database("master_db")

    # lets do delete and after re-create and do query to see if its empty.

    # data = db.query("SELECT * FROM weldingEvents;")
    # print('Data raw: ', data.raw)

    # delete clients
    for clientNumber in range(int(NUMBER_CLIENTS)):
        db.drop_database(CLIENT_NAME + str(clientNumber+1) + cli_end)

    # delete master
    db.drop_database("master_db")

    # ------------------------





