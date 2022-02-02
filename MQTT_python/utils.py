"""
Utilities file with several aiding functions to help
"""

import logging


def mqtt_protocol_print(message):
    """
    Prints message received from MQTT Protocol along with the topic, QoS and the Retain Flag.

    Prints Received Message decoded from UTF-8 Encoding
    Prints Topic where message was posted
    Prints Quality of Service of message received
        0 - At Most Once
        1 - At Least Once
        2 - Exactly Once
    Prints Message Retain Flag
        True - Keeps last updated message to topic to display to recent subscribers
        False - Doesn't keep track of last message received to topic
    """

    decoded_message = str(message.payload.decode("utf-8"))

    logging.info("Message Received: %s", decoded_message)
    logging.info("Message Topic: %s", message.topic)
    logging.info("Message QoS: %s", message.qos)  # 0, 1 or 2. | at most once, at least once, exactly once
    logging.info("Message Retain Flag: %s", message.retain)  # True or False
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


# -----------------------------------------------------------------------

# OLD CLIENT.PY FUNCTIONS

"""
def send_client_data(thread_name):
    ""
    Send client's DB data to master's DB
    ""
    global MASTER_REQ_INFO

    logging.info("%s: Starting", thread_name)
    db.switch_database(CLIENT_DB_NAME)
    # global DB_LOCK
    # DB_LOCK.acquire()
    client_write_start_time = time.perf_counter()
    # machine_workflow_thread.do_run = False
    send_data = db.query('SELECT * INTO master_db..weldingEvents FROM weldingEvents GROUP BY *;')

    # machine_workflow_thread.do_run = True
    # bind_params={"$client_db_name": client_db_name}
    # )

    # params = {"client_db_name": client_db_name}
    # send_data = db.query('SELECT * INTO master_db..weldingEvents FROM client_db_name WHERE client_db_name=$client_db_name GROUP BY *;', bind_params=params)  # ,

    ''
    query('SELECT * FROM alerts '
          'WHERE time>=$start AND time<$stop '
          'AND client_id=$client AND rule_id=$rule',
          bind_params=params
          )
    ''

    client_write_end_time = time.perf_counter()
    # DB_LOCK.release()
    print(CLIENT_ID + " sent all its Data to Master\'s DB in: {time}s".format(
        time=client_write_end_time - client_write_start_time))
    # print("Query Successful: ", send_data)
    client.publish(CLIENT_TOPIC, "ALL_INFORMATION_SENT")
    global CLIENT_SENT_ALL_DATA
    CLIENT_SENT_ALL_DATA = True
    MASTER_REQ_INFO = False
    ''
     remove_data = db.query(
         query='DROP SERIES FROM weldingEvents WHERE client=$client;',
         params={"client": client_id}
     )
     ''

# ---------------------------

This was used to put the .csv to influxDB -> grafana
Currently not using influxDB to view this information

def save_thread_time():
    measurement_name = "thread_timestamp_events"
    global GEN_THREAD_TIME_DATA, GEN_THREAD_ITERATION_DATA
    for i in range(len(GEN_THREAD_ITERATION_DATA)):
        ELAPSED_TIME_DATA.append(
            "{measurement},client={client} thread_time={thread_time},thread_iteration={thread_iteration}i"
                .format(measurement=measurement_name,
                        client=CLIENT_ID,
                        thread_time=GEN_THREAD_TIME_DATA[i],
                        thread_iteration=GEN_THREAD_ITERATION_DATA[i]))
                        
                        
                        
# ------------------------------
    WAS IN THE START OF MAIN
    
    # TIMESTAMP
    # tmp_time = int(time.time() * 1000)  # milliseconds   
    
# ------------------------------      

def generate_data(thread_name):
    global WELDING_DATA, THREAD_START_TIME, THREAD_END_TIME, GEN_THREAD_TIME_DATA, GEN_THREAD_ITERATION_AUX, GEN_THREAD_REQUEST
    # GLOBAL_THREAD_START_TIME = time.thread_time()
    thread_start_time = time.thread_time_ns()
    measurement_name = "weldingEvents"
    number_of_points = NUMBER_POINTS_PER_CYCLE
    # curr_time = tmp_time
    for i in range(number_of_points):
        welding_value = format(round(random.uniform(0, 30), 4))
        WELDING_DATA.append("{measurement},client={client} welding_value={welding_value} {timestamp}"
                            .format(measurement=measurement_name,
                                    client=CLIENT_ID,
                                    welding_value=welding_value,
                                    timestamp=now()))

    # GLOBAL_THREAD_END_TIME = time.thread_time()
    thread_end_time = time.thread_time_ns()
    GEN_THREAD_TIME_DATA.append(thread_end_time - thread_start_time)  # thread_time
    GEN_THREAD_ITERATION_DATA.append(GEN_THREAD_ITERATION_AUX)  # thread_iterator number
    if MASTER_REQ_INFO:
        GEN_THREAD_REQUEST.append("REQUEST")
    else:
        GEN_THREAD_REQUEST.append("IDLE")
    GEN_THREAD_ITERATION_AUX += 1
    # print(GEN_THREAD_TIME_DATA)
    # print("GEN_THREAD_TIME_DATA: ", GEN_THREAD_TIME_DATA)
    # print("GEN_THREAD_ITERATION_DATA: ", GEN_THREAD_ITERATION_DATA)
    # print("GEN_THREAD_ITERATION_AUX: ", GEN_THREAD_ITERATION_AUX)
    # print("The time spent is {}".format(thread_end_time - thread_start_time))             
"""
# -- MASTER.PY
"""


"""
