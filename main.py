#!/usr/bin/python3
# -*- coding: utf-8 -*-
#
# master.py
# Created on 2018-09-01 11:38
#
# Author: Trevor van der Linden
#
# Copyright (c) 2018 Trevor van der Linden, IoT Systems Design
#
# Description: Queries the Jensen POS for new sales transactions and sends to the reporting server via encrypted MQTT
#
# Single file, multi-thread application.
#
import json
import time
import paho.mqtt.client as mqtt
import random
import psutil
import socket
import pymssql
import datetime
import os
import sqlite3
import subprocess


def get_mac():
    mac = None
    result = subprocess.run(['ifconfig', 'eth0'], stdout=subprocess.PIPE)
    if result.returncode == 0:
        mac = result.stdout.decode().split("ether ")[1][0:17].replace(":", "")
    return mac


def configDictionary():
    mac = get_mac()
    dsn = 'sqlserverdatasource'
    user = config_item('sql_user')
    password = config_item('sql_pass')
    database = config_item('database')
    con_string = 'DSN=%s;UID=%s;PWD=%s;DATABASE=%s;' % (dsn, user, password, database)
    return {
        'CUSTOMERCODE': "sjpb",
        'TIMEZONE': "Australia/Sydney",
        'DB_CONNECTION_STR': con_string,
        'MQTT_SERVER': 's2.syd.ipour.com.au',
        'MQTT_PORT': 8883,
        'MQTT_PROTOCOL': 'MQTTS',
        'MQTT_USER': 'ipour-pos-sjpb',
        'MQTT_PASS': 'V7ZtvUqq5Vo4',
        'MQTT_SERIAL': str(hex(mac))[2:],
        'sqlite_file': 'configDB.sqlite'
    }


def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


def config_item(name):
    sqlite_file = "configDB.sqlite"
    sqlite_conn = sqlite3.connect(sqlite_file)
    sqlite_conn.row_factory = dict_factory
    sqlite_cursor = sqlite_conn.cursor()
    sqlite_cursor.execute("SELECT * FROM config WHERE name='{0}'".format(name))
    try:
        result = sqlite_cursor.fetchone()['value']
    except:
        result = None
    sqlite_cursor.close()
    sqlite_conn.close()
    return result


def update_config(key, value):
    old_item = config_item(key)
    sqlite_file = "configDB.sqlite"
    sqlite_conn = sqlite3.connect(sqlite_file)
    sqlite_cursor = sqlite_conn.cursor()
    if old_item is None:
        sqlite_cursor.execute("INSERT INTO 'config' ('name', 'value') VALUES ('{0}', '{1}')".format(key, value))
        last_row = sqlite_cursor.lastrowid
    elif old_item != value:
        sqlite_cursor.execute("UPDATE 'config' SET value='{1}' WHERE name='{0}'".format(key, value))
        last_row = sqlite_cursor.rowcount
    else:
        last_row = None
    sqlite_conn.commit()
    sqlite_conn.close()
    if old_item == value:
        last_row = 0
    if last_row is not None:
        if key != 'last_trans_id':
            writeLog(5, "Update config item {0}. Old value {1}, new value {2}".format(key, old_item, value))
        else:
            writeLog(3, "Update config item {0}. Old value {1}, new value {2}".format(key, old_item, value))
    else:
        writeLog(5, "ERROR updating config item {0}. Old value {1}, new value {2}".format(key, old_item, value))
    return last_row


def local_ip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect((config_item('mqtt_server'), 80))
    ip = s.getsockname()[0]
    s.close()
    return ip


def systemHealth(serial, custCode):
    svmem = psutil.virtual_memory()
    disk = psutil.disk_usage("/")
    data = {
        "mt": "hlth",
        "mac": serial,
        "cust": custCode,
        "ts": int(time.time()),
        "mTtl": float(str(svmem.total / 1024 / 1024)[:5]),
        "mUsd": float(str(svmem.used / 1024 / 1024)[:5]),
        "mAv": float(str(svmem.available / 1024 / 1024)[:5]),
        "dTtl": float(str(disk.total / 1024 / 1024 / 1024)[:4]),
        "dUsd": float(str(disk.used / 1024 / 1024 / 1024)[:4]),
        "dFr": float(str(disk.free / 1024 / 1024 / 1024)[:4]),
        "mUI": "MB",
        "dUI": "GB",
        "lip": str(local_ip())
    }
    return data


def add_errors(dictionary, error):
    dictionary[len(dictionary.keys()) + 1] = error
    return dictionary


def appendOrWrite(filename):
    if os.path.isfile(filename):
        return "a"
    else:
        return "w"


def writeLog(logLevel, msg):
    if logLevel >= int(config_item('debug_level')):
        msg_str = time.strftime("%Y-%m-%d %H:%M:%S") + "|" + str(msg)
        if logLevel >= 1:
            print(msg_str)
        fileObject = open("log.txt", appendOrWrite("log.txt"))
        fileObject.write(msg_str + "\n")
        fileObject.close()


def messageToDictionary(msgPayload):
    try:
        data = json.loads(msgPayload)
    except ValueError:
        writeLog(3, "\tERROR: A problem with the JSON formatting of the message")
        return None
    except AttributeError:
        writeLog(3, "\tERROR: String received does not convert to a dictionary object")
        return None
    return data


def on_message_to_self(client, userdata, msg):
    writeLog(2, "==on_message_to_self: " + msg.topic + " " + str(msg.payload))


# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
    global mqtt_connected
    writeLog(5, "== ** == Connected with result code " + str(rc))
    if rc == 0:
        mqtt_connected = True;
        mqttc.subscribe(inTopic, 1)
        writeLog(4, "\tmqttc.publish(outTopic, registerString, 1, True)")
        mqttc.publish(outTopic, registerString, 1, True)
    else:
        writeLog(5, "MQTT not connected. Connection attempt result code is {0}".format(rc))


def on_disconnect(client, userdata, rc):
    global mqtt_connected
    writeLog(5, "== ** == DISCONNECT with result code {0}".format(str(rc)))
    mqtt_connected = False


def on_message(client, userdata, msg):
    global loopRun
    global get_data_from_db
    writeLog(4, "==on_message: " + msg.topic + " " + str(msg.payload))
    msgPayload = str(msg.payload.decode("utf-8"))
    data = messageToDictionary(msgPayload)
    if data is None:
        return None
    errors = {}
    sender = data.get('sender', None)
    message_type = data.get('message_type', None)
    if sender is None:
        errors = add_errors(errors, "Sender not found in message")
    if message_type is None:
        errors = add_errors(errors, "message type not found in message")
    if len(errors.keys()) > 0:
        writeLog(3, "\tERROR: " + str(errors))
        return dict(
            success=False,
            err=dict(sender=str(serial), message_type="err", error_code="on_message_01", error_msg=errors,
                     data=str(msg.payload))
        )

    if sender != "1a00000000a1":
        errors = add_errors(errors, "Sender not authorised on this topic")
        writeLog(3, "\tERROR: " + str(errors))
        return dict(
            success=False,
            err=dict(sender=str(serial), message_type="err", error_code="ahm04", error_msg=errors, data=str(msg.payload)
                     )
        )

    if message_type == "get_data_from_db":
        val = data.get('val', None)
        if val is not None and (val is True or val is False):
            get_data_from_db = val

    if message_type == "config":
        writeLog(4, "\tConfig: ")

        customer_code = data.get('customer_code', None)
        timezone = data.get('timezone', None)
        db_connection_str = data.get('db_connection_str', None)
        mqtt_server = data.get('mqtt_server', None)
        mqtt_port = data.get('mqtt_port', None)
        mqtt_protocol = data.get('mqtt_protocol', None)
        mqtt_user = data.get('mqtt_user', None)
        mqtt_pass = data.get('mqtt_pass', None)
        errors = dict()
        if customer_code is not None:
            writeLog(4, "\t\tcustomer_code")
            if update_config('customer_code', customer_code) is None:
                errors = add_errors(errors, "Error updating customer code")
        if timezone is not None:
            writeLog(4, "\t\ttimezone")
            if update_config('timezone', timezone) is None:
                errors = add_errors(errors, "Error updating timezone")
        if db_connection_str is not None:
            writeLog(4, "\t\tdb_connection")
            if update_config('db_connection_str', db_connection_str) is None:
                errors = add_errors(errors, "Error updating db_connection_str")
        if mqtt_server is not None:
            writeLog(4, "\t\tmqtt_server")
            if update_config('mqtt_server', mqtt_server) is None:
                errors = add_errors(errors, "Error updating mqtt_server")
        if mqtt_port is not None:
            writeLog(4, "\t\tmqtt_port")
            if update_config('mqtt_port', mqtt_port) is None:
                errors = add_errors(errors, "Error updating mqtt_port")
        if mqtt_protocol is not None:
            writeLog(4, "\t\tmqtt_protocol")
            if update_config('mqtt_protocol', mqtt_protocol) is None:
                errors = add_errors(errors, "Error updating mqtt_protocol")
        if mqtt_user is not None:
            writeLog(4, "\t\tmqtt_user")
            if update_config('mqtt_user', mqtt_user) is None:
                errors = add_errors(errors, "Error updating mqtt_user")
        if mqtt_pass is not None:
            writeLog(4, "\t\tmqtt_pass")
            if update_config('mqtt_pass', mqtt_pass) is None:
                errors = add_errors(errors, "Error updating mqtt_pass")
        if len(errors.keys()) > 0:
            message = dict(
                sender=str(serial),
                message_type="err",
                error_code="on_message",
                error_msg=errors,
                data=json.loads(msg.payload.decode("utf-8"))
            )
            mqttc.publish(msg.topic.replace('/out', '/in'), json.dumps(message, separators=(',', ':')), 1, False)
        else:
            message = dict(
                sender=str(serial),
                message_type="ok",
                data=json.loads(msg.payload.decode("utf-8"))
            )
            mqttc.publish(msg.topic.replace('/out', '/in'), json.dumps(message, separators=(',', ':')), 1, False)

    if message_type == "restart":
        loopRun = False


def process_key_value_pair(key, value, count):
    dateTimeObj = datetime.datetime.strptime(value[1], "%d %b %Y %H:%M:%S")
    timestamp = int(dateTimeObj.timestamp())

    writeLog(2, "data: {0}".format(value))
    writeLog(2, "transID from db: {0}, last_trans_id: {1}".format(value[8], last_trans_id))

    # 2019-09-03 08:15:36|data: ('77950-23', '23 Aug 2019 06:18:39', 23, 'Willow Cafe 1', 'Gymea Willow Cafe', '          401005', 'LATTE LGE', 1.0, 793845)
    # 2019-09-03 08:15:36|transID from db: 793845, last_trans_id: 792000
    if value[5].strip() not in ['1', '2']:
        try:
            jsonMsg = json.dumps({
                "message_type": "trans",
                "sender": str(serial),
                "id": value[0],
                "timestamp": timestamp,
                "pos_id": value[2],
                "pos_desc": value[3].strip(),
                "location": value[4].strip(),
                "plu": value[5].strip(),
                "desc": value[6].strip(),
                "qty": int(value[7])
            }, separators=(',', ':'))
        except Exception as e:
            if count['before_next_dbExec'] == 0:
                writeLog(5, "\tError converting db data to JSON message. Error msg: {0}".format(e))
                errorMsg = json.dumps({
                    "custCode": config_item('customer_code'),
                    "app": "pos-interface",
                    "device": str(serial),
                    "error": "Error converting db data to JSON message. Error msg: {0}".format(e),
                    "timestamp": int(time.time())
                }, separators=(',', ':'))
                mqttc.publish("ipour/errors", errorMsg, 1, False)
                count['before_next_dbExec'] = 6
            else:
                count['before_next_dbcon'] = count['before_next_dbcon'] - 1
        else:
            writeLog(4, str(jsonMsg))
            mqttc.publish(outTopic, jsonMsg, 1, False)
            update_config('last_trans_id', value[8])
            writeLog(3, "last_trans_id set to: {0}".format(last_trans_id))
    return count


def connect_to_sql():
    writeLog(4, "Host:{0},Instance:{1},Port:{2},User:{3},Pass:{4},DB:{5}".format(
        config_item('sql_host'),
        config_item('sql_instance'),
        config_item('sql_port'),
        config_item('sql_user'),
        config_item('sql_pass'),
        config_item('database')
    ))
    con = pymssql.connect(
        host=config_item('sql_host'),
        server=config_item('sql_instance'),
        port=config_item('sql_port'),
        user=config_item('sql_user'),
        password=config_item('sql_pass'),
        database=config_item('database'),
        appname="dbo.iPourSales"
    )
    return con


def handle_dbcon_exception(e):
    if count['before_next_dbcon'] is 0:
        writeLog(5, "\tException connecting to DB with error: {0}".format(e))
        errorMsg = json.dumps({
            "custCode": config_item('customer_code'),
            "app": "pos-interface",
            "device": str(serial),
            "error": "Exception connecting to DB with error: {0}".format(e),
            "timestamp": int(time.time())
        }, separators=(',', ':'))
        mqttc.publish("ipour/errors", errorMsg, 1, False)
        count['before_next_dbcon'] = 6
    else:
        count['before_next_dbcon'] -= 1
    return count


def handle_execute_exception(e):
    if count['before_next_dbExec'] == 0:
        writeLog(5, "\tException executing sql {0} with error: {1}".format(sql, e))
        errorMsg = json.dumps({
            "custCode": config_item('customer_code'),
            "app": "pos-interface",
            "device": str(serial),
            "error": "Exception executing sql: {0}, with error: {1}".format(sql, e),
            "timestamp": int(time.time())
        }, separators=(',', ':'))
        mqttc.publish("ipour/errors", errorMsg, 1, False)
        count['before_next_dbExec'] = 6
    else:
        count['before_next_dbcon'] -= 1
    return count


while True:
    writeLog(5, '************  STARTING main.py  *******************')
    mqtt_connected = False;
    get_data_from_db = True;
    mac = get_mac()
    custCode = config_item('customer_code')
    serial = mac
    writeLog(5, "Serial: {0}".format(serial))

    outTopic = "ipour/pos/" + custCode + "/" + serial + "/out"
    inTopic = "ipour/pos/" + custCode + "/" + serial + "/in"
    writeLog(2, "\tSetting topics: outTopic: {0} inTopic: {1}".format(str(outTopic), str(inTopic)))
    registerString = json.dumps(dict(messageType='reg', sender=serial), separators=(',', ':'))
    unRegisterString = json.dumps(dict(messageType='unreg', sender=serial), separators=(',', ':'))
    writeLog(2, "\tSetting reg/unreg msgs: reg:{0} unreg:{1}".format(str(registerString), str(unRegisterString)))
    loopRun = True  # while in main loop, set to false to drop from loop

    # Set up the MQTT client
    mqttc = mqtt.Client('POS-' + custCode + str(serial), clean_session=False)
    mqttc.on_connect = on_connect
    mqttc.on_message = on_message
    mqttc.on_disconnect = on_disconnect
    mqttc.username_pw_set(config_item('mqtt_user'), config_item('mqtt_pass'))
    mqttc.will_set(outTopic, unRegisterString)
    mqttc.tls_set()

    # Connect to MQTT broker

    mqttc.connect_async(config_item('mqtt_server'), int(config_item('mqtt_port')), 180)

    random.seed(time.time() - 122395)

    writeLog(4, "\tmqttc.loop_start")
    mqttc.loop_start()

    writeLog(4, "\twhile loopRun:")
    last_trans_id = config_item('last_trans_id')
    count = {
        "before_next_dbcon": 0,
        "before_next_dbExec": 0,
        "mqtt_not_connected": 0,
        "before_next_health": 0
    }
    random_integer = random.randint(0, 600)
    while loopRun:
        if mqtt_connected:
            time_now = int(time.time() - random_integer)
            if time_now % 600 is 0:
                jsonMsg = json.dumps(systemHealth(serial, custCode), separators=(',', ':'))
                mqttc.publish("ipour/health", jsonMsg, 1, False)

            if get_data_from_db and time_now % 10 is 0:
                try:
                    con = connect_to_sql()
                except Exception as e:
                    count = handle_dbcon_exception(e)
                else:
                    count['before_next_dbcon'] = 0
                    cur = con.cursor()
                    i = 0
                    last_trans_id = config_item('last_trans_id')
                    sql = "EXEC iPourSalesV2 {0}".format(last_trans_id)
                    writeLog(3, "SQL Command: {0}".format(sql))
                    try:
                        cur.execute(sql)
                        rows = cur.fetchall()
                    except Exception as e:
                        count = handle_execute_exception(e)
                    else:
                        if rows is not None:
                            for k, v in enumerate(rows):
                                count = process_key_value_pair(k, v, count)
                        else:
                            writeLog(5, "No data")
                    con.close()
                    writeLog(3, "Close db connection")
        else:
            count['mqtt_not_connected'] += 1
            if count['mqtt_not_connected'] > 6:
                writeLog(5, "Still no MQTT connection")
                count['mqtt_not_connected'] = 0
        try:
            time.sleep(1)
        except KeyboardInterrupt:
            writeLog(5, "Keyboard interrupt")
            break

    writeLog(5, "\tmqttc.loop_stop()")
    mqttc.loop_stop()
    mqttc.publish(outTopic, unRegisterString, 1, True)
    mqttc.disconnect()
    writeLog(5, '************  STOPPING main.py  *******************')

