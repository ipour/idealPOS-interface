#!/usr/bin/python3
# -*- coding: utf-8 -*-
#
# dt-master master.py
# Created on 2017-06-19 05:38
#
# Author: Trevor van der Linden
#
# Copyright (c) 2017 Trevor van der Linden, IoT Systems Design
#
# License: MIT license (https://opensource.org/licenses/MIT) once agreed fee is paid
#
# Description: Monitor the broker, react to relevant messages, update database, respond to requests for info
#
# Installing MySQLdb:
#     yum install mysql-devel gcc gcc-devel python-devel
#     easy_install mysql-python
#     yum search python3 | grep devel
#     Find the version of devel to install ie python34-devel.x86_64
#     yum install -y python34-devel.x86_64
#     pip3 install mysqlclient
#
#
import os
import time
import base64
import datetime


def appendOrWrite(filename):
    if os.path.isfile(filename):
        return "a"
    else:
        return "w"


def writeLog(logLevel, msg):
    if (logLevel > 0):
        msg_str = time.strftime("%Y-%m-%d %H:%M:%S") + "|" + str(msg)
        print(msg_str)  #uncomment for local dev work, otherwise comment or it will flood /var/log/messages
        fileObject = open("log.txt", appendOrWrite("log.txt"))
        fileObject.write(msg_str + "\n")
        fileObject.close()

def convertImageToBase64(file):
    with open(file, "rb") as image_file:
        encoded = base64.b64encode(image_file.read())
        return encoded

def secs():
    return int(round(time.time()))


