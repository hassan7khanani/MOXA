import socketio
import time

import getmac
import json
import paho.mqtt.client as mqtt

sio = socketio.Client()
sio.connect('http://localhost:3100')

from redis.commands.json.path import Path
import redis.commands.search.aggregation as aggregations
import redis.commands.search.reducers as reducers
from redis.commands.search.field import TextField, NumericField, TagField
from redis.commands.search.indexDefinition import IndexDefinition, IndexType
from redis.commands.search.query import NumericFilter, Query
import redis





global unixtime
unixtime= int(time.time() * 1000)
print(unixtime)

def write_json(new_data, filename='config.json'):
    with open(filename,'r+') as file:
          # First we load existing data into a dict.
        file_data = json.load(file)
        # Join new_data with file_data inside emp_details
        file_data["channels"].append(new_data)
        # Sets file's current position at offset.
        file.seek(0)
        # convert back to json.
        json.dump(file_data, file, indent = 4)

d = dict()
d2 = dict()
unixtime= int(time.time() * 1000)
print(unixtime)
date_column = 0

def getmoxamac():
    mac = getmac.get_mac_address()
    print(mac)
    macup = mac.upper()
    return macup

mac1=getmoxamac()
print("mac1 is",mac1)
mac2 = mac1.lower().replace(":","")


with open('config.json','r+') as file:

        settings = json.load(file)
#        print(settings)
        parity = settings['config']['parity']
 #       parity = "serial.PARITY_" + parity
        baud_rate = settings['config']['baudrate']
        stop_bits = settings['config']['stopbits']
        channels = settings['channels']
        byte_size = settings['config']['bytesize']
##        print(channels)
        print(len(channels))
        live_data = settings['live_data']



try:
    sio = socketio.Client()
    sio.connect('http://localhost:3100')
    print("socket connected")
except Exception as s:
    print("cant connect to socket : " , s)


try:
##    rts = Client(host = '192.168.0.100', port=6379)
    r = redis.Redis(host='localhost', port=6379)
    print("redis loaded")
except Exception as r:
    print("error loading redis : ", r)

try:
##    schema = (TextField("$.user.name", as_name="name"),TagField("$.user.city", as_name="city"), NumericField("$.user.age", as_name="age"))
    schema = (NumericField("$.timestamp", as_name="timestamp"))
    r.ft().create_index(schema, definition=IndexDefinition(prefix=[""], index_type=IndexType.JSON))
except Exception as schema_error:
    print("cant create schema : ", schema_error)


@sio.event
def connect():
    print('connection established')

@sio.event
def my_response(data):
    print('message received with', data)

@sio.event
def disconnect():
    print('disconnected from server')
    
# @sio.event
# def _DATA(data):
#     print('Received message:', data)





@sio.on('__DATA')
def handle_message(data):
    print('Received response message:', data)
    
@sio.on('__DATA1')
def handle_message(data):
    # print('Received message on __DATA1 ', data)
    # d=data
    d=json.loads(data)
    print('Received response message on __DATA1 :', d)
    print("len of data is : " ,len(data))

    try:
        r.json().set(unixtime, Path.rootPath(), d)
        print("data inserted in redis")

    except Exception as e:
        print("unable to insert data into redis : " , e)

    print(d)
    d = {}
