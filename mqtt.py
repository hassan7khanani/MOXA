import socketio
import time
import getmac
import json
import paho.mqtt.client as mqtt
from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTClient

sio = socketio.Client()
sio.connect('http://localhost:3100')

BASE_URL = '143.198.137.203'
# 143.198.137.203
MQTT_PORT = 1883
username = "processPumps" 
password = "4ee3xBjJ6mfZ1vskFkAVjr0QSsE9Dcvn"




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
    client = mqtt.Client()
    client.username_pw_set(username, password)
    client.connect(BASE_URL,1883)
    print("mqtt connected")
except Exception as m:
    print("cant connect to mqtt : ", m)



@sio.event
def connect():
    print('connection established')

@sio.event
def my_response(data):
    print('message received with', data)

@sio.event
def disconnect():
    print('disconnected from server')

@sio.on('__DATA')
def handle_message(data):
    print('Received response message:', data)
    
@sio.on('__DATA1')
def handle_message(data):
    print('Received response message on __DATA1 :', data)
    d=data
    data1 = json.dumps(d)
    try:    #Uploading data to local mqtt/kafka
            client.publish(mac1, data1)
            print("data published on mqtt")

    except Exception as e:
            print("could not publish data on mqtt: ", e)
            print("herrrreeeee")        
    d = {}
