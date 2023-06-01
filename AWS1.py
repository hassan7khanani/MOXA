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


private_key = "/home/pi/Desktop/Moxa-new-pi/private.key"
print(private_key)
cert_file = "/home/pi/Desktop/Moxa-new-pi/certificate.crt"
root_ca = "/home/pi/Desktop/Moxa-new-pi/root1.pem"
try:
    myMQTTClient = AWSIoTMQTTClient(mac2)
    myMQTTClient.configureEndpoint("a1b1m98fn7jasi-ats.iot.us-east-1.amazonaws.com",8883)
    myMQTTClient.configureCredentials(root_ca,private_key,cert_file)
    myMQTTClient.configureOfflinePublishQueueing(-1) # Infinite offline Publish queueing
    myMQTTClient.configureDrainingFrequency(2) # Draining: 2 Hz
    myMQTTClient.configureConnectDisconnectTimeout(10) # 10 sec
    myMQTTClient.configureMQTTOperationTimeout(5) # 5 sec
    myMQTTClient.connect()
    print("AWS Connected")
except Exception as awserror:
    print("error occured while connecting to aws : ", awserror)

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
    d=json.loads(data)
    print("type of d is--------------------- ",type(d))
    try:    
        print("Uploading data to aws iot core")
        d.update({'_id': None})
        d.update({'_id' : "moxa-"+mac2})
        data1 = json.dumps(d)
        myMQTTClient.publish(
        topic=mac2,
        QoS=1,
        payload=data1)

    except Exception as e: 
        print("error occured while sending data to awsiot", e)
        
    d = {}
