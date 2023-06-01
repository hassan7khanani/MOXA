import socketio
import time
import getmac
import json
# import paho.mqtt.client as mqtt
# from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTClient
import singlestoredb as s2

sio = socketio.Client()
sio.connect('http://localhost:3100')

BASE_URL = '143.198.137.203'
# 143.198.137.203
MQTT_PORT = 1883
username = "processPumps" 
password = "4ee3xBjJ6mfZ1vskFkAVjr0QSsE9Dcvn"

# # conn = s2.connect('root:SvU9O2Wahd35c5prNDLEVjHtu2ffRmEX@localhost:3306/utf_test')
# conn = s2.connect('svc_moxa:5lds89ln&*nmckjg48@svc-10cb4228-0e23-4eef-b7c3-25f0d3bc8b39-ddl.azr-virginia-3.svc.singlestore.com:3306/moxa')
# conn = s2.connect(
#     host='svc-10cb4228-0e23-4eef-b7c3-25f0d3bc8b39-ddl.azr-virginia-3.svc.singlestore.com',
#     user='svc_moxa',
#     password='5lds89ln&*nmckjg48',
#     database='moxa'
# )

# cur = conn.cursor()
# cur.execute('USE moxa')
# cur.close()
# cur.execute("CREATE TABLE MOXA (CHANNELS VARCHAR(255), details VARCHAR(255))")
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
    # print(mac)
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
        # print(len(channels))
        live_data = settings['live_data']



try:
    sio = socketio.Client()
    sio.connect('http://localhost:3100')
    print("socket connected")
except Exception as s:
    print("cant connect to socket : " , s)


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
    # print('Received response message on __DATA1 :', data)
    d=json.loads(data)
    print("value of d is ",d)
    print("type of d is--------------------- ",type(d))
    d.update({'_id': None})
    d.update({'_id' : "moxa-"+mac2})
    print("d is ",d)
    print("keys OF D",d.keys())
    print("VALUES OF D",d.values())
    key_length=len(d.keys())
    keys=list(d.keys())
    values=list(d.values())
    for i in range (key_length):
        try:
            conn = s2.connect(host='svc-10cb4228-0e23-4eef-b7c3-25f0d3bc8b39-ddl.azr-virginia-3.svc.singlestore.com',user='svc_moxa',password='5lds89ln&*nmckjg48',database='moxa')
            cur = conn.cursor()
            cur.execute('USE moxa')
            cur.execute('USE moxa')
            sql = "INSERT INTO MOXA (CHANNELS, details) VALUES (%s, %s)"
            print("keys",keys[i])
            print("values",values[i])
            # val=(d[d.keys()[i]],d[d.keys()[i]])
            val = (keys[i], values[i])
            cur.execute(sql, val)
            # cur.close()
        except Exception as e:
            print("error in inserting values",e)
    d = {}





# 


