from datetime import datetime
import json
# from redis.commands.json.path import Path
# import redis.commands.search.aggregation as aggregations
# import redis.commands.search.reducers as reducers
# from redis.commands.search.field import TextField, NumericField, TagField
# from redis.commands.search.indexDefinition import IndexDefinition, IndexType
# from redis.commands.search.query import NumericFilter, Query
# import redis

import getmac
import time
import serial
from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTClient
# import requests
import paho.mqtt.client as mqtt
import socketio
BASE_URL = '143.198.137.203'
# 143.198.137.203
MQTT_PORT = 1883
username = "processPumps" 
password = "4ee3xBjJ6mfZ1vskFkAVjr0QSsE9Dcvn"

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

date_column = 0

def getmoxamac():
    mac = getmac.get_mac_address()
    print(mac)
    macup = mac.upper()
    return macup

mac1=getmoxamac()
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
    client = mqtt.Client()
    client.username_pw_set(username, password)
    client.connect('3.141.141.38',1883)
    print("mqtt connected")
except Exception as m:
    print("cant connect to mqtt : ", m)

try:
    sio = socketio.Client()
    sio.connect('http://localhost:3100')
    print("socket connected")
except Exception as s:
    print("cant connect to socket : " , s)

# try:
# ##    rts = Client(host = '192.168.0.100', port=6379)
#     r = redis.Redis(host='localhost', port=6379)
#     print("redis loaded")
# except Exception as r:
#     print("error loading redis : ", r)

# try:
# ##    schema = (TextField("$.user.name", as_name="name"),TagField("$.user.city", as_name="city"), NumericField("$.user.age", as_name="age"))
#     schema = (NumericField("$.timestamp", as_name="timestamp"))
#     r.ft().create_index(schema, definition=IndexDefinition(prefix=[""], index_type=IndexType.JSON))
# except Exception as schema_error:
#     print("cant create schema : ", schema_error)

# private_key = "awsiot/private.key"
# #private_key = "awsiot/"+mac2+".key"
# print(private_key)
# cert_file = "awsiot/certificate.crt"
# #cert_file = "awsiot/"+mac2+".crt"
# root_ca = "awsiot/root1.pem"
# #root_ca = "awsiot/rootca.pem"

# try:
#     myMQTTClient = AWSIoTMQTTClient(mac2)
#     myMQTTClient.configureEndpoint("a1b1m98fn7jasi-ats.iot.us-east-1.amazonaws.com",8883)
#     myMQTTClient.configureCredentials(root_ca,private_key,cert_file)
#     myMQTTClient.configureOfflinePublishQueueing(-1) # Infinite offline Publish queueing
#     myMQTTClient.configureDrainingFrequency(2) # Draining: 2 Hz
#     myMQTTClient.configureConnectDisconnectTimeout(10) # 10 sec
#     myMQTTClient.configureMQTTOperationTimeout(5) # 5 sec
#     myMQTTClient.connect()
#     print("AWS Connected")
# except Exception as awserror:
#     print("error occured while connecting to aws : ", awserror)


# ser =serial.Serial(port='/dev/ttyM0',baudrate=baud_rate,parity=parity,bytesize=byte_size,stopbits = stop_bits)
# print("hello 2")
# print(x)
# ser.flush()

while True:

    # try:
        with open("data2.txt","r+", encoding= 'utf-8') as file:
            for line in file:
            # if ser.in_waiting>0:
                try:
                    print("data receievd")
                    # line = ser.readline()
                    # line1 = line.rstrip().decode()
                    data = line.split(',')
    #                print(data)
                    print(len(data))
                    # data = line.split(',')
                    # print(data)
                    date1 = data[0]
                    date_time_obj = datetime.strptime(date1, '%m/%d/%Y %H:%M')
                    print("datetime object =  " , date_time_obj)
                    
                    timestamp = datetime.timestamp(date_time_obj)
                    print("timestamp =", timestamp)
                    data[0] = timestamp
                    print(data)
                    print("sending data on DATA1 channel")
                    sio.emit('DATA1', data)
                    unixtime= int(time.time() * 1000)
                    print(unixtime)

                    print("len of data is : " ,len(data))
                    try : 
                        if len(data)==len(channels):
                            # d = dict()
                            d.update({'timestamp': None})
                            d.update({'timestamp' : unixtime})
                            for x in range(len(channels)):
                                d.update({channels[x]: None})
                                d.update({channels[x]: data[x]})
                            # try:
                            #     r.json().set(unixtime, Path.root_path(), d)
                            #     print("data inserted in redis")

                            # except Exception as e:
                            #     print("unable to insert data into redis : " , e)

                            print(d)


                        
                        elif len(data)>len(channels):
                            d = dict()
                            print("the data has more columns then config")
                            d.update({'timestamp': None})
                            d.update({'timestamp' : unixtime})
                            for x in range(len(channels)):
                                d.update({channels[x]: None})
                                d.update({channels[x]: data[x]})
                                
                            for x in range(len(channels), len(data)):
                                d.update({'x' + str(x): None})
                                d.update({'x' + str(x): data[x]})
                                
                                try:
                                    
                                    write_json('x'+str(x))
                                    channels.append('x'+str(x))
                                except Exception as e:
                                    print("unable to append channel name in config.json : " , e)

                            # try:
                            #     r.json().set(unixtime, Path.root_path(), d)

                            # except Exception as e:
                            #     print("unable to insert data into redis : " , e)



                        elif len(data)<len(channels):
                            
                            d= dict()
                            print("the data has less columns then config")
                            d.update({'timestamp': None})
                            d.update({'timestamp' : unixtime})
                            for x in range(len(data)):
                                d.update({channels[x]: None})
                                d.update({channels[x]: data[x]})

                            # try:
                            #     r.json().set(unixtime, Path.root_path(), d)

                            # except Exception as e:
                            #     print("unable to insert data into redis : " , e)
                            
        ##                    print(d)

                    except Exception as l:
                        print("error occured in object : " , l)
                    
                    # data1 = json.dumps(d)
                    # try:    #Uploading data to local mqtt/kafka
                    #         client.publish(mac1, data1)
                    #         print("data published on mqtt")

                    # except Exception as e:
                    #         print("could not publish data on mqtt: ", e)
            #            print("herrrreeeee")
                
                    # try:    #Uploading data to aws iot core
                    #     d.update({'_id': None})
                    #     d.update({'_id' : "moxa-"+mac2})
                    #     data1 = json.dumps(d)
                    #     myMQTTClient.publish(
                    #     topic=mac2,
                    #     QoS=1,
                    #     payload=data1)

                    # except Exception as e: 
                    #     print("error occured while sending data to awsiot", e)


                
                    d2 = dict() #live_data
                    d2.update({'timestamp': None})
                    d2.update({'timestamp' : unixtime})
                    for x in live_data:
                        d2.update({x : None})
                        d2.update({x: d[x]})
                    
                    data2 = json.dumps(d2)
                    try:    #Uploading live data to socket
                        sio.emit("__DATA",data2)
                        sio.emit("__DATA1",data2)
                        print("live data emitted on socket")
                                            
                    except Exception as er:
                        print("error : Unable to emit live data on socket" , er)
            ##
                    d = {}
                    d2 = {}
                    time.sleep(5)

                except Exception as e:
                    print("error : ", e)

                

