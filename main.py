from datetime import datetime
import json
import getmac
import time
import serial
# import requests
import socketio


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
    sio = socketio.Client()
    sio.connect('http://localhost:3100')
    print("socket connected")
except Exception as s:
    print("cant connect to socket : " , s)





# ser =serial.Serial(port='/dev/ttyM0',baudrate=baud_rate,parity=parity,bytesize=byte_size,stopbits = stop_bits)
# print("hello 2")
# print(x)
# ser.flush()

while True:

    # try:
    # time.sleep(2)
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

                    elif len(data)<len(channels):
                        
                        d= dict()
                        print("the data has less columns then config")
                        d.update({'timestamp': None})
                        d.update({'timestamp' : unixtime})
                        for x in range(len(data)):
                            d.update({channels[x]: None})
                            d.update({channels[x]: data[x]})


                except Exception as l:
                    print("error occured in object : " , l)
                
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
                time.sleep(1)

            except Exception as e:
                print("error1 : ", e)

                

