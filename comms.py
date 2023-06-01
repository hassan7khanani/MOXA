import socketio
import eventlet
import json
import os
import time
import subprocess
from datetime import datetime
import getmac
import json
sio = socketio.Server(cors_allowed_origins=[])
app = socketio.WSGIApp(sio)

def getmoxamac():
    mac = getmac.get_mac_address()
    print(mac)
    macup = mac.upper()
    return macup


mac1 = getmoxamac()
mac_dict = {'id' : mac1}
mac_json = json.dumps(mac_dict)
print("mac address of moxa : ", mac1)

@sio.on('connect')
def connect(sid, environ):
    print('connect ', sid)
    sio.emit('__ON_CONNECT', mac_json)
    # time.sleep(1)


@sio.on('__CONFIG_GET')
def message(sid, data):
    print(sid, data)

    with open('config.json', 'r+') as f:

        settings = json.load(f)
        print(settings)

    sio.emit('__CONFIG_GET', settings)


@sio.on('__CONFIG_SAVE')
def message(sid, data):
    print(sid, data)

    with open("config.json", "w") as file:
        json.dump(data, file)

@sio.on('__DATA')
def message(sid, data):
    print(sid, data)
    sio.emit('__DATA', data)

@sio.on('__DATA1')
def message(sid, data):
    print(sid, data)
    sio.emit('__DATA1', data)

@sio.on('__DATA_EXPORT')
def message(sid, data):
    print(sid, data)
    print("message in data export channel")
    print(data)
    starttime = data['from']
    endtime = data['to']
    try:
        # getdata(rts, starttime, endtime)
        sio.emit('__EXPORT_FILE', "exportfile.csv")
    except Exception as e:
        print("error occured while generating report : ", e)
        sio.emit('__EXPORT_FILE', "NO_DATA_FOUND")

@sio.on('__EXPORT_FILE')
def message(sid, data):
    print(sid, data)
    sio.emit('__EXPORT_FILE', data)


if __name__ == '__main__':
    eventlet.wsgi.server(eventlet.listen(('', 3100)), app)
