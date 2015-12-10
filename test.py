from __future__ import print_function


import sys
import time
import random
import os
import dds
import json
import threading

from signal import signal, SIGINT

d = dds.DDS('UCSTopics_C')

t1 = d.get_topic('LDM.Maritime.Primary_Mission_Control.Vehicle_Management.GlobalPoseSensor.GlobalPoseStatusType')
t2 = d.get_topic('LDM.Maritime.Primary_Mission_Control.Sensor_Management.VisualSensor.VisualSensorCommandType')
# msg = {
#     'sourceSystemID' : 'pwahahaha',
#     'altitudeAGL' : 99,
#     'altitudeASF' : 102,
#     'altitudeMSL' : 304,
#     'attitude' : {
#         # 'azimuth'   : 359,
#         'elevation' : 30,
#         'rotation'  : 90,
#     },
#     'attitudeRMS' : { 'orientationError' : 0},
#     'depth' : 3,
#     'position' : {
#         'altitude'  : 88,
#         'latitude'  : 117.3,
#         'longitude' : 32.5,
#     },
#     'timeStamp' : 1234567,
#     'xyPositionRMS' : {'positionError':100},
#     'zPositionRMS' : {'distanceError':66},
# }


t2_msg = {
    'targetSystemID' : 'blah.blah.blah',
    'thermalImageMode' : 'WHITE_HOT_IRPolarityEnumTypeLDM',
    'timeStamp' : random.random(),
}
t2.publish(t2_msg)

# time.sleep(1)

# print("Calling dispose")

# t2.dispose(t2_msg)

running = True

def handle_sigint(signal, frame):
    global running
    print("\n--Disposing of topics--")
    running = False
    t1.dispose(t1_msg)
    t2.dispose(t2_msg)

signal(SIGINT, handle_sigint)

while running:
    t1_msg = {
        'sourceSystemID' : 'pwahahaha',
        'position' : {
            'altitude' : random.randint(0,100),
        },
        'depth' : random.randint(0,100),
    }
    t1.publish(t1_msg)
    t1_msg = {
        'sourceSystemID' : '19',
        'position' : {
            'altitude' : random.randint(0,100),
        },
        'depth' : random.randint(0,100),
    }
    t1.publish(t1_msg)
    t2_msg = {
        'targetSystemID' : 'blah.blah.blah',
        'thermalImageMode' : 'WHITE_HOT_IRPolarityEnumTypeLDM',
        'timeStamp' : random.random(),
    }
    t2.publish(t2_msg)
    time.sleep(1)

