from __future__ import print_function


import sys
import time
import random
import os
import dds
import json
import threading

d = dds.DDS('UCSTopics_C')
# d = dds.DDS(library='BasicLibrary', profile='BasicQos', participant_name='recvtest')
# topics = dds.Library("libUCSTopics_C.dylib")

t = d.get_topic('LDM.Maritime.Primary_Mission_Control.Vehicle_Management.GlobalPoseSensor.GlobalPoseStatusType')

tf = t.subscribe(lambda data: print(json.dumps(data, indent=4)),
            instance_revoked_cb=lambda: print(t.name, "instance revoked ..."),
            liveliness_lost_cb=lambda: print(t.name, "liveliness lost ..."),
            filter_expression="sourceSystemID MATCH '19'") #

t.subscribe(lambda data: print(json.dumps(data, indent=4)),
            instance_revoked_cb=lambda: print(t.name, "instance revoked ..."),
            liveliness_lost_cb=lambda: print(t.name, "liveliness lost ..."),
            filter_expression="depth > 20 and depth < 90") #

t2 = d.get_topic('LDM.Maritime.Primary_Mission_Control.Sensor_Management.VisualSensor.VisualSensorCommandType')

t2.subscribe(lambda data: print(json.dumps(data, indent=4)),
             instance_revoked_cb=lambda: print(t2.name, "instance revoked ..."),
             liveliness_lost_cb=lambda: print(t2.name, "liveliness lost ..."))


# time.sleep(2)

# print("unsubscribing")
# t.unsubscribe(tf)
while True:
    time.sleep(10)
print("exiting")