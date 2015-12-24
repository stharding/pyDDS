from __future__ import print_function

import dds
import time
import json
import signal

signal.signal(signal.SIGINT, signal.SIG_DFL)

def print_data(data):
    print(data['name'])
    print(json.dumps(data, indent=4), '\n')

dds.subscribe_to_all_topics(['UCSTopics_C', 'CASHMITopics_C'], print_data)

while True:
    time.sleep(100)