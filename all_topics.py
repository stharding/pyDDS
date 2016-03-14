from __future__ import print_function

import dds
import time
import json
import signal
import sys

signal.signal(signal.SIGINT, signal.SIG_DFL)

def main(topic_libs):
    def print_data(data):
        print(json.dumps(data, indent=4), '\n')

    dds.subscribe_to_all_topics(topic_libs,
                                print_data,
                                instance_revoked_cb=lambda x: print(x, 'instance revoked ...'),
                                liveliness_lost_cb=lambda x: print(x, 'liveliness lost ...'),
    )

    while True:
        time.sleep(100)


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print(
            'Usage: ' + sys.argv[0] + ' <topic_libraries>\n'
            '    where topic_libraries is a space delimeted list of\n'
            '    libraries which define the topics.'
        )
        sys.exit(1)
    main(sys.argv[1:])
