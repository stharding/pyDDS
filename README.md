pyDDS (a Python 2.7 wrapper for RTI DDS.)
=========================================

Overview
--------

To use this wrapper, you must compile your generated topics (the output of
`rtiddsgen`) into a shared library. This will be a .so on Linux, .DLL on windows
and .dylib on OS X. The location of this library and the RTI libraries `nddsc`
and `nddscore` must be in the library search path (LD_LIBRARY_PATH on Linux,
PATH on windows, and DYLD_LIBRARY_PATH on OS X).

Use
---

Lets say you have a topic that looks like:

```C++
#ifndef MY_DDS
#define MY_DDS

module my {
    module dds {
        enum my_enum {
            mode_1,
            mode_2,
            mode_3
        };

        struct my_custom_topic
        {
            string   name; //@key
            string   value;
            my_enum  mode;
        };
    };
};

#endif // MY_DDS
```

Once this topic is generated (rtiddsgen), compiled, and packaged as a shared
library, you can use it with this library.

####Subscribe:####

Lets say you just want to print out a representation of the topic data. Lets
also say that your topic library is called `libmy_topics.so` and the location
of this library is in `LD_LIBRARY_PATH`. To subscribe:

```python
import dds
import json
import time

def print_repr(data):
    print json.dumps(data, indent=4)


dds_instance = dds.DDS('my_topics')
topic = dds_instance.get_topic('my.dds.my_custom_topic')
topic.subscribe(print_repr)

while True:
    time.sleep(100)
```

That's all there is to basic topic subscription! All data samples are delivered
to the callback as a python dictionary. Also of note: the callback will occur in
a separate thread, so you must take that into consideration for any non thread-safe
operations.

If desired, you can also specify a few other options:

instance_revoked_cb=None, liveliness_lost_cb=None, filter_expression=None, _send_topic_info=False):
 - **instance revoked** A publisher can revoke a topic instance. To be notified
   of these events, specify a callback with the keyword argument `instance_revoked_cb`
 - **liveliness_lost** If a publisher goes down, or the network connection fails
   DDS can notify subscribers that a publisher seems to have gone down. To be
   notified of this condition, specify the keyword argument `liveliness_lost_cb`
 - **content filtering** Sometimes you are only interested in topic data that
   matches some condition. Instead of examining all the topic data samples and
   throwing out the ones you don't want, you can tell DDS to only send you samples
   that match a specified filter. For example, say you only want samples that
   have `mode == mode_2`. This can be accomplished by specifying the keyword
   argument `filter_expression="mode MATCH 'mode_3'"`. see [the docs](https://community.rti.com/static/documentation/connext-dds/5.2.0/doc/manuals/connext_dds/html_files/RTI_ConnextDDS_CoreLibraries_UsersManual/Content/UsersManual/SQL_Filter_Expression_Notation.htm)
   for more details.

Subscriptions can also be canceled by calling `topic.unsubscribe()`

####Publish:####

To publish a data sample, you simply construct a python dictionary that matches
the structure of the topic. e.g.:

```python
import dds

dds_instance = dds.DDS('my_topics')
topic = dds_instance.get_topic('my_custom_topic')

sample = {
    'name': 'my key name',
    'value': 'whatever value you want',
    'mode': 'mode_2'
}

topic.publish(sample)
```

For more detailed documentation, see the inline docs in `dds.py`
