from __future__ import print_function

import ctypes
import weakref
import collections
import uuid
import platform
import threading

def libname(name):
    if platform.uname()[0] == 'Windows':
        return name + '.dll'
    elif platform.uname()[0] == 'Darwin':
        return 'lib' + name + '.dylib'
    else:
        return 'lib' + name + '.so'

_ddscore_lib = ctypes.CDLL(libname('nddscore'), ctypes.RTLD_GLOBAL)
_ddsc_lib = ctypes.CDLL(libname('nddsc'))

# some types
enum = ctypes.c_int

DDS_Char             = ctypes.c_char
DDS_Wchar            = ctypes.c_wchar
DDS_Octet            = ctypes.c_ubyte
DDS_Short            = ctypes.c_int16
DDS_UnsignedShort    = ctypes.c_uint16
DDS_Long             = ctypes.c_int32
DDS_UnsignedLong     = ctypes.c_uint32
DDS_LongLong         = ctypes.c_int64
DDS_UnsignedLongLong = ctypes.c_uint64
DDS_Float            = ctypes.c_float
DDS_Double           = ctypes.c_double
DDS_LongDouble       = ctypes.c_longdouble
DDS_Boolean          = ctypes.c_bool
DDS_Enum             = DDS_UnsignedLong

DDS_DynamicDataMemberId = DDS_Long
DDS_ReturnCode_t        = enum
DDS_ExceptionCode_t     = enum

def ex():
    return ctypes.byref(DDS_ExceptionCode_t())

DDS_DomainId_t = ctypes.c_int32
DDS_TCKind = enum

DDS_SampleStateMask   = DDS_UnsignedLong
DDS_ViewStateMask     = DDS_UnsignedLong
DDS_InstanceStateMask = DDS_UnsignedLong
DDS_StatusMask        = DDS_UnsignedLong
DDS_SampleFlag        = DDS_Long
DDS_SampleStateKind   = DDS_Long
DDS_ViewStateKind     = DDS_Long
DDS_InstanceStateKind = DDS_Long

DDS_LENGTH_UNLIMITED      = DDS_Long(-1)
DDS_NOT_READ_SAMPLE_STATE = 2
DDS_ALIVE_INSTANCE_STATE  = 1
DDS_NOT_ALIVE_DISPOSED_INSTANCE_STATE   = 2
DDS_NOT_ALIVE_NO_WRITERS_INSTANCE_STATE = 4

DDS_DYNAMIC_DATA_MEMBER_ID_UNSPECIFIED = 0

DDS_DURATION_INFINITE_SEC  = 2**31 - 1
DDS_DURATION_INFINITE_NSEC = 2**31 - 1

DDS_INCONSISTENT_TOPIC_STATUS                     = 1 <<  0
DDS_OFFERED_DEADLINE_MISSED_STATUS                = 1 <<  1
DDS_REQUESTED_DEADLINE_MISSED_STATUS              = 1 <<  2
DDS_OFFERED_INCOMPATIBLE_QOS_STATUS               = 1 <<  5
DDS_REQUESTED_INCOMPATIBLE_QOS_STATUS             = 1 <<  6
DDS_SAMPLE_LOST_STATUS                            = 1 <<  7
DDS_SAMPLE_REJECTED_STATUS                        = 1 <<  8
DDS_DATA_ON_READERS_STATUS                        = 1 <<  9
DDS_DATA_AVAILABLE_STATUS                         = 1 << 10
DDS_LIVELINESS_LOST_STATUS                        = 1 << 11
DDS_LIVELINESS_CHANGED_STATUS                     = 1 << 12
DDS_PUBLICATION_MATCHED_STATUS                    = 1 << 13
DDS_SUBSCRIPTION_MATCHED_STATUS                   = 1 << 14
DDS_DATA_WRITER_APPLICATION_ACKNOWLEDGMENT_STATUS = 1 << 22
DDS_DATA_WRITER_INSTANCE_REPLACED_STATUS          = 1 << 23
DDS_RELIABLE_WRITER_CACHE_CHANGED_STATUS          = 1 << 24
DDS_RELIABLE_READER_ACTIVITY_CHANGED_STATUS       = 1 << 25
DDS_DATA_WRITER_CACHE_STATUS                      = 1 << 26
DDS_DATA_WRITER_PROTOCOL_STATUS                   = 1 << 27
DDS_DATA_READER_CACHE_STATUS                      = 1 << 28
DDS_DATA_READER_PROTOCOL_STATUS                   = 1 << 29
DDS_DATA_WRITER_DESTINATION_UNREACHABLE_STATUS    = 1 << 30
DDS_DATA_WRITER_SAMPLE_REMOVED_STATUS             = 1 << 31

# Error checkers

class Error(Exception):
    pass

class NoDataError(Exception):
    pass


def check_code(result, func, arguments):
    if result == 11:
        raise NoDataError()
    if result != 0:
        # raise Error(str(result))
        raise Error({
            1:  'error',
            2:  'unsupported',
            3:  'bad parameter',
            4:  'precondition not met',
            5:  'out of resources',
            6:  'not enabled',
            7:  'immutable policy',
            8:  'inconsistant policy',
            9:  'already deleted',
            10: 'timeout',
            11: 'no data',
            12: 'illegal operation',
        }[result])

def check_null(result, func, arguments):
    if not result:
        raise Error('Null check failed')
    return result

def check_ex(result, func, arguments):
    if arguments[-1]._obj.value != 0:
        raise Error({
            1:  '(user)',
            2:  '(system)',
            3:  'bad param (system)',
            4:  'no memory (system)',
            5:  'bad typecode (system)',
            6:  'badkind (user)',
            7:  'bounds (user)',
            8:  'immutable typecode (system)',
            9:  'bad member name (user)',
            10: 'bad member id (user)',
        }[arguments[-1]._obj.value])
    return result

def check_true(result, func, arguments):
    if not result:
        raise Error()

# Function and structure accessors

def get(name, data_type):
    return ctypes.cast(getattr(_ddsc_lib, 'DDS_' + name), ctypes.POINTER(data_type)).contents

@apply
class DDSFunc(object):
    pass

@apply
class DDSType(object):
    def __getattr__(self, attr):
        contents = type(attr, (ctypes.Structure,), {})

        def g(self2, attr2):
            f = getattr(DDSFunc, attr + '_' + attr2)
            def m(*args):
                return f(self2, *args)
            setattr(self2, attr2, m)
            return m
        # make structs dynamically present bound methods
        contents.__getattr__ = g
        # take advantage of POINTERs being cached to make type pointers do the same
        ctypes.POINTER(contents).__getattr__ = g

        setattr(self, attr, contents)
        return contents


DDSType.Duration_t._fields_ = [
    ('sec', DDS_Long),
    ('nanosec', DDS_UnsignedLong)
]


DDSType.BuiltinTopicKey_t._fields_ = [
    ('value', ctypes.c_int32 * 4),
]

DDSType.TopicBuiltinTopicData._fields_ = [
    ('key', DDSType.BuiltinTopicKey_t),
    ('name', ctypes.c_char_p),
    ('type_name', ctypes.c_char_p),
]

DDSType.PublicationBuiltinTopicData._fields_ = [
    ('key', DDSType.BuiltinTopicKey_t),
    ('participant_key', DDSType.BuiltinTopicKey_t),
    ('topic_name', ctypes.c_char_p),
    ('type_name', ctypes.c_char_p),
    # ('durability', DDSType.DurabilityQosPolicy),
    # ('durability_service', DDSType.DurabilityServiceQosPolicy),
    # ('deadline', DDSType.DeadlineQosPolicy),
    # ('latency_budget', DDSType.LatencyBudgetQosPolicy),
    # ('liveliness', DDSType.LivelinessQosPolicy),
    # ('reliability', DDSType.ReliabilityQosPolicy),
    # ('lifespan', DDSType.LifespanQosPolicy),
    # ('user_data', DDSType.UserDataQosPolicy),
    # ('ownership', DDSType.OwnershipQosPolicy),
    # ('ownership_strength', DDSType.OwnershipStrengthQosPolicy),
    # ('destination_order', DDSType.DestinationOrderQosPolicy),
    # ('presentation', DDSType.PresentationQosPolicy),
    # ('partition', DDSType.PartitionQosPolicy),
    # ('topic_data', DDSType.TopicDataQosPolicy),
    # ('group_data', DDSType.GroupDataQosPolicy),
    # ('type_code', ctypes.POINTER(DDSType.TypeCode)),
    # ('publisher_key', DDSType.BuiltinTopicKey_t),
    # ('property', DDSType.PropertyQosPolicy),
    # ('unicast_locators', DDSType.LocatorSeq),
    # ('virtual_guid', DDSType.GUID_t),
    # ('service', DDSType.ServiceQosPolicy),
    # ('rtps_protocol_version', DDSType.ProtocolVersion_t),
    # ('rtps_vendor_id', DDSType.VendorId_t),
    # ('product_version', DDSType.ProductVersion_t),
    # ('locator_filter', DDSType.LocatorFilterQosPolicy),
    # ('disable_positive_acks', DDSType.Boolean),
    # ('publication_name', DDSType.EntityNameQosPolicy),
]

DDSType.Time_t._fields_ = [
    ('sec', DDS_Long),
    ('nanosec', DDS_UnsignedLong),
]

DDSType.Topic._fields_ = [
    ('_as_Entity', ctypes.c_void_p),
    ('_as_TopicDescription', ctypes.POINTER(DDSType.TopicDescription)),
]

DDSType.ContentFilteredTopic._fields_ = [
    ('_as_TopicDescription', ctypes.POINTER(DDSType.TopicDescription)),
    ('_narrow', ctypes.POINTER(DDSType.ContentFilteredTopic)),
    ('_get_filter_expression', ctypes.c_char_p),
    ('_get_expression_parameters', DDSType.DDS_ReturnCode_t),
    ('_set_expression_parameters', DDSType.DDS_ReturnCode_t),
]

ctypes.POINTER(DDSType.Topic).as_topicdescription = lambda self: self.contents._as_TopicDescription
ctypes.POINTER(DDSType.ContentFilteredTopic).as_topicdescription = lambda self: self.contents._as_TopicDescription

DDSType.InstanceHandleSeq._fields_ = DDSType.PublicationBuiltinTopicDataSeq._fields_ = \
                                     DDSType.DynamicDataSeq._fields_ = DDSType.SampleInfoSeq._fields_ = \
                                     DDSType.StringSeq._fields_ = DDSType.ConditionSeq._fields_ = [
    ('_owned', ctypes.c_bool),
    ('_contiguous_buffer', ctypes.c_void_p),
    ('_discontiguous_buffer', ctypes.c_void_p),
    ('_maximum', ctypes.c_ulong),
    ('_length', ctypes.c_ulong),
    ('_sequence_init', ctypes.c_long),
    ('_read_token1', ctypes.c_void_p),
    ('_read_token2', ctypes.c_void_p),
    ('_elementAllocParams', DDSType.SeqElementTypeAllocationParams_t),
    ('_elementDeallocParams', DDSType.SeqElementTypeDeallocationParams_t),
]

DDSType.InstanceHandle_t._fields_ = [
    ('keyHash_value', ctypes.c_byte * 16),
    ('keyHash_length', ctypes.c_uint32),
    ('isValid', ctypes.c_int),
]
DDS_HANDLE_NIL = DDSType.InstanceHandle_t((ctypes.c_byte * 16)(*[0]*16), 16, False)

DDSType.SampleInfo._fields_ = [
    ('sample_state', DDS_SampleStateKind),
    ('view_state', DDS_ViewStateKind),
    ('instance_state', DDS_InstanceStateKind),
    ('source_timestamp', DDSType.Time_t),
    ('instance_handle', DDSType.InstanceHandle_t),
    ('publication_handle', DDSType.InstanceHandle_t),
    ('disposed_generation_count', DDS_Long),
    ('no_writers_generation_count', DDS_Long),
    ('sample_rank', DDS_Long),
    ('generation_rank', DDS_Long),
    ('absolute_generation_rank', DDS_Long),
    ('valid_data', DDS_Boolean),
    ('reception_timestamp', DDSType.Time_t),
    ('publication_sequence_number', DDSType.SequenceNumber_t),
    ('reception_sequence_number', DDSType.SequenceNumber_t),
    ('original_publication_virtual_guid', DDSType.GUID_t),
    ('original_publication_virtual_sequence_number', DDSType.SequenceNumber_t),
    ('related_publication_virtual_guid', DDSType.GUID_t),
    ('related_publication_virtual_sequence_number', DDSType.SequenceNumber_t),
    ('flag', DDS_SampleFlag),
    ('source_guid', DDSType.GUID_t),
    ('related_source_guid', DDSType.GUID_t),
    ('related_subscription_guid', DDSType.GUID_t),
]

DDSType.Listener._fields_ = [
    ('listener_data', ctypes.c_void_p),
]

DDSType.DataReaderListener._fields_ = [
    ('as_listener', DDSType.Listener),
    ('on_requested_deadline_missed', ctypes.CFUNCTYPE(None, ctypes.c_void_p, ctypes.POINTER(DDSType.DataReader), ctypes.POINTER(DDSType.RequestedDeadlineMissedStatus))),
    ('on_requested_incompatible_qos', ctypes.CFUNCTYPE(None, ctypes.c_void_p, ctypes.POINTER(DDSType.DataReader), ctypes.POINTER(DDSType.RequestedIncompatibleQosStatus))),
    ('on_sample_rejected', ctypes.CFUNCTYPE(None, ctypes.c_void_p, ctypes.POINTER(DDSType.DataReader), ctypes.POINTER(DDSType.SampleRejectedStatus))),
    ('on_liveliness_changed', ctypes.CFUNCTYPE(None, ctypes.c_void_p, ctypes.POINTER(DDSType.DataReader), ctypes.POINTER(DDSType.LivelinessChangedStatus))),
    ('on_data_available', ctypes.CFUNCTYPE(None, ctypes.c_void_p, ctypes.POINTER(DDSType.DataReader))),
    ('on_subscription_matched', ctypes.CFUNCTYPE(None, ctypes.c_void_p, ctypes.POINTER(DDSType.DataReader), ctypes.POINTER(DDSType.SubscriptionMatchedStatus))),
    ('on_sample_lost', ctypes.CFUNCTYPE(None, ctypes.c_void_p, ctypes.POINTER(DDSType.DataReader), ctypes.POINTER(DDSType.SampleLostStatus))),
]

DDSType.LivelinessChangedStatus._fields_ = [
    ('alive_count', DDS_Long),
    ('not_alive_count', DDS_Long),
    ('alive_count_change', DDS_Long),
    ('not_alive_count_change', DDS_Long),
    ('last_publication_handle', DDSType.InstanceHandle_t),
]

class TCKind(object):
    NULL             =  0
    SHORT            =  1
    LONG             =  2
    USHORT           =  3
    ULONG            =  4
    FLOAT            =  5
    DOUBLE           =  6
    BOOLEAN          =  7
    CHAR             =  8
    OCTET            =  9
    STRUCT           =  10
    UNION            =  11
    ENUM             =  12
    STRING           =  13
    SEQUENCE         =  14
    ARRAY            =  15
    ALIAS            =  16
    LONGLONG         =  17
    ULONGLONG        =  18
    LONGDOUBLE       =  19
    WCHAR            =  20
    WSTRING          =  21
    VALUE            =  22
    SPARSE           =  23
    RAW_BYTES        =  0x7e
    RAW_BYTES_KEYED  =  0x7f

DATA_AVAILABLE_STATUS = 1 << 10

# Function prototypes

_dyn_basic_types = {
    TCKind.LONG      : ('long', DDS_Long, (-2**31, 2**31)),
    TCKind.ULONG     : ('ulong', DDS_UnsignedLong, (0, 2**32)),
    TCKind.SHORT     : ('short', DDS_Short, (-2**15, 2**15)),
    TCKind.USHORT    : ('ushort', DDS_UnsignedShort, (0, 2**16)),
    TCKind.LONGLONG  : ('longlong', DDS_LongLong, (-2**63, 2**63)),
    TCKind.ULONGLONG : ('ulonglong', DDS_UnsignedLongLong, (0, 2**64)),
    TCKind.FLOAT     : ('float', DDS_Float, None),
    TCKind.DOUBLE    : ('double', DDS_Double, None),
    TCKind.BOOLEAN   : ('boolean', DDS_Boolean, None),
    TCKind.OCTET     : ('octet', DDS_Octet, (0, 2**8)),
    TCKind.CHAR      : ('char', DDS_Char, None),
    TCKind.WCHAR     : ('wchar', DDS_Wchar, None),
}
def _define_func((p, errcheck, restype, argtypes)):
    f = getattr(_ddsc_lib, 'DDS_' + p)
    if errcheck is not None:
        f.errcheck = errcheck
    f.restype = restype
    f.argtypes = argtypes
    setattr(DDSFunc, p, f)
map(_define_func, [
    ('DomainParticipantFactory_get_instance',
        check_null, ctypes.POINTER(DDSType.DomainParticipantFactory),
        []),
    ('DomainParticipantFactory_set_default_participant_qos_with_profile',
        check_code, DDS_ReturnCode_t,
        [ctypes.POINTER(DDSType.DomainParticipantFactory), ctypes.c_char_p, ctypes.c_char_p]),
    ('DomainParticipantFactory_create_participant',
        check_null, ctypes.POINTER(DDSType.DomainParticipant),
        [ctypes.POINTER(DDSType.DomainParticipantFactory), DDS_DomainId_t, ctypes.POINTER(DDSType.DomainParticipantQos), ctypes.POINTER(DDSType.DomainParticipantListener), DDS_StatusMask]),
    ('DomainParticipantFactory_create_participant_with_profile',
        check_null, ctypes.POINTER(DDSType.DomainParticipant),
        [ctypes.POINTER(DDSType.DomainParticipantFactory), DDS_DomainId_t, ctypes.c_char_p, ctypes.c_char_p, ctypes.POINTER(DDSType.DomainParticipantListener), DDS_StatusMask]),
    ('DomainParticipantFactory_delete_participant',
        check_code, DDS_ReturnCode_t,
        [ctypes.POINTER(DDSType.DomainParticipantFactory), ctypes.POINTER(DDSType.DomainParticipant)]),
    ('DomainParticipant_set_default_library',
        check_code, DDS_ReturnCode_t,
        [ctypes.POINTER(DDSType.DomainParticipantFactory), ctypes.c_char_p]),
    ('DomainParticipant_set_default_profile',
        check_code, DDS_ReturnCode_t,
        [ctypes.POINTER(DDSType.DomainParticipantFactory), ctypes.c_char_p, ctypes.c_char_p]),
    ('Entity_enable',
        check_code, DDS_ReturnCode_t,
        [ctypes.POINTER(DDSType.Entity)]),

    ('DomainParticipant_create_publisher',
        check_null, ctypes.POINTER(DDSType.Publisher),
        [ctypes.POINTER(DDSType.DomainParticipant), ctypes.POINTER(DDSType.PublisherQos), ctypes.POINTER(DDSType.PublisherListener), DDS_StatusMask]),
    ('DomainParticipant_create_publisher_with_profile',
        check_null, ctypes.POINTER(DDSType.Publisher),
        [ctypes.POINTER(DDSType.DomainParticipant), ctypes.c_char_p, ctypes.c_char_p, ctypes.POINTER(DDSType.PublisherListener), DDS_StatusMask]),
    ('DomainParticipant_delete_publisher',
        check_code, DDS_ReturnCode_t,
        [ctypes.POINTER(DDSType.DomainParticipant), ctypes.POINTER(DDSType.Publisher)]),
    ('DomainParticipant_create_subscriber',
        check_null, ctypes.POINTER(DDSType.Subscriber),
        [ctypes.POINTER(DDSType.DomainParticipant), ctypes.POINTER(DDSType.SubscriberQos), ctypes.POINTER(DDSType.SubscriberListener), DDS_StatusMask]),
    ('DomainParticipant_create_subscriber_with_profile',
        check_null, ctypes.POINTER(DDSType.Subscriber),
        [ctypes.POINTER(DDSType.DomainParticipant), ctypes.c_char_p, ctypes.c_char_p, ctypes.POINTER(DDSType.SubscriberListener), DDS_StatusMask]),
    ('DomainParticipant_delete_subscriber',
        check_code, DDS_ReturnCode_t,
        [ctypes.POINTER(DDSType.DomainParticipant), ctypes.POINTER(DDSType.Subscriber)]),
    ('DomainParticipant_create_topic',
        check_null, ctypes.POINTER(DDSType.Topic),
        [ctypes.POINTER(DDSType.DomainParticipant), ctypes.c_char_p, ctypes.c_char_p, ctypes.POINTER(DDSType.TopicQos), ctypes.POINTER(DDSType.TopicListener), DDS_StatusMask]),
    ('DomainParticipant_delete_topic',
        check_code, DDS_ReturnCode_t,
        [ctypes.POINTER(DDSType.DomainParticipant), ctypes.POINTER(DDSType.Topic)]),
    ('DomainParticipant_create_contentfilteredtopic',
        check_null, ctypes.POINTER(DDSType.ContentFilteredTopic),
        [ctypes.POINTER(DDSType.DomainParticipant), ctypes.c_char_p, ctypes.POINTER(DDSType.Topic), ctypes.c_char_p, ctypes.POINTER(DDSType.StringSeq)]),
    ('DomainParticipant_delete_contentfilteredtopic',
        check_code, DDS_ReturnCode_t,
        [ctypes.POINTER(DDSType.DomainParticipant), ctypes.POINTER(DDSType.ContentFilteredTopic)]),
    ('DomainParticipant_get_builtin_subscriber',
        None, ctypes.POINTER(DDSType.Subscriber),
        [ctypes.POINTER(DDSType.DomainParticipant)]),
    ('DomainParticipant_get_discovered_topics',
        check_code, DDS_ReturnCode_t,
        [ctypes.POINTER(DDSType.DomainParticipant), ctypes.POINTER(DDSType.InstanceHandleSeq)]),
    ('DomainParticipant_get_discovered_topic_data',
        check_code, DDS_ReturnCode_t,
        [ctypes.POINTER(DDSType.DomainParticipant), ctypes.POINTER(DDSType.TopicBuiltinTopicData), ctypes.POINTER(DDSType.InstanceHandle_t)]),

    ('TopicBuiltinTopicDataDataReader_narrow',
        check_null, ctypes.POINTER(DDSType.TopicBuiltinTopicDataDataReader),
        [ctypes.POINTER(DDSType.DataReader)]),

    ('TopicBuiltinTopicDataDataReader_get_key_value',
        check_code, DDS_ReturnCode_t,
        [ctypes.POINTER(DDSType.TopicBuiltinTopicDataDataReader), ctypes.POINTER(DDSType.TopicBuiltinTopicData), ctypes.POINTER(DDSType.InstanceHandle_t)]),

    ('TopicBuiltinTopicDataTypeSupport_create_data',
        None, DDSType.TopicBuiltinTopicData, []),

    ('InstanceHandleSeq_initialize',
        check_true, DDS_Boolean, [ctypes.POINTER(DDSType.InstanceHandleSeq)]),
    ('InstanceHandleSeq_finalize',
        check_true, DDS_Boolean, [ctypes.POINTER(DDSType.InstanceHandleSeq)]),
    ('InstanceHandleSeq_get_length',
        None, DDS_Long, [ctypes.POINTER(DDSType.InstanceHandleSeq)]),
    ('InstanceHandleSeq_get_length',
        None, DDS_Long, [ctypes.POINTER(DDSType.InstanceHandleSeq)]),
    ('InstanceHandleSeq_get_reference',
        check_null, ctypes.POINTER(DDSType.InstanceHandle_t), [ctypes.POINTER(DDSType.InstanceHandleSeq), DDS_Long]),

    ('ParticipantBuiltinTopicDataDataReader_narrow',
        check_null, ctypes.POINTER(DDSType.ParticipantBuiltinTopicDataDataReader),
        [ctypes.POINTER(DDSType.DataReader)]),

    ('SubscriptionBuiltinTopicDataDataReader_narrow',
        check_null, ctypes.POINTER(DDSType.SubscriptionBuiltinTopicDataDataReade),
        [ctypes.POINTER(DDSType.DataReader)]),

    ('PublicationBuiltinTopicDataDataReader_narrow',
        check_null, ctypes.POINTER(DDSType.PublicationBuiltinTopicDataDataReader),
        [ctypes.POINTER(DDSType.DataReader)]),
    ('PublicationBuiltinTopicDataDataReader_take',
        check_code, DDS_ReturnCode_t,
        [ctypes.POINTER(DDSType.PublicationBuiltinTopicDataDataReader), ctypes.POINTER(DDSType.PublicationBuiltinTopicDataSeq), ctypes.POINTER(DDSType.SampleInfoSeq), DDS_Long, DDS_SampleStateMask, DDS_ViewStateMask, DDS_InstanceStateMask]),
    ('PublicationBuiltinTopicDataDataReader_return_loan',
        check_code, DDS_ReturnCode_t,
        [ctypes.POINTER(DDSType.PublicationBuiltinTopicDataDataReader), ctypes.POINTER(DDSType.PublicationBuiltinTopicDataSeq), ctypes.POINTER(DDSType.SampleInfoSeq)]),
    ('PublicationBuiltinTopicDataSeq_initialize',
        check_true, DDS_Boolean, [ctypes.POINTER(DDSType.PublicationBuiltinTopicDataSeq)]),
    ('PublicationBuiltinTopicDataSeq_finalize',
        check_true, DDS_Boolean, [ctypes.POINTER(DDSType.PublicationBuiltinTopicDataSeq)]),
    ('PublicationBuiltinTopicDataSeq_get_length',
        None, DDS_Long, [ctypes.POINTER(DDSType.PublicationBuiltinTopicDataSeq)]),
    ('PublicationBuiltinTopicDataSeq_get_reference',
        check_null, ctypes.POINTER(DDSType.PublicationBuiltinTopicData), [ctypes.POINTER(DDSType.PublicationBuiltinTopicDataSeq), DDS_Long]),





    ('Publisher_create_datawriter',
        check_null, ctypes.POINTER(DDSType.DataWriter),
        [ctypes.POINTER(DDSType.Publisher), ctypes.POINTER(DDSType.Topic), ctypes.POINTER(DDSType.DataWriterQos), ctypes.POINTER(DDSType.DataWriterListener), DDS_StatusMask]),
    ('Publisher_delete_datawriter',
        check_code, DDS_ReturnCode_t,
        [ctypes.POINTER(DDSType.Publisher), ctypes.POINTER(DDSType.DataWriter)]),

    ('Subscriber_create_datareader',
        check_null, ctypes.POINTER(DDSType.DataReader),
        [ctypes.POINTER(DDSType.Subscriber), ctypes.POINTER(DDSType.TopicDescription), ctypes.POINTER(DDSType.DataReaderQos), ctypes.POINTER(DDSType.DataReaderListener), DDS_StatusMask]),
    ('Subscriber_delete_datareader',
        check_code, DDS_ReturnCode_t,
        [ctypes.POINTER(DDSType.Subscriber), ctypes.POINTER(DDSType.DataReader)]),
    ('Subscriber_lookup_datareader',
        check_null, ctypes.POINTER(DDSType.DataReader),
        [ctypes.POINTER(DDSType.Subscriber), ctypes.c_char_p]),

    ('DataReader_set_listener',
        check_code, DDS_ReturnCode_t,
        [ctypes.POINTER(DDSType.DataReader), ctypes.POINTER(DDSType.DataReaderListener), DDS_StatusMask]),

    ('DynamicDataTypeSupport_new',
        check_null, ctypes.POINTER(DDSType.DynamicDataTypeSupport),
        [ctypes.POINTER(DDSType.TypeCode), ctypes.POINTER(DDSType.DynamicDataTypeProperty_t)]),
    ('DynamicDataTypeSupport_delete',
        None, None,
        [ctypes.POINTER(DDSType.DynamicDataTypeSupport)]),
    ('DynamicDataTypeSupport_register_type',
        check_code, DDS_ReturnCode_t,
        [ctypes.POINTER(DDSType.DynamicDataTypeSupport), ctypes.POINTER(DDSType.DomainParticipant), ctypes.c_char_p]),
    ('DynamicDataTypeSupport_unregister_type',
        check_code, DDS_ReturnCode_t,
        [ctypes.POINTER(DDSType.DynamicDataTypeSupport), ctypes.POINTER(DDSType.DomainParticipant), ctypes.c_char_p]),
    ('DynamicDataTypeSupport_create_data',
        check_null, ctypes.POINTER(DDSType.DynamicData),
        [ctypes.POINTER(DDSType.DynamicDataTypeSupport)]),
    ('DynamicDataTypeSupport_delete_data',
        check_code, DDS_ReturnCode_t,
        [ctypes.POINTER(DDSType.DynamicDataTypeSupport), ctypes.POINTER(DDSType.DynamicData)]),
    ('DynamicDataTypeSupport_print_data',
        None, None,
        [ctypes.POINTER(DDSType.DynamicDataTypeSupport), ctypes.POINTER(DDSType.DynamicData)]),

    ('DynamicData_new',
        check_null, ctypes.POINTER(DDSType.DynamicData),
        [ctypes.POINTER(DDSType.TypeCode), ctypes.POINTER(DDSType.DynamicDataProperty_t)]),
] + [
    ('DynamicData_get_' + func_name, check_code, DDS_ReturnCode_t, [ctypes.POINTER(DDSType.DynamicData), ctypes.POINTER(data_type), ctypes.c_char_p, DDS_DynamicDataMemberId])
        for func_name, data_type, bounds in _dyn_basic_types.itervalues()
] + [
    ('DynamicData_set_' + func_name, check_code, DDS_ReturnCode_t, [ctypes.POINTER(DDSType.DynamicData), ctypes.c_char_p, DDS_DynamicDataMemberId, data_type])
        for func_name, data_type, bounds  in _dyn_basic_types.itervalues()
] + [
    ('DynamicData_get_string',
        check_code, DDS_ReturnCode_t,
        [ctypes.POINTER(DDSType.DynamicData), ctypes.POINTER(ctypes.c_char_p), ctypes.POINTER(DDS_UnsignedLong), ctypes.c_char_p, DDS_DynamicDataMemberId]),
    ('DynamicData_get_ulong',
        check_code, DDS_ReturnCode_t,
        [ctypes.POINTER(DDSType.DynamicData), ctypes.POINTER(DDS_UnsignedLong), ctypes.c_char_p, DDS_DynamicDataMemberId]),
    ('DynamicData_get_wstring',
        check_code, DDS_ReturnCode_t,
        [ctypes.POINTER(DDSType.DynamicData), ctypes.POINTER(ctypes.c_wchar_p), ctypes.POINTER(DDS_UnsignedLong), ctypes.c_char_p, DDS_DynamicDataMemberId]),
    ('DynamicData_set_string',
        check_code, DDS_ReturnCode_t,
        [ctypes.POINTER(DDSType.DynamicData), ctypes.c_char_p, DDS_DynamicDataMemberId, ctypes.c_char_p]),
    ('DynamicData_set_wstring',
        check_code, DDS_ReturnCode_t,
        [ctypes.POINTER(DDSType.DynamicData), ctypes.c_char_p, DDS_DynamicDataMemberId, ctypes.c_wchar_p]),
    ('DynamicData_bind_complex_member',
        check_code, DDS_ReturnCode_t,
        [ctypes.POINTER(DDSType.DynamicData), ctypes.POINTER(DDSType.DynamicData), ctypes.c_char_p, DDS_DynamicDataMemberId]),
    ('DynamicData_unbind_complex_member',
        check_code, DDS_ReturnCode_t,
        [ctypes.POINTER(DDSType.DynamicData), ctypes.POINTER(DDSType.DynamicData)]),
    ('DynamicData_get_member_type',
        check_code, DDS_ReturnCode_t,
        [ctypes.POINTER(DDSType.DynamicData), ctypes.POINTER(ctypes.POINTER(DDSType.TypeCode)), ctypes.c_char_p, DDS_DynamicDataMemberId]),
    ('DynamicData_get_member_count',
        None, DDS_UnsignedLong,
        [ctypes.POINTER(DDSType.DynamicData)]),
    ('DynamicData_get_type',
        check_null, ctypes.POINTER(DDSType.TypeCode),
        [ctypes.POINTER(DDSType.DynamicData)]),
    ('DynamicData_get_type_kind',
        None, DDS_TCKind,
        [ctypes.POINTER(DDSType.DynamicData)]),
    ('DynamicData_delete',
        None, None,
        [ctypes.POINTER(DDSType.DynamicData)]),

    ('DynamicDataWriter_narrow',
        check_null, ctypes.POINTER(DDSType.DynamicDataWriter),
        [ctypes.POINTER(DDSType.DataWriter)]),
    ('DynamicDataWriter_write',
        check_code, DDS_ReturnCode_t,
        [ctypes.POINTER(DDSType.DynamicDataWriter), ctypes.POINTER(DDSType.DynamicData), ctypes.POINTER(DDSType.InstanceHandle_t)]),
    ('DynamicDataWriter_dispose',
        check_code, DDS_ReturnCode_t,
        [ctypes.POINTER(DDSType.DynamicDataWriter), ctypes.POINTER(DDSType.DynamicData), ctypes.POINTER(DDSType.InstanceHandle_t)]),


    ('DynamicDataReader_narrow',
        check_null, ctypes.POINTER(DDSType.DynamicDataReader),
        [ctypes.POINTER(DDSType.DataReader)]),
    ('DynamicDataReader_take',
        check_code, DDS_ReturnCode_t,
        [ctypes.POINTER(DDSType.DynamicDataReader), ctypes.POINTER(DDSType.DynamicDataSeq), ctypes.POINTER(DDSType.SampleInfoSeq), DDS_Long, DDS_SampleStateMask, DDS_ViewStateMask, DDS_InstanceStateMask]),
    ('DynamicDataReader_take_next_sample',
        check_code, DDS_ReturnCode_t,
        [ctypes.POINTER(DDSType.DynamicDataReader), ctypes.POINTER(DDSType.DynamicData), ctypes.POINTER(DDSType.SampleInfo)]),
    ('DynamicDataReader_return_loan',
        check_code, DDS_ReturnCode_t,
        [ctypes.POINTER(DDSType.DynamicDataReader), ctypes.POINTER(DDSType.DynamicDataSeq), ctypes.POINTER(DDSType.SampleInfoSeq)]),

    ('TypeCode_name',
        check_ex, ctypes.c_char_p, [ctypes.POINTER(DDSType.TypeCode), ctypes.POINTER(DDS_ExceptionCode_t)]),
    ('TypeCode_kind',
        check_ex, DDS_TCKind, [ctypes.POINTER(DDSType.TypeCode), ctypes.POINTER(DDS_ExceptionCode_t)]),
    ('TypeCode_member_count',
        check_ex, DDS_UnsignedLong, [ctypes.POINTER(DDSType.TypeCode), ctypes.POINTER(DDS_ExceptionCode_t)]),
    ('TypeCode_member_name',
        check_ex, ctypes.c_char_p, [ctypes.POINTER(DDSType.TypeCode), DDS_UnsignedLong, ctypes.POINTER(DDS_ExceptionCode_t)]),
    ('TypeCode_member_type',
        check_ex, ctypes.POINTER(DDSType.TypeCode), [ctypes.POINTER(DDSType.TypeCode), DDS_UnsignedLong, ctypes.POINTER(DDS_ExceptionCode_t)]),
    ('TypeCode_find_member_by_name',
        check_ex, DDS_UnsignedLong, [ctypes.POINTER(DDSType.TypeCode), ctypes.c_char_p, ctypes.POINTER(DDS_ExceptionCode_t)]),

    ('DynamicDataSeq_initialize',
        check_true, DDS_Boolean, [ctypes.POINTER(DDSType.DynamicDataSeq)]),
    ('DynamicDataSeq_finalize',
        check_true, DDS_Boolean, [ctypes.POINTER(DDSType.DynamicDataSeq)]),
    ('DynamicDataSeq_get_length',
        None, DDS_Long, [ctypes.POINTER(DDSType.DynamicDataSeq)]),
    ('DynamicDataSeq_get_reference',
        check_null, ctypes.POINTER(DDSType.DynamicData), [ctypes.POINTER(DDSType.DynamicDataSeq), DDS_Long]),

    ('SampleInfoSeq_initialize',
        check_true, DDS_Boolean, [ctypes.POINTER(DDSType.SampleInfoSeq)]),
    ('SampleInfoSeq_finalize',
        check_true, DDS_Boolean, [ctypes.POINTER(DDSType.SampleInfoSeq)]),
    ('SampleInfoSeq_get_length',
        None, DDS_Long, [ctypes.POINTER(DDSType.SampleInfoSeq)]),
    ('SampleInfoSeq_get_reference',
        check_null, ctypes.POINTER(DDSType.SampleInfo), [ctypes.POINTER(DDSType.SampleInfoSeq), DDS_Long]),

    ('String_free',
        None, None, [ctypes.c_char_p]),

    ('Wstring_free',
        None, None, [ctypes.c_wchar_p]),

    ('StringSeq_from_array',
        None, DDS_Boolean, [ctypes.POINTER(DDSType.StringSeq), ctypes.POINTER(ctypes.c_char_p), DDS_Long]),
    ('StringSeq_initialize',
        check_true, DDS_Boolean, [ctypes.POINTER(DDSType.StringSeq)]),
    ('StringSeq_finalize',
        check_true, DDS_Boolean, [ctypes.POINTER(DDSType.StringSeq)]),



    ('WaitSet_new', None, ctypes.POINTER(DDSType.WaitSet), []),
    ('WaitSet_attach_condition',
        check_code, DDS_ReturnCode_t,
        [ctypes.POINTER(DDSType.WaitSet), ctypes.POINTER(DDSType.Condition)]),
    ('WaitSet_wait',
        check_code, DDS_ReturnCode_t,
        [ctypes.POINTER(DDSType.WaitSet), ctypes.POINTER(DDSType.ConditionSeq), ctypes.POINTER(DDSType.Duration_t)]),

    ('Entity_get_statuscondition',
        None, ctypes.POINTER(DDSType.StatusCondition),
        [ctypes.POINTER(DDSType.Entity)]),
    ('StatusCondition_set_enabled_statuses',
        check_code, DDS_ReturnCode_t,
        [ctypes.POINTER(DDSType.StatusCondition), DDS_Long]),

    ('ConditionSeq_initialize',
        check_true, DDS_Boolean, [ctypes.POINTER(DDSType.ConditionSeq)]),
    ('ConditionSeq_finalize',
        check_true, DDS_Boolean, [ctypes.POINTER(DDSType.ConditionSeq)]),
])

def write_into_dd_member(obj, dd, member_name=None, member_id=DDS_DYNAMIC_DATA_MEMBER_ID_UNSPECIFIED):
    tc = ctypes.POINTER(DDSType.TypeCode)()
    dd.get_member_type(ctypes.byref(tc), member_name, member_id, ex())

    kind = tc.kind(ex())
    if kind in _dyn_basic_types:
        func_name, data_type, bounds = _dyn_basic_types[kind]
        if bounds is not None and not bounds[0] <= obj < bounds[1]:
            raise ValueError('%r not in range [%r, %r)' % (obj, bounds[0], bounds[1]))
        getattr(dd, 'set_' + func_name)(member_name, member_id, obj)
    elif kind == TCKind.STRUCT or kind == TCKind.SEQUENCE or kind == TCKind.ARRAY:
        inner = DDSFunc.DynamicData_new(None, get('DYNAMIC_DATA_PROPERTY_DEFAULT', DDSType.DynamicDataProperty_t))
        try:
            dd.bind_complex_member(inner, member_name, member_id)
            try:
                write_into_dd(obj, inner)
            finally:
                dd.unbind_complex_member(inner)
        finally:
            inner.delete()
    elif kind == TCKind.STRING:
        if '\0' in obj:
            raise ValueError('strings can not contain null characters')
        dd.set_string(member_name, member_id, obj)
    elif kind == TCKind.WSTRING:
        dd.set_wstring(member_name, member_id, obj)
    elif kind == TCKind.ENUM:
        assert isinstance(obj, str) or isinstance(obj, unicode)
        index = tc.find_member_by_name(obj, ex())
        dd.set_ulong(member_name, member_id, index)
    else:
        raise NotImplementedError(kind)

def write_into_dd(obj, dd):
    kind = dd.get_type_kind()
    if kind == TCKind.STRUCT:
        assert isinstance(obj, dict)
        tc = dd.get_type()
        for i in xrange(tc.member_count(ex())):
            name = tc.member_name(i, ex())
            write_into_dd_member(obj[name], dd, member_name=name)
    elif kind == TCKind.ARRAY or kind == TCKind.SEQUENCE:
        assert isinstance(obj, list)
        for i, x in enumerate(obj):
            write_into_dd_member(x, dd, member_id=i+1)
    else:
        raise NotImplementedError(kind)

def unpack_dd_member(dd, member_name=None, member_id=DDS_DYNAMIC_DATA_MEMBER_ID_UNSPECIFIED):
    tc = ctypes.POINTER(DDSType.TypeCode)()
    dd.get_member_type(ctypes.byref(tc), member_name, member_id, ex())

    kind = tc.kind(ex())
    if kind in _dyn_basic_types:
        func_name, data_type, bounds = _dyn_basic_types[kind]
        inner = data_type()
        getattr(dd, 'get_' + func_name)(ctypes.byref(inner), member_name, member_id)
        return inner.value
    elif kind == TCKind.STRUCT or kind == TCKind.SEQUENCE or kind == TCKind.ARRAY:
        inner = DDSFunc.DynamicData_new(None, get('DYNAMIC_DATA_PROPERTY_DEFAULT', DDSType.DynamicDataProperty_t))
        try:
            dd.bind_complex_member(inner, member_name, member_id)
            try:
                return unpack_dd(inner)
            finally:
                dd.unbind_complex_member(inner)
        finally:
            inner.delete()
    elif kind == TCKind.STRING:
        inner = ctypes.c_char_p(None)
        try:
            dd.get_string(ctypes.byref(inner), None, member_name, member_id)
            return inner.value
        finally:
            DDSFunc.String_free(inner)
    elif kind == TCKind.WSTRING:
        inner = ctypes.c_wchar_p(None)
        try:
            dd.get_wstring(ctypes.byref(inner), None, member_name, member_id)
            return inner.value
        finally:
            DDSFunc.Wstring_free(inner)
    elif kind == TCKind.ENUM:
        val = ctypes.c_uint()
        dd.get_ulong(ctypes.byref(val), member_name, member_id)
        return tc.member_name(val, ex())
    else:
        raise NotImplementedError(kind)

def unpack_dd(dd):
    kind = dd.get_type_kind()
    if kind == TCKind.STRUCT:
        obj = {}
        tc = dd.get_type()
        for i in xrange(tc.member_count(ex())):
            name = tc.member_name(i, ex())
            obj[name] = unpack_dd_member(dd, member_name=name)
        return obj
    elif kind == TCKind.ARRAY or kind == TCKind.SEQUENCE:
        obj = []
        for i in xrange(dd.get_member_count()):
            obj.append(unpack_dd_member(dd, member_id=i+1))
        return obj
    else:
        raise NotImplementedError(kind)

_outside_refs = set()
_refs = set()
_filtered_topic_refs = {}

class TopicSuper(object):
    def __init__(self, dds, name, data_type, related_topic=None, filter_expression=None):
        self._dds = dds
        self.name = name
        self.data_type = data_type
        self._related_topic = related_topic
        self._filter_expression = filter_expression
        self._data_seq = None
        self._info_seq = None

        self._support = support = DDSFunc.DynamicDataTypeSupport_new(self.data_type._get_typecode(), get('DYNAMIC_DATA_TYPE_PROPERTY_DEFAULT', DDSType.DynamicDataTypeProperty_t))
        self._type_name = self.data_type._get_typecode().name(ex())
        self._support.register_type(self._dds._participant, self._type_name)

        self._topic  = topic      = self._create_topic()
        self._writer = writer     = self._create_writer()
        self._dyn_narrowed_writer = DDSFunc.DynamicDataWriter_narrow(self._writer)
        self._listener            = None

        self._reader = reader = self._dds._subscriber.create_datareader(
            self._topic.as_topicdescription(),
            get('DATAREADER_QOS_DEFAULT', DDSType.DataReaderQos),
            self._listener,
            0,
        )
        self._dyn_narrowed_reader = DDSFunc.DynamicDataReader_narrow(self._reader)

        self._data_available_callback = None
        self._instance_revoked_cb     = None
        self._liveliness_lost_cb      = None

        if not _filtered_topic_refs.has_key(name): _filtered_topic_refs[name] = []

        def _cleanup(ref):
            if type(topic) is ctypes.POINTER(DDSType.Topic):
                dds._publisher.delete_datawriter(writer)
                dds._subscriber.delete_datareader(reader)
                for ft in _filtered_topic_refs[name]:
                    dds._publisher.delete_datawriter(ft._writer)
                    dds._subscriber.delete_datareader(ft._reader)
                    dds._participant.delete_contentfilteredtopic(ft._topic)
                dds._participant.delete_topic(topic)
                support.unregister_type(dds._participant, data_type._get_typecode().name(ex()))
                support.delete()
                del _filtered_topic_refs[name]
                _refs.remove(ref)

        _refs.add(weakref.ref(self, _cleanup))

    def _create_topic(self):
        raise NotImplementedError("You must make an instance of a subclass that implements this method")

    def _create_writer(self):
        raise NotImplementedError("You must make an instance of a subclass that implements this method")

    def _enable_listener(self):
        assert self._listener is None
        self._listener = DDSType.DataReaderListener(
            on_data_available=ctypes.CFUNCTYPE(None, ctypes.c_void_p, ctypes.POINTER(DDSType.DataReader))(self._on_data_available)
            # on_liveliness_changed=ctypes.CFUNCTYPE(None, ctypes.c_void_p, ctypes.POINTER(DDSType.DataReader), ctypes.POINTER(DDSType.LivelinessChangedStatus))(self._on_liveliness_changed)
        )
        self._reader.set_listener(self._listener, DATA_AVAILABLE_STATUS)
        _outside_refs.add(self) # really want self._listener, but this does the same thing

    def _on_liveliness_changed(self, listener_data, reader, status):
        print("\nstatus.alive_count:", status.alive_count,
              "\nstatus.not_alive_count:", status.not_alive_count,
              "\nstatus.alive_count_change:", status.alive_count_change,
              "\nstatus.not_alive_count_change:", status.not_alive_count_change)

    def _disable_listener(self):
        assert self._listener is not None
        self._reader.set_listener(None, 0)
        self._listener = None
        _outside_refs.remove(self)

    def add_data_available_callback(self, cb):
        '''Warning: callback is called back in another thread!'''
        if not self._data_available_callback:
            self._enable_listener()
        self._data_available_callback = cb

    def unsubscribe(self, topic):

        """
        Cancels a subscription made with `subscribe' with a given topic.

        Parameters:
            topic (Topic or ContentFilteredTopic) the topic to cancel the
                                                  subscription on. This value
                                                  is returned by `subscribe'
        """

        topic._data_available_callback = None
        if topic._listener:
            topic._disable_listener()

    def _on_data_available(self, listener_data, datareader):
        if not self._data_seq:
            self._data_seq = DDSType.DynamicDataSeq()
        self._data_seq.initialize()
        if not self._info_seq:
            self._info_seq = DDSType.SampleInfoSeq()
        self._info_seq.initialize()

        try:
            self._dyn_narrowed_reader.take(
                ctypes.byref(self._data_seq),
                ctypes.byref(self._info_seq),
                DDS_LENGTH_UNLIMITED,
                get('ANY_SAMPLE_STATE', DDS_SampleStateMask),
                get('ANY_VIEW_STATE', DDS_ViewStateMask),
                get('ANY_INSTANCE_STATE', DDS_InstanceStateMask)
            )

            for i in xrange(self._data_seq.get_length()):
                info = self._info_seq.get_reference(i).contents
                data = unpack_dd(self._data_seq.get_reference(i))

                if info.instance_state == DDS_NOT_ALIVE_DISPOSED_INSTANCE_STATE and self._instance_revoked_cb:
                    if self._send_topic_info: self._instance_revoked_cb(self._type_name, data)
                    else: self._instance_revoked_cb(data)

                if info.instance_state == DDS_NOT_ALIVE_NO_WRITERS_INSTANCE_STATE and self._liveliness_lost_cb:
                    if self._send_topic_info: self._liveliness_lost_cb(self._type_name, data)
                    else: self._liveliness_lost_cb(data)

                if info.instance_state == DDS_ALIVE_INSTANCE_STATE and info.valid_data and self._data_available_callback:
                    if self._send_topic_info:
                        data = {'name': self._type_name, 'data': data}
                    self._data_available_callback(data)

        except NoDataError:
            return

        finally:
            self._dyn_narrowed_reader.return_loan(ctypes.byref(self._data_seq), ctypes.byref(self._info_seq))
            self._data_seq.finalize()
            self._info_seq.finalize()

    def _generate_instance(self):
        sample = self._support.create_data()
        instance = unpack_dd(sample)
        self._support.delete_data(sample)
        return instance

    def _update(self, obj, data):
        for k, v in data.iteritems():
            if isinstance(v, collections.Mapping):
                r = self._update(obj.get(k, {}), v)
                obj[k] = r
            else:
                obj[k] = data[k]
        return obj

    def publish(self, data):

        """
        Publishes an instance of this topic on the DDS bus with the provided data.
        The input data may be sparse, but every entry in the data must be a field
        in the topic. If the provided data is sparse, a full instance of the topic
        will be published and the non-specified fields will receive default values.

        Parameters:
            data (Dict) the data to publish on the bus.
        """

        instance = self._update(self._generate_instance(), data)
        self._send(instance)

    def _send(self, msg):
        sample = self._support.create_data()

        try:
            write_into_dd(msg, sample)
            self._dyn_narrowed_writer.write(sample, DDS_HANDLE_NIL)
        finally:
            self._support.delete_data(sample)

class FilteredTopic(TopicSuper):
    def __init__(self, dds, name, data_type, related_topic, filter_expression):
        super(FilteredTopic, self).__init__(dds, name, data_type, related_topic, filter_expression)

    def _create_writer(self):
        return self._dds._publisher.create_datawriter(
            self._related_topic,
            get('DATAWRITER_QOS_DEFAULT', DDSType.DataWriterQos),
            None,
            0,
        )

    def _create_topic(self):
        self.filter_name = str(uuid.uuid4())
        self._filter_params = DDSType.StringSeq()
        # self._filter_params.initialize()
        # self._filter_params.from_array((ctypes.c_char_p * 0)(), 0)
        # DDSFunc.StringSeq_from_array(self._filter_params, (ctypes.c_char_p * 0)(), 0)  ## TODO: add in full support for parameterized filters

        return self._dds._participant.create_contentfilteredtopic(
            self.filter_name,
            self._related_topic,
            self._filter_expression,
            self._filter_params
        )

class Topic(TopicSuper):
    def __init__(self, dds, name, data_type):
        super(Topic, self).__init__(dds, name, data_type)
        self._filtered_topics = {}

    def _create_writer(self):
        return self._dds._publisher.create_datawriter(
            self._topic,
            get('DATAWRITER_QOS_DEFAULT', DDSType.DataWriterQos),
            None,
            0,
        )

    def _create_topic(self):
        return self._dds._participant.create_topic(
            self.name,
            self.data_type._get_typecode().name(ex()),
            get('TOPIC_QOS_DEFAULT', DDSType.TopicQos),
            None,
            0,
        )


    def subscribe(self, data_available_callback, instance_revoked_cb=None, liveliness_lost_cb=None, filter_expression=None, _send_topic_info=False):

        """
        Makes a DDS subscription for this topic with the provided callback.
        Optionally, 'instance revoked' and 'liveliness lost' callbacks may also
        be provided. If desired, a filter expression [1] can be specified and only topics
        matching the filter will be passed to the callback.

        To cancel a subscription, you call `unsubscribe' with a `topic' argument. This
        method returns the topic instance for this purpose.

        NOTE: currently filter parameters are not supported. Only provide filters
              without parameters!

        Parameters:
            data_available_callback  (function) Required. This function will be called with a
                                                dictionary containing the topic (name:value) pairs

            instance_revoked_cb      (function) Optional. This function will be called with a
                                                dictionary containing the topic (name:value) pairs

            liveliness_lost_cb       (function) Optional. This function will be called with a
                                                dictionary containing the topic (name:value) pairs

            filter_expression        (String)   Optional. The filter expression

        Returns:
            topic (Topic or ContentFilteredTopic) The topic to pass to `unsubscribe' if desired.

        [1] https://community.rti.com/static/documentation/connext-dds/5.2.0/doc/manuals/connext_dds/html_files/RTI_ConnextDDS_CoreLibraries_UsersManual/Content/UsersManual/SQL_Filter_Expression_Notation.htm
        """


        if filter_expression:
            filtered_topic = FilteredTopic(self._dds, self.name, self.data_type, self._topic, filter_expression)
            filtered_topic._instance_revoked_cb = instance_revoked_cb
            filtered_topic._liveliness_lost_cb  = liveliness_lost_cb
            filtered_topic._send_topic_info     = _send_topic_info
            filtered_topic.add_data_available_callback(data_available_callback)
            self._filtered_topics[filtered_topic.filter_name] = filtered_topic
            _filtered_topic_refs[self.name].append(filtered_topic)
            return filtered_topic
        else:
            self._send_topic_info     = _send_topic_info
            self._instance_revoked_cb = instance_revoked_cb
            self._liveliness_lost_cb  = liveliness_lost_cb
            self.add_data_available_callback(data_available_callback)
            return self

    def dispose(self, data):

        """
        Disposes a message instance. The provided message must have the 'key'
        fields set to the desired values. All topics of this type with matching
        keys will be disposed.

        Parameters:
            data (Dict) The provided message.
        """

        instance = self._update(self._generate_instance(), data)
        sample   = self._support.create_data()

        try:
            write_into_dd(instance, sample)
            self._dyn_narrowed_writer.dispose(sample, DDS_HANDLE_NIL)
        finally:
            self._support.delete_data(sample)

def subscribe_to_all_topics(topic_libraries, data_available_callback, instance_revoked_cb=None, liveliness_lost_cb=None):
    """
    Subscribes to all topics published on the DDS bus.
    It will subscribe to topics that are already publised and
    will also subscribe to new topics as they are published.

    The provided data_available_callback will be called with a
    dictionary of the form:
        {
            'name': <full topic name>,
            'data': <topic data (this is a dictionary)>
        }

    Parameters:
        topic_libraries         ([string] or string) The library of list of library names for the topics.
        data_available_callback (function)           The function to call with the topic data.
        instance_revoked_cb     (function)           The function to call if the topic instance is revoked. (Optional)
                                                     The function will be called with the topic name.
    """
    return DDS(topic_libraries,
            _get_all=True,
            _all_data_available_cb=data_available_callback,
            _all_ir_cb=instance_revoked_cb,
            _all_ll_cb=liveliness_lost_cb
    )


class DDS(object):
    """
    The main DDS interface.

    Parameters:
        topic_libraries ([String]) The list of topic libraries. If there is only one topic library,
                                   you may pass just the name instead of a list.
        qos_library     (String)   The name of the QOS library to use (Optional)
        qos_profile     (String)   The name of the QOS profile to use (Optional)
        domain_id       (Integer)  The domain ID (defaults to 0)
    """
    def __init__(self, topic_libraries, qos_library=None, qos_profile=None, domain_id=0,
                 _get_all=False, _all_data_available_cb=None, _all_ir_cb=None, _all_ll_cb=None):

        self._data_seq      = None
        self._info_seq      = None
        self._condition_seq = None
        self._initialized   = False

        if type(topic_libraries) != list:
            topic_libraries = [topic_libraries]

        if qos_library and qos_profile:
            DDSFunc.DomainParticipantFactory_get_instance().set_default_participant_qos_with_profile(qos_library, qos_profile)

        self._participant = participant = DDSFunc.DomainParticipantFactory_get_instance().create_participant(
            domain_id,
            get('PARTICIPANT_QOS_DEFAULT', DDSType.DomainParticipantQos),
            None,
            0,
        )

        if _get_all:
            self._all_data_available_cb = _all_data_available_cb or (lambda x: None)
            self._all_ir_cb             = _all_ir_cb             or (lambda x: None)
            self._all_ll_cb             = _all_ll_cb             or (lambda x: None)
            self._all_topics = {}
            self._builtin_subscriber = self._participant.get_builtin_subscriber()
            self._publication_dr = DDSFunc.PublicationBuiltinTopicDataDataReader_narrow(self._builtin_subscriber.lookup_datareader('DCPSPublication'))

            # I don't know why, but this initialization needs to happen here on Windows. Otherwise nddsc segfaults when the waitset fires.
            self._topics = Library(map(libname, topic_libraries))

            threading.Thread(target=self._get_all_topics).start()


        self._publisher = publisher = self._participant.create_publisher(
            get('PUBLISHER_QOS_DEFAULT', DDSType.PublisherQos),
            None,
            0,
        )

        self._subscriber = subscriber = self._participant.create_subscriber(
            get('SUBSCRIBER_QOS_DEFAULT', DDSType.SubscriberQos),
            None,
            0,
        )

        self._open_topics = weakref.WeakValueDictionary()
        if not _get_all:
            self._topics = Library(map(libname, topic_libraries))

        def _cleanup(ref):
            participant.delete_subscriber(subscriber)
            participant.delete_publisher(publisher)

            # very slow for some reason
            DDSFunc.DomainParticipantFactory_get_instance().delete_participant(participant)

            _refs.remove(ref)
        _refs.add(weakref.ref(self, _cleanup))

        if _get_all:
            for topic in self._all_topics:
                self._all_topics[topic] = self.get_topic(topic, sep='::')
                self._all_topics[topic].subscribe(
                    self._all_data_available_cb,
                    instance_revoked_cb=self._all_ir_cb,
                    liveliness_lost_cb=self._all_ll_cb,
                    _send_topic_info=True
                )
        self._initialized = True


    def _all_topics_data_available(self):

        self._data_seq.initialize()
        self._info_seq.initialize()
        self._publication_dr.take(
            ctypes.byref(self._data_seq),
            ctypes.byref(self._info_seq),
            DDS_LENGTH_UNLIMITED,
            get('ANY_SAMPLE_STATE', DDS_SampleStateMask),
            get('ANY_VIEW_STATE', DDS_ViewStateMask),
            get('ANY_INSTANCE_STATE', DDS_InstanceStateMask)
        )

        for i in xrange(self._data_seq.get_length()):
            pd = self._data_seq.get_reference(i)
            if pd.contents.type_name and pd.contents.type_name not in self._all_topics:
                if self._initialized:
                    self._all_topics[pd.contents.type_name] = self.get_topic(pd.contents.type_name, sep='::')
                    self._all_topics[pd.contents.type_name].subscribe(
                        self._all_data_available_cb,
                        instance_revoked_cb=self._all_ir_cb,
                        liveliness_lost_cb=self._all_ll_cb,
                        _send_topic_info=True
                    )
                else:
                    self._all_topics[pd.contents.type_name] = None


    def _get_all_topics(self):

        if not self._data_seq:
            self._data_seq = DDSType.PublicationBuiltinTopicDataSeq()
        self._data_seq.initialize()

        if not self._info_seq:
            self._info_seq = DDSType.SampleInfoSeq()
        self._info_seq.initialize()

        self.waitset = DDSFunc.WaitSet_new()
        self.condition = DDSFunc.Entity_get_statuscondition(ctypes.cast(self._publication_dr, ctypes.POINTER(DDSType.Entity)))
        self.waitset.attach_condition(ctypes.cast(self.condition, ctypes.POINTER(DDSType.Condition)))
        self.condition.set_enabled_statuses(DDS_DATA_AVAILABLE_STATUS)

        if not self._condition_seq:
            self._condition_seq = DDSType.ConditionSeq()
        self._condition_seq.initialize()

        while True:
            self.waitset.wait(
                ctypes.byref(self._condition_seq),
                ctypes.byref(DDSType.Duration_t(DDS_DURATION_INFINITE_SEC, DDS_DURATION_INFINITE_NSEC))
            )
            try:
                self._all_topics_data_available()

            finally:
                self._publication_dr.return_loan(ctypes.byref(self._data_seq), ctypes.byref(self._info_seq))
                self._info_seq.finalize()
                self._data_seq.finalize()


    def get_topic(self, qualified_name, sep='.'):
        name = qualified_name.split(sep)[-1]
        data_type = getattr(self._topics, qualified_name.replace(sep, '_'))
        return self._get_topic(name, data_type)

    def _get_topic(self, name, data_type):
        res = self._open_topics.get(name, None)
        if res is not None:
            if data_type != res.data_type:
                raise ValueError('_get_topic called with a previous name but a different data_type')
            return res
        res = Topic(self, name, data_type)
        self._open_topics[name] = res
        return res


class LibraryType(object):
    def __init__(self, libs, name):
        self._libs, self.name = libs, name
        del libs, name

        assert self._get_typecode().name(ex()).replace('::', '_') == self.name.replace('::', '_')

    def _get_typecode(self):
        for lib in self._libs:
            if hasattr(lib, self.name + '_get_typecode'):
                f = getattr(lib, self.name + '_get_typecode')
                f.argtypes = []
                f.restype = ctypes.POINTER(DDSType.TypeCode)
                f.errcheck = check_null
                return f()
        raise ValueError("Couldn't find the topic in the provided libraries")

class Library(object):
    def __init__(self, so_paths):
        self._libs = map(ctypes.CDLL, so_paths)

    def __getattr__(self, attr):
        res = LibraryType(self._libs, attr)
        setattr(self, attr, res)
        return res
