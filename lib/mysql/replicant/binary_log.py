# Copyright (c) 2010, Mats Kindahl, Charles Bell, and Lars Thalmann
# All rights reserved.
#
# Use of this source code is goverened by a BSD licence that can be
# found in the LICENCE file.

"""Module for browsing and parsing binary logs.
"""

UNKNOWN_EVENT = 0
START_EVENT = 1
QUERY_EVENT = 2
STOP_EVENT = 3
ROTATE_EVENT = 4 
INTVAR_EVENT = 5
LOAD_EVENT = 6
SLAVE_EVENT = 7
CREATE_FILE_EVENT = 8
APPEND_BLOCK_EVENT = 9
EXEC_LOAD_EVENT = 10
DELETE_FILE_EVENT = 11
NEWLOAD_EVENT = 12
RAND_EVENT = 13
USER_VAR_EVENT = 14
FORMAT_DESCRIPTION_EVENT = 15
XID_EVENT = 16
BEGIN_LOAD_QUERY_EVENT = 17
EXECUTE_LOAD_QUERY_EVENT = 18
TABLE_MAP_EVENT = 19
PRE_GA_WRITE_ROWS_EVENT = 20
PRE_GA_UPDATE_ROWS_EVENT = 21
PRE_GA_DELETE_ROWS_EVENT = 22
WRITE_ROWS_EVENT = 23
UPDATE_ROWS_EVENT = 24
DELETE_ROWS_EVENT = 25
INCIDENT_EVENT = 26
HEARTBEAT_EVENT = 27
IGNORABLE_EVENT = 28
ROWS_QUERY_EVENT = 29

import struct
import time

import mysql.replicant.errors as _errors

class _DecodeBuffer(object):
    """Helper class to decode a string by feeding it pieces of format
    strings.
    """
    def __init__(self, string, offset = 0):
        self.__string = string
        self.offset = offset

    def readfrm(self, frm):
        """Read a fixed-size format from a string starting at an
        offset.
        """
        if not isinstance(frm, struct.Struct):
            frm = struct.Struct(frm)
        result = frm.unpack_from(self.__string, self.offset)
        self.offset += frm.size
        return result

    def readstr(self, count = None):
        if count is None:
            count, = struct.unpack_from("<B", self.__string, self.offset)
            self.offset += 1
        result = self.__string[self.offset:self.offset + count]
        self.offset += count
        return result

_EVENT_FRM = """# at {0}
# {1} {2} - server ID: {3}, end_log_pos: {4}"""

class Event(object):
    "Base class for all events"

    type_name = None

    __slots__ = ('pos', 'when', 'type_code', 'server_id', 'size',
                 'end_pos', 'flags')

    def __init__(self, stub):
        self.pos = stub.pos
        self.when = stub.when
        self.type_code = stub.type_code
        self.server_id = stub.server_id
        self.size = stub.size
        self.end_pos = stub.end_pos
        self.flags = stub.flags
    
    def __str__(self):
        return self.to_string()

    def format(self, frm):
        l_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(self.when))
        frm.format({
                'pos': self.pos, 'type_name': self.type_name,
                'type_code': self.type_code, 'size': self.size,                
                'server_id': self.server_id, 'end_pos': self.end_pos,
                'datetime': l_time, 'when': self.when,
                })

    def to_string(self):
        return self._mkstr()

    def _mkstr(self, extras=None):
        if extras is None:
            extras = {}
        tstr = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(self.when))
        result = _EVENT_FRM.format(self.pos, tstr, self.type_name,
                                   self.server_id, self.end_pos)
        if len(extras) > 0:
            ext_str = ('{0}: {1}'.format(k, v) for (k, v) in extras.items())
            result += ", {0}\n".format(', '.join(ext_str))
        return result

class UnknownEvent(Event):
    "An Unknown event"

    type_name = "Unknown"

    def __init__(self, stub):
        Event.__init__(stub)

class StartEvent(Event):
    "A start event"

    type_name = "Start"

    def __init__(self, stub):
        super(StartEvent, self).__init__(stub)

class QueryEvent(Event):
    type_name = "Query"

    def __init__(self, stub):
        super(QueryEvent, self).__init__(stub)
        dbuf = _DecodeBuffer(stub.body)

        # Decode the post-header fields
        field = dbuf.readfrm("<LLBHH")
        self.thread_id = field[0]
        self.exec_time = field[1]
        db_len = field[2]
        self.error_code = field[3]

        # Fields after this are post-5.0 !
        # TODO: handle pre-5.0 events

        # Decode the status variables
        sv_end = field[4] + dbuf.offset
        while dbuf.offset < sv_end:
            code, = dbuf.readfrm("<B")
            if code == 0:                       # Q_FLAGS2_CODE
                self.flags2 = dbuf.readfrm("<L")[0]
            elif code == 1:                     # Q_SQL_MODE_CODE
                self.sql_mode = dbuf.readfrm("<Q")[0]
            elif code == 2:                     # Q_CATALOG_CODE
                assert code != 2                # Not handled yet
            elif code == 3:                     # Q_AUTO_INCREMENT
                val = dbuf.readfrm("<HH")
                self.autoinc = {
                    'increment': val[0],
                    'offset': val[1],
                    }
            elif code == 4:                     # Q_CHARSET_CODE
                self.charset = dbuf.readfrm("6B")[0]
            elif code == 5:                     # Q_TIME_ZONE_CODE
                self.timezone = dbuf.readstr()
            elif code == 6:                     # Q_CATALOG_NZ_CODE
                self.catalog = dbuf.readstr()
            elif code == 7:                     # Q_LC_TIME_NAMES_CODE
                self.time_names_no = dbuf.readfrm("<H")[0]
            elif code == 8:                   # Q_CHARSET_DATABASE_CODE
                self.db_no = dbuf.readfrm("<H")[0]
            elif code == 9:              # Q_TABLE_MAP_FOR_UPDATE_CODE
                self.table_map = dbuf.readfrm("<Q")[0]
            elif code == 10:              # Q_MASTER_DATA_WRITTEN_CODE
                self.data_written = dbuf.readfrm("<L")[0]
            elif code == 11:                    # Q_INVOKER
                self.invoker = {
                    'user': dbuf.readstr(),
                    'host': dbuf.readstr(),
                    }
            else:
                msg = "Unknown Status Variable Code: {0}".format(code)
                raise _errors.BadStatusVariableError(msg)
        self.database = dbuf.readstr(db_len)
        # Database name is NUL-terminated, so we skip that
        dbuf.offset += 1
        self.query = dbuf.readstr(stub.size - dbuf.offset)

    def to_string(self):
        result = super(QueryEvent, self)._mkstr({
            'thread_id': self.thread_id,
            'exec_time': self.exec_time,
            'error_code': self.error_code,
            })
        result += self.query  + ";"
        return result
        
class StopEvent(Event):
    type_name = "Stop"

    def __init__(self, stub):
        super(StopEvent, self).__init__(stub)
        

class RotateEvent(Event):
    type_name = "Rotate"

    def __init__(self, stub):
        super(RotateEvent, self).__init__(stub)
        dbuf = _DecodeBuffer(stub.body)
        self.next_pos = dbuf.readfrm("<Q")[0]
        self.next_file = dbuf.readstr(stub.size - dbuf.offset)

_INTVAR_TYPE = [
    { 'brief': 'Invalid int', 'ident': '*INVALID*' },
    { 'brief': 'Last insert ID', 'ident': 'LAST_INSERT_ID' },
    { 'brief': 'Insert ID', 'ident': 'INSERT_ID' },
    ]

class IntvarEvent(Event):
    type_name = "Intvar"
    INVALID_INT, LAST_INSERT_ID, INSERT_ID = range(0, 3)

    def __init__(self, stub):
        super(IntvarEvent, self).__init__(stub)
        dbuf = _DecodeBuffer(stub.body)
        self.variable, self.value = dbuf.readfrm("<BQ")

    def to_string(self):
        intvar_type = _INTVAR_TYPE[self.variable]
        result = super(IntvarEvent, self)._mkstr({
            'variable': intvar_type['brief'],
            })
        result += "SET {0} = {1};".format(intvar_type['ident'], self.value)
        return result

class LoadEvent(Event):
    type_name = "Load"

    def __init__(self, stub):
        super(LoadEvent, self).__init__(stub)

class SlaveEvent(Event):
    type_name = "Slave"

    def __init__(self, stub):
        super(SlaveEvent, self).__init__(stub)

class CreateFileEvent(Event):
    type_name = "CreateFile"

    def __init__(self, stub):
        super(CreateFileEvent, self).__init__(stub)

class AppendBlockEvent(Event):
    type_name = "AppendBlock"

    def __init__(self, stub):
        super(AppendBlockEvent, self).__init__(stub)

class ExecLoadEvent(Event):
    type_name = "ExecLoad"

    def __init__(self, stub):
        super(ExecLoadEvent, self).__init__(stub)

class DeleteFileEvent(Event):
    type_name = "DeleteFile"

    def __init__(self, stub):
        super(DeleteFileEvent, self).__init__(stub)

class NewLoadEvent(Event):
    type_name = "NewLoad"

    def __init__(self, stub):
        super(NewLoadEvent, self).__init__(stub)

class RandEvent(Event):
    type_name = "Rand"

    def __init__(self, stub):
        super(RandEvent, self).__init__(stub)

_VALUE_TYPE = [
    # STRING_RESULT
    { 'name': 'String',
      'decode': (lambda d,l: d.readstr(l)),
      },

    # REAL_RESULT
    { 'name': 'Real',
      'decode': (lambda d,l: d.readfrm("<d")[0]),
      },
    # INT_RESULT
    { 'name': 'Integer',
      'decode': (lambda d,l: d.readfrm("<Q")[0]),
      },
    # ROW_RESULT
    { 'name': 'Row' },

    # DECIMAL_RESULT
    { 'name': 'Decimal' },
    ]

class UservarEvent(Event):
    type_name = "Uservar"

    def __init__(self, stub):
        super(UservarEvent, self).__init__(stub)
        dbuf = _DecodeBuffer(stub.body)
        name_len, = dbuf.readfrm("<L")
        self.variable = dbuf.readstr(name_len)
        self.is_null, = dbuf.readfrm("?")
        if self.is_null:
            self.value = None
        else:
            val_type, self.charset, val_len = dbuf.readfrm("<BLL")
            self.__valtype = _VALUE_TYPE[val_type]
            self.value = self.__valtype['decode'](dbuf, val_len)

    def to_string(self):
        result = super(UservarEvent, self)._mkstr({
            'value_type': self.__valtype['name'],
            })
        result += "SET @`{0}` = {1};".format(self.variable, self.value)
        return result
        

class FormatDescriptionEvent(Event):
    type_name = "FormatDescription"

    def __init__(self, stub):
        super(FormatDescriptionEvent, self).__init__(stub)
        dbuf = _DecodeBuffer(stub.body)
        field = dbuf.readfrm("<H50sL")
        self.binlog_version = field[0]
        self.server_version = field[1].rstrip('\0')
        self.created = field[2]

class XidEvent(Event):
    type_name = "Xid"

    def __init__(self, stub):
        super(XidEvent, self).__init__(stub)

class BeginLoadQueryEvent(Event):
    type_name = "BeginLoadQuery"

    def __init__(self, stub):
        super(BeginLoadQueryEvent, self).__init__(stub)

class ExecuteLoadQueryEvent(Event):
    type_name = "ExecuteLoadQuery"

    def __init__(self, stub):
        super(ExecuteLoadQueryEvent, self).__init__(stub)

class TableMapEvent(Event):
    type_name = "TableMap"

    def __init__(self, stub):
        super(TableMapEvent, self).__init__(stub)

class PreGaWriteRowsEvent(Event):
    type_name = "PreGaWriteRows"

    def __init__(self, stub):
        super(PreGaWriteRowsEvent, self).__init__(stub)

class PreGaUpdateRowsEvent(Event):
    type_name = "PreGaUpdateRows"

    def __init__(self, stub):
        super(PreGaUpdateRowsEvent, self).__init__(stub)

class PreGaDeleteRowsEvent(Event):
    type_name = "PreGaDeleteRows"

    def __init__(self, stub):
        super(PreGaDeleteRowsEvent, self).__init__(stub)

class WriteRowsEvent(Event):
    type_name = "WriteRows"

    def __init__(self, stub):
        super(WriteRowsEvent, self).__init__(stub)

class UpdateRowsEvent(Event):
    type_name = "UpdateRows"

    def __init__(self, stub):
        super(UpdateRowsEvent, self).__init__(stub)

class DeleteRowsEvent(Event):
    type_name = "DeleteRows"

    def __init__(self, stub):
        super(DeleteRowsEvent, self).__init__(stub)

class IncidentEvent(Event):
    type_name = "Incident"

    def __init__(self, stub):
        super(IncidentEvent, self).__init__(stub)

class HeartbeatEvent(Event):
    type_name = "Heartbeat"

    def __init__(self, stub):
        super(HeartbeatEvent, self).__init__(stub)

class IgnorableEvent(Event):
    type_name = "Ignorable"

    def __init__(self, stub):
        super(IgnorableEvent, self).__init__(stub)

class RowsQueryEvent(Event):
    type_name = "RowsQuery"

    def __init__(self, stub):
        super(RowsQueryEvent, self).__init__(stub)

_CLASS_FOR = [
    UnknownEvent,
    StartEvent,
    QueryEvent,
    StopEvent,
    RotateEvent,
    IntvarEvent,
    LoadEvent,
    SlaveEvent,
    CreateFileEvent,
    AppendBlockEvent,
    ExecLoadEvent,
    DeleteFileEvent,
    NewLoadEvent,
    RandEvent,
    UservarEvent,
    FormatDescriptionEvent,
    XidEvent,
    BeginLoadQueryEvent,
    ExecuteLoadQueryEvent,
    TableMapEvent,
    PreGaWriteRowsEvent,
    PreGaUpdateRowsEvent,
    PreGaDeleteRowsEvent,
    WriteRowsEvent,
    UpdateRowsEvent,
    DeleteRowsEvent,
    IncidentEvent,
    HeartbeatEvent,
    IgnorableEvent,
    RowsQueryEvent,
    ]


class Stub(object):             # pylint: disable=R0902
    """An undecoded event.
    """

    HEADER_LENGTH = 19

    def __init__(self, istream):
        """Read the common header into the class and also fetch the
        rest of the event bytes.
        """

        self.pos = istream.tell()
        header = istream.read(self.HEADER_LENGTH)
        if (len(header) < self.HEADER_LENGTH):
            raise EOFError("Stream empty")
        field = struct.unpack("<LBLLLH", header)
        self.when = field[0]
        self.type_code = field[1]
        self.server_id = field[2]
        self.size = field[3]
        self.end_pos = field[4]
        self.flags = field[5]
        self.body = istream.read(self.size - self.HEADER_LENGTH)

    def __str__(self):
        tstr = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(self.when))
        return _EVENT_FRM.format(self.pos, tstr,
                                 _CLASS_FOR[self.type_code].type_name,
                                 self.server_id, self.end_pos)

    def decode(self):
        try:
            return _CLASS_FOR[self.type_code](self)
        except IndexError:
            return UnknownEvent(self)

class Reader(object):
    "Base class for all readers."
    def __init__(self, istream):
        self.istream = istream

class FileReader(Reader):
    """Class to read the binary log from files.

    To use this file, provide the base name for the binary log. The
    base name will be used to locate the index file, and the binary
    log will be extracted by reading the files mentioned in the index
    file.
    """

    MAGIC = "\xFEbin"

    def __init__(self, filename):
        instream = open(filename, 'rb')
        super(FileReader, self).__init__(instream)
        magic = self.istream.read(4)
        if magic != self.MAGIC:
            raise _errors.BadMagicError("Incorrect magic bytes for file")

# TODO: Add a MySQL Reader
_READER = {
    'file': FileReader,
    }

def create_reader(url):
    """Create a reader given a URL.

    This function will create a reader from a URL given. There are
    three protocols available:

    file
       Create a reader to read from the given file.
    index
       Create a reader that reads the files in the given index file.
    mysql
       Create a reader that connects to a server and request a dump of
       the binary logs.
    """
    try:
        scheme, rest = url.split(':', 1)
        return _READER[scheme](rest)
    except ValueError:                          # Split failed
        return _READER['file'](url)
    except KeyError:                            # No reader
        msg = "'{0}' is not a recognized scheme".format(scheme)
        raise _errors.UnrecognizedSchemeError(msg)

class BinaryLog(object):
    "Container for sequence of events"

    def __init__(self, reader):
        """Create a binary log.
        
        If a string is provided, it is assumed to be a URL and is used
        to construct a reader for reading events. Any other value is
        assumed to behave as a Reader and used directly.
        """
        if (isinstance(reader, basestring)):
            reader = create_reader(reader)
        self.__reader = reader
        self.format_description = None

    def events(self):
        try:
            event = Stub(self.__reader.istream)
            self.format_description = event.decode()
            yield event
            while True:
                yield Stub(self.__reader.istream)
        except EOFError:
            pass
