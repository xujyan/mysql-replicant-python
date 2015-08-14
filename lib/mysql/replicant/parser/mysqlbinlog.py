# Copyright (c) 2010, Mats Kindahl, Charles Bell, and Lars Thalmann
# All rights reserved.
#
# Use of this source code is goverened by a BSD licence that can be
# found in the LICENCE file.

"""Module holding support for reading the output of mysqlbinlog and
present it using an API.

"""

import subprocess
import re
import time

from .. import errors

class UnrecognizedFormatError(errors.Error):
    """Exception thrown when an unrecognizable format is encountered."""
    pass


def _match_regex(regex, string):
    """Match a regular expression on a string and return the groups as
    a tuple. Throw an UnrecognizedFormatError if it does not match."""
    
    mobj = regex.match(string)
    if not mobj:
        raise UnrecognizedFormatError, string
    return mobj.groups()

class LogEvent(object):
    """Base class for all binary log events.

    """

    def __init__(self, event_type, start_pos, end_pos, timestamp, server_id):
        self.event_type = event_type
        self.start_pos = start_pos
        self.end_pos = end_pos
        self.timestamp = timestamp
        self.server_id = server_id

    @property
    def size(self):
        """
        Compute the size of the event as a derived attribute.
        """
        return self.end_pos - self.start_pos

class QueryEvent(LogEvent):
    """
    Class for representing query log events.
    """
    
    def __init__(self, event_type, start_log_pos, end_log_pos, server_id,
                 timestamp, thread_id, exec_time, error_code, query):
        LogEvent.__init__(self, event_type, start_log_pos, end_log_pos,
                          timestamp, server_id)
        self.thread_id = thread_id
        self.exec_time = exec_time
        self.error_code = error_code
        self.query = query

class IntvarEvent(LogEvent):
    """
    Integer variable event.
    """
    def __init__(self, event_type, start_log_pos, end_log_pos, timestamp,
                 server_id, name, value):
        LogEvent.__init__(self, event_type, start_log_pos, end_log_pos,
                          timestamp, server_id)
        self.name = name
        self.value = value

class UservarEvent(LogEvent):
    """
    User variable event.
    """
    def __init__(self, event_type, start_log_pos, end_log_pos, timestamp,
                 server_id, name, value):
        LogEvent.__init__(self, event_type, start_log_pos, end_log_pos,
                          timestamp, server_id)
        self.name = name
        self.value = value

class XidEvent(LogEvent):
    """
    XID event 
    """

    def __init__(self, event_type, start_log_pos, end_log_pos, timestamp,
                 server_id, xid):
        LogEvent.__init__(self, event_type, start_log_pos, end_log_pos,
                          timestamp, server_id)
        self.xid = xid

class StartEvent(LogEvent):
    """
    Start event or format description log event.
    """
    def __init__(self, event_type, start_log_pos, end_log_pos, timestamp,
                 server_id, binlog_version, server_version):
        LogEvent.__init__(self, event_type, start_log_pos, end_log_pos,
                          timestamp, server_id)
        self.binlog_version = binlog_version
        self.server_version = server_version

class UnknownEvent(LogEvent):
    """An unknown event"""
    def __init__(self, event_type, start_log_pos, end_log_pos, timestamp,
                 server_id, binlog_version, server_version):
        LogEvent.__init__(self, event_type, start_log_pos, end_log_pos,
                          timestamp, server_id)


class LogEventReader(object):
    """Base class for log event readers.

    All event readers inherit from this class.

    """

    def __init__(self, event_type, start_pos, end_pos, server_id,
                 timestamp, delimiter):
        self.event_type = event_type
        self.start_pos = int(start_pos)
        self.end_pos = int(end_pos)
        self.server_id = int(server_id)
        self.timestamp = time.strptime(timestamp, "%y%m%d %H:%M:%S")
        self.delimiter = delimiter
        self.line = None

    def read(self, rest, istream):
        return None

    def _eat_until_next_event(self, istream):
        """Read lines until it matches the beginning of a new event"""
        mobj, line = None, None
        while not mobj:
            line = istream.next()
            mobj = re.match(r'# at (\d+)', line)
        self.line = line

class UnrecognizedEventReader(LogEventReader):
    """
    Reader that will always return an event representing 
    """
    def __init__(self, event_type, start_pos, end_pos, server_id,
                 timestamp, delimiter):
        LogEventReader.__init__(self, event_type, start_pos, end_pos,
                                server_id, timestamp, delimiter)
        self.line = None

    def read(self, rest, istream):
        self._eat_until_next_event(istream)
        return UnknownEvent(self.event_type, self.start_pos, self.end_pos,
                            self.server_id, self.timestamp)

class QueryEventReader(LogEventReader):
    """
    Query event reader class.
    """

    TYPE_STRING = "Query"

    _QUERY_CRE = re.compile(r'\s*thread_id=(\d+)\s+'
                            r'exec_time=(\d+)\s+'
                            r'error_code=(\d+)\s+')

    def __init__(self, event_type, start_pos, end_pos, server_id,
                 timestamp, delimiter):
        LogEventReader.__init__(self, event_type, start_pos, end_pos,
                                server_id, timestamp, delimiter)
        self.line = None

    def read(self, rest, istream):
        """
        Will return a new event and the first line that is not part of
        the event, or None if there are no more lines.
        """

        thread_id, exec_time, error_code = _match_regex(self._QUERY_CRE, rest)

        # After the header line, there is a sequence of lines ending
        # in the delimiter. These are generated by mysqlbinlog to
        # encode special information.
        self.line = istream.next()
        while self.line[-len(self.delimiter):] == self.delimiter:
            self.line = istream.next()

        # Now comes the query, consisting of one or more lines ending
        # with a single line with the delimiter.
        query_lines = []
        while self.line[:-1] != self.delimiter:
            query_lines.append(self.line)
            self.line = istream.next()

        # Then we need to ensure that self.line is the next line that
        # is not part of the event.
        self.line = istream.next()

        return QueryEvent(self.event_type, self.start_pos, self.end_pos,
                          self.server_id, self.timestamp,
                          thread_id, exec_time, error_code,
                          ''.join(query_lines))

class IntvarEventReader(LogEventReader):
    """Reader to read an Intvar event for handling auto_increment values."""

    TYPE_STRING = "Intvar"

    def __init__(self, event_type, start_pos, end_pos, server_id,
                 timestamp, delimiter):
        LogEventReader.__init__(self, event_type, start_pos, end_pos,
                                server_id, timestamp, delimiter)

    def read(self, rest, istream):
        self.line = istream.next()
        mobj = re.match("set\s+(\w+)\s*=\s*(.+)", self.line, re.IGNORECASE)
        if not mobj:
            raise UnrecognizedFormatError, self.line
        name, value = mobj.groups()
        self._eat_until_next_event(istream)
        return IntvarEvent(self.event_type, self.start_pos, self.end_pos,
                           self.server_id, self.timestamp, name, value)

class UservarEventReader(LogEventReader):
    """Reader for processing a user variable assignment event."""

    TYPE_STRING = "User_var"

    def __init__(self, event_type, start_pos, end_pos, server_id,
                 timestamp, delimiter):
        LogEventReader.__init__(self, event_type, start_pos, end_pos,
                                server_id, timestamp, delimiter)
        
    def read(self, rest, istream):
        self.line = istream.next()
        mobj = re.match("set\s+(@\S+)\s*:=\s*(.+)", self.line, re.IGNORECASE)
        if not mobj:
            raise UnrecognizedFormatError, self.line
        name, value = mobj.groups()
        self._eat_until_next_event(istream)
        return UservarEvent(self.event_type, self.start_pos, self.end_pos,
                            self.server_id, self.timestamp, name, value)
            

class XidEventReader(LogEventReader):
    TYPE_STRING = "Xid"

    def __init__(self, event_type, start_pos, end_pos, server_id,
                 timestamp, delimiter):
        LogEventReader.__init__(self, event_type, start_pos, end_pos,
                                server_id, timestamp, delimiter)

    def read(self, rest, istream):
        mobj = re.match("\s*=\s*(\d+)", rest)
        if not mobj:
            raise UnrecognizedFormatError, rest
        xid = mobj.groups()
        self._eat_until_next_event(istream)
        return XidEvent(self.event_type, self.start_pos, self.end_pos,
                        self.server_id, self.timestamp, xid)

class StartEventReader(LogEventReader):
    """Start event reader.

    Class to read a start event from the textual output of mysqlbinlog.
    """
    TYPE_STRING = "Start"

    _START_CRE = re.compile(r': binlog v\s*(\d+),\s*'
                            r'server v\s*(\S+)\s+'
                            r'created\s*(\d{6}\s+\d?\d:\d\d:\d\d)')

    def __init__(self, event_type, start_pos, end_pos, server_id,
                 timestamp, delimiter):
        LogEventReader.__init__(self, event_type, start_pos, end_pos,
                                server_id, timestamp, delimiter)
        self.line = None

    def read(self, rest, istream):
        matches = _match_regex(self._START_CRE, rest)
        binlog_version, server_string = matches[0:2]
        self._eat_until_next_event(istream)
        return StartEvent(self.event_type, self.start_pos, self.end_pos,
                          self.server_id, self.timestamp,
                          binlog_version, server_string)

# We could probably loop the dictionary and find all the events of the
# form "...Reader", but I prefer to be explicit here and name the
# classes.
_READER = {
    QueryEventReader.TYPE_STRING: QueryEventReader,
    IntvarEventReader.TYPE_STRING: IntvarEventReader,
    UservarEventReader.TYPE_STRING: UservarEventReader,
    XidEventReader.TYPE_STRING: XidEventReader,
    StartEventReader.TYPE_STRING: StartEventReader,
}


_TYPE_CRE = re.compile(r'#(\d{6}\s+\d?\d:\d\d:\d\d)\s+' # Datetime
                       r'server id\s+(\d+)\s+'          # Server ID
                       r'end_log_pos\s+(\d+)\s+'        # End log pos
                       r'(\w+)')                        # Type
                       
def read_events(istream):
    """
    Generator that accepts a stream of input lines and parses it into
    a sequence of events.

    It assumes that the input stream represents the complete output
    from mysqlbinlog and will first read the header, continuing with
    each individual event.
    """

    # Read header
    mobj = None
    while not mobj:
        line = istream.next()
        mobj = re.match(r'delimiter (\S+)', line, re.IGNORECASE)

    delimiter = mobj.group(1)

    while not re.match(r'# at \d+', line):
        line = istream.next()

    while line:
        # Here, line matches "# at \d+" or "DELIMITER ..."
        mobj = re.match(r'# at (\d+)', line)
        if not mobj:
            if re.match("DELIMITER", line):
                return          # End of binlog file
            else:
                raise UnrecognizedFormatError, line
        bytepos = mobj.group(1)

        # Read the type by reading the beginning of the next line
        line = istream.next()
        mobj = _TYPE_CRE.match(line)
        if not mobj:
            raise UnrecognizedFormatError, line

        # Fetch the correct reader class and initialize it with the
        # remains of the line and passing in the file handle to allow
        # it to read lines.
        reader_class = _READER.get(mobj.group(4), UnrecognizedEventReader)
        reader = reader_class(mobj.group(4), bytepos, mobj.group(3),
                              mobj.group(2), mobj.group(1), delimiter)
        yield reader.read(line[mobj.end():], istream)
        line = reader.line

def mysqlbinlog(files, user=None, passwd=None, host="localhost", port=3306):
    command = [
        "mysqlbinlog",
        "--force",
        "--read-from-remote-server",
        "--host=%s" % (host,),
        "--port=%s" % (port,),
        ]
    if user:
        command.append("--user=%s" % user)
    if passwd:
        command.append("--password=%s" % passwd)
        
    proc = subprocess.Popen(command + files, stdout=subprocess.PIPE)

    read_events(proc.stdout)


