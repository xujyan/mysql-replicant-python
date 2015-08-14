# Copyright (c) 2010, Mats Kindahl, Charles Bell, and Lars Thalmann
# All rights reserved.
#
# Use of this source code is goverened by a BSD licence that can be
# found in the LICENCE file.

"""Module holding server definitions

"""

import MySQLdb as _connector
import collections
import warnings
import copy

from uuid import UUID

from mysql.replicant import (
    configmanager,
    roles,
    errors,
    )

class Position(collections.namedtuple('Position', 'file,pos')):
    """A binlog position for a specific server.

    """

    def __cmp__(self, other):
        """Compare two positions lexicographically.  If the positions
        are from different servers, a ValueError exception will be
        raised.
        """
        return cmp((self.file, self.pos), (other.file, other.pos))

User = collections.namedtuple('User', 'name,passwd')

GTID = collections.namedtuple('GTID', 'uuid,gid')

def _normalize(rngs):
    """Normalize a list of ranges by merging ranges, if possible, and
    turning single-position ranges into tuples.

    The normalization is sort the ranges first on the tuples, which
    makes comparisons easy when merging range sets.
    """

    result = []
    last = None
    for rng in sorted(rngs):
        if len(rng) == 1:
            rng = (rng[0], rng[0])
        if last is None:
            last = rng
        elif rng[1] <= last[1]:
            pass
        elif rng[0] <= last[1] or last[1] + 1 >= rng[0]:
            last = (last[0], max(rng[1], last[1]))
        else:
            result.append(last)
            last = rng
    result.append(last)
    return result

    
def _compare_sets(lhs, rhs):
    """Compare two GTID sets.

    Return a tuple (lhs, rhs) where lhs is a boolean indicating that
    the left hand side had at least one more item than the right hand
    side, and vice verse.
    """

    lcheck, rcheck = False, False

    # Create a union of the lhs and rhs for comparison
    both = copy.deepcopy(lhs)
    both.union(rhs)

    for uuid, rngs in both._GTIDSet__gtids.items():
        if lcheck and rcheck:
            return lcheck, rcheck     # They are incomparable, just return

        def _inner_compare(gtid_set):
            if uuid not in gtid_set._GTIDSet__gtids:
                return True # UUID not in lhs ==> right hand side has more
            else:
                for rng1, rng2 in zip(rngs, gtid_set._GTIDSet__gtids[uuid]):
                    if rng1 != rng2:
                        return True
            return False

        if _inner_compare(lhs):
            rcheck = True
        if _inner_compare(rhs):
            lcheck = True

    return lcheck, rcheck

class GTIDSet(object):
    def __init__(self, obj):
        gtids = {}
        if not isinstance(obj, basestring):
            obj = str(obj)      # Try to make it into a string that we parse

        # Parse the string and construct a GTID set
        for uuid_set in obj.split(','):
            parts = uuid_set.split(':')

            # This fandango is done to handle other forms of UUID that
            # the UUID class can handle. We, however, use the standard
            # form for our UUIDs.
            uuid = str(UUID(parts.pop(0)))

            if len(parts) == 0 or not parts[0]:
                raise ValueError("At least one range have to be provided")
            rngs = [ tuple(int(x) for x in part.split('-')) for part in parts ]
            for rng in rngs:
                if len(rng) > 2 or len(rng) == 2 and int(rng[0]) > int(rng[1]):
                    raise ValueError("Range %s in '%s' is not a valid range" % (
                            '-'.join(str(i) for i in rng), rng
                            ))
            gtids[uuid] = _normalize(rngs)
        self.__gtids = gtids

    def __str__(self):
        sets = []
        for uuid, rngs in sorted(self.__gtids.items()):
            uuid_set = ':'.join(
                [str(uuid)] + [ '-'.join(str(i) for i in rng) for rng in rngs ]
                )
            sets.append(uuid_set)
        return ','.join(sets)

    def union(self, other):
        """Compute the union of this GTID set and the GTID set in
        other.

        The update of the GTID set is done in-place, so if you want to
        compute the union of two sets 'lhs' and 'rhs' you have to do
        something like::

           result = copy.deepcopy(lhs)
           result.union(rhs)
           
        """

        # If it wasn't already a GTIDSet, try to make it one.
        if not isinstance(other, GTIDSet):
            other = GTIDSet(other)

        gtids = self.__gtids
        for uuid, rngs in other.__gtids.items():
            if uuid not in gtids:
                gtids[uuid] = rngs
            else:
                gtids[uuid] = _normalize(gtids[uuid] + rngs)
        self.__gtids = gtids

    def __lt__(self, other):
        lhs, rhs = _compare_sets(self, other)
        return not lhs and rhs 

    def __le__(self, other):
        lhs, _ = _compare_sets(self, other)
        return not lhs

    def __eq__(self, other):
        lhs, rhs = _compare_sets(self, other)
        return not (lhs or rhs)

    def __ne__(self, other):
        return not self.__eq__(other)

    def __ge__(self, other):
        return other.__le__(self)

    def __gt__(self, other):
        return other.__lt__(self)

    def __or__(self, other):
        result = copy.deepcopy(self)
        result.union(other)

class Server(object):
    """A representation of a MySQL server.

    A server object is used as a proxy for operating with the
    server. The basic primitives include connecting to the server and
    executing SQL statements and/or shell commands."""

    class Row(object):
        """A row (iterator) returned when executing an SQL statement.

        For statements that return a single row, the object can be
        treated as a row as well.

        """

        def __init__(self, cursor):
            self.__cursor = cursor
            self.__row = cursor.fetchone()

        def __iter__(self):
            return self

        def next(self):
            row = self.__row
            if row is None:
                raise StopIteration
            else:
                self.__row = self.__cursor.fetchone()
                return row
        
        def __getitem__(self, key):
            from mysql.replicant.errors import EmptyRowError
            if self.__row is not None:
                return self.__row[key]
            else:
                raise EmptyRowError

        def __str__(self):
            from mysql.replicant.errors import EmptyRowError
            if len(self.__row) == 1:
                return str(self.__row.values()[0])
            else:
                raise EmptyRowError
    
    def __init__(self, name, sql_user, machine, ssh_user=None,
                 config_manager=configmanager.ConfigManagerFile(),
                 role=roles.Vagabond(), 
                 server_id=None, host='localhost', port=3306,
                 socket='/tmp/mysqld.sock', defaults_file=None,
                 config_section='mysqld'):
        """Initialize the server object with data.

        If a configuration file path is provided, it will be used to
        read server options that were not provided. There are three
        mandatory options:

        sql_user
           This is a user for connecting to the server to execute SQL
           commands. This is a MySQL server user.

        ssh_user
           This is a user for connecting to the machine where the
           server is installed in order to perform tasks that cannot
           be done through the MySQL client interface.

        machine
           This is a machine object for performing basic operations on
           the server such as starting and stopping the server.

        The following additional keyword parameters are used:

        name
           This parameter is used to create names for the pid-file,
           log-bin, and log-bin-index options. If it is not provided,
           the name will be deduced from the pid-file, log-bin, or
           log-bin-index (in that order), or a default will be used.

        host
           The hostname of the server, which defaults to 'localhost',
           meaning that it will connect using the socket and not
           TCP/IP.

        socket
           Socket to use when connecting to the server. This parameter
           is only used if host is 'localhost'. It defaults to
           '/tmp/mysqld.sock'.

        port
           Port to use when connecting to the server when host is not
           'localhost'. It defaults to 3306.

        server_id
           Server ID to use. If none is assigned, the server ID is
           fetched from the configuration file. If the configuration
           files does not contain a server ID, no server ID is
           assigned.

        """

        if not defaults_file:
            defaults_file = machine.defaults_file

        self.name = name
        self.sql_user = sql_user
        self.ssh_user = ssh_user

        # These attributes are explicit right now, we have to
        # implement logic for fetching them from the configuration if
        # necessary.
        self.host = host
        self.port = port
        self.server_id = server_id
        self.socket = socket
        self.defaults_file = defaults_file
        
        self.config_section = config_section

        self.__machine = machine
        self.__config_manager = config_manager
        self.__conn = None
        self.__config = None
        self.__tmpfile = None
        self.__warnings = None

        self.__role = role
        self.imbue(role)
            
    def _connect(self, database=''):
        """Method to connect to the server, preparing for execution of
        SQL statements.  If a connection is already established, this
        function does nothing."""
        if not self.__conn:
            self.__conn = _connector.connect(
                host=self.host, port=self.port,
                unix_socket=self.socket,
                db=database,
                user=self.sql_user.name,
                passwd=self.sql_user.passwd)
        elif database:
            self.__conn.select_database(database)
                                      
    def imbue(self, role):
        """Imbue a server with a new role."""
        self.__role.unimbue(self)
        self.__role = role
        self.__role.imbue(self)
        
    def disconnect(self):
        """Method to disconnect from the server."""
        self.__conn = None
        return self
                                      
    def sql(self, command, args=None, database=''):
        """Execute a SQL command on the server.

        This first requires a connection to the server.

        The function will return an iteratable to the result of the
        execution with one row per iteration.  The function can be
        used in the following way::

           for database in server.sql("SHOW DATABASES")
              print database["Database"]

         """

        self._connect(database)
        cur = self.__conn.cursor(_connector.cursors.DictCursor)
        with warnings.catch_warnings(record=True) as warn:
            cur.execute(command, args)
            self.__warnings = warn
        return Server.Row(cur)

    def ssh(self, command):
        """Execute a shell command on the server.

        The function will return an iteratable (currently a list) to
        the result of the execution with one line of the output for
        each iteration.  The function can be used in the following
        way:

        for line in server.ssh(["ls"])
            print line

        For remote commands we do not allow X11 forwarding, and the
        stdin will be redirected to /dev/null.

        """

        from subprocess import Popen, PIPE, STDOUT

        if self.host == "localhost":
            cmd = ["sudo", "-u" + self.ssh_user.name] + command
            process = Popen(cmd, stdout=PIPE, stderr=STDOUT)
        else:
            fullname = self.ssh_user.name + "@" + self.host
            process = Popen(["ssh", "-fqTx", fullname, ' '.join(command)],
                            stdout=PIPE, stderr=STDOUT)
        output = process.communicate()[0]
        return output.split("\n")

    def fetch_config(self, path=None):
        return self.__config_manager.fetch_config(self, path)

    def replace_config(self, config, path=None):
        self.__config_manager.replace_config(self, config, path)
        return self

    def stop(self):
        self.__machine.stop_server(self)
        return self

    def start(self):
        self.__machine.start_server(self)
        return self

