# Copyright (c) 2010, Mats Kindahl, Charles Bell, and Lars Thalmann
# All rights reserved.
#
# Use of this source code is goverened by a BSD licence that can be
# found in the LICENCE file.

import ConfigParser
import MySQLdb as _connector

class Role(object):
    """Base class for representing a server role.

    The responsibility of a role is to configure a server for working
    in that role. The configuration might involve changing the
    configuration file for the server as well as setting variables for
    the server.

    Note that the role only is effective in the initial deployment of
    the server. The reason is that the role of a server may change
    over the lifetime of the server, so there is no way to enforce a
    server to stay in a particular role.

    Each role is imbued into a server using the call::

       role.imbue(server)
    """
    def _set_server_id(self, server, config):
        """A helper method that will set the server id unless is was
        already set. In that case, we correct our own server id to be
        what the configuration file says."""
        try:
            server.server_id = config.get('server-id')
        except ConfigParser.NoOptionError:
            config.set('server-id', server.server_id)

        
    def _create_repl_user(self, server, user):
        """Helper method to create a replication user for the
        server.

        The replication user will then be set as an attribute of the
        server so that it is available for slaves connecting to the
        server."""
        try:
            server.sql("DROP USER %s", (user.name))
            server.sql("CREATE USER %s IDENTIFIED BY %s",
                       (user.name, user.passwd))
        except _connector.OperationalError:
            pass                # It is OK if this one fails
        finally:
            server.sql("GRANT REPLICATION SLAVE ON *.* TO %s",
                       (user.name))

    def _enable_binlog(self, server):
        """Enable the binlog by setting the value of the log-bin
        option and log-bin-index. The values of these options will
        only be set if there were no value previously set for log-bin;
        if log-bin is already set, it is assumed that it is correctly
        set and information is fetched."""
        config = server.fetch_config()
        try:
            config.get('log-bin')
        except ConfigParser.NoOptionError:
            config.set('log-bin', server.name + '-bin')
            config.set('log-bin-index', server.name + '-bin.index')
            server.replace_config(config)

    def _disable_binlog(self, server):
        """Disable the binary log by removing the log-bin option and
        the log-bin-index option."""
        config = server.fetch_config()
        try:
            config.remove('log-bin')
            config.remove('log-bin-index')
            server.replace_config(config)
        except ConfigParser.NoOptionError:
            pass

    def imbue(self, server):
        pass

    def unimbue(self, server):
        pass

class Vagabond(Role):
    """A vagabond is a server that is not part of the deployment."""

    def imbue(self, server):
        pass

    def unimbue(self, server):
        pass

class Master(Role):
    """A master slave is a server whose purpose is to act as a
    master. It means that it has a replication user with the right
    privileges and also have the binary log activated.

    There is a "smart" way to update the password of the user::

      INSERT INTO mysql.user(user,host,password) VALUES(%s,'%%', PASSWORD(%s))
      ON DUPLICATE KEY UPDATE password=PASSWORD(%s)

    However, there are some missing defaults for the following fields,
    causing warnings when executed:

    - ssl_cipher

    - x509_issuer

    - x509_subject
    """

    def __init__(self, repl_user):
        super(Master, self).__init__()
        self.__user = repl_user

    def imbue(self, server):
        # Fetch and update the configuration file
        try:
            config = server.fetch_config()
            self._set_server_id(server, config)
            self._enable_binlog(server)


            # Put the new configuration file in place
            server.stop().replace_config(config)

        except ConfigParser.ParsingError:
            pass                # Didn't manage to update config file
        except IOError:
            pass
        finally:
            server.start()
            
        # Add a replication user
        self._create_repl_user(server, self.__user)
        server.repl_user = self.__user
        server.disconnect()

class Final(Role):
    """A final server is a server that does not have a binary log.
    The purpose of such a server is only to answer queries but never
    to change role."""

    def __init__(self, master):
        super(Final, self).__init__()
        self.__master = master

    def imbue(self, server):
        # Fetch and update the configuration file
        config = server.fetch_config()
        self._set_server_id(server, config)
        self._disable_binlog(server)

        # Put the new configuration in place
        server.stop().replace_config(config).start()

        server.repl_user = self.__master.repl_user

class Relay(Role):
    """A relay server is a server whose sole purpose is to forward
    events from the binary log to slaves that are able to answer
    queries.  The server has a binary log and also writes events
    executed by the slave thread to the binary log.  Since it is not
    necessary to be able to answer queries, all tables use the
    BLACKHOLE engine."""

    def __init__(self, master):
        super(Relay, self).__init__()
        self.__master = master

    def imbue(self, server):
        config = server.fetch_config()
        self._set_server_id(server, config)
        self._enable_binlog(server)
        config.set('log-slave-updates')
        server.stop().replace_config(config).start()
        server.sql("SET SQL_LOG_BIN = 0")
        for row in server.sql("SHOW DATABASES"):
            database = row["Database"]
            if database in ('information_schema', 'mysql'):
                continue
            for table in server.sql("SHOW TABLES FROM %s" % (database,)):
                server.sql("ALTER TABLE %s.%s ENGINE=BLACKHOLE" %
                           (database, table["Tables_in_" + database]))
        server.sql("SET SQL_LOG_BIN = 1")
        
