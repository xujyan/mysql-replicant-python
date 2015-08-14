# Copyright (c) 2010, Mats Kindahl, Charles Bell, and Lars Thalmann
# All rights reserved.
#
# Use of this source code is goverened by a BSD licence that can be
# found in the LICENCE file.

from mysql.replicant.server import Server
from mysql.replicant.common import User
from mysql.replicant.machine import Linux
from mysql.replicant.roles import Master, Final

import time, os.path

class MultiLinux(Linux):
    """Class to handle the case where there are multiple servers
    running at the same box, all managed by mysqld_multi."""
    def __init__(self, number):
        self.__number = number

    def stop_server(self, server):
        server.ssh(["mysqld_multi", "stop", str(self.__number)])
        pidfile = ''.join("/var/run/mysqld", server.name, ".pid")
        while os.path.exists(pidfile):
            time.sleep(1)

    def start_server(self, server):
        import time
        print "Starting server...",
        server.ssh(["mysqld_multi", "start", str(self.__number)])
        time.sleep(1)           # Need some time for server to start
        print "done"

_replicant_user = User("mysql_replicant")
_REPL_USER = User("repl_user", "xyzzy")

def _cnf(name):
    test_dir = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(test_dir, '..', name + ".cnf")

master = Server(server_id=1, name="mysqld1",
                sql_user=_replicant_user,
                ssh_user=User("mysql"),
                machine=Linux(), role=Master(_REPL_USER),
                port=3307,
                socket='/var/run/mysqld/mysqld1.sock',
                defaults_file=_cnf("mysqld1"),
                config_section="mysqld1")
slaves = [Server(server_id=2, name="mysqld2",
                 sql_user=_replicant_user,
                 ssh_user=User("mysql"),
                 machine=Linux(), role=Final(master),
                 port=3308,
                 socket='/var/run/mysqld/mysqld2.sock',
                 defaults_file=_cnf("mysqld2"),
                 config_section="mysqld2"),
          Server(server_id=3, name="mysqld3",
                 sql_user=_replicant_user,
                 ssh_user=User("mysql"),
                 machine=Linux(), role=Final(master),
                 port=3309,
                 socket='/var/run/mysqld/mysqld3.sock',
                 defaults_file=_cnf("mysqld3"),
                 config_section="mysqld3"),
          Server(server_id=4, name="mysqld4",
                 sql_user=_replicant_user,
                 ssh_user=User("mysql"),
                 machine=Linux(), role=Final(master),
                 port=3310,
                 socket='/var/run/mysqld/mysqld4.sock',
                 defaults_file=_cnf("mysqld4"),
                 config_section="mysqld4")]
servers = [master] + slaves
