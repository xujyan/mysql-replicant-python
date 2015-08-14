# Copyright (c) 2010, Mats Kindahl, Charles Bell, and Lars Thalmann
# All rights reserved.
#
# Use of this source code is goverened by a BSD licence that can be
# found in the LICENCE file.

class Machine(object):
    """Base class for all machines. This hold primitives for
    accomplishing tasks on different hosts."""
    pass

class Linux(Machine):
    """Class holding operating system specific methods for (Debian)
    Linux."""
    defaults_file = "/etc/mysql/my.cnf"

    def stop_server(self, server):
        server.ssh(["/etc/init.d/mysql", "stop"])

    def start_server(self, server):
        server.ssh(["/etc/init.d/mysql", "start"])


class Solaris(Machine):
    """Class holding operating system specific methods for Solaris."""
    defaults_file = "/etc/mysql/my.cnf"

    def stop_server(self, server):
        server.ssh(["/etc/sbin/svcadm", "disable", "mysql"])

    def start_server(self, server):
        server.ssh(["/etc/sbin/svcadm", "enable", "mysql"])
