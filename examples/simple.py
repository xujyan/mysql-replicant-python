# Copyright (c) 2010, Mats Kindahl, Charles Bell, and Lars Thalmann
# All rights reserved.
#
# Use of this source code is goverened by a BSD licence that can be
# found in the LICENCE file.

import sys, os.path
rootpath = os.path.split(os.path.dirname(os.path.abspath(__file__)))[0]
sys.path.append(rootpath) 

import my_deployment
from mysql.replicant.server import (
    User,
    )
from mysql.replicant.roles import (
    Master,
    Final,
    )
from mysql.replicant.commands import (
    change_master,
    )

master_role = Master(User("repl_user", "xyzzy"))
final_role = Final(my_deployment.master)

try:
    master_role.imbue(my_deployment.master)
except IOError, e:
    print "Cannot imbue master with Master role:", e

for slave in my_deployment.slaves:
    try:
        final_role.imbue(slave)
    except IOError, e:
        print "Cannot imbue slave with Final role:", e
