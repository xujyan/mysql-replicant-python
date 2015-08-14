# Copyright (c) 2010, Mats Kindahl, Charles Bell, and Lars Thalmann
# All rights reserved.
#
# Use of this source code is goverened by a BSD licence that can be
# found in the LICENCE file.

import sys
import os.path
rootpath = os.path.split(os.path.dirname(os.path.abspath(__file__)))[0]
sys.path.append(rootpath)

from mysql.replicant.commands import (
    fetch_master_pos,
    fetch_slave_pos,
    )

from mysql.replicant.errors import (
    NotMasterError,
    NotSlaveError,
    )

import my_deployment

print "# Executing 'show databases'"
for db in my_deployment.master.sql("show databases"):
    print db["Database"]

print "# Executing 'ls'"
for line in my_deployment.master.ssh(["ls"]):
    print line

try:
    print "Master position is:", fetch_master_pos(my_deployment.master)
except NotMasterError:
    print my_deployment.master.name, "is not configured as a master"

for slave in my_deployment.slaves:
    try:
        print "Slave position is:", fetch_slave_pos(slave)
    except NotSlaveError:
        print slave.name, "not configured as a slave"
