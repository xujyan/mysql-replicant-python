# Copyright (c) 2010, Mats Kindahl, Charles Bell, and Lars Thalmann
# All rights reserved.
#
# Use of this source code is goverened by a BSD licence that can be
# found in the LICENCE file.

import sys
import os.path
here = os.path.dirname(os.path.abspath(__file__))
root = os.path.split(here)[0]
sys.path.append(root) 

import unittest

from mysql.replicant.roles import (
    Final,
    Master,
    )

from mysql.replicant.server import (
    User,
    )

from mysql.replicant.commands import (
    change_master,
    fetch_master_position,
    fetch_slave_position,
    slave_wait_and_stop,
    slave_wait_for_pos,
    )

from tests.utils import load_deployment

class TestCommands(unittest.TestCase):
    """Test case to test various commands"""

    def __init__(self, methodNames, options={}):
        super(TestCommands, self).__init__(methodNames)
        my = load_deployment(options['deployment'])
        self.master = my.master
        self.masters = my.servers[0:1]
        self.slaves = my.servers[2:3]

    def setUp(self):

        master_role = Master(User("repl_user", "xyzzy"))
        for master in self.masters:
            master_role.imbue(master)

        final_role = Final(self.masters[0])
        for slave in self.slaves:
            try:
                final_role.imbue(slave)
            except IOError:
                pass

    def testChangeMaster(self):
        "Test the change_master function"
        for slave in self.slaves:
            change_master(slave, self.master)

        self.master.sql("DROP TABLE IF EXISTS t1", db="test")
        self.master.sql("CREATE TABLE t1 (a INT)", db="test")
        self.master.disconnect()

        for slave in self.slaves:
            slave.sql("SHOW TABLES", db="test")

    def testSlaveWaitForPos(self):
        "Test the slave_wait_for_pos function"

        slave = self.slaves[0]
        master = self.master

        slave.sql("STOP SLAVE")
        pos1 = fetch_master_position(master)
        change_master(slave, master, pos1)
        slave.sql("START SLAVE")

        master.sql("DROP TABLE IF EXISTS t1", db="test")
        master.sql("CREATE TABLE t1 (a INT)", db="test")
        master.sql("INSERT INTO t1 VALUES (1),(2)", db="test")
        pos2 = fetch_master_position(master)
        slave_wait_for_pos(slave, pos2)
        pos3 = fetch_slave_position(slave)
        self.assert_(pos3 >= pos2)

    def testSlaveWaitAndStop(self):
        "Test the slave_wait_and_stop function"

        slave = self.slaves[0]
        master = self.master

        slave.sql("STOP SLAVE")
        pos1 = fetch_master_position(master)
        change_master(slave, master, pos1)
        slave.sql("START SLAVE")

        master.sql("DROP TABLE IF EXISTS t1", db="test")
        master.sql("CREATE TABLE t1 (a INT)", db="test")
        master.sql("INSERT INTO t1 VALUES (1),(2)", db="test")
        pos2 = fetch_master_position(master)
        master.sql("INSERT INTO t1 VALUES (3),(4)", db="test")
        pos3 = fetch_master_position(master)
        slave_wait_and_stop(slave, pos2)
        pos4 = fetch_slave_position(slave)
        self.assertEqual(pos2, pos4)
        row = slave.sql("SELECT COUNT(*) AS count FROM t1", db="test")
        self.assertEqual(row['count'], 2)
        slave.sql("START SLAVE")
        slave_wait_and_stop(slave, pos3)
        row = slave.sql("SELECT COUNT(*) AS count FROM t1", db="test")
        self.assertEqual(row['count'], 4)

    def testSlaveStatusWaitUntil(self):
        "Test slave_status_wait_until"
        slave = self.slaves[0]
        master = self.master

        slave.sql("STOP SLAVE")
        pos1 = fetch_master_position(master)
        change_master(slave, master, pos1)
        slave.sql("START SLAVE")
        

def suite(options={}):
    if not options['deployment']:
        return None
    return tests.utils.create_suite(__name__, options)

if __name__ == '__main__':
    unittest.main(defaultTest='suite')


