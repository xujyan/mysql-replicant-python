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

import re
import unittest

from mysql.replicant.errors import (
    NotMasterError,
    NotSlaveError,
    )

from mysql.replicant.commands import (
    fetch_master_position,
    fetch_slave_position,
    lock_database,
    unlock_database,
    )

import tests.utils

_POS_CRE = re.compile(r"Position\(('\w+-bin.\d+', \d+)?\)")

class TestServerBasics(unittest.TestCase):
    """Test case to test server basics. It relies on an existing MySQL
    server."""

    def __init__(self, method_name, options):
        super(TestServerBasics, self).__init__(method_name)
        depl = tests.utils.load_deployment(options['deployment'])
        self.master = depl.master
        self.slave = depl.slaves[0]
        self.slaves = depl.slaves

    def setUp(self):
        pass

    def testConfig(self):
        "Get some configuration information from the server"
        self.assertEqual(self.master.host, "localhost")
        self.assertEqual(self.master.port, 3307)
        self.assertEqual(self.master.socket, '/var/run/mysqld/mysqld1.sock')

    def testFetchReplace(self):
        "Fetching a configuration file, adding some options, and replacing it"
        config = self.master.fetch_config(os.path.join(here, 'test.cnf'))
        self.assertEqual(config.get('user'), 'mysql')
        self.assertEqual(config.get('log-bin'), '/var/log/mysql/master-bin')
        self.assertEqual(config.get('slave-skip-start'), None)
        config.set('no-value')
        self.assertEqual(config.get('no-value'), None)
        config.set('with-int-value', 4711)
        self.assertEqual(config.get('with-int-value'), '4711')
        config.set('with-string-value', 'Careful with that axe, Eugene!')
        self.assertEqual(config.get('with-string-value'),
                         'Careful with that axe, Eugene!')
        self.master.replace_config(config, os.path.join(here, 'test-new.cnf'))
        lines1 = file(os.path.join(here, 'test.cnf')).readlines()
        lines2 = file(os.path.join(here, 'test-new.cnf')).readlines()
        lines1 += ["\n", "no-value\n", "with-int-value = 4711\n",
                   "with-string-value = Careful with that axe, Eugene!\n"]
        lines1.sort()
        lines2.sort()
        self.assertEqual(lines1, lines2)
        os.remove(os.path.join(here, 'test-new.cnf'))

        
    def testSsh(self):
        "Testing ssh() call"
        self.assertEqual(''.join(self.master.ssh(["echo", "-n", "Hello"])),
                         "Hello")
 
    def testSql(self):
        "Testing (read-only) SQL execution"
        result = self.master.sql("select 'Hello' as val")['val']
        self.assertEqual(result, "Hello")

    def testLockUnlock(self):
        "Test that the lock and unlock functions can be called"
        lock_database(self.master)
        unlock_database(self.master)

    def testGetMasterPosition(self):
        "Fetching master position from the master and checking format"
        try:
            position = fetch_master_position(self.master)
            self.assertTrue(position is None or _POS_CRE.match(str(position)),
                            "Position '%s' is not correct" % (str(position)))
        except NotMasterError:
            self.fail(
                "Unable to test fetch_master_position since"
                " master is not configured as a master"
                )

    def testGetSlavePosition(self):
        "Fetching slave positions from the slaves and checking format"
        for slave in self.slaves:
            try:
                position = fetch_slave_position(slave)
                self.assertTrue(_POS_CRE.match(str(position)),
                                "Incorrect position '%s'" % (str(position)))
            except NotSlaveError:
                pass

def suite(options={}):
    if not options['deployment']:
        return None
    return tests.utils.create_suite(__name__, options)

if __name__ == '__main__':
    unittest.main(defaultTest='suite')

