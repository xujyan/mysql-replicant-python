# Copyright (c) 2010, Mats Kindahl, Charles Bell, and Lars Thalmann
# All rights reserved.
#
# Use of this source code is goverened by a BSD licence that can be
# found in the LICENCE file.

import sys, os.path
here = os.path.dirname(os.path.abspath(__file__))
rootpath = os.path.split(here)[0]
sys.path.append(rootpath) 

import mysql.replicant
import unittest

import tests.utils

class TestRoles(unittest.TestCase):
    """Test case to test role usage."""

    def __init__(self, methodName, options):
        super(TestRoles, self).__init__(methodName)
        my = tests.utils.load_deployment(options['deployment'])
        self.master = my.master
        self.slave = my.slaves[0]
        self.slaves = my.slaves

    def setUp(self):
        pass

    def _imbueRole(self, role):
        # We are likely to get an IOError because we cannot write the
        # configuration file, but this is OK.
        try:
            role.imbue(self.master)
        except IOError:
            pass

    def testMasterRole(self):
        "Test how the master role works"
        user = mysql.replicant.server.User("repl_user", "xyzzy")
        self._imbueRole(mysql.replicant.roles.Master(user))
        
    def testSlaveRole(self):
        "Test that the slave role works"
        self._imbueRole(mysql.replicant.roles.Final(self.master))

    def testRelayRole(self):
        "Test that the slave role works"
        self._imbueRole(mysql.replicant.roles.Relay(self.master))

def suite(options={}):
    if not options['deployment']:
        return None
    return tests.utils.create_suite(__name__, options)

if __name__ == '__main__':
    unittest.main(defaultTest='suite')
