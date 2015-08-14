# Copyright (c) 2010, Mats Kindahl, Charles Bell, and Lars Thalmann
# All rights reserved.
#
# Use of this source code is goverened by a BSD licence that can be
# found in the LICENCE file.

import sys
import os.path
rootpath = os.path.split(os.path.dirname(os.path.abspath(__file__)))[0]
sys.path.append(rootpath) 

import unittest

import tests.utils

from mysql.replicant.backup import (
    PhysicalBackup,
    )

class TestBackup(unittest.TestCase):
    "Test case for various backup techniques"

    def __init__(self, methodName, options=None):
        super(TestBackup, self).__init__(methodName)
        if options is None:
            options = {}
        deployment = tests.utils.load_deployment(options['deployment'])
        self.master = deployment.master

    def setUp(self):
        self.backup = PhysicalBackup("file:/tmp/backup.tar.gz")
        
    def testPhysicalBackup(self):
        self.master.sql("CREATE TABLE IF NOT EXISTS dummy (a INT)", db="test")
        self.master.sql("INSERT INTO dummy VALUES (1),(2)", db="test")
        for row in self.master.sql("SELECT * FROM dummy", db="test"):
            self.assertTrue(row['a'] in [1, 2])
        self.backup.backup_server(self.master, ['test'])
        self.master.sql("DROP TABLE dummy", db="test")
        self.backup.restore_server(self.master)
        tbls = self.master.sql("SHOW TABLES", db="test")
        self.assertTrue('dummy' in [t["Tables_in_test"] for t in tbls])
        self.master.sql("DROP TABLE dummy", db="test")

def suite(options=None):
    if options is None:
        options = {}
    if not options['deployment']:
        return None
    return tests.utils.create_suite(__name__, options)

if __name__ == '__main__':
    unittest.main(defaultTest='suite')
