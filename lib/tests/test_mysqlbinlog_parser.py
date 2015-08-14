# Copyright (c) 2010, Mats Kindahl, Charles Bell, and Lars Thalmann
# All rights reserved.
#
# Use of this source code is goverened by a BSD licence that can be
# found in the LICENCE file.

"""
Test of the binary log reader.
"""

import sys, os.path
_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOTPATH = os.path.split(_HERE)[0]
sys.path.append(_ROOTPATH)

import glob
import mysql.replicant.parser.mysqlbinlog as parser
import tests.utils
import unittest

class TestBinlogReader(unittest.TestCase):
    """
    Unit test for testing that the binlog reader works as
    expected. This test will read from a dump from mysqlbinlog to
    check that the parsing works as expected.
    """

    def __init__(self, methodName, options={}):
        super(TestBinlogReader, self).__init__(methodName)

    def setUp(self):
        pattern = os.path.join(_HERE, "data/mysqld-bin.*.txt")
        self.filenames = glob.iglob(pattern)

    def testSizeAndPos(self):
        """Test that the length and end position of each event matches
        what is expected.
        """
        for fname in self.filenames:
            istream = open(fname)
            current_pos = 4
            for event in parser.read_events(istream):
                self.assertEqual(event.start_pos, current_pos)
                current_pos = event.end_pos

    def testEventTypes(self):
        for fname in self.filenames:
            istream = open(fname)
            for event in parser.read_events(istream):
                pass

def suite(options={}):
    """Create a test suite for the binary log reader.
    """
    return tests.utils.create_suite(__name__, options)

if __name__ == '__main__':
    unittest.main(defaultTest='suite')
