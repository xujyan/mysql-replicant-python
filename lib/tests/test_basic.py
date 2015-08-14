# Copyright (c) 2010, Mats Kindahl, Charles Bell, and Lars Thalmann
# All rights reserved.
#
# Use of this source code is goverened by a BSD licence that can be
# found in the LICENCE file.

import sys, os.path
root = os.path.split(os.path.dirname(os.path.abspath(__file__)))[0]
sys.path.append(root) 

import inspect
import mysql.replicant
import unittest
import tests.utils

from mysql.replicant import (
    server,
    errors,
    )

from mysql.replicant.server import (
    GTIDSet,
)

class TestPosition(unittest.TestCase):
    "Test case for binlog positions class."

    def __init__(self, method_name, options={}):
        super(TestPosition, self).__init__(method_name)

    def _checkPos(self, p):
        """Check that a position is valid and can be converted to
        string and back.

        """
        from mysql.replicant.server import Position
        self.assertEqual(p, eval(repr(p)))
        
    def testSimple(self):
        positions = [
            server.Position('master-bin.00001', 4711),
            server.Position('master-bin.00001', 9393),
            server.Position('master-bin.00002', 102),
            ]
 
        for position in positions:
            self._checkPos(position)

        # Check that comparison works as expected.
        for i, i_pos in enumerate(positions):
            for j, j_pos in enumerate(positions):
                if i < j:
                    self.assertTrue(i_pos < j_pos)
                elif i == j:
                    self.assertEqual(i_pos, j_pos)
                else:
                    self.assertTrue(i_pos > j_pos)

class TestGTID(unittest.TestCase):
    "Test case for GTID classes."

    def __init__(self, method_name, options={}):
        super(TestGTID, self).__init__(method_name)
    
    def testSimple(self):
        good_gtids = [
            ('523f5f6d-36ec-11e3-b034-0021cc6850ca:1', '523f5f6d-36ec-11e3-b034-0021cc6850ca:1-1'),
            '523f5f6d-36ec-11e3-b034-0021cc6850ca:1-5',
            '523f5f6d-36ec-11e3-b034-0021cc6850ca:10-10',
            ('523f5f6d-36ec-11e3-b034-0021cc6850ca:1-5:6-10', '523f5f6d-36ec-11e3-b034-0021cc6850ca:1-10'),
            ('523f5f6d-36ec-11e3-b034-0021cc6850ca:6-10:1-5', '523f5f6d-36ec-11e3-b034-0021cc6850ca:1-10'),
            ('4f4fada0-37b6-11e3-854d-0021cc6850ca:1:2:3:4', '4f4fada0-37b6-11e3-854d-0021cc6850ca:1-4'),
            ('4f4fada0-37b6-11e3-854d-0021cc6850ca:1-5:2-3:4-10', '4f4fada0-37b6-11e3-854d-0021cc6850ca:1-10'),
            '4f4fada0-37b6-11e3-854d-0021cc6850ca:1-1000,523f5f6d-36ec-11e3-b034-0021cc6850ca:1-10',
            ]

        for gtid in good_gtids:
            if isinstance(gtid, tuple):
                expected = gtid[1]
                gtid = gtid[0]
            else:
                expected = gtid
            self.assertEqual(str(GTIDSet(gtid)), expected)

        bad_gtids = [
            # Malformed GTID sets
            '523f5f6d-36ec-11e3-b034-0021cc6850ca:1-5-6-10',
            '523f5f6d-36ec-11e3-b034-0021cc6850ca:',
            '523f5f6d-36ec-11e3-b034-0021cc6850ca',
            '0021cc6850ca',

            # Bad ranges
            '523f5f6d-36ec-11e3-b034-0021cc6850ca:10-6',
            ]

        for gtid in bad_gtids:
            self.assertRaises(ValueError, GTIDSet, gtid)
        
    def testUnion(self):
        gtid = GTIDSet('523f5f6d-36ec-11e3-b034-0021cc6850ca:1-5')
        gtid.union('523f5f6d-36ec-11e3-b034-0021cc6850ca:6-10')
        self.assertEqual(str(gtid), '523f5f6d-36ec-11e3-b034-0021cc6850ca:1-10')

        gtid = GTIDSet('523f5f6d-36ec-11e3-b034-0021cc6850ca:1-5')
        gtid.union('4f4fada0-37b6-11e3-854d-0021cc6850ca:1-4')
        self.assertEqual(str(gtid), '4f4fada0-37b6-11e3-854d-0021cc6850ca:1-4,523f5f6d-36ec-11e3-b034-0021cc6850ca:1-5')

        gtid = GTIDSet('523f5f6d-36ec-11e3-b034-0021cc6850ca:2-5:7-9')
        gtid.union('523f5f6d-36ec-11e3-b034-0021cc6850ca:1-10')
        self.assertEqual(str(gtid), '523f5f6d-36ec-11e3-b034-0021cc6850ca:1-10')


    def testNormalize(self):
        "Test internal normalisation function."

        normalize = mysql.replicant.server._normalize

        self.assertEqual(normalize([(1,5),(6,10)]), [(1,10)])
        self.assertEqual(normalize([(1,5),(3,7)]), [(1,7)])
        self.assertEqual(normalize([(1,5),(7,10)]), [(1,5), (7,10)])

    def testCompare(self):
        "Test internal compare function"
        compare = mysql.replicant.server._compare_sets

        lhs = GTIDSet('523f5f6d-36ec-11e3-b034-0021cc6850ca:1-5')
        rhs = GTIDSet('523f5f6d-36ec-11e3-b034-0021cc6850ca:1-5')
        self.assertEqual(compare(lhs, rhs), (False, False))

        lhs = GTIDSet('523f5f6d-36ec-11e3-b034-0021cc6850ca:2-5:7-9')
        rhs = GTIDSet('523f5f6d-36ec-11e3-b034-0021cc6850ca:1-10')
        self.assertEqual(compare(lhs, rhs), (False, True))

        lhs = GTIDSet('523f5f6d-36ec-11e3-b034-0021cc6850ca:2-5:7-12')
        rhs = GTIDSet('523f5f6d-36ec-11e3-b034-0021cc6850ca:1-10')
        self.assertEqual(compare(lhs, rhs), (True, True))

        lhs = GTIDSet('523f5f6d-36ec-11e3-b034-0021cc6850ca:1-10')
        rhs = GTIDSet('523f5f6d-36ec-11e3-b034-0021cc6850ca:2-5')
        self.assertEqual(compare(lhs, rhs), (True, False))

        lhs = GTIDSet('523f5f6d-36ec-11e3-b034-0021cc6850ca:6-10')
        rhs = GTIDSet('523f5f6d-36ec-11e3-b034-0021cc6850ca:1-5')
        self.assertEqual(compare(lhs, rhs), (True, True))

        lhs = GTIDSet('523f5f6d-36ec-11e3-b034-0021cc6850ca:1-5,4f4fada0-37b6-11e3-854d-0021cc6850ca:1-4')
        rhs = GTIDSet('523f5f6d-36ec-11e3-b034-0021cc6850ca:1-5')
        self.assertEqual(compare(lhs, rhs), (True, False))

    def testCompareOperators(self):
        "Test comparison operators"

        lhs = GTIDSet('523f5f6d-36ec-11e3-b034-0021cc6850ca:1-5')
        rhs = GTIDSet('523f5f6d-36ec-11e3-b034-0021cc6850ca:1-5')
        self.assertEqual(lhs, rhs)
        self.assertLessEqual(lhs, rhs)
        self.assertGreaterEqual(lhs, rhs)

        lhs = GTIDSet('523f5f6d-36ec-11e3-b034-0021cc6850ca:2-5:7-9')
        rhs = GTIDSet('523f5f6d-36ec-11e3-b034-0021cc6850ca:1-10')
        self.assertLess(lhs, rhs)
        self.assertLessEqual(lhs, rhs)
        self.assertNotEqual(lhs, rhs)

        lhs = GTIDSet('523f5f6d-36ec-11e3-b034-0021cc6850ca:2-5:7-12')
        rhs = GTIDSet('523f5f6d-36ec-11e3-b034-0021cc6850ca:1-10')
        self.assertFalse(lhs < rhs)
        self.assertFalse(lhs > rhs)
        self.assertFalse(lhs == rhs)
        self.assertNotEqual(lhs, rhs)

        lhs = GTIDSet('523f5f6d-36ec-11e3-b034-0021cc6850ca:1-10')
        rhs = GTIDSet('523f5f6d-36ec-11e3-b034-0021cc6850ca:2-5')
        self.assertGreater(lhs, rhs)
        self.assertGreaterEqual(lhs, rhs)
        self.assertNotEqual(lhs, rhs)

        lhs = GTIDSet('523f5f6d-36ec-11e3-b034-0021cc6850ca:6-10')
        rhs = GTIDSet('523f5f6d-36ec-11e3-b034-0021cc6850ca:1-5')
        self.assertFalse(lhs < rhs)
        self.assertFalse(lhs > rhs)
        self.assertFalse(lhs == rhs)
        self.assertNotEqual(lhs, rhs)

        lhs = GTIDSet('523f5f6d-36ec-11e3-b034-0021cc6850ca:1-5,4f4fada0-37b6-11e3-854d-0021cc6850ca:1-4')
        rhs = GTIDSet('523f5f6d-36ec-11e3-b034-0021cc6850ca:1-5')
        self.assertGreater(lhs, rhs)
        self.assertGreaterEqual(lhs, rhs)
        self.assertNotEqual(lhs, rhs)


def suite(options={}):
    return tests.utils.create_suite(__name__, options)

if __name__ == '__main__':
    unittest.main(defaultTest='suite')
