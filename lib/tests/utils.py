# Copyright (c) 2010, Mats Kindahl, Charles Bell, and Lars Thalmann
# All rights reserved.
#
# Use of this source code is goverened by a BSD licence that can be
# found in the LICENCE file.

import inspect
import sys
import unittest

def load_deployment(deployment):
    parts = deployment.split('.')
    pkg = __import__('.'.join(parts[:-1]), globals(), locals(), parts[-1:])
    return getattr(pkg, parts[-1])

def is_test_case(cls):
    return inspect.isclass(cls) and issubclass(cls, unittest.TestCase)

def create_suite(modname, options):
    test_suite = unittest.TestSuite()
    classes = inspect.getmembers(sys.modules[modname], is_test_case)
    for name, cls in classes:
        for test in unittest.defaultTestLoader.getTestCaseNames(cls):
            test_suite.addTest(cls(test, options))
    return test_suite

