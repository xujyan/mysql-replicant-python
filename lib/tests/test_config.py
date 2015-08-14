# Copyright (c) 2010, Mats Kindahl, Charles Bell, and Lars Thalmann
# All rights reserved.
#
# Use of this source code is goverened by a BSD licence that can be
# found in the LICENCE file.

import sys, os.path
here = os.path.dirname(os.path.abspath(__file__))
root = os.path.split(here)[0]
sys.path.append(root) 

import mysql.replicant
import tests.utils
import unittest

def config_file(name):
    return os.path.join(here, name)
    
class TestConfigFile(unittest.TestCase):
    "Test case for the ConfigFile class."

    def __init__(self, method_name, options):
        super(TestConfigFile, self).__init__(method_name)

    def testBasic(self):
        "Basic tests that a config file can be loaded"
        from mysql.replicant.configmanager import ConfigManagerFile

        config = ConfigManagerFile.Config(path=config_file('test.cnf'),
                                          section='mysqld1')

        self.assertEqual(config.get('user'), 'mysql')
        self.assertEqual(config.get('log-bin'),
                         '/var/log/mysql/master-bin')
        self.assertEqual(config.get('slave-skip-start'), None)

    def testFetchReplace(self):
        "Fetching a configuration file, adding some options, and replacing it"

        from mysql.replicant.configmanager import ConfigManagerFile

        config = ConfigManagerFile.Config(path=config_file('test.cnf'),
                                          section='mysqld1')

        config.set('no-value')
        self.assertEqual(config.get('no-value'), None)

        config.set('with-int-value', 4711)
        self.assertEqual(config.get('with-int-value'), '4711')

        config.set('with-string-value', 'Careful with that axe, Eugene!')
        self.assertEqual(config.get('with-string-value'),
                         'Careful with that axe, Eugene!')

        config.write(os.path.join(here, 'test-new.cnf'))
        lines1 = file(config_file('test.cnf')).readlines()
        lines2 = file(os.path.join(here, 'test-new.cnf')).readlines()
        lines1 += ["\n", "no-value\n", "with-int-value = 4711\n",
                   "with-string-value = Careful with that axe, Eugene!\n"]
        lines1.sort()
        lines2.sort()
        self.assertEqual(lines1, lines2)
        os.remove(os.path.join(here, 'test-new.cnf'))

def suite(options={}):
    return tests.utils.create_suite(__name__, options)

if __name__ == '__main__':
    unittest.main(defaultTest='suite')

    
