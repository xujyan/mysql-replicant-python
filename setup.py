# Copyright (c) 2010, Mats Kindahl, Charles Bell, and Lars Thalmann
# All rights reserved.
#
# Use of this source code is goverened by a BSD licence that can be
# found in the LICENCE file.

import glob
import os
import os.path
import sys
import unittest

import distutils.command.build
import distutils.core

from distutils.core import (
    setup,
    )

here = os.path.dirname(os.path.realpath(__file__))

def fetch_version(module_name):
    original_path = sys.path
    sys.path[0:1] = [ os.path.join(here, 'lib') ]
    mod = __import__(module_name, globals(), locals(), ['__version__'])
    sys.path = original_path
    return mod.__version__

class TestCommand(distutils.core.Command):
    description = "Run unit tests"

    user_options = [
        ('build-dir=', 'b', 'Build directory'),
        ('deployment=', 'd', "Deployment to use (a module name)"),
        ]

    def initialize_options(self):
        self.build_lib = None
        self.deployment = None

    def finalize_options(self):
        self.set_undefined_options(
            'build',
            ('build_lib', 'build_lib'),
            )

    def run(self):
        """Find and execute all the tests modules.

        This function add those modules (in .py files) that define a
        "suite" function, other modules are just ignored and is
        assumed to be support files for the tests.

        In other words, don't define a "suite" function in modules
        that are not tests (call it something else).

        """

        sys.path[0:1] = [
            os.path.join(here, self.build_lib),
            os.path.join(here, 'lib'),
            ]
        suite = unittest.TestSuite()
        tests = glob.glob(os.path.join(here, 'lib/tests', 'test*.py'))
        pkgs = [ os.path.basename(test[:-3]) for test in tests ]
        tests_mod = __import__('tests', globals(), locals(), pkgs)
        for test in pkgs:
            test_suite = getattr(tests_mod, test).suite({
                    'deployment': self.deployment,
                    })
            if test_suite is not None:
                suite.addTest(test_suite)
        result = unittest.TextTestRunner(verbosity=1).run(suite)
        sys.exit(not result.wasSuccessful())


setup(
    name='mysql-replicant',
    version=fetch_version('mysql.replicant.version'),
    description='Package for controlling servers in a replication deployment',
    author='Mats Kindahl',
    author_email='mats@kindahl.net',
    url="http://launchpad.net/mysql-replicant-python",
    packages=[
        'mysql.replicant',
        'mysql.replicant.parser',
        ],
    package_dir={
        '': 'lib',
        },
    classifiers=[
        'Development Status :: 4 - Beta',
        'Programming Language :: Python',
        'License :: OSI Approved :: BSD License',
        'Inteded Audience :: Developers',
        'Inteded Audience :: System Administrators',
        'Inteded Audience :: Database Administrators',
        'Topic :: Database :: Database Engines/Servers',
        ],
    cmdclass = {
        'test': TestCommand,
        },
)
