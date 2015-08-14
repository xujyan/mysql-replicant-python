# Copyright (c) 2010, Mats Kindahl, Charles Bell, and Lars Thalmann
# All rights reserved.
#
# Use of this source code is goverened by a BSD licence that can be
# found in the LICENCE file.

import sys, os.path
rootpath = os.path.split(os.path.dirname(os.path.abspath(__file__)))[0]
sys.path.append(rootpath) 

import MySQLdb as _connector
import mysql.replicant.errors

import my_deployment

class AlreadyInPoolError(mysql.replicant.errors.Error):
    pass

class NotInPoolError(mysql.replicant.errors.Error):
    pass

_CREATE_TABLE = """
CREATE TABLE common.nodes(
    host VARCHAR(48),
    port INT,
    sock VARCHAR(64),
    type SET('READ','WRITE'))
"""

_INSERT_SERVER = """
INSERT INTO nodes(host, port, sock, type)
    VALUES (%s, %s, %s, %s)
"""

_DELETE_SERVER = "DELETE FROM nodes WHERE host = %s AND port = %s"

_UPDATE_SERVER = "UPDATE nodes SET type = %s WHERE host = %s AND port = %s"

def pool_add(common, server, type=[]):
    try:
        common.sql(_INSERT_SERVER,
                   (server.host, server.port, server.socket, ','.join(type)),
                   db="common");
    except _connector.IntegrityError:
        raise AlreadyInPoolError


def pool_del(common, server):
    common.sql(_DELETE_SERVER, (server.host, server.port), db="common")

def pool_set(common, server, type):
    common.sql(_UPDATE_SERVER, (','.join(type), server.host, server.port),
               db="common")

import unittest
    
class TestLoadBalancer(unittest.TestCase):
    "Class to test the load balancer functions."

    def setUp(self):
        from my_deployment import common, master, slaves
        common.sql("DROP DATABASE IF EXISTS common")
        common.sql("CREATE DATABASE common")
        common.sql(_CREATE_TABLE)
        
    def tearDown(self):
        from my_deployment import common, servers
        for server in servers:
            pool_del(common, server)
        common.sql("DROP DATABASE common")

    def testServers(self):
        from my_deployment import common, master, slaves

        try:
            pool_add(common, master, ['READ', 'WRITE'])
        except AlreadyInPoolError:
            pool_set(common, master, ['READ', 'WRITE'])

        for slave in slaves:
            try:
                pool_add(common, slave, ['READ'])
            except AlreadyInPoolError:
                pool_set(common, slave, ['READ'])

        for row in common.sql("SELECT * FROM nodes", db="common"):
            if row['port'] == master.port:
                self.assertEqual(row['type'], 'READ,WRITE')
            elif row['port'] in [slave.port for slave in slaves]:
                self.assertEqual(row['type'], 'READ')

if __name__ == '__main__':
    unittest.main()
    

