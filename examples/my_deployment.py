# Copyright (c) 2010, Mats Kindahl, Charles Bell, and Lars Thalmann
# All rights reserved.
#
# Use of this source code is goverened by a BSD licence that can be
# found in the LICENCE file.

from mysql.replicant.server import (
    Server,
    User,
    )

from mysql.replicant.machine import (
    Linux,
    )

from mysql.replicant.config import (
    ConfigManagerFile,
)

servers = [Server('master',
                  server_id=1,
                  sql_user=User("mysql_replicant"),
                  ssh_user=User("mats"),
                  machine=Linux(),
                  port=3307,
                  socket='/var/run/mysqld/mysqld1.sock',
                  ),
           Server('slave1', server_id=2,
                  sql_user=User("mysql_replicant"),
                  ssh_user=User("mats"),
                  machine=Linux(),
                  port=3308,
                  socket='/var/run/mysqld/mysqld2.sock'),
           Server('slave2', 
                  sql_user=User("mysql_replicant"),
                  ssh_user=User("mats"),
                  machine=Linux(),
                  port=3309,
                  socket='/var/run/mysqld/mysqld3.sock'),
           Server('slave3',
                  sql_user=User("mysql_replicant"),
                  ssh_user=User("mats"),
                  machine=Linux(),
                  port=3310,
                  socket='/var/run/mysqld/mysqld4.sock')]

master = servers[0]
common = servers[0]              # Where the common database is stored
slaves = servers[1:]
