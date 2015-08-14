# Copyright (c) 2010, Mats Kindahl, Charles Bell, and Lars Thalmann
# All rights reserved.
#
# Use of this source code is goverened by a BSD licence that can be
# found in the LICENCE file.

"""Module containing different backup methods.

"""

import os
import os.path
import subprocess
import urlparse

class BackupImage(object):
    """A backup image.

    """

    def __init__(self, backup_url):
        self.url = urlparse.urlparse(backup_url)

    def backup_server(self, server, database):
        "Backup databases from a server and add them to the backup image."
        pass

    def restore_server(self, server):
        "Restore the databases in an image on the server"
        pass

class PhysicalBackup(BackupImage):
    "A physical backup of a database"

    def backup_server(self, server, database="*"):

        from mysql.replicant.commands import (
            fetch_master_position,
            )

        datadir = server.fetch_config().get('datadir')
        if database == "*":
            database = [d for d in os.listdir(datadir)
                  if os.path.isdir(os.path.join(datadir, d))]
        server.sql("FLUSH TABLES WITH READ LOCK")
        position = fetch_master_position(server)
        if server.host != "localhost":
            path = os.path.basename(self.url.path)
        else:
            path = self.url.path
        server.ssh(["tar", "zpscf", path, "-C", datadir] + database)
        if server.host != "localhost":
            subprocess.check_call([
                    "scp", server.host + ":" + path, self.url.path
                    ])
        server.sql("UNLOCK TABLES")
        return position

    def restore_server(self, server):
        if server.host == "localhost":
            path = self.url.path
        else:
            path = os.path.basename(self.url.path)

        datadir = server.fetch_config().get('datadir')

        try:
            server.stop()
            if server.host != "localhost":
                subprocess.check_call([
                        "scp", self.url.path, server.host + ":" + path
                        ])
            server.ssh(["tar", "zxf", path, "-C", datadir])
        finally:
            server.start()
