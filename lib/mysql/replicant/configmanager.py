# Copyright (c) 2010, Mats Kindahl, Charles Bell, and Lars Thalmann
# All rights reserved.
#
# Use of this source code is goverened by a BSD licence that can be
# found in the LICENCE file.

import os, shutil, re, tempfile, subprocess, ConfigParser

_NONE_MARKER = "<>"

def _fetch_file(host, user, filename):
    """Function to fetch a file from the server and copy it to the
    local machine. A temporary file name is created for """
    
    handle, tmpfile = tempfile.mkstemp(text=True)
    os.close(handle)

    if host != "localhost":
        source = user + "@" + host + ":" + filename
        subprocess.check_call(["scp", "-qB", source, tmpfile])
    else:
        shutil.copyfile(filename, tmpfile)
    return tmpfile

def _replace_file(host, user, filename, source):
    if host != "localhost":
        target = user + "@" + host + ":" + filename
        subprocess.check_call(["scp", "-qB", source, target])
    else:
        shutil.copyfile(source, filename)

class ConfigManagerFile(object):
    "Configuration manager that fetches and restores files directly."

    class Config(object):
        """Class for handling server configuration files."""
    
        def __init__(self, path=None, section=None):
            self.__config = None
            self.__section = section or 'mysqld'
            if path:
                self.read(path)

        def _clean_config_file(self, fname):
            infile = file(fname, 'r')
            lines = infile.readlines()
            infile.close()

            output = file(fname, 'w')
            for line in lines:
                if re.match("#.*|\[\w+\]|[\w\d_-]+\s*=\s*.*", line):
                    pass
                elif re.match("\s*[\w\d_-]+\s*", line):
                    line = "%s = %s\n" % (line.rstrip("\n"), _NONE_MARKER)
                else:
                    line = "#!#" + line
                output.write(line)
            output.close()

        def _unclean_config_file(self, filename):
            infile = file(filename, 'r')
            lines = infile.readlines()
            infile.close()

            output = file(filename, 'w')
            for line in lines:
                mobj = re.match("([\w\d_-]+)\s*=\s*(.*)", line)
                if mobj and mobj.group(2) == _NONE_MARKER:
                    output.write(mobj.group(1) + "\n")
                    continue
                if re.match("#!#.*", line):
                    output.write(line[3:])
                    continue
                output.write(line)
            output.close()

        def read(self, path):
            """Read configuration from a file."""
            # We use ConfigParser, but since it cannot read
            # configuration files we options without values, we have
            # to clean the output once it is fetched before calling
            # ConfigParser

            handle, tmpfile = tempfile.mkstemp(text=True)
            os.close(handle)
            shutil.copy(path, tmpfile)
            self._clean_config_file(tmpfile)

            self.__config = ConfigParser.SafeConfigParser()
            self.__config.read(tmpfile)

        def write(self, path):
            """Write the configuration to a file."""
            output = open(path, 'w')
            self.__config.write(output)
            # Since ConfigParser cannot handle options without values
            # (yet), we have to unclean the file before replacing it.
            output.close()
            self._unclean_config_file(path)

        def get(self, option):
            """Method to get the value of an option."""
            value = self.__config.get(self.__section, option)
            if value == _NONE_MARKER:
                value = None
            return value

        def set(self, option, value=None):
            """Method to set the value of an option. If set to None,
            the option is created but will not be given a value."""
            if value is None:
                value = _NONE_MARKER
            self.__config.set(self.__section, option, str(value))


        def remove(self, option):
            """Method to remove the option from the configuration
            entirely."""
            self.__config.remove_option(self.__section, option)
    
    def fetch_config(self, server, path=None):
        """Method to fetch the configuration options from the server
        into memory.

        The method operates by creating a temporary file to which it
        copies the settings. Once the config settings are replaced,
        the temporary file is removed and to edit the options again,
        it is necessary to fetch the file again."""
        
        if not path:
            path = server.defaults_file
        tmpfile = _fetch_file(server.host, server.ssh_user.name, path)
        return ConfigManagerFile.Config(tmpfile, server.config_section)

    def replace_config(self, server, config, path=None):
        """Method to replace the configuration file with the options
        in this object."""
        
        if not path:
            path = server.defaults_file

        handle, tmpfile = tempfile.mkstemp(text=True)
        os.close(handle)
        config.write(tmpfile)

        _replace_file(server.host, server.ssh_user.name, path, tmpfile)

