"""
The config module provides support for handling configuration
such as the location to store files, etc.

The configuration is composed from different sources in a
hierarchical fashion:
    * If it exists: the credentials.cfg file in the directory
      of this module
    * If it exists: the credentials.cfg file in the 'current'
      directory
    * If provided: the file given to the constructor
The lower a source is in the list, to more it has precedence.

Not all files must contain all configuration properties. A
configuration file can also overwrite only a subset of the
configuration properties.
"""

from ConfigParser import SafeConfigParser
import inspect
import os, sys

class Config(SafeConfigParser):
    """
    The config class implements the ConfigParser interface.
    More specifically, it inherits from SafeConfigParser.

    See the documentation of SafeConfigParser for the available
    methods.
    """

    def __init__(self, configfile=None):
        SafeConfigParser.__init__(self)
        configfiles = []
        # Add the filename for the config file in the modules
        # directory
        self.fluksotableau_libdir = os.path.dirname(os.path.abspath(
            inspect.getfile(inspect.currentframe())))
        configfiles.append(os.path.join(self.fluksotableau_libdir, 'credentials.cfg'))
        # Add the filename for the config file in the 'current' directory
        configfiles.append('credentials.cfg')
        # Add the filename for the config file passed in
        if configfile:
            configfiles.append(configfile)
        self.read(configfiles)