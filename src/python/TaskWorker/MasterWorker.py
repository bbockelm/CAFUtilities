import time
import logging
import os

from WMCore.Configuration import loadConfigurationFile, Configuration

from TaskWorker.DataObjects.Task import Task
from TaskWorker.Worker import Worker
from TaskWorker.WorkerExceptions import *
from TaskWorker.DBPoller import DBPoller


def validateConfig(config):
    """Verify that the input configuration contains all needed info

    :arg WMCore.Configuration config: input configuration
    :return bool, string: flag for validation result and a message."""
    if getattr(config, 'TaskWorker', None) is None:
        return False, "Configuration problem: Task worker section is missing. "
    return True, 'Ok'

def validateDbConfig(config):
    """Verify that the input configuration contains all needed info

    :arg WMCore.Configuration config: input configuration
    :return bool, string: flag for validation result and a message."""
    if getattr(config, 'CoreDatabase', None) is None:
        return False, "Configuration problem: Core Database section is missing. "
    return True, 'Ok'

class MasterWorker(object):
    """I am the master of the TaskWorker"""

    def __init__(self, config, dbconfig, quiet, debug):
        """Initializer

        :arg WMCore.Configuration config: input TaskWorker configuration
        :arg WMCore.Configuration dbconfig: input for database configuration/secret
        :arg logging logger: the logger
        :arg bool quiet: it tells if a quiet logger is needed
        :arg bool debug: it tells if needs a verbose logger."""
        def getLogging(quiet, debug):
            """Retrieves a logger and set the proper level

            :arg bool quiet: it tells if a quiet logger is needed
            :arg bool debug: it tells if needs a verbose logger
            :return logger: a logger with the appropriate logger level."""
            loglevel = logging.INFO
            if quiet:
                loglevel = logging.WARNING
            if debug:
                loglevel = logging.DEBUG
            logging.basicConfig(level=loglevel)
            logger = logging.getLogger(type(self).__name__)
            logger.debug("Logging level initialized to %s." %loglevel)
            return logger
        self.logger = getLogging(quiet, debug)
        self.config = config
        self.dbconfig = dbconfig
        self.db = DBPoller(dbconfig=dbconfig)
        self.slaves = Worker(self.config, self.dbconfig)
        self.slaves.begin()

    def algorithm(self):
        """I'm the intelligent guy taking care of getting the work
           and distribuiting it to the slave processes."""
        self.logger.debug("Starting")
        while(True):
            pendingWork = self.db.getNew(self.slaves.freeSlaves())
            self.logger.info("Retrieved a total of %d works", len(pendingWork))
            self.slaves.injectWorks(pendingWork)
            self.logger.info('Worker status:')
            self.logger.info(' - busy slaves: %d' % self.slaves.busySlaves())
            self.logger.info(' - free slaves: %d' % self.slaves.freeSlaves())
            self.db.updateFinished(self.slaves.checkFinished())
            time.sleep(self.config.TaskWorker.polling)
        self.logger.debug("Stopping")

    def __del__(self):
        """Shutting down all the slaves"""
        self.slaves.end()

if __name__ == '__main__':
    from optparse import OptionParser

    usage  = "usage: %prog [options] [args]"
    parser = OptionParser(usage=usage)

    parser.add_option( "-d", "--debug",
                       action = "store_true",
                       dest = "debug",
                       default = False,
                       help = "print extra messages to stdout" )
    parser.add_option( "-q", "--quiet",
                       action = "store_true",
                       dest = "quiet",
                       default = False,
                       help = "don't print any messages to stdout" )

    parser.add_option( "--config",
                       dest = "config",
                       default = None,
                       metavar = "FILE",
                       help = "configuration file path" )

    parser.add_option( "--db-config",
                       dest = "dbconfig",
                       default = None,
                       metavar = "FILE",
                       help = "database configuration file path" )

    (options, args) = parser.parse_args()

    if not options.config:
        raise ConfigException("Configuration not found")

    configuration = loadConfigurationFile( os.path.abspath(options.config) )
    status, msg = validateConfig(configuration)
    if not status:
        raise ConfigException(msg)

    dbconfiguration = loadConfigurationFile( os.path.abspath(options.dbconfig) )
    status, msg = validateDbConfig(dbconfiguration)
    if not status:
        raise ConfigException(msg)

    mw = MasterWorker(configuration, dbconfiguration, quiet=options.quiet, debug=options.debug)
    mw.algorithm()
    mw.slaves.stop()
    del mw
