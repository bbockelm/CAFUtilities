import time
import logging
import os

from TaskWorker.DataObjects.Task import Task
from TaskWorker.Worker import Worker
from TaskWorker.WorkerExceptions import *

from WMCore.Configuration import loadConfigurationFile, Configuration


def validateConfig(config):
    """Verify that the input configuration contains all needed info

    :arg WMCore.Configuration config: input configuration
    :return bool, string: flag for validation result and a message."""
    if getattr(config, 'TaskWorker', None) is None:
        return False, "Configuration problem: Task worker section is missing. "
    return True, 'Ok'

class MasterWorker(object):
    """I am the master of the TaskWorker"""

    def __init__(self, config, quiet, debug):
        """Initializer

        :arg WMCore.Configuration config: input configuration
        :arg logging logger: the logger."""
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
        self.slaves = Worker(self.config, self.config.TaskWorker.nslaves)
        self.slaves.begin()

    def pollSourceDB(self, worklimit):
        """This method aims to contain the logic to retrieve the work
           from the source database.

           :arg int worklimit: the maximum amount of works to retrieve
           :return: the list of input work to process."""
        if worklimit == 0:
            return []
        elif worklimit <= 0:
            self.logger.error('More work then slaves acquired. Unexpected behaviour.')
            return []
        # now emulating this 
        from TaskWorker.Actions.Handler import handleResubmit, handleNewTask 
        return [(handleNewTask, Task({'name': 'testme', 'dataset':'/DYJetsToLL_M-50_TuneZ2Star_8TeV-madgraph-tarball/Summer12_DR53X-PU_S10_START53_V7A-v1/AODSIM',
                                      'splitargs': {'halt_job_on_file_boundaries': False, 'lumis_per_job': 50, 'splitOnRun': False},
                                      'splitalgo': 'LumiBased'}), None),
                (handleNewTask, Task({'name': 'pippo', 'dataset':'/DYJetsToLL_M-50_TuneZ2Star_8TeV-madgraph-tarball/Summer12_DR53X-PU_S10_START53_V7A-v1/AODSIM',
                                      'splitargs': {'halt_job_on_file_boundaries': False, 'lumis_per_job': 50, 'splitOnRun': False},
                                      'splitalgo': 'LumiBased'}), None)]

    def algorithm(self):
        """I'm the intelligent guy taking care of getting the work
           and distribuiting it to the slave processes."""
        self.logger.debug("Starting")
        while(True):
            pendingWork = self.pollSourceDB(self.slaves.freeSlaves())
            self.logger.info("Retrieved a total of %d works", len(pendingWork))
            self.slaves.injectWorks(pendingWork)
            self.logger.info('Worker status:')
            self.logger.info(' - busy slaves: %d' % self.slaves.busySlaves())
            self.logger.info(' - free slaves: %d' % self.slaves.freeSlaves())
            self.slaves.checkFinished()
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

    (options, args) = parser.parse_args()

    if not options.config:
        raise ConfigException("Configuration not found")

    configuration = loadConfigurationFile( os.path.abspath(options.config) )

    status, msg = validateConfig(configuration)
    if not status:
        raise ConfigException(msg)

    mw = MasterWorker(configuration, quiet=options.quiet, debug=options.debug)
    mw.algorithm()
    mw.slaves.stop()
    del mw
