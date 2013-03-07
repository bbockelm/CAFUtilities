# Class definition:
#   NordugridATLASExperiment
#   This class is the ATLAS experiment class for Nordugrid inheriting from Experiment
#   Instances are generated with ExperimentFactory via pUtil::getExperiment()

# import relevant python/pilot modules
from Experiment import Experiment  # Main experiment class
from pUtil import tolog            # Logging method that sends text to the pilot log
from pUtil import readpar          # Used to read values from the schedconfig DB (queuedata)
from pUtil import isAnalysisJob    # Is the current job a user analysis job or a production job?

class NordugridATLASExperiment(ATLASExperiment):

    # private data members
    __experiment = "Nordugrid-ATLAS"
    __instance = None
    __warning = ""
    __analysisJob = False
    __job = None

    # Required methods

    def __init__(self):
        """ Default initialization """
# not needed?
        # e.g. self.__errorLabel = errorLabel
        pass

    def __new__(cls, *args, **kwargs):
        """ Override the __new__ method to make the class a singleton """

        if not cls.__instance:
            cls.__instance = super(ATLASExperiment, cls).__new__(cls, *args, **kwargs)

        return cls.__instance

    def getExperiment(self):
        """ Return a string with the experiment name """

        return self.__experiment

    def setParameters(self, *args, **kwargs):
        """ Set any internally needed variables """

        # set initial values
        self.__job = kwargs.get('job', None)
        if self.__job:
            self.__analysisJob = isAnalysisJob(self.__job.trf)
        else:
            self.__warning = "setParameters found no job object"

    def getJobExecutionCommand(self):
        """ Define and test the command(s) that will be used to execute the payload """
        # E.g. cmd = "source <path>/setup.sh; <path>/python "

        cmd = ""

        return cmd

    def willDoFileLookups(self):
        """ Should (LFC) file lookups be done by the pilot or not? """

        return False

    def willDoFileRegistration(self):
        """ Should (LFC) file registration be done by the pilot or not? """

        return False

    # Additional optional methods

    def getWarning(self):
        """ Return any warning message passed to __warning """

        return self.__warning

if __name__ == "__main__":

    print "Implement test cases here"
    
