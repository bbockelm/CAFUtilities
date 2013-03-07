# Class definition:
#   CMSExperiment
#   This class is the prototype of an experiment class inheriting from Experiment
#   Instances are generated with ExperimentFactory via pUtil::getExperiment()
#   Implemented as a singleton class
#   http://stackoverflow.com/questions/42558/python-and-the-singleton-pattern

# Import relevant python/pilot modules
from Experiment import Experiment          # Main experiment class
from PilotErrors import PilotErrors        # Error codes
from pUtil import tolog                    # Logging method that sends text to the pilot log
from pUtil import readpar                  # Used to read values from the schedconfig DB (queuedata)
from pUtil import isAnalysisJob            # Is the current job a user analysis job or a production job?
from pUtil import getCmtconfig             # Get the cmtconfig from the job def or queuedata
from pUtil import verifyReleaseString      # To verify the release string (move to Experiment later)
from pUtil import setPilotPythonVersion    # Which python version is used by the pilot
from pUtil import getSiteInformation       # Get the SiteInformation object corresponding to the given experiment

# Standard python modules
import os
import commands
import shlex
import getopt

class CMSExperiment(Experiment):

    # private data members
    __experiment = "CMS"                   # String defining the experiment
    __instance = None                      # Boolean used by subclasses to become a Singleton
    __error = PilotErrors()                # PilotErrors object

    # Required methods

    def __init__(self):
        """ Default initialization """

        # e.g. self.__errorLabel = errorLabel
        pass

    def __new__(cls, *args, **kwargs):
        """ Override the __new__ method to make the class a singleton """

        if not cls.__instance:
            cls.__instance = super(CMSExperiment, cls).__new__(cls, *args, **kwargs)

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

    def setPython(self):
        """ set the python executable """

        ec = 0
        pilotErrorDiag = ""
        pybin = ""

        python_list = ['python', 'python32', 'python2']
        pybin = python_list[0]
        for _python in python_list:
            _pybin = commands.getoutput('which %s' % (_python))
            if _pybin.startswith('/'):
                # found python executable
                pybin = _pybin
                break

        tolog("Using %s" % (pybin))
        return ec, pilotErrorDiag, pybin

    def getJobExecutionCommand(self, job, jobSite, pilot_initdir):
        """ Define and test the command(s) that will be used to execute the payload """

        # Method is called from runJob

        # Input tuple: (method is called from runJob)
        #   job: Job object
        #   jobSite: Site object
        #   pilot_initdir: launch directory of pilot.py
        #
        # Return tuple:
        #   pilot_error_code, pilot_error_diagnostics, job_execution_command, special_setup_command, JEM, cmtconfig
        # where
        #   pilot_error_code       : self.__error.<PILOT ERROR CODE as defined in PilotErrors class> (value should be 0 for successful setup)
        #   pilot_error_diagnostics: any output from problematic command or explanatory error diagnostics
        #   job_execution_command  : command to execute payload, e.g. cmd = "source <path>/setup.sh; <path>/python trf.py [options]"
        #   special_setup_command  : any special setup command that can be insterted into job_execution_command and is sent to stage-in/out methods
        #   JEM                    : Job Execution Monitor activation state (default value "NO", meaning JEM is not to be used. See JEMstub.py)
        #   cmtconfig              : cmtconfig symbol from the job def or schedconfig, e.g. "x86_64-slc5-gcc43-opt"

        pilotErrorDiag = ""
        cmd = ""
        special_setup_cmd = ""
        pysiteroot = ""
        siteroot = ""
        JEM = "NO"

        # Is it's an analysis job or not?
        analysisJob = isAnalysisJob(job.trf)

        # Get the region string
        region = readpar('region')

        # Command used to download trf
        wgetCommand = 'wget'

        # Get the cmtconfig value
        cmtconfig = getCmtconfig(job.cmtconfig)
        tolog("Mancinellidebug cmtconfig = %s --- job.cmtconfig = %s" % (cmtconfig, job.cmtconfig))

        # Set python executable (after SITEROOT has been set)
        if siteroot == "":
            try:
                siteroot = os.environ['SITEROOT']
            except:
                tolog("Warning: $SITEROOT unknown at this stage (3)")
        ec, pilotErrorDiag, pybin = self.setPython()
        if ec == self.__error.ERR_MISSINGINSTALLATION:
            return ec, pilotErrorDiag, "", special_setup_cmd, JEM, cmtconfig

        # Define the job execution command
        if analysisJob:
            # Try to download the analysis trf
            status, pilotErrorDiag, trfName = self.getAnalysisTrf(wgetCommand, job.trf, pilot_initdir)
            if status != 0:
                return status, pilotErrorDiag, "", special_setup_cmd, JEM, cmtconfig

            # Set up the run command
            if job.prodSourceLabel == 'ddm' or job.prodSourceLabel == 'software':
                cmd = '%s %s %s' % (pybin, trfName, job.jobPars)
            else:
                tolog("Mancinellidebug: in 1")
                scramArchSetup = self.getScramArchSetupCommand(job)
                ec, pilotErrorDiag, cmdtrf = self.getAnalysisRunCommand(job, jobSite, trfName)
                cmd = "%s %s" % (scramArchSetup, cmdtrf)
                if ec != 0:
                    return ec, pilotErrorDiag, "", special_setup_cmd, JEM, cmtconfig

        elif verifyReleaseString(job.homePackage) != 'NULL' and job.homePackage != ' ':
            tolog("Mancinellidebug: in 2")
            cmd = "%s %s/%s %s" % (pybin, job.homePackage, job.trf, job.jobPars)
        else:
            tolog("Mancinellidebug: in 3")
            cmd = "%s %s %s" % (pybin, job.trf, job.jobPars)

        # Set special_setup_cmd if necessary
        special_setup_cmd = self.getSpecialSetupCommand()

        # Should the Job Execution Monitor (JEM) be used?
        if readpar('cloud') == "DE": # Currently only available in DE cloud
            metaOut = {}
            try:
                import sys
                from JEMstub import updateRunCommand4JEM
                # If JEM should be used, the command will get updated by the JEMstub automatically.
                cmd = updateRunCommand4JEM(cmd, job, jobSite, tolog, metaOut=metaOut)
            except:
                # On failure, cmd stays the same
                tolog("Failed to update run command for JEM - will run unmonitored.")

            # Is JEM to be used?
            if metaOut.has_key("JEMactive"):
                JEM = metaOut["JEMactive"]

            tolog("Use JEM: %s (dictionary = %s)" % (JEM, str(metaOut)))

        elif '--enable-jem' in cmd:
            tolog("!!WARNING!!1111!! JEM can currently only be used on certain sites in DE")

        # Pipe stdout/err for payload to files
        cmd += " 1>%s 2>%s" % (job.stdout, job.stderr)
        tolog("\nCommand to run the job is: \n%s" % (cmd))

        if special_setup_cmd != "":
            tolog("Special setup command: %s" % (special_setup_cmd))

        return 0, pilotErrorDiag, cmd, special_setup_cmd, JEM, cmtconfig


    def getScramArchSetupCommand(self, job):
        """ Looks for the scramArch option in the job.jobPars attribute and build 
            the command to export the SCRAMARCH env variable with the correct value """

        if "CMSRunAnaly" in job.trf:
            """ for the moment the export of scramarch is only needed for CMSRunAnaly trf """
            strpars = job.jobPars

            try:
                #tolog("Mancinellidebug prima di shlex.split")
                cmdopt = shlex.split(strpars)
                #tolog("Mancinellidebug cmdopt = %s " % cmdopt) 
                opts, args = getopt.getopt(cmdopt, "a:o:",
                               ["sourceURL=","jobNumber=","inputFile=","lumiMask=","cmsswVersion=","scramArch="])
                #tolog("Mancinellidebug opts = %s args = %s" % (opts, args))
                for o, a in opts:
                    if o == "--scramArch":
                        scramArch = a
                        cmdScramArchSetup = "export SCRAM_ARCH=%s;" % scramArch
                        tolog("Mancinellidebug scramArch = %s" % scramArch) 
                        return cmdScramArchSetup
            except Exception, e:
                tolog("Failed to parse option command in job.jobPars = %s -- cause: %s" % (strpars, e))
                return ""
        return ""

    def getSpecialSetupCommand(self):
        """ Set special_setup_cmd if necessary """

        # Note: this special setup command is hardly used and could probably be removed
        # in case any special setup should be added to the setup string before the trf is executed, the command defined in this method
        # could be added to the run command by using method addSPSetupToCmd().
        # the special command is also forwarded to the get and put functions (currently not used)

        special_setup_cmd = ""

        # add envsetup to the special command setup on tier-3 sites
        # (unknown if this is still needed)

        si = getSiteInformation(self.__experiment)
        if si.isTier3():
            _envsetup = readpar('envsetup')
            if _envsetup != "":
                special_setup_cmd += _envsetup
                if not special_setup_cmd.endswith(';'):
                    special_setup_cmd += ";"

        return special_setup_cmd


    def willDoFileLookups(self):
        """ Should (LFC) file lookups be done by the pilot or not? """

        return False

    def willDoFileRegistration(self):
        """ Should (LFC) file registration be done by the pilot or not? """

        return False

    def isOutOfMemory(self, **kwargs):
        """ Try to identify out of memory errors in the stderr/out """

        return False

    def getNumberOfEvents(self, **kwargs):
        """ Return the number of events """

        return 0

    def specialChecks(self):
        """ Implement special checks here """
        # Return False if fatal failure, otherwise return True
        # The pilot will abort if this method returns a False

        status = False

        #tolog("No special checks for \'%s\'" % (self.__experiment))
        # set the python version used by the pilot
        setPilotPythonVersion()

        tolog("SetPilotPython version:  special checks for \'%s\'" % (self.__experiment))


        return True # obviously change this to 'status' once implemented

    def getPayloadName(self, job):
        """ figure out a suitable name for the payload  """
        payloadname = "cmssw"
        return payloadname

    def verifySwbase(self):
        """ Called by pilot.py, check needed for handleQueuedata method """
        tolog("CMSExperiment - verifySwbase - nothing to do")

        return 0

    def checkSpecialEnvVars(self):
        """ Called by pilot.py, check needed for runMain method """
        tolog("CMSExperiment - checkSpecialEnvVars - nothing to do")

        return 0

    def extractAppdir(self):
        """ Called by pilot.py, runMain method """
        tolog("CMSExperiment - extractAppdir - nothing to do")

        return 0, ""
   

    def getMetadataForRegistration(self, guid):
        # Return metadata (not known yet) for server LFC registration
        # use the GUID as identifier (the string "<GUID>-surltobeset" will later be replaced with the SURL)        
        xmlstring = ''
        xmlstring += '    <metadata att_name="surl" att_value="%s-surltobeset"/>\n' % (guid) 
        xmlstring += '    <metadata att_name="full_lfn" att_value="%s-surltobeset"/>\n' % (guid)

        return xmlstring

    def getAttrForRegistration(self):
        # Return the attribute of the PFCxml to be updated with surl value
        
        attr = 'full_lfn'

        return attr        


    def getExpSpecificMetadata(self, job, workdir):
        """ Return metadata extracted from FrameworkJobReport.xml"""

        tolog("Mancinellidebug in getExpSpecificMetadata")
        fwjrMetadata = ''
        fwjrFile = os.path.join(workdir,"FrameworkJobReport.xml")
        if os.path.exists(fwjrFile):
            tolog("Mancinellidebug found FWJR: %s" % fwjrFile)
            fwjrMetadata = '<!--  CMS FrameWorkJobReport meta-data  -->\n'
            try:
                f = open(fwjrFile, 'r')
                for line in f.readlines():
                    fwjrMetadata += line
            except Exception, e:
                tolog("Failed to open FrameWorkJobReport file: %s" % str(e))
        else:
            tolog("Mancinellidebug FrameworkJobReport.xml not found in %s " % fwjrFile)

        return fwjrMetadata


if __name__ == "__main__":

    print "Implement test cases here"
    
