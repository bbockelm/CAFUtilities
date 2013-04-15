# Class definition:
#   ATLASExperiment
#   This class is the ATLAS experiment class inheriting from Experiment
#   Instances are generated with ExperimentFactory via pUtil::getExperiment()
#   Implemented as a singleton class
#   http://stackoverflow.com/questions/42558/python-and-the-singleton-pattern

# Import relevant python/pilot modules
from Experiment import Experiment               # Main experiment class
from pUtil import tolog                         # Logging method that sends text to the pilot log
from pUtil import readpar                       # Used to read values from the schedconfig DB (queuedata)
from pUtil import isAnalysisJob                 # Is the current job a user analysis job or a production job?
from pUtil import setPilotPythonVersion         # Which python version is used by the pilot
from pUtil import grep                          # Grep function - reimplement using cli command
from pUtil import getCmtconfig                  # Get the cmtconfig from the job def or queuedata
from pUtil import getCmtconfigAlternatives      # Get a list of locally available cmtconfigs
from pUtil import getSwbase                     # To build path for software directory, e.g. using schedconfig.appdir (move to subclass)
from pUtil import verifyReleaseString           # To verify the release string (move to Experiment later)
from pUtil import verifySetupCommand            # Verify that a setup file exists
from pUtil import getProperTimeout              # 
from pUtil import timedCommand                  # Protect cmd with timed_command
from pUtil import getSiteInformation            # Get the SiteInformation object corresponding to the given experiment
from PilotErrors import PilotErrors             # Error codes
from RunJobUtilities import dumpOutput          # ASCII dump

# Standard python modules
import re
import os
import commands
from glob import glob

class ATLASExperiment(Experiment):

    # private data members
    __experiment = "ATLAS"                 # String defining the experiment
    __instance = None                      # Boolean used by subclasses to become a Singleton
    __warning = ""
    __analysisJob = False
    __job = None                           # Current Job object
    __error = PilotErrors()                # PilotErrors object
    __doFileLookups = False                # True for LFC based file lookups

    # Required methods

    def __init__(self):
        """ Default initialization """

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

        # Set the INDS env variable (used by runAthena)
        if analysisJob:
            self.setINDS(job.realDatasetsIn)

        # Get the region string
        region = readpar('region')

        # Command used to download runAthena or runGen
        wgetCommand = 'wget'

        # Get the cmtconfig value
        cmtconfig = getCmtconfig(job.cmtconfig)

        # Get the local path for the software
        swbase = getSwbase(jobSite.appdir, job.atlasRelease, job.homePackage, job.processingType, cmtconfig)
        tolog("Local software path: swbase = %s" % (swbase))

        # Get cmtconfig alternatives
        cmtconfig_alternatives = getCmtconfigAlternatives(cmtconfig, swbase)
        tolog("Found alternatives to cmtconfig: %s (the first item is the default cmtconfig value)" % str(cmtconfig_alternatives))

#        # special setup for NG
#        if region == 'Nordugrid':
#            status, pilotErrorDiag, cmd = setupNordugridTrf(job, analysisJob, wgetCommand, pilot_initdir)
#            if status != 0:
#                return status, pilotErrorDiag, "", special_setup_cmd, JEM, cmtconfig

        # Is it a standard ATLAS job? (i.e. with swRelease = 'Atlas-...')
        if job.atlasEnv :

            # Define the job runtime environment
            if not analysisJob and job.trf.endswith('.py'): # for production python trf

                tolog("Production python trf")

                if os.environ.has_key('VO_ATLAS_SW_DIR'):
                    scappdir = readpar('appdir')
                    # is this release present in the tags file?
                    if scappdir == "":
                        rel_in_tags = self.verifyReleaseInTagsFile(os.environ['VO_ATLAS_SW_DIR'], job.atlasRelease)
                        if not rel_in_tags:
                            tolog("WARNING: release was not found in tags file: %s" % (job.atlasRelease))
#                            tolog("!!FAILED!!3000!! ...")
#                            failJob(0, self.__error.ERR_MISSINGINSTALLATION, job, pilotserver, pilotport, ins=ins)
#                        swbase = os.environ['VO_ATLAS_SW_DIR'] + '/software'

                    # Get the proper siteroot and cmtconfig
                    ec, pilotErrorDiag, status, siteroot, cmtconfig = self.getProperSiterootAndCmtconfig(swbase, job.atlasRelease, job.homePackage, cmtconfig)
                    if not status:
                        tolog("!!WARNING!!3000!! Since setup encountered problems, any attempt of trf installation will fail (bailing out)")
                        tolog("ec=%d" % (ec))
                        tolog("pilotErrorDiag=%s" % (pilotErrorDiag))
                        return ec, pilotErrorDiag, "", special_setup_cmd, JEM, cmtconfig
                    else:
                        tolog("Will use SITEROOT=%s" % (siteroot))
                        pysiteroot = siteroot
                else:
                    if verifyReleaseString(job.atlasRelease) != "NULL":
                        siteroot = os.path.join(swbase, job.atlasRelease)
                    else:
                        siteroot = swbase
                    siteroot = siteroot.replace('//','/')

                # Get the install dir and update siteroot if necessary (dynamically install patch if not available)
                ec, pilotErrorDiag, siteroot, installDir = self.getInstallDir(job, jobSite.workdir, siteroot, swbase, cmtconfig)
                if ec != 0:
                    return ec, pilotErrorDiag, "", special_setup_cmd, JEM, cmtconfig

                # Get the cmtsite setup command
                ec, pilotErrorDiag, cmd1 = self.getCmtsiteCmd(swbase, job.atlasRelease, job.homePackage, cmtconfig, siteroot=pysiteroot)
                if ec != 0:
                    return ec, pilotErrorDiag, "", special_setup_cmd, JEM, cmtconfig

                # Make sure the CMTCONFIG is available and valid
                ec, pilotErrorDiag, dummy, dummy, dummy = self.checkCMTCONFIG(cmd1, cmtconfig, job.atlasRelease, siteroot=pysiteroot)
                if ec != 0:
                    return ec, pilotErrorDiag, "", special_setup_cmd, JEM, cmtconfig

                # Get cmd2 for production jobs for set installDirs (not the case for unset homepackage strings)
                if installDir != "":
                    cmd2, pilotErrorDiag = self.getProdCmd2(installDir, job.homePackage)
                    if pilotErrorDiag != "":
                        return self.__error.ERR_SETUPFAILURE, pilotErrorDiag, "", special_setup_cmd, JEM, cmtconfig
                else:
                    cmd2 = ""

                # Set special_setup_cmd if necessary
                special_setup_cmd = self.getSpecialSetupCommand()

            else: # for analysis python trf

                tolog("Preparing analysis job setup command")

                # try alternatives to cmtconfig if necessary
                first = True
                first_ec = 0
                first_pilotErrorDiag = ""
                for cmtconfig in cmtconfig_alternatives:
                    ec = 0
                    pilotErrorDiag = ""
                    tolog("Testing cmtconfig=%s" % (cmtconfig))

                    # Get the cmd2 setup command before cmd1 is defined since cacheDir/Ver can be used in cmd1
                    cmd2, cacheDir, cacheVer = self.getAnalyCmd2(swbase, cmtconfig, job.homePackage, job.atlasRelease)

                    # Add sub path in case of AnalysisTransforms homePackage
                    if verifyReleaseString(job.homePackage) != "NULL":
                        reSubDir = re.search('AnalysisTransforms[^/]*/(.+)', job.homePackage)
                        subDir = ""
                        if reSubDir != None:
                            subDir = reSubDir.group(1)
                        tolog("subDir = %s" % (subDir))
                    else:
                        subDir = ""
                    path = os.path.join(swbase, subDir)

                    # Define cmd0 and cmd1
                    if verifyReleaseString(job.atlasRelease) != "NULL":
                        if job.atlasRelease < "16.1.0":
                            cmd0 = "source %s/%s/setup.sh;" % (path, job.atlasRelease)
                            tolog("cmd0 = %s" % (cmd0))
                        else:
                            cmd0 = ""
                            tolog("cmd0 will not be used for release %s" % (job.atlasRelease))
                    else:
                        cmd0 = ""

                    # Get the cmtsite setup command
                    ec, pilotErrorDiag, cmd1 = \
                        self.getCmtsiteCmd(swbase, job.atlasRelease, job.homePackage, cmtconfig, analysisJob=True, siteroot=siteroot, cacheDir=cacheDir, cacheVer=cacheVer)
                    if ec != 0:
                        # Store the first error code
                        if first:
                            first = False
                            first_ec = ec
                            first_pilotErrorDiag = pilotErrorDiag

                        # Function failed, try the next cmtconfig value or exit
                        continue

                    tolog("cmd1 = %s" % (cmd1))

                    # Make sure the CMTCONFIG is available and valid
                    ec, pilotErrorDiag, siteroot, atlasVersion, atlasProject = \
                        self.checkCMTCONFIG(cmd1, cmtconfig, job.atlasRelease, siteroot=siteroot, cacheDir=cacheDir, cacheVer=cacheVer)
                    if ec != 0 and ec != self.__error.ERR_COMMANDTIMEOUT:
                        # Store the first error code
                        if first:
                            first = False
                            first_ec = ec
                            first_pilotErrorDiag = pilotErrorDiag

                        # Function failed, try the next cmtconfig value or exit
                        continue
                    else:
                        tolog("Aborting alternative cmtconfig loop (will use cmtconfig=%s)" % (cmtconfig))
                        break

                # Exit if the tests above failed
                if ec != 0:
                    # Use the first error code if set
                    if first_ec != 0:
                        tolog("Will report the first encountered problem: ec=%d, pilotErrorDiag=%s" % (first_ec, first_pilotErrorDiag))
                        ec = first_ec
                        pilotErrorDiag = first_pilotErrorDiag

                    return ec, pilotErrorDiag, "", special_setup_cmd, JEM, cmtconfig

                # Cannot update cmd2/siteroot for unset release/homepackage strings
                if verifyReleaseString(job.atlasRelease) == "NULL" or verifyReleaseString(job.homePackage) == "NULL":
                    tolog("Will not update cmd2/siteroot since release/homepackage string is NULL")
                else:
                    # Update cmd2 with AtlasVersion and AtlasProject from setup (and siteroot if not set)
                    _useAsetup = self.useAtlasSetup(swbase, job.atlasRelease, job.homePackage, cmtconfig)
                    cmd2 = self.updateAnalyCmd2(cmd2, atlasVersion, atlasProject, _useAsetup)

                tolog("cmd2 = %s" % (cmd2))
                tolog("siteroot = %s" % (siteroot))

                # Set special_setup_cmd if necessary
                special_setup_cmd = self.getSpecialSetupCommand()

                # Prepend cmd0 to cmd1 if set and if release < 16.1.0
                if cmd0 != "" and job.atlasRelease < "16.1.0":
                    cmd1 = cmd0 + cmd1

            # construct the command of execution
            if analysisJob:
                # Try to download the trf
                status, pilotErrorDiag, trfName = self.getAnalysisTrf(wgetCommand, job.trf, pilot_initdir)
                if status != 0:
                    return status, pilotErrorDiag, "", special_setup_cmd, JEM, cmtconfig

                # Set up runAthena
                ec, pilotErrorDiag, cmd3 = self.getAnalysisRunCommand(job, jobSite, trfName)
                if ec != 0:
                    return ec, pilotErrorDiag, "", special_setup_cmd, JEM, cmtconfig

                # NOTE: if TURL based PFC creation fails, getAnalysisRunCommand() needs to be rerun
                # Might not be possible, so if a user analysis job fails during TURL based PFC creation, fail the job
                # Or can remote I/O features just be turned off and cmd3 corrected accordingly?

            elif job.trf.endswith('.py'): # for python prod trf
                if os.environ.has_key('VO_ATLAS_SW_DIR'):
                    # set python executable (after SITEROOT has been set)
                    if siteroot == "":
                        try:
                            siteroot = os.environ['SITEROOT']
                        except:
                            tolog("Warning: $SITEROOT unknown at this stage (2)")
                    if pysiteroot == "":
                        tolog("Will use SITEROOT: %s (2)" % (siteroot))
                        ec, pilotErrorDiag, pybin = self.setPython(siteroot, job.atlasRelease, job.homePackage, cmtconfig, jobSite.sitename)
                    else:
                        tolog("Will use pysiteroot: %s (2)" % (pysiteroot))
                        ec, pilotErrorDiag, pybin = self.setPython(pysiteroot, job.atlasRelease, job.homePackage, cmtconfig, jobSite.sitename)

                    if ec == self.__error.ERR_MISSINGINSTALLATION:
                        return ec, pilotErrorDiag, "", special_setup_cmd, JEM, cmtconfig

                    # Prepare the cmd3 command with the python from the release and the full path to the trf
                    _cmd = cmd1
                    if cmd2 != "": # could be unset (in the case of unset homepackage strings)
                        _cmd += ";" + cmd2
                    cmd3 = self.getProdCmd3(_cmd, pybin, job.trf, job.jobPars)
                else:
                    cmd3 = "%s %s" % (job.trf, job.jobPars)

            elif verifyReleaseString(job.homePackage) != 'NULL':
                cmd3 = "%s/kitval/KitValidation/JobTransforms/%s/%s %s" %\
                       (swbase, job.homePackage, job.trf, job.jobPars)
            else:
                cmd3 = "%s/kitval/KitValidation/JobTransforms/%s %s" %\
                       (swbase, job.trf, job.jobPars)

            tolog("cmd3 = %s" % (cmd3))

            # Create the final command string
            cmd = cmd1
            if cmd2 != "":
                cmd += ";" + cmd2
            if cmd3 != "":
                cmd += ";" + cmd3

        else: # Generic, non-ATLAS specific jobs, or at least a job with undefined swRelease

            tolog("Generic job")

            # Set python executable (after SITEROOT has been set)
            if siteroot == "":
                try:
                    siteroot = os.environ['SITEROOT']
                except:
                    tolog("Warning: $SITEROOT unknown at this stage (3)")

            tolog("Will use $SITEROOT: %s (3)" % (siteroot))
            ec, pilotErrorDiag, pybin = self.setPython(siteroot, job.atlasRelease, job.homePackage, cmtconfig, jobSite.sitename)
            if ec == self.__error.ERR_MISSINGINSTALLATION:
                return ec, pilotErrorDiag, "", special_setup_cmd, JEM, cmtconfig

            if analysisJob:
                # Try to download the analysis trf
                status, pilotErrorDiag, trfName = self.getAnalysisTrf(wgetCommand, job.trf, pilot_initdir)
                if status != 0:
                    return status, pilotErrorDiag, "", special_setup_cmd, JEM, cmtconfig

                # Set up the run command
                if job.prodSourceLabel == 'ddm' or job.prodSourceLabel == 'software':
                    cmd = '%s %s %s' % (pybin, trfName, job.jobPars)
                else:
                    ec, pilotErrorDiag, cmd = self.getAnalysisRunCommand(job, jobSite, trfName)
                    if ec != 0:
                        return ec, pilotErrorDiag, "", special_setup_cmd, JEM, cmtconfig

            elif verifyReleaseString(job.homePackage) != 'NULL' and job.homePackage != ' ':
                cmd = "%s %s/%s %s" % (pybin, job.homePackage, job.trf, job.jobPars)
            else:
                cmd = "%s %s %s" % (pybin, job.trf, job.jobPars)

            # Set special_setup_cmd if necessary
            special_setup_cmd = self.getSpecialSetupCommand()

        # add FRONTIER debugging and RUCIO env variables
        cmd = self.addEnvVars2Cmd(cmd, job.jobId, job.processingType)

        if readpar('cloud') == "DE":
            # Should JEM be used?
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

        tolog("ATLAS_PYTHON_PILOT = %s" % (os.environ['ATLAS_PYTHON_PILOT']))

        if special_setup_cmd != "":
            tolog("Special setup command: %s" % (special_setup_cmd))

        return 0, pilotErrorDiag, cmd, special_setup_cmd, JEM, cmtconfig

    def getFileLookups(self):
        """ Return the file lookup boolean """

        return self.__doFileLookups

    def doFileLookups(self, doFileLookups):
        """ Update the file lookups boolean """

        self.__doFileLookups = doFileLookups

    def willDoFileLookups(self):
        """ Should (LFC) file lookups be done by the pilot or not? """

        status = False

        if readpar('lfchost') != "" and self.getFileLookups():
            status = True

        if status:
            tolog("Will do file lookups in the LFC")
        else:
            tolog("Will not do any file lookups")

        return status

    def willDoFileRegistration(self):
        """ Should (LFC) file registration be done by the pilot or not? """

        status = False

        # should the LFC file registration be done by the pilot or by the server?
        if readpar('lfcregister') != "server":
            status = True

        # make sure that the lcgcpSiteMover (and thus lcg-cr) is not used
        if readpar('copytool') == "lcgcp" or readpar('copytool') == "lcg-cp":
            status = False

        return status

    # Additional optional methods

    def getWarning(self):
        """ Return any warning message passed to __warning """

        return self.__warning

    def tryint(self, x):
        """ Used by numbered string comparison (to protect against unexpected letters in version number) """

        try:
            return int(x)
        except ValueError:
            return x

    def splittedname(self, s):
        """ Used by numbered string comparison """

        # Can also be used for sorting:
        # > names = ['YT4.11', '4.3', 'YT4.2', '4.10', 'PT2.19', 'PT2.9']
        # > sorted(names, key=splittedname)
        # ['4.3', '4.10', 'PT2.9', 'PT2.19', 'YT4.2', 'YT4.11']

        return tuple(self.tryint(x) for x in re.split('([0-9]+)', s))
                        
    def isAGreaterThanB(self, A, B):
        """ Is numbered string A > B? """
        # > a="1.2.3"
        # > b="2.2.2"
        # > e.isAGreaterThanB(a,b)
        # False
        
        return self.splittedname(A) > self.splittedname(B)

    def displayChangeLog(self):
        """ Display the cvmfs ChangeLog is possible """

        # 'head' the ChangeLog on cvmfs (/cvmfs/atlas.cern.ch/repo/sw/ChangeLog)
        from SiteInformation import SiteInformation
        si = SiteInformation()
        appdir = readpar('appdir')
        if appdir == "":
            if os.environ.has_key('VO_ATLAS_SW_DIR'):
                appdir = os.environ['VO_ATLAS_SW_DIR']
            else:
                appdir = ""

        if appdir != "":
            # there might be more than one appdir, try them all
            appdirs = si.getAppdirs(appdir)
            tolog("appdirs = %s" % str(appdirs))
            for appdir in appdirs:
                path = os.path.join(appdir, 'ChangeLog')
                if os.path.exists(path):
                    try:
                        rs = commands.getoutput("head %s" % (path))
                    except Exception, e:
                        tolog("!!WARNING!!1232!! Failed to read the ChangeLog: %s" % (e))
                    else:
                        rs = "\n"+"-"*80 + "\n" + rs
                        rs += "\n"+"-"*80
                        tolog("head of %s: %s" % (path, rs))
                else:
                    tolog("No such path: %s (ignore)" % (path))
        else:
            tolog("Can not display ChangeLog: Found no appdir")

    def testImportLFCModule(self):
        """ Can the LFC module be imported? """

        status = False

        try:
            import lfc
        except Exception, e:
            tolog("!!WARNING!!3111!! Failed to import the LFC module: %s" % (e))
        else:
            tolog("Successfully imported the LFC module")
            status = True

        return status

    def getNumberOfEvents(self, **kwargs):
        """ Return the number of events """
        # ..and a string of the form N|N|..|N with the number of jobs in the trf(s)

        job = kwargs.get('job', None)
        number_of_jobs = kwargs.get('number_of_jobs', 1)

        if not job:
            tolog("!!WARNING!!2332!! getNumberOfEvents did not receive a job object")
            return 0, 0, ""

        tolog("Looking for number of processed events (pass 0: metadata.xml)")

        nEventsRead = self.processMetadata(job.workdir)
        nEventsWritten = 0
        if nEventsRead > 0:
            return nEventsRead, nEventsWritten, str(nEventsRead)
        else:
            nEventsRead = 0

        tolog("Looking for number of processed events (pass 1: Athena summary file(s))")
        nEventsRead, nEventsWritten = self.processAthenaSummary(job.workdir)
        if nEventsRead > 0:
            return nEventsRead, nEventsWritten, str(nEventsRead)

        tolog("Looking for number of processed events (pass 2: Resorting to brute force grepping of payload stdout)")
        nEvents_str = ""
        for i in range(number_of_jobs):
            _stdout = job.stdout
            if number_of_jobs > 1:
                _stdout = _stdout.replace(".txt", "_%d.txt" % (i + 1))
            filename = os.path.join(job.workdir, _stdout)
            N = 0
            if os.path.exists(filename):
                tolog("Processing stdout file: %s" % (filename))
                matched_lines = grep(["events processed so far"], filename)
                if len(matched_lines) > 0:
                    if "events read and" in matched_lines[-1]:
                        # event #415044, run #142189 2 events read and 0 events processed so far
                        N = int(re.match('.* run #\d+ \d+ events read and (\d+) events processed so far.*', matched_lines[-1]).group(1))
                    else:
                        # event #4, run #0 3 events processed so far
                        N = int(re.match('.* run #\d+ (\d+) events processed so far.*', matched_lines[-1]).group(1))

            if len(nEvents_str) == 0:
                nEvents_str = str(N)
            else:
                nEvents_str += "|%d" % (N)
            nEventsRead += N

        return nEventsRead, nEventsWritten, nEvents_str

    def processMetadata(self, workdir):
        """ Extract number of events from metadata.xml """

        N = 0

        filename = os.path.join(workdir, "metadata.xml")
        if os.path.exists(filename):
            # Get the metadata
            try:
                f = open(filename, "r")
            except IOError, e:
                tolog("!!WARNING!!1222!! Exception: %s" % (e))
            else:
                xmlIN = f.read()
                f.close()

                # Get the XML objects
                from xml.dom import minidom
                xmldoc = minidom.parseString(xmlIN)
                fileList = xmldoc.getElementsByTagName("File")

                # Loop over all files, assume that the number of events are the same in all files
                for _file in fileList:
                    lrc_metadata_dom = _file.getElementsByTagName("metadata")
                    for i in range(len(lrc_metadata_dom)):
                        _key = str(_file.getElementsByTagName("metadata")[i].getAttribute("att_name"))
                        _value = str(_file.getElementsByTagName("metadata")[i].getAttribute("att_value"))
                        if _key == "events":
                            try:
                                N = int(_value)
                            except Exception, e:
                                tolog("!!WARNING!!1222!! Number of events not an integer: %s" % (e))
                            else:
                                tolog("Number of events from metadata file: %d" % (N))
                            break
        else:
            tolog("%s does not exist" % (filename))

        return N

    def processAthenaSummary(self, workdir):
        """ extract number of events etc from athena summary file(s) """

        N1 = 0
        N2 = 0
        file_pattern_list = ['AthSummary*', 'AthenaSummary*']

        file_list = []
        # loop over all patterns in the list to find all possible summary files
        for file_pattern in file_pattern_list:
            # get all the summary files for the current file pattern
            files = glob(os.path.join(workdir, file_pattern))
            # append all found files to the file list
            for summary_file in files:
                file_list.append(summary_file)

        if file_list == [] or file_list == ['']:
            tolog("Did not find any athena summary files")
        else:
            # find the most recent and the oldest files
            oldest_summary_file = ""
            recent_summary_file = ""
            oldest_time = 9999999999
            recent_time = 0
            if len(file_list) > 1:
                for summary_file in file_list:
                    # get the modification time
                    try:
                        st_mtime = os.path.getmtime(summary_file)
                    except Exception, e:
                        tolog("!!WARNING!!1800!! Could not read modification time of file %s: %s" % (summary_file, str(e)))
                    else:
                        if st_mtime > recent_time:
                            recent_time = st_mtime
                            recent_summary_file = summary_file
                        if st_mtime < oldest_time:
                            oldest_time = st_mtime
                            oldest_summary_file = summary_file
            else:
                oldest_summary_file = file_list[0]
                recent_summary_file = oldest_summary_file
                oldest_time = os.path.getmtime(oldest_summary_file)
                recent_time = oldest_time

            if oldest_summary_file == recent_summary_file:
                tolog("Summary file: %s: Will be processed for errors and number of events" %\
                      (os.path.basename(oldest_summary_file)))
            else:
                tolog("Most recent summary file: %s (updated at %d): Will be processed for errors" %\
                      (os.path.basename(recent_summary_file), recent_time))
                tolog("Oldest summary file: %s (updated at %d): Will be processed for number of events" %\
                      (os.path.basename(oldest_summary_file), oldest_time))

            # Get the number of events from the oldest summary file
            try:
                f = open(oldest_summary_file, "r")
            except Exception, e:
                tolog("!!WARNING!!1800!! Failed to get number of events from summary file. Could not open file: %s" % str(e))
            else:
                lines = f.readlines()
                f.close()

                if len(lines) > 0:
                    for line in lines:
                        if "Events Read:" in line:
                            N1 = int(re.match('Events Read\: *(\d+)', line).group(1))
                        if "Events Written:" in line:
                            N2 = int(re.match('Events Written\: *(\d+)', line).group(1))
                        if N1 > 0 and N2 > 0:
                            break
                else:
                    tolog("!!WARNING!!1800!! Failed to get number of events from summary file. Encountered an empty summary file.")

                tolog("Number of events: %d (read)" % (N1))
                tolog("Number of events: %d (written)" % (N2))

            # Get the errors from the most recent summary file
            # ...

        return N1, N2

    def isOutOfMemory(self, **kwargs):
        """ Try to identify out of memory errors in the stderr/out """
        # (Used by ErrorDiagnosis)

        # make this function shorter, basically same code twice

        out_of_memory = False

        job = kwargs.get('job', None)
        number_of_jobs = kwargs.get('number_of_jobs', 1)

        if not job:
            tolog("!!WARNING!!3222!! isOutOfMemory() did not receive a job object")
            return False

        tolog("Checking for memory errors in stderr")
        for i in range(number_of_jobs):
            _stderr = job.stderr
            if number_of_jobs > 1:
                _stderr = _stderr.replace(".txt", "_%d.txt" % (i + 1))
            filename = os.path.join(job.workdir, _stderr)
            if os.path.exists(filename):
                tolog("Processing stderr file: %s" % (filename))
                if os.path.getsize(filename) > 0:
                    tolog("WARNING: %s produced stderr, will dump to log" % (job.payload))
                    stderr_output = dumpOutput(filename)
                    if stderr_output.find("MemoryRescueSvc") >= 0 and \
                           stderr_output.find("FATAL out of memory: taking the application down") > 0:
                        out_of_memory = True
            else:
                tolog("Warning: File %s does not exist" % (filename))

        # try to identify out of memory errors in the stdout
        tolog("Checking for memory errors in stdout..")
        for i in range(number_of_jobs):
            _stdout = job.stdout
            if number_of_jobs > 1:
                _stdout = _stdout.replace(".txt", "_%d.txt" % (i + 1))
            filename = os.path.join(job.workdir, _stdout)
            if os.path.exists(filename):
                tolog("Processing stdout file: %s" % (filename))
                matched_lines = grep(["St9bad_alloc", "std::bad_alloc"], filename)
                if len(matched_lines) > 0:
                    tolog("Identified an out of memory error in %s stdout:" % (job.payload))
                    for line in matched_lines:
                        tolog(line)
                    out_of_memory = True
            else:
                tolog("Warning: File %s does not exist" % (filename))

        return out_of_memory

    def verifyReleaseInTagsFile(self, vo_atlas_sw_dir, atlasRelease):
        """ verify that the release is in the tags file """

        status = False

        # make sure the release is actually set
        if verifyReleaseString(atlasRelease) == "NULL":
            return status

        tags = dumpOutput(vo_atlas_sw_dir + '/tags')
        if tags != "":
            # is the release among the tags?
            if tags.find(atlasRelease) >= 0:
                tolog("Release %s was found in tags file" % (atlasRelease))
                status = True
            else:
                tolog("!!WARNING!!3000!! Release %s was not found in tags file" % (atlasRelease))
                # error = PilotErrors()
                # failJob(0, self.__error.ERR_MISSINGINSTALLATION, job, pilotserver, pilotport, ins=ins)
        else:
            tolog("!!WARNING!!3000!! Next pilot release might fail at this stage since there was no tags file")

        return status

    def getInstallDir(self, job, workdir, siteroot, swbase, cmtconfig):
        """ Get the path to the release, install patch if necessary """

        ec = 0
        pilotErrorDiag = ""

        # do not proceed for unset homepackage strings (treat as release strings in the following function)
        if verifyReleaseString(job.homePackage) == "NULL":
            return ec, pilotErrorDiag, siteroot, ""

        # install the trf in the work dir if it is not installed on the site
        # special case for nightlies (rel_N already in siteroot path, so do not add it)
        if "rel_" in job.homePackage:
            installDir = siteroot
        else:
            installDir = os.path.join(siteroot, job.homePackage)
        installDir = installDir.replace('//','/')

        tolog("Atlas release: %s" % (job.atlasRelease))
        tolog("Job home package: %s" % (job.homePackage))
        tolog("Trf installation dir: %s" % (installDir))

        # special case for nightlies (no RunTime dir)
        if "rel_" in job.homePackage:
            sfile = os.path.join(installDir, "setup.sh")
        else:
            sfile = installDir + ('/%sRunTime/cmt/setup.sh' % job.homePackage.split('/')[0])
        sfile = sfile.replace('//','/')
        if not os.path.isfile(sfile):

#            pilotErrorDiag = "Patch not available (will not attempt dynamic patch installation)"
#            tolog("!!FAILED!!3000!! %s" % (pilotErrorDiag))
#            ec = self.__error.ERR_DYNTRFINST

# uncomment this section (and remove the comments in the above three lines) for dynamic path installation

            tolog("!!WARNING!!3000!! Trf setup file does not exist at: %s" % (sfile))
            tolog("Will try to install trf in work dir...")

            # Install trf in the run dir
            try:
                ec, pilotErrorDiag = self.installPyJobTransforms(job.atlasRelease, job.homePackage, swbase, cmtconfig)
            except Exception, e:
                pilotErrorDiag = "installPyJobTransforms failed: %s" % str(e)
                tolog("!!FAILED!!3000!! %s" % (pilotErrorDiag))
                ec = self.__error.ERR_DYNTRFINST
            else:
                if ec == 0:
                    tolog("Successfully installed trf")
                    installDir = workdir + "/" + job.homePackage

                    # replace siteroot="$SITEROOT" with siteroot=rundir
                    os.environ['SITEROOT'] = workdir
                    siteroot = workdir

# comment until here

        else:
            tolog("Found trf setup file: %s" % (sfile))
            tolog("Using install dir = %s" % (installDir))

        return ec, pilotErrorDiag, siteroot, installDir

    def installPyJobTransforms(self, release, package, swbase, cmtconfig):
        """ Install new python based TRFS """

        status = False
        pilotErrorDiag = ""

        import string

        if package.find('_') > 0: # jobdef style (e.g. "AtlasProduction_12_0_7_2")
            ps = package.split('_')
            if len(ps) == 5:
                status = True
                # dotver = string.join(ps[1:], '.')
                # pth = 'AtlasProduction/%s' % dotver
            else:
                status = False
        else: # Panda style (e.g. "AtlasProduction/12.0.3.2")
            # Create pacman package = AtlasProduction_12_0_7_1
            ps = package.split('/')
            if len(ps) == 2:
                ps2 = ps[1].split('.')
                if len(ps2) == 4 or len(ps2) == 5:
                    dashver = string.join(ps2, '_')
                    pacpack = '%s_%s' % (ps[0], dashver)
                    tolog("Pacman package name: %s" % (pacpack))
                    status = True
                else:
                    status = False
            else:
                status = False

        if not status:
            pilotErrorDiag = "installPyJobTransforms: Prod cache has incorrect format: %s" % (package)
            tolog("!!FAILED!!2999!! %s" % (pilotErrorDiag))
            return self.__error.ERR_DYNTRFINST, pilotErrorDiag

        # Check if it exists already in rundir
        tolog("Checking for path: %s" % (package))

        # Current directory should be the job workdir at this point
        if os.path.exists(package):
            tolog("Found production cache, %s, in run directory" % (package))
            return 0, pilotErrorDiag

        # Install pacman
        status, pilotErrorDiag = self.installPacman() 
        if status:
            tolog("Pacman installed correctly")
        else:
            return self.__error.ERR_DYNTRFINST, pilotErrorDiag

        # Prepare release setup command
        if self.useAtlasSetup(swbase, release, package, cmtconfig):
            setup_pbuild = self.getProperASetup(swbase, release, package, cmtconfig, source=False)
        else:
            setup_pbuild = '%s/%s/cmtsite/setup.sh -tag=%s,AtlasOffline,%s' % (swbase, release, release, cmtconfig)
        got_JT = False
        caches = [
            'http://classis01.roma1.infn.it/pacman/Production/cache',
            'http://atlas-computing.web.cern.ch/atlas-computing/links/kitsDirectory/Analysis/cache'
            ]

        # shuffle list so same cache is not hit by all jobs
        from random import shuffle
        shuffle(caches)
        for cache in caches:
            # Need to setup some CMTROOT first
            # Pretend platfrom for non-slc3, e.g. centOS on westgrid
            # Parasitacally, while release is setup, get DBRELEASE version too
            cmd = 'source %s' % (setup_pbuild)
            cmd+= ';CMT_=`echo $CMTCONFIG | sed s/-/_/g`'
            cmd+= ';cd pacman-*;source ./setup.sh;cd ..;echo "y"|'
            cmd+= 'pacman -pretend-platform SLC -get %s:%s_$CMT_ -trust-all-caches'%\
                  (cache, pacpack)
            tolog('Pacman installing JT %s from %s' % (pacpack, cache))

            exitcode, output = timedCommand(cmd, timeout=60*60)
            if exitcode != 0:
                pilotErrorDiag = "installPyJobTransforms failed: %s" % str(output)
                tolog("!!WARNING!!2999!! %s" % (pilotErrorDiag))
            else:
                tolog('Installed JobTransforms %s from %s' % (pacpack, cache))
                got_JT = True
                break

        if got_JT:
            ec = 0
        else:
            ec = self.__error.ERR_DYNTRFINST

        return ec, pilotErrorDiag

    def installPacman(self):
        """ Pacman installation """

        pilotErrorDiag = ""

        # Pacman version
        pacman = 'pacman-3.18.3.tar.gz'

        urlbases = [
            'http://physics.bu.edu/~youssef/pacman/sample_cache/tarballs',
            'http://atlas.web.cern.ch/Atlas/GROUPS/SOFTWARE/OO/Production'
            ]

        # shuffle list so same server is not hit by all jobs
        from random import shuffle
        shuffle(urlbases)
        got_tgz = False
        for urlbase in urlbases:
            url = urlbase + '/' + pacman
            tolog('Downloading: %s' % (url))
            try:
                # returns httpMessage
                from urllib import urlretrieve
                (filename, msg) = urlretrieve(url, pacman)
                if 'content-type' in msg.keys():
                    if msg['content-type'] == 'application/x-gzip':
                        got_tgz = True
                        tolog('Got %s' % (url))
                        break
                    else:
                        tolog('!!WARNING!!4000!! Failed to get %s' % (url))
            except Exception ,e:
                tolog('!!WARNING!!4000!! URL: %s throws: %s' % (url, e))

        if got_tgz:
            tolog('Success')
            cmd = 'tar -zxf %s' % (pacman)
            tolog("Executing command: %s" % (cmd))
            (exitcode, output) = commands.getstatusoutput(cmd)
            if exitcode != 0:
                # Got a tgz but can't unpack it. Could try another source but will fail instead.
                pilotErrorDiag = "%s failed: %d : %s" % (cmd, exitcode, output)
                tolog('!!FAILED!!4000!! %s' % (pilotErrorDiag))
                return False, pilotErrorDiag
            else:
                tolog('Pacman tarball install succeeded')
                return True, pilotErrorDiag
        else:
            pilotErrorDiag = "Failed to get %s from any source url" % (pacman)
            tolog('!!FAILED!!4000!! %s' % (pilotErrorDiag))
            return False, pilotErrorDiag

    def getCmtsiteCmd(self, swbase, atlasRelease, homePackage, cmtconfig, siteroot=None, analysisJob=False, cacheDir=None, cacheVer=None):
        """ Get the cmtsite setup command """

        ec = 0
        pilotErrorDiag = ""
        cmd = ""

        if verifyReleaseString(homePackage) == "NULL":
            homePackage = ""

        # Handle sites using builds area in a special way
        if swbase[-len('builds'):] == 'builds' or verifyReleaseString(atlasRelease) == "NULL":
            _path = swbase
        else:
            _path = os.path.join(swbase, atlasRelease)

        if self.useAtlasSetup(swbase, atlasRelease, homePackage, cmtconfig):
            # homePackage=AnalysisTransforms-AtlasTier0_15.5.1.6
            # cacheDir = AtlasTier0
            # cacheVer = 15.5.1.6
            m_cacheDirVer = re.search('AnalysisTransforms-([^/]+)', homePackage)
            if m_cacheDirVer != None:
                cacheDir, cacheVer = self.getCacheInfo(m_cacheDirVer, atlasRelease)
            elif "," in homePackage or "rel_" in homePackage: # if nightlies are used, e.g. homePackage = "AtlasProduction,rel_0"
                cacheDir = homePackage
            cmd = self.getProperASetup(swbase, atlasRelease, homePackage, cmtconfig, cacheVer=cacheVer, cacheDir=cacheDir)
        else:
            # Get the tags
            tags = self.getTag(analysisJob, swbase, atlasRelease, cacheDir, cacheVer)

            ec, pilotErrorDiag, status = self.isForceConfigCompatible(swbase, atlasRelease, homePackage, cmtconfig, siteroot=siteroot)
            if ec == self.__error.ERR_MISSINGINSTALLATION:
                return ec, pilotErrorDiag, cmd
            else:
                if status:
                    if "slc5" in cmtconfig and os.path.exists("%s/gcc43_inst" % (_path)):
                        cmd = "source %s/gcc43_inst/setup.sh;export CMTCONFIG=%s;" % (_path, cmtconfig)
                    elif "slc5" in cmtconfig and "slc5" in swbase and os.path.exists(_path):
                        cmd = "source %s/setup.sh;export CMTCONFIG=%s;" % (_path, cmtconfig)
                    else:
                        cmd = "export CMTCONFIG=%s;" % (cmtconfig)
                    cmd += "source %s/cmtsite/setup.sh %s,forceConfig" % (_path, tags)
                else:
                    cmd = "source %s/cmtsite/setup.sh %s" % (_path, tags)

        return ec, pilotErrorDiag, cmd

    def getCacheInfo(self, m_cacheDirVer, atlasRelease):
        """ Get the cacheDir and cacheVer """

        cacheDirVer = m_cacheDirVer.group(1)
        if re.search('_', cacheDirVer) != None:
            cacheDir = cacheDirVer.split('_')[0]
            cacheVer = re.sub("^%s_" % cacheDir, '', cacheDirVer)
        else:
            cacheDir = 'AtlasProduction'
            if atlasRelease in ['13.0.25']:
                cacheDir = 'AtlasPoint1'
            cacheVer = cacheDirVer
        tolog("cacheDir = %s" % (cacheDir))
        tolog("cacheVer = %s" % (cacheVer))

        return cacheDir, cacheVer

    def getTag(self, analysisJob, path, release, cacheDir, cacheVer):
        """ Define the setup tags """

        _setup = False
        tag = "-tag="

        if analysisJob:
            if cacheDir and cacheDir != "" and cacheVer and cacheVer != "" and cacheVer.count('.') < 4:
                # E.g. -tag=AtlasTier0,15.5.1.6,32,setup
                tag += "%s" % (cacheDir)
                tag += ",%s" % (cacheVer)
                _setup = True
            else:
                # E.g. -tag=AtlasOffline,15.5.1
                tag += "AtlasOffline"
                if verifyReleaseString(release) != "NULL":
                    tag += ",%s" % (release)
            # only add the "32" part if CMTCONFIG has been out-commented in the requirements file
            if self.isCMTCONFIGOutcommented(path, release):
                tag += ",32"
            if _setup:
                tag += ",setup"
        else:
            # for production jobs
            tag = "-tag=AtlasOffline"
            if verifyReleaseString(release) != "NULL":
                tag += ",%s" % (release)

        # always add the runtime
        tag += ",runtime"

        return tag

    def isCMTCONFIGOutcommented(self, path, release):
        """ Is CMTCONFIG out-commented in requirements file? """

        status = False
        filename = "%s%s/cmtsite/requirements" % (path, release)
        if os.path.exists(filename):
            cmd = "grep CMTCONFIG %s" % (filename)
            ec, rs = commands.getstatusoutput(cmd)
            if ec == 0:
                if rs.startswith("#"):
                    status = True

        return status

    def verifyCmtsiteCmd(self, exitcode, _pilotErrorDiag):
        """ Verify the cmtsite command """

        pilotErrorDiag = "unknown"

        if "#CMT> Warning: template <src_dir> not expected in pattern install_scripts (from TDAQCPolicy)" in _pilotErrorDiag:
            tolog("Detected CMT warning (return code %d)" % (exitcode))
            tolog("Installation setup command passed test (with precaution)")
        elif "Error:" in _pilotErrorDiag or "Fatal exception:" in _pilotErrorDiag:
            pilotErrorDiag = "Detected severe CMT error: %d, %s" % (exitcode, _pilotErrorDiag)
            tolog("!!WARNING!!2992!! %s" % (pilotErrorDiag))
        elif exitcode != 0:
            from futil import is_timeout
            if is_timeout(exitcode):
                pilotErrorDiag = "cmtside command was timed out: %d, %s" % (exitcode, _pilotErrorDiag)
            else:
                if "timed out" in _pilotErrorDiag:
                    pilotErrorDiag = "cmtsite command was timed out: %d, %s" % (exitcode, _pilotErrorDiag)
                else:
                    pilotErrorDiag = "cmtsite command failed: %d, %s" % (exitcode, _pilotErrorDiag)

            tolog("!!WARNING!!2992!! %s" % (pilotErrorDiag))
        else:
            tolog("Release test command returned exit code %d" % (exitcode))
            pilotErrorDiag = ""

        return pilotErrorDiag

    def verifyCMTCONFIG(self, releaseCommand, cmtconfig_required, siteroot="", cacheDir="", cacheVer=""):
        """ Make sure that the required CMTCONFIG match that of the local system """
        # ...and extract CMTCONFIG, SITEROOT, ATLASVERSION, ATLASPROJECT

        status = True
        pilotErrorDiag = ""

        _cmd = "%s;echo CMTCONFIG=$CMTCONFIG;echo SITEROOT=$SITEROOT;echo ATLASVERSION=$AtlasVersion;echo ATLASPROJECT=$AtlasProject" % (releaseCommand)

        exitcode, output = timedCommand(_cmd, timeout=getProperTimeout(_cmd))
        tolog("Command output: %s" % (output))

        # Verify the cmtsite command
        pilotErrorDiag = self.verifyCmtsiteCmd(exitcode, output)
        if pilotErrorDiag != "":
            return False, pilotErrorDiag, siteroot, "", ""
        
        # Get cmtConfig
        re_cmtConfig = re.compile('CMTCONFIG=(.+)')
        _cmtConfig = re_cmtConfig.search(output)
        if _cmtConfig:
            cmtconfig_local = _cmtConfig.group(1)
        else:
            tolog("CMTCONFIG not found in command output: %s" % (output))
            cmtconfig_local = ""

        # Set siteroot if not already set
        if siteroot == "":
            re_sroot = re.compile('SITEROOT=(.+)')
            _sroot = re_sroot.search(output)
            if _sroot:
                siteroot = _sroot.group(1)
            else:
                tolog("SITEROOT not found in command output: %s" % (output))

        # Get atlasVersion
        re_atlasVersion = re.compile('ATLASVERSION=(.+)')
        _atlasVersion = re_atlasVersion.search(output)
        if _atlasVersion:
            atlasVersion = _atlasVersion.group(1)
        else:
            tolog("AtlasVersion not found in command output: %s" % (output))
            if cacheVer != "":
                tolog("AtlasVersion will be set to: %s" % (cacheVer))
            atlasVersion = cacheVer

        # Get atlasProject
        re_atlasProject = re.compile('ATLASPROJECT=(.+)')
        _atlasProject = re_atlasProject.search(output)
        if _atlasProject:
            atlasProject = _atlasProject.group(1)
        else:
            tolog("AtlasProject not found in command output: %s" % (output))
            if cacheDir != "":
                tolog("AtlasProject will be set to: %s" % (cacheDir))
            atlasProject = cacheDir

        # Verify cmtconfig
        if cmtconfig_local == "":
            cmtconfig_local = "local cmtconfig not set"
        elif cmtconfig_local == "NotSupported":
            pilotErrorDiag = "CMTCONFIG is not supported on the local system: %s (required of task: %s)" %\
                             (cmtconfig_local, cmtconfig_required)
            tolog("!!WARNING!!2990!! %s" % (pilotErrorDiag))
            status = False
        elif cmtconfig_local == "NotAvailable":
            if "non-existent" in output:
                output = output.replace("\n",",")
                pilotErrorDiag = "Installation problem: %s" % (output)
            else:
                pilotErrorDiag = "CMTCONFIG is not available on the local system: %s (required of task: %s)" %\
                                 (cmtconfig_local, cmtconfig_required)
            tolog("!!WARNING!!2990!! %s" % (pilotErrorDiag))
            status = False

        if cmtconfig_required == "" or cmtconfig_required == None:
            cmtconfig_required = "required cmtconfig not set"

        # Does the required CMTCONFIG match that of the local system?
        if status and cmtconfig_required != cmtconfig_local:
            pilotErrorDiag = "Required CMTCONFIG (%s) incompatible with that of local system (%s)" %\
                             (cmtconfig_required, cmtconfig_local)
            tolog("!!WARNING!!2990!! %s" % (pilotErrorDiag))
            status = False

        return status, pilotErrorDiag, siteroot, atlasVersion, atlasProject

    def checkCMTCONFIG(self, cmd1, cmtconfig, atlasRelease, siteroot="", cacheDir="", cacheVer=""):
        """ Make sure the CMTCONFIG is available and valid """

        ec = 0
        pilotErrorDiag = ""
        atlasProject = ""
        atlasVersion = ""

        # verify CMTCONFIG for set release strings only
        if verifyReleaseString(atlasRelease) != "NULL":
            status, pilotErrorDiag, siteroot, atlasVersion, atlasProject = self.verifyCMTCONFIG(cmd1, cmtconfig, siteroot=siteroot, cacheDir=cacheDir, cacheVer=cacheVer)
            if status:
                tolog("CMTCONFIG verified for release: %s" % (atlasRelease))
                if siteroot != "":
                    tolog("Got siteroot = %s from CMTCONFIG verification" % (siteroot))
                if atlasVersion != "":
                    tolog("Got atlasVersion = %s from CMTCONFIG verification" % (atlasVersion))
                if atlasProject != "":
                    tolog("Got atlasProject = %s from CMTCONFIG verification" % (atlasProject))
            else:
                if "Installation problem" in pilotErrorDiag:
                    errorText = "Installation problem discovered in release: %s" % (atlasRelease)
                    ec = self.__error.ERR_MISSINGINSTALLATION
                elif "timed out" in pilotErrorDiag:
                    errorText = "Command used for extracting CMTCONFIG, SITEROOT, etc, timed out"
                    ec = self.__error.ERR_COMMANDTIMEOUT
                else:
                    errorText = "CMTCONFIG verification failed for release: %s" % (atlasRelease)
                    ec = self.__error.ERR_CMTCONFIG
                tolog("!!WARNING!!1111!! %s" % (errorText))
        else:
            tolog("Skipping CMTCONFIG verification for unspecified release")

        return ec, pilotErrorDiag, siteroot, atlasVersion, atlasProject

    def getProdCmd2(self, installDir, homePackage):
        """ Get cmd2 for production jobs """

        pilotErrorDiag = ""

        # Define cmd2
        try:
            # Special case for nightlies
            if "rel_" in homePackage: # rel_N is already in installDir, do not add like below
                cmd2 = '' #unset CMTPATH;'
            else:
                cmd2 = 'unset CMTPATH;cd %s/%sRunTime/cmt;source ./setup.sh;cd -;' % (installDir, homePackage.split('/')[0])

                # Correct setup for athena post 14.5 (N.B. harmless for version < 14.5)
                cmd2 += 'export AtlasVersion=%s;export AtlasPatchVersion=%s' % (homePackage.split('/')[-1], homePackage.split('/')[-1])
        except Exception, e:
            pilotErrorDiag = "Bad homePackage format: %s, %s" % (homePackage, str(e))
            tolog("!!FAILED!!2999!! %s" % (pilotErrorDiag))
            cmd2 = ""

        return cmd2, pilotErrorDiag

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

    def getAnalyCmd2(self, swbase, cmtconfig, homePackage, atlasRelease):
        """ Return a proper cmd2 setup command """

        cacheDir = None
        cacheVer = None
        cmd2 = ""

        # cannot set cmd2 for unset release/homepackage strings
        if verifyReleaseString(atlasRelease) == "NULL" or verifyReleaseString(homePackage) == "NULL":
            return cmd2, cacheDir, cacheVer

        # homePackage=AnalysisTransforms-AtlasTier0_15.5.1.6
        # cacheDir = AtlasTier0
        # cacheVer = 15.5.1.6
        m_cacheDirVer = re.search('AnalysisTransforms-([^/]+)', homePackage)
        if m_cacheDirVer != None:
            cacheDir, cacheVer = self.getCacheInfo(m_cacheDirVer, atlasRelease)
            if not self.useAtlasSetup(swbase, atlasRelease, homePackage, cmtconfig):
                cmd2 = "export CMTPATH=$SITEROOT/%s/%s" % (cacheDir, cacheVer)

        return cmd2, cacheDir, cacheVer

    def updateAnalyCmd2(self, cmd2, atlasVersion, atlasProject, useAsetup):
        """ Add AtlasVersion and AtlasProject to cmd2 """

        # Add everything to cmd2 unless AtlasSetup is used
        if not useAsetup:
            if atlasVersion != "" and atlasProject != "":
                if cmd2 == "" or cmd2.endswith(";"):
                    pass
                else:
                    cmd2 += ";"
                cmd2 += "export AtlasVersion=%s;export AtlasProject=%s" % (atlasVersion, atlasProject)

        return cmd2

    def setPython(self, site_root, atlasRelease, homePackage, cmtconfig, sitename):
        """ set the python executable """

        ec = 0
        pilotErrorDiag = ""
        pybin = ""

        if os.environ.has_key('VO_ATLAS_SW_DIR') and verifyReleaseString(atlasRelease) != "NULL":
            ec, pilotErrorDiag, _pybin = self.findPythonInRelease(site_root, atlasRelease, homePackage, cmtconfig, sitename)
            if ec == self.__error.ERR_MISSINGINSTALLATION:
                return ec, pilotErrorDiag, pybin

            if _pybin != "":
                pybin = _pybin

        if pybin == "":
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

    def findPythonInRelease(self, siteroot, atlasRelease, homePackage, cmtconfig, sitename):
        """ Set the python executable in the release dir (LCG sites only) """

        ec = 0
        pilotErrorDiag = ""
        py = ""

        tolog("Trying to find a python executable for release: %s" % (atlasRelease))
        scappdir = readpar('appdir')

        # only use underscored cmtconfig paths on older cvmfs systems and only for now (remove at a later time)
        _cmtconfig = cmtconfig.replace("-", "_")

        if scappdir != "":
            _swbase = self.getLCGSwbase(scappdir)
            tolog("Using swbase: %s" % (_swbase))

            # get the site information object
            si = getSiteInformation(self.__experiment)

            if self.useAtlasSetup(_swbase, atlasRelease, homePackage, cmtconfig):
                cmd = self.getProperASetup(_swbase, atlasRelease, homePackage, cmtconfig, tailSemiColon=True)
                tolog("Using new AtlasSetup: %s" % (cmd))
            elif os.path.exists("%s/%s/%s/cmtsite/setup.sh" % (_swbase, _cmtconfig, atlasRelease)) and (si.isTier3() or "CERNVM" in sitename):
                # use cmtconfig sub dir on CERNVM or tier3 (actually for older cvmfs systems)
                cmd  = "source %s/%s/%s/cmtsite/setup.sh -tag=%s,32,runtime;" % (_swbase, _cmtconfig, atlasRelease, atlasRelease)
            else:
                ec, pilotErrorDiag, status = self.isForceConfigCompatible(_swbase, atlasRelease, homePackage, cmtconfig, siteroot=siteroot)
                if ec == self.__error.ERR_MISSINGINSTALLATION:
                    return ec, pilotErrorDiag, py
                else:
                    if status:
                        if "slc5" in cmtconfig and os.path.exists("%s/%s/gcc43_inst" % (_swbase, atlasRelease)):
                            cmd = "source %s/%s/gcc43_inst/setup.sh;export CMTCONFIG=%s;" % (_swbase, atlasRelease, cmtconfig)
                        elif "slc5" in cmtconfig and "slc5" in _swbase and os.path.exists("%s/%s" % (_swbase, atlasRelease)):
                            cmd = "source %s/%s/setup.sh;export CMTCONFIG=%s;" % (_swbase, atlasRelease, cmtconfig)
                        else:
                            cmd = "export CMTCONFIG=%s;" % (cmtconfig)
                        cmd += "source %s/%s/cmtsite/setup.sh -tag=AtlasOffline,%s,forceConfig,runtime;" % (_swbase, atlasRelease, atlasRelease)
                    else:
                        cmd  = "source %s/%s/cmtsite/setup.sh -tag=%s,32,runtime;" % (_swbase, atlasRelease, atlasRelease)
        else:
            vo_atlas_sw_dir = os.path.expandvars('$VO_ATLAS_SW_DIR')
            if "gcc43" in cmtconfig and vo_atlas_sw_dir != '' and os.path.exists('%s/software/slc5' % (vo_atlas_sw_dir)):
                cmd  = "source $VO_ATLAS_SW_DIR/software/slc5/%s/setup.sh;" % (atlasRelease)
                tolog("Found explicit slc5 dir in path: %s" % (cmd))
            else:
                # no known appdir, default to VO_ATLAS_SW_DIR
                _appdir = vo_atlas_sw_dir
                _swbase = self.getLCGSwbase(_appdir)
                tolog("Using swbase: %s" % (_swbase))
                if self.useAtlasSetup(_swbase, atlasRelease, homePackage, cmtconfig):
                    cmd = self.getProperASetup(_swbase, atlasRelease, homePackage, cmtconfig, tailSemiColon=True)
                    tolog("Using new AtlasSetup: %s" % (cmd))
                else:
                    _path = os.path.join(_appdir, "software/%s/cmtsite/setup.sh" % (atlasRelease))
                    if os.path.exists(_path):
                        cmd = "source " + _path + ";"
                    else:
                        cmd = ""
                        tolog("!!WARNING!!1888!! No known path for setup script (using default python version)")

        cmd += "which python"
        exitcode, output = timedCommand(cmd, timeout=getProperTimeout(cmd))

        if exitcode == 0:
            if output.startswith('/'):
                tolog("Found: %s" % (output))
                py = output
            else:
                if '\n' in output:
                    output = output.split('\n')[-1]

                if output.startswith('/'):
                    tolog("Found: %s" % (output))
                    py = output
                else:
                    tolog("!!WARNING!!4000!! No python executable found in release dir: %s" % (output))
                    tolog("!!WARNING!!4000!! Will use default python")
                    py = "python"
        else:
            tolog("!!WARNING!!4000!! Find command failed: %d, %s" % (exitcode, output))
            tolog("!!WARNING!!4000!! Will use default python")
            py = "python"

        return ec, pilotErrorDiag, py

    def getLCGSwbase(self, scappdir):
        """ Return the LCG swbase """

        if os.path.exists(os.path.join(scappdir, 'software/releases')):
            _swbase = os.path.join(scappdir, 'software/releases')
        elif os.path.exists(os.path.join(scappdir, 'software')):
            _swbase = os.path.join(scappdir, 'software')
        else:
            _swbase = scappdir

        return _swbase

    def getProdCmd3(self, cmd, pybin, jobtrf, jobPars):
        """ Prepare the cmd3 command with the python from the release and the full path to the trf """
        # When python is invoked using the full path, it also needs the full path to the script

        # First try to figure out where the trf is inside the release
        if not cmd.endswith(";"):
            cmd += ";"
        _cmd = "%swhich %s" % (cmd, jobtrf)
        _timedout = False

        exitcode, _trf = timedCommand(_cmd, timeout=getProperTimeout(cmd))
        if exitcode != 0:
            _timedout = True
        tolog("Trf: %s" % (_trf))

        # split the output if necessary (the path should be the last entry)
        if "\n" in _trf:
            _trf = _trf.split("\n")[-1]
            tolog("Trf: %s (extracted)" % (_trf))

        # could the trf be found?
        if "which: no" in _trf or jobtrf not in _trf or _timedout:
            tolog("!!WARNING!!2999!! Will not use python from the release since the trf path could not be figured out")
            cmd3 = "%s %s" % (jobtrf, jobPars)
        else:
            tolog("Will use python from the release: %s" % (pybin))
            tolog("Path to trf: %s" % (_trf))
            cmd3 = "%s %s %s" % (pybin, _trf, jobPars)

        return cmd3

    def addEnvVars2Cmd(self, cmd, jobId, processingType):
        """ Add FRONTIER debugging and RUCIO env variables """

        _frontier1 = 'export FRONTIER_ID=\"[%s]\";' % (jobId)
        _frontier2 = 'export CMSSW_VERSION=$FRONTIER_ID;'

        if processingType == "":
            _rucio = ''
            tolog("!!WARNING!!1887!! RUCIO_APPID needs job.processingType but it is not set!")
        else:
            _rucio = 'export RUCIO_APPID=\"%s\";' % (processingType)

        return _frontier1 + _frontier2 + _rucio + cmd

    def isForceConfigCompatible(self, _dir, release, homePackage, cmtconfig, siteroot=None):
        """ Test if the installed AtlasSettings and AtlasLogin versions are compatible with forceConfig """
        # The forceConfig cmt tag was introduced in AtlasSettings-03-02-07 and AtlasLogin-00-03-07

        status = True
        ec = 0
        pilotErrorDiag = ""

        # only perform the forceConfig test for set release strings
        if verifyReleaseString(release) == "NULL":
            return ec, pilotErrorDiag, False

        names = {"AtlasSettings":"AtlasSettings-03-02-07", "AtlasLogin":"AtlasLogin-00-03-07" }
        for name in names.keys():
            try:
                ec, pilotErrorDiag, v, siteroot = self.getHighestVersionDir(release, homePackage, name, _dir, cmtconfig, siteroot=siteroot)
            except Exception, e:
                tolog("!!WARNING!!2999!! Exception caught: %s" % str(e))
                v = None
            if v:
                if v >= names[name]:
                    tolog("%s version verified: %s" % (name, v))
                else:
                    tolog("%s version too old: %s (older than %s, not forceConfig compatible)" % (name, v, names[name]))
                    status = False
            else:
                tolog("%s version not verified (not forceConfig compatible)" % (name))
                status = False

        return ec, pilotErrorDiag, status

    def getHighestVersionDir(self, release, homePackage, name, swbase, cmtconfig, siteroot=None):
        """ Grab the directory (AtlasLogin, AtlasSettings) with the highest version number """
        # e.g. v = AtlasLogin-00-03-26

        highestVersion = None
        ec = 0
        pilotErrorDiag = ""

        # get the siteroot
        if not siteroot:
            ec, pilotErrorDiag, status, siteroot, cmtconfig = self.getProperSiterootAndCmtconfig(swbase, release, homePackage, cmtconfig)
        else:
            status = True

        if ec != 0:
            return ec, pilotErrorDiag, None, siteroot

        if status and siteroot != "" and os.path.exists(os.path.join(siteroot, name)):
            _dir = os.path.join(siteroot, name)
        else:
            if swbase[-len('builds'):] == 'builds':
                _dir = os.path.join(swbase, name)
            else:
                _dir = os.path.join(swbase, release, name)

        if not os.path.exists(_dir):
            tolog("Directory does not exist: %s" % (_dir))
            return ec, pilotErrorDiag, None, siteroot

        tolog("Probing directory: %s" % (_dir))
        if os.path.exists(_dir):
            dirs = os.listdir(_dir)
            _dirs = []
            if dirs != []:
                tolog("Found directories: %s" % str(dirs))
                for d in dirs:
                    if d.startswith(name):
                        _dirs.append(d)
                if _dirs != []: 
                    # sort the directories
                    _dirs.sort()
                    # grab the directory with the highest version
                    highestVersion = _dirs[-1]
                    tolog("Directory with highest version: %s" % (highestVersion))
                else:
                    tolog("WARNING: Found no %s dirs in %s" % (name, _dir))
            else:
                tolog("WARNING: Directory is empty: %s" % (_dir))
        else:
            tolog("Directory does not exist: %s" % (_dir))

        return ec, pilotErrorDiag, highestVersion, siteroot

    def getProperSiterootAndCmtconfig(self, swbase, release, homePackage, _cmtconfig, cmtconfig_alternatives=None):
        """ return a proper $SITEROOT and cmtconfig """

        status = False
        siteroot = ""
        ec = 0 # only non-zero for fatal errors (missing installation)
        pilotErrorDiag = ""

        # make sure the cmtconfig_alternatives is not empty/not set
        if not cmtconfig_alternatives:
            cmtconfig_alternatives = [_cmtconfig]

        if readpar('region') == 'CERN':
            if swbase[-len('builds'):] == 'builds':
                status = True
                return ec, pilotErrorDiag, status, swbase, _cmtconfig

        # loop over all available cmtconfig's until a working one is found (the default cmtconfig value is the first to be tried)
        for cmtconfig in cmtconfig_alternatives:
            ec = 0
            pilotErrorDiag = ""
            tolog("Testing cmtconfig=%s" % (cmtconfig))

            if self.useAtlasSetup(swbase, release, homePackage, cmtconfig):
                cmd = self.getProperASetup(swbase, release, homePackage, cmtconfig, tailSemiColon=True)
                cmd += " echo SITEROOT=$SITEROOT"
            elif "slc5" in cmtconfig and "gcc43" in cmtconfig:
                cmd = "source %s/%s/cmtsite/setup.sh -tag=AtlasOffline,%s,%s,runtime; echo SITEROOT=$SITEROOT" % (swbase, release, release, cmtconfig)
            else:
                cmd = "source %s/%s/cmtsite/setup.sh -tag=AtlasOffline,%s,runtime; echo SITEROOT=$SITEROOT" % (swbase, release, release)

            # verify that the setup path actually exists before attempting the source command
            ec, pilotErrorDiag = verifySetupCommand(self.__error, cmd)
            if ec != 0:
                pilotErrorDiag = "getProperSiterootAndCmtconfig: Missing installation: %s" % (pilotErrorDiag)
                tolog("!!WARNING!!1996!! %s" % (pilotErrorDiag))
                ec = self.__error.ERR_MISSINGINSTALLATION
                continue

            (exitcode, output) = timedCommand(cmd, timeout=getProperTimeout(cmd))

            if exitcode != 0 or "Error:" in output or "(ERROR):" in output:
                # if time out error, don't bother with trying another cmtconfig

                tolog("ATLAS setup for SITEROOT failed")
                if "No such file or directory" in output:
                    pilotErrorDiag = "getProperSiterootAndCmtconfig: Missing installation: %s" % (output)
                    tolog("!!WARNING!!1996!! %s" % (pilotErrorDiag))
                    ec = self.__error.ERR_MISSINGINSTALLATION
                    continue
                elif "Error:" in output:
                    pilotErrorDiag = "getProperSiterootAndCmtconfig: Caught CMT error: %s" % (output)
                    tolog("!!WARNING!!1996!! %s" % (pilotErrorDiag))
                    ec = self.__error.ERR_SETUPFAILURE
                    continue
                elif "AtlasSetup(ERROR):" in output:
                    pilotErrorDiag = "getProperSiterootAndCmtconfig: Caught AtlasSetup error: %s" % (output)
                    tolog("!!WARNING!!1996!! %s" % (pilotErrorDiag))
                    ec = self.__error.ERR_SETUPFAILURE
                    continue

            if output:
                tolog("Command output: %s" % (output))
                if 'SITEROOT' in output:
                    re_sroot = re.compile('SITEROOT=(.+)')
                    _sroot = re_sroot.search(output)
                    if _sroot:
                        siteroot = _sroot.group(1)
                        status = True
                        break
                    else:
                        # should this case be accepted?
                        ec = self.__error.ERR_SETUPFAILURE
                        pilotErrorDiag = "SITEROOT not found in command output: %s" % (output)
                        tolog("WARNING: %s" % (pilotErrorDiag))
                        continue
                else:
                    siteroot = os.path.join(swbase, release)
                    siteroot = siteroot.replace('//','/')
                    status = True
                    break
            else:
                pilotErrorDiag = "getProperSiterootAndCmtconfig: Command produced no output"
                tolog("WARNING: %s" % (pilotErrorDiag))

        # reset errors if siteroot was found
        if status:
            ec = 0
            pilotErrorDiag = ""
        return ec, pilotErrorDiag, status, siteroot, cmtconfig

    def useAtlasSetup(self, swbase, release, homePackage, cmtconfig):
        """ determine whether AtlasSetup is to be used """

        status = False

        # are we using at least release 16.1.0?
        if release >= "16.1.0":
            # try with the cmtconfig in the path
            rel_N = None
            path = None
            # are we using nightlies?
            if "rel_" in homePackage:
                # extract the rel_N bit and use it in the path
                rel_N = self.extractRelN(homePackage)
                tolog("Extracted %s from homePackage=%s" % (rel_N, homePackage))
                if rel_N:
                    path = "%s/%s/%s/%s/cmtsite/asetup.sh" % (swbase, cmtconfig, release, rel_N)
            if not path:
                path = "%s/%s/%s/cmtsite/asetup.sh" % (swbase, cmtconfig, release)
            status = os.path.exists(path)
            if status:
                tolog("Using AtlasSetup (%s exists with cmtconfig in the path)" % (path))
            else:
                tolog("%s does not exist (trying without cmtconfig in the path)" % (path))
                if rel_N:
                    path = "%s/%s/%s/cmtsite/asetup.sh" % (swbase, release, rel_N)
                else:
                    path = "%s/%s/cmtsite/asetup.sh" % (swbase, release)
                status = os.path.exists(path)
                if status:
                    tolog("Using AtlasSetup (%s exists)" % (path))
                else:
                    tolog("Cannot use AtlasSetup since %s does not exist either" % (path))
        else:
            pass
            # tolog("Release %s is too old for AtlasSetup (need at least 16.1.0)" % (release))

        return status

    def getProperASetup(self, swbase, atlasRelease, homePackage, cmtconfig, tailSemiColon=False, source=True, cacheVer=None, cacheDir=None):
        """ return a proper asetup.sh command """

        # handle sites using builds area in a special way
        if swbase[-len('builds'):] == 'builds' or verifyReleaseString(atlasRelease) == "NULL":
            path = swbase
        else:
            if os.path.exists(os.path.join(swbase, cmtconfig)):
                if os.path.exists(os.path.join(os.path.join(swbase, cmtconfig), atlasRelease)):
                    path = os.path.join(os.path.join(swbase, cmtconfig), atlasRelease)
                else:
                    path = os.path.join(swbase, atlasRelease)
            else:
                path = os.path.join(swbase, atlasRelease)

        # need to tell asetup where the compiler is in the US (location of special config file)
        _path = "%s/AtlasSite/AtlasSiteSetup" % (path)
        if readpar('region') == "US" and os.path.exists(_path):
            _input = "--input %s" % (_path)
        else:
            _input = ""

        # add a tailing semicolon if needed
        if tailSemiColon:
            tail = ";"
        else:
            tail = ""

        # add the source command (default), not wanted for installPyJobTransforms()
        if source:
            cmd = "source"
        else:
            cmd = ""

        # define the setup options
        if not cacheVer:
            cacheVer = atlasRelease

        # add the fast option if possible (for the moment, check for locally defined env variable)
        if os.environ.has_key("ATLAS_FAST_ASETUP"):
            options = cacheVer + ",notest,fast"
        else:
            options = cacheVer + ",notest"
        if cacheDir and cacheDir != "":
            options += ",%s" % (cacheDir)

        # nightlies setup?
        if "rel_" in homePackage:
            # extract the rel_N bit and use it in the path
            rel_N = self.extractRelN(homePackage)
            if rel_N:
                tolog("Extracted %s from homePackage=%s" % (rel_N, homePackage))
                asetup_path = "%s/%s/cmtsite/asetup.sh" % (path, rel_N)
                # use special options for nightlies (not the release info set above)
                options = rel_N + ",notest"
            else:
                tolog("!!WARNING!!1111!! Failed to extract rel_N from homePackage=%s (forced to use default cmtsite setup)" % (homePackage))
                asetup_path = "%s/cmtsite/asetup.sh" % (path)
        else:
            asetup_path = "%s/cmtsite/asetup.sh" % (path)

        return "%s %s %s --cmtconfig %s %s%s" % (cmd, asetup_path, options, cmtconfig, _input, tail)

    def extractRelN(self, homePackage):
        """ Extract the rel_N bit from the homePackage string """

        # s = "AtlasProduction,rel_0"
        # -> rel_N = "rel_0"

        rel_N = None

        if "AnalysisTransforms" in homePackage and "_rel_" in homePackage:
            pattern = re.compile(r"AnalysisTransforms\-[A-Za-z0-9]+\_(rel\_\d+)")
            found = re.findall(pattern, homePackage)
            if len(found) > 0:
                rel_N = found[0]

        elif not "," in homePackage:
            rel_N = homePackage

        elif homePackage != "":
            pattern = re.compile(r"[A-Za-z0-9]+,(rel\_\d+)")
            found = re.findall(pattern, homePackage)
            if len(found) > 0:
                rel_N = found[0]

        return rel_N

    def dump(self, path, cmd="cat"):
        """ Dump the content of path to the log """

        if cmd != "cat":
            _cmd = "%s %s" % (cmd, path)
            tolog("%s:\n%s" % (_cmd, commands.getoutput(_cmd)))
        else:
            if os.path.exists(path):
                _cmd = "%s %s" % (cmd, path)
                tolog("%s:\n%s" % (_cmd, commands.getoutput(_cmd)))
            else:
                tolog("Path %s does not exist" % (path))

    def displayArchitecture(self):
        """ Display system architecture """

        tolog("Architecture information:")

        cmd = "lsb_release -a"
        tolog("Excuting command: %s" % (cmd))
        out = commands.getoutput(cmd)
        if "Command not found" in out:
            # Dump standard architecture info files if available
            self.dump("/etc/lsb-release")
            self.dump("/etc/SuSE-release")
            self.dump("/etc/redhat-release")
            self.dump("/etc/debian_version")
            self.dump("/etc/issue")
            self.dump("$MACHTYPE", cmd="echo")
        else:
            tolog("\n%s" % (out))

    def specialChecks(self, **kwargs):
        """ Implement special checks here """

        status = False
        # appdir = kwargs.get('appdir', '')

        # Display system architecture
        self.displayArchitecture()

        # Display the cvmfs ChangeLog is possible
        self.displayChangeLog()

        # Set the python version used by the pilot
        setPilotPythonVersion()

        # Test the LFC module
        status = self.testImportLFCModule()

        return status

    def getPayloadName(self, job):
        """ Figure out a suitable name for the payload stdout """

        if job.processingType in ['prun']:
            name = job.processingType
        else:
            jobtrf = job.trf.split(",")[0]
            if jobtrf.find("panda") > 0 and jobtrf.find("mover") > 0:
                name = "pandamover"
            elif jobtrf.find("Athena") > 0 or jobtrf.find("trf") > 0 or jobtrf.find("_tf") > 0:
                name = "athena"
            else:
                if isBuildJob(job.outFiles):
                    name = "buildjob"
                else:
                    name = "payload"

        return name

    def getMetadataForRegistration(self, guid):
        """ Return metadata (not known yet) for LFC registration """

        # Use the GUID as identifier (the string "<GUID>-surltobeset" will later be replaced with the SURL)        
        return '    <metadata att_name="surl" att_value="%s-surltobeset"/>\n' % (guid) 

    def getAttrForRegistration(self):
        """ Return the attribute of the metadata XML to be updated with surl value """
        
        return 'surl'

if __name__ == "__main__":

    a=ATLASExperiment()
    a.specialChecks()

    appdir='/cvmfs/atlas.cern.ch/repo/sw'
    #a.specialChecks(appdir=appdir)
