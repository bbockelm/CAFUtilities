# Class definition:
#   ErrorDiagnosis
#

import os
import commands
from Diagnosis import Diagnosis
from PilotErrors import PilotErrors
from pUtil import tolog, getExperiment
from RunJobUtilities import getStdoutFilename, getSourceSetup, findVmPeaks

class ErrorDiagnosis(Diagnosis):

    # private data members
    __instance = None                      # Boolean used by subclasses to become a Singleton
    __error = PilotErrors()                # PilotErrors object

    def __init__(self):
        """ Default initialization """

        # e.g. self.__errorLabel = errorLabel
        pass

    def __new__(cls, *args, **kwargs):
        """ Override the __new__ method to make the class a singleton """

        if not cls.__instance:
            cls.__instance = super(ErrorDiagnosis, cls).__new__(cls, *args, **kwargs)

        return cls.__instance

    def interpretPayload(self, job, res, getstatusoutput_was_interrupted, current_job_number, runCommandList, failureCode, experiment):
        """ Interpret the payload, look for specific errors in the stdout """

        # Extract job information (e.g. number of events)
        job = self.extractJobInformation(job, runCommandList, experiment) # add more arguments as needed

        # interpret the stdout
        job = self.interpretPayloadStdout(job, res, getstatusoutput_was_interrupted, current_job_number, runCommandList, failureCode, experiment)

        return job

    def extractJobInformation(self, job, runCommandList, experiment):
        """ Extract relevant job information, e.g. number of events """

        # get the experiment object
        thisExperiment = getExperiment(experiment)
        if not thisExperiment:
            job.pilotErrorDiag = "ErrorDiagnosis did not get an experiment object from the factory"
            job.result[2] = error.ERR_GENERALERROR # change to better/new error code
            tolog("!!WARNING!!3234!! %s" % (job.pilotErrorDiag))
            return job

        # note that this class should not be experiment specific, so move anything related to ATLAS to ATLASExperiment.py
        # and use thisExperiment.whatever() to retrieve it here

        # grab the number of events
        try:
            # nEvents_str can be a string of the form N|N|..|N with the number of jobs in the trf(s) [currently not used]
            # Add to Job class if necessary
            job.nEvents, job.nEventsW, nEvents_str = thisExperiment.getNumberOfEvents(job=job, number_of_jobs=len(runCommandList))
        except Exception, e:
            tolog("!!WARNING!!2999!! Failed to get number of events: %s (ignore)" % str(e))

        return job

    def interpretPayloadStdout(self, job, res, getstatusoutput_was_interrupted, current_job_number, runCommandList, failureCode, experiment):
        """ payload error handling """

        # NOTE: Move away ATLAS specific info in this method, e.g. vmPeak stuff

        error = PilotErrors()
        transExitCode = res[0]%255

        # Get the proper stdout filename
        number_of_jobs = len(runCommandList)
        filename = getStdoutFilename(job.workdir, job.stdout, current_job_number, number_of_jobs)

        # get the experiment object
        thisExperiment = getExperiment(experiment)
        if not thisExperiment:
            job.pilotErrorDiag = "ErrorDiagnosis did not get an experiment object from the factory"
            job.result[2] = error.ERR_GENERALERROR # change to better/new error code
            tolog("!!WARNING!!3334!! %s" % (job.pilotErrorDiag))
            return job

        # Try to identify out of memory errors in the stderr
        out_of_memory = thisExperiment.isOutOfMemory(job=job, number_of_jobs=number_of_jobs)
        failed = out_of_memory # failed boolean used below

        # Always look for the max and average VmPeak?
        setup = getSourceSetup(runCommandList[0])
        job.vmPeakMax, job.vmPeakMean, job.RSSMean = findVmPeaks(setup)

        # A killed job can have empty output but still transExitCode == 0
        no_payload_output = False
        installation_error = False
        if getstatusoutput_was_interrupted:
            if os.path.exists(filename):
                if os.path.getsize(filename) > 0:
                    tolog("Payload produced stdout but was interrupted (getstatusoutput threw an exception)")
                else:
                    no_payload_output = True
                failed = True
            else:
                failed = True
                no_payload_output = True
        elif len(res[1]) < 20: # protect the following comparison against massive outputs
            if res[1] == 'Undefined':
                failed = True
                no_payload_output = True
        elif failureCode:
            failed = True
        else:
            # check for installation error
            res_tmp = res[1][:1024]
            if res_tmp[0:3] == "sh:" and 'setup.sh' in res_tmp and 'No such file or directory' in res_tmp:
                failed = True
                installation_error = True

        # note: several errors below are atlas specific (not all), should be handled through ATLASExperiment via thisExperiment object
        # move entire section below to ATLASExperiment, define prototype [empty] methods in Experiment and OtherExperiment classes, implement in ATLASExperiment
        # non experiment specific errors should be handled here (e.g. no_payload_output)

        # handle non-zero failed job return code but do not set pilot error codes to all payload errors
        if transExitCode or failed:
            if failureCode:
                job.pilotErrorDiag = "Payload failed: Interrupt failure code: %d" % (failureCode)
                # (do not set pilot error code)
            elif getstatusoutput_was_interrupted:
                raise Exception, "Job execution was interrupted (see stderr)"
            elif out_of_memory:
                job.pilotErrorDiag = "Payload ran out of memory"
                job.result[2] = error.ERR_ATHENAOUTOFMEMORY
            elif no_payload_output:
                job.pilotErrorDiag = "Payload failed: No output"
                job.result[2] = error.ERR_NOPAYLOADOUTPUT
            elif installation_error:
                job.pilotErrorDiag = "Payload failed: Missing installation"
                job.result[2] = error.ERR_MISSINGINSTALLATION
            elif transExitCode:
                # Handle PandaMover errors
                if transExitCode == 176:
                    job.pilotErrorDiag = "PandaMover staging error: File is not cached"
                    job.result[2] = error.ERR_PANDAMOVERFILENOTCACHED
                elif transExitCode == 86:
                    job.pilotErrorDiag = "PandaMover transfer failure"
                    job.result[2] = error.ERR_PANDAMOVERTRANSFER
                else:
                    # check for specific errors in athena stdout
                    if os.path.exists(filename):
                        e1 = "prepare 5 database is locked"
                        e2 = "Error SQLiteStatement"
                        _out = commands.getoutput('grep "%s" %s | grep "%s"' % (e1, filename, e2))
                        if 'sqlite' in _out:
                            job.pilotErrorDiag = "NFS/SQLite locking problems: %s" % (_out)
                            job.result[2] = error.ERR_NFSSQLITE
                        else:
                            tolog("Mancinellidebug Error - failurecode = %s, transExitCode = %s, res = %s, res[0] = %s, res[0]255 = %s" % (failureCode, transExitCode, res, res[0], res[0]%255)) 
                            job.pilotErrorDiag = "Job failed: Non-zero failed job return code: %d" % (transExitCode)
                            # (do not set a pilot error code)
                    else:
                        job.pilotErrorDiag = "Job failed: Non-zero failed job return code: %d (%s does not exist)" % (transExitCode, filename)
                        # (do not set a pilot error code)
            else:
                job.pilotErrorDiag = "Payload failed due to unknown reason (check payload stdout)"
                job.result[2] = error.ERR_UNKNOWN
            tolog("!!FAILED!!3000!! %s" % (job.pilotErrorDiag))

        # set the trf diag error
        if res[2] != "":
            tolog("TRF diagnostics: %s" % (res[2]))
            job.exeErrorDiag = res[2]

        job.result[1] = transExitCode
        return job

    
if __name__ == "__main__":

    print "Implement test cases here"

    ed = ErrorDiagnosis()
    # ed.hello()
    
