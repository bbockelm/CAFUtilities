import os
from commands import getstatusoutput
from shutil import copy2

from PilotErrors import PilotErrors
from pUtil import tolog, readpar, timeStamp, getBatchSystemJobID, getCPUmodel, PFCxml, updateMetadata, addSkippedToPFC, makeHTTPUpdate, tailPilotErrorDiag, isLogfileCopied, updateJobState, getAtlasRelease, updateXMLWithSURLs, getMetadata
from JobState import JobState
from FileState import FileState

class PandaServerClient:
    """
    Client to the Panda Server
    Methods for communicating with the Panda Server
    """

    # private data members
    __errorString = "!!WARNING!!1992!! %s" # default error string
    __error = PilotErrors() # PilotErrors object
    __pilot_version_tag = ""
    __pilot_initdir = ""
    __jobSchedulerId = ""
    __pilotId = ""
    __updateServer = True
    __jobrec = False
    __pshttpurl = ""

    def __init__(self, pilot_version="", pilot_version_tag="", pilot_initdir="", jobSchedulerId=None, pilotId=None, updateServer=True, jobrec=False, pshttpurl=""):
        """ Default initialization """

        self.__pilot_version_tag = pilot_version_tag
        self.__pilot_initdir = pilot_initdir
        self.__jobSchedulerId = jobSchedulerId
        self.__pilotId = pilotId
        self.__updateServer = updateServer
        self.__jobrec = jobrec
        self.__pshttpurl = pshttpurl
        self.__pilot_version = pilot_version

    def getNodeStructureFromFile(self, workDir, jobId):
        """ get the node structure from the Job State file """

        JS = JobState()
        _node = None

        # open the job state file
        tolog("workDir: %s" % (workDir))
        tolog("jobId: %s" % (jobId))
        filename = JS.getFilename(workDir, jobId)
        tolog("filename: %s" % (filename))
        if os.path.exists(filename):
            # load the objects
            if JS.get(filename):
                # decode the job state info
                _job, _site, _node, _recoveryAttempt = JS.decode()
            else:
                tolog("JS.decode() failed to load objects")
        else:
            tolog("%s does not exist" % (filename))
        return _node

    def copyNodeStruct4NG(self, node):
        """ store the node structure for ARC """

        from pickle import dump
        try:
            _fname = "%s/panda_node_struct.pickle" % os.getcwd()
            fp = open(_fname, "w")
        except Exception, e:
            tolog("!!WARNING!!2999!! Could not store panda node structure: %s" % str(e))
        else:
            try:
                dump(node, fp)
                fp.close()
            except Exception, e:
                tolog("!!WARNING!!2999!! Could not dump panda node structure: %s" % str(e))
            else:
                tolog("Stored panda node structure at: %s" % (_fname))
                tolog("node : %s" % (str(node)))
                try:
                    copy2(_fname, self.__pilot_initdir)
                except Exception, e:
                    tolog("!!WARNING!!2999!! Could not copy panda node structure to init dir: %s" % str(e))
                else:
                    tolog("Copied panda node structure (%s) to init dir: %s" % (_fname, self.__pilot_initdir))

    def getJobMetrics(self, job, workerNode):
        """ Return a properly formatted job metrics string """

        # style: Number of events read | Number of events written | vmPeak maximum | vmPeak average | RSS average | JEM activation
        # format: nEvents=<int> nEventsW=<int> vmPeakMax=<int> vmPeakMean=<int> RSSMean=<int> JEM=<string>
        #         hs06=<float> shutdownTime=<int> cpuFactor=<float> cpuLimit=<float> diskLimit=<float> jobStart=<int> memLimit=<int> runLimit=<float>
        jobMetrics = ""
        if job.nEvents > 0:
            jobMetrics += "nEvents=%d" % (job.nEvents)
        if job.nEventsW > 0:
            jobMetrics += " nEventsW=%d" % (job.nEventsW)
        if job.vmPeakMax > 0:
            jobMetrics += " vmPeakMax=%d" % (job.vmPeakMax)
        if job.vmPeakMean > 0:
            jobMetrics += " vmPeakMean=%d" % (job.vmPeakMean)
        if job.RSSMean > 0:
            jobMetrics += " RSSMean=%d" % (job.RSSMean)

        # report FAX transfers if at least one successful FAX transfer
        if job.filesWithFAX > 0:
            jobMetrics += " filesWithFAX=%d" % (job.filesWithFAX)
            jobMetrics += " filesWithoutFAX=%d" % (job.filesWithoutFAX)

        # report alternative stage-out in case alt SE method was used
        # (but not in job recovery mode)
        recovery_mode = False
        if job.filesAltStageOut > 0 and not recovery_mode:
            _jobMetrics = ""
            _jobMetrics += " filesAltStageOut=%d" % (job.filesAltStageOut)
            _jobMetrics += " filesNormalStageOut=%d" % (job.filesNormalStageOut)
            tolog("Could have reported: %s" % (_jobMetrics))

        # only add the JEM bit if explicitly set to YES, otherwise assumed to be NO
        if job.JEM == "YES":
            jobMetrics += " JEM=1"
            # old format: jobMetrics += " JEM=%s" % (job.JEM)

        # machine and job features
        # jobMetrics += workerNode.addToJobMetrics()
        _jobMetrics = ""
        _jobMetrics += workerNode.addToJobMetrics()
        if _jobMetrics != "":
            tolog("Could have added: %s to job metrics" % (workerNode.addToJobMetrics()))

        # correct for potential initial space
        jobMetrics = jobMetrics.lstrip(' ')

        if jobMetrics != "":
            tolog('Job metrics=\"%s\"' % (jobMetrics))
        else:
            tolog("No job metrics (all values are zero)")

        # is jobMetrics within allowed size?
        if len(jobMetrics) > 500:
            tolog("!!WARNING!!2223!! jobMetrics out of size (%d)" % (len(jobMetrics)))

            # try to reduce the field size and remove the last entry which might be cut
            jobMetrics = jobMetrics[:-500]
            jobMetrics = " ".join(jobMetrics.split(" ")[:-1])

            tolog("jobMetrics has been reduced to: %s" % (jobMetrics))

        return jobMetrics

    def getNodeStructure(self, job, site, workerNode, spaceReport=False, log=None):
        """ define the node structure expected by the server """

        node = {}

        node['node'] = workerNode.nodename
        node['workdir'] = job.workdir
        node['siteName'] = site.sitename
        node['jobId'] = job.jobId
        node['state'] = job.result[0]
        node['timestamp'] = timeStamp()
        if job.attemptNr > -1:
            node['attemptNr'] = job.attemptNr
        if self.__jobSchedulerId:
            node['schedulerID'] = self.__jobSchedulerId
        if self.__pilotId:
            # report the batch system job id, if available
            batchSystemType, _id = getBatchSystemJobID()
            if batchSystemType:
                tolog("Batch system: %s" % (batchSystemType))
                tolog("Batch system job ID: %s" % (_id))
                node['pilotID'] = "%s|%s|%s|%s|%s" % (self.__pilotId, _id, batchSystemType, self.__pilot_version_tag, self.__pilot_version)
                node['batchID'] = _id
                tolog("Will send batchID: %s and pilotID: %s" % (node['batchID'], node['pilotID']))
            else:
                tolog("Batch system type was not identified (will not be reported)")
                node['pilotID'] = "%s|%s|%s" % (self.__pilotId, self.__pilot_version_tag, self.__pilot_version)
                tolog("Will send pilotID: %s" % (node['pilotID']))
            tolog("pilotId: %s" % str(self.__pilotId)) 
        if log and (job.result[0] == 'failed' or job.result[0] == 'holding' or "outbound connections" in log):
            node['pilotLog'] = log

        # build the jobMetrics
        node['jobMetrics'] = self.getJobMetrics(job, workerNode)

        # send pilotErrorDiag for finished, failed and holding jobs
        if job.result[0] == 'finished' or job.result[0] == 'failed' or job.result[0] == 'holding':
            # get the pilot error diag
            if job.pilotErrorDiag:
                if job.pilotErrorDiag == "":
                    node['pilotErrorDiag'] = tailPilotErrorDiag(self.__error.getPilotErrorDiag(job.result[2]))
                    job.pilotErrorDiag = node['pilotErrorDiag']
                    tolog("Empty pilotErrorDiag set to: %s" % (job.pilotErrorDiag))
                elif job.pilotErrorDiag.upper().find("<HTML>") >= 0:
                    tolog("Found html in pilotErrorDiag: %s" % (job.pilotErrorDiag))
                    node['pilotErrorDiag'] = self.__error.getPilotErrorDiag(job.result[2])
                    job.pilotErrorDiag = node['pilotErrorDiag']
                    tolog("Updated pilotErrorDiag: %s" % (job.pilotErrorDiag))
                else:
                    # truncate if necesary
                    if len(job.pilotErrorDiag) > 250:
                        tolog("pilotErrorDiag will be truncated to size 250")
                        tolog("Original pilotErrorDiag message: %s" % (job.pilotErrorDiag))
                        job.pilotErrorDiag = job.pilotErrorDiag[:250]
                    # set the pilotErrorDiag, but only the last 256 characters
                    node['pilotErrorDiag'] = tailPilotErrorDiag(job.pilotErrorDiag)
            else:
                # set the pilotErrorDiag, but only the last 256 characters
                job.pilotErrorDiag = self.__error.getPilotErrorDiag(job.result[2])
                node['pilotErrorDiag'] = tailPilotErrorDiag(job.pilotErrorDiag)
                tolog("Updated pilotErrorDiag from None: %s" % (job.pilotErrorDiag))

            # get the number of events
            if job.nEvents != 0:
                node['nEvents'] = job.nEvents
                tolog("Total number of processed events: %d (read)" % (job.nEvents))
            else:
                tolog("runJob did not report on the total number of read events")

        if job.result[0] == 'finished' or job.result[0] == 'failed':
            # make sure there is no mismatch between the transformation error codes (when both are reported)
            # send transformation errors depending on what is available
            if job.exeErrorDiag != "":
                node['exeErrorCode'] = job.exeErrorCode
                node['exeErrorDiag'] = job.exeErrorDiag
            else:
                node['transExitCode'] = job.result[1]
            if (job.result[0] == 'failed') and (job.exeErrorCode != 0) and (job.result[1] != job.exeErrorCode):
                if log:
                    mismatch = "MISMATCH | Trf error code mismatch: exeErrorCode = %d, transExitCode = %d" %\
                               (job.exeErrorCode, job.result[1])
                    if node.has_key('pilotLog'):
                        node['pilotLog'] = mismatch + node['pilotLog']
                    else:
                        tolog("!!WARNING!!1300!! Could not write mismatch error to log extracts: %s" % mismatch)

            # check if Pilot-controlled resubmission is required:
            if (job.result[0] == "failed" and 'ANALY' in site.sitename):
                pilotExitCode = job.result[2]
                error = PilotErrors()
                if (error.isPilotResubmissionErrorCode(pilotExitCode) or job.isPilotResubmissionRequired):
                    # negate PilotError, ensure it's negative
                    job.result[2] = -abs(pilotExitCode)
                    tolog("(Negated error code)")
                else:
                    tolog("(No need to negate error code)")

            node['pilotErrorCode'] = job.result[2]
            tolog("Pilot error code: %d" % (node['pilotErrorCode']))

            # report CPUTime and CPUunit at the end of the job
            node['cpuConsumptionTime'] = job.cpuConsumptionTime
            try:
                node['cpuConsumptionUnit'] = job.cpuConsumptionUnit + "+" + getCPUmodel()
            except:
                node['cpuConsumptionUnit'] = '?'
            node['cpuConversionFactor'] = job.cpuConversionFactor

            # report specific time measures
            # node['pilotTiming'] = "getJob=%s setup=%s stageIn=%s payload=%s stageOut=%s" % (job.timeGetJob, job.timeSetup, job.timeStageIn, job.timeExe, job.timeStageOut)
            node['pilotTiming'] = "%s|%s|%s|%s|%s" % (job.timeGetJob, job.timeStageIn, job.timeExe, job.timeStageOut, job.timeSetup)
#            node['pilotTiming'] = "%s|%s|%s|%s|%s" % (str(job.timeGetJob), str(job.timeStageIn), str(job.timeExe), str(job.timeStageOut), str(job.timeSetup))
        elif job.result[0] == 'holding':
            node['exeErrorCode'] = job.result[2]
            node['exeErrorDiag'] = self.__error.getPilotErrorDiag(job.result[2])

        else:
            node['cpuConsumptionUnit'] = getCPUmodel()

        if spaceReport and site.dq2space != -1: # non-empty string and the space check function runs well
            node['remainingSpace'] = site.dq2space
            node['messageLevel'] = site.dq2spmsg

        return node

    def getXML(self, job, sitename, workdir, xmlstr=None, jr=False):
        """ Get the metadata xml """

        node_xml = ""
        tolog("getXML called")

        # for backwards compatibility
        try:
            experiment = job.experiment
        except:
            experiment = "unknown"

        # do not send xml for state 'holding' (will be sent by a later pilot during job recovery)
        if job.result[0] == 'holding' and sitename != "CERNVM":
            pass
        else:
            # only create and send log xml if the log was transferred
            if job.result[0] == 'failed' and isLogfileCopied(workdir):
                # generate the xml string for log file
                # at this time the job.workdir might have been removed (because this function can be called
                # after the removal of workdir is done), so we make a new dir
                xmldir = "%s/XML4PandaJob_%s" % (workdir, job.jobId)
                # group rw permission added as requested by LYON
                ec, rv = getstatusoutput("mkdir -m g+rw %s" % (xmldir))
                if ec != 0:
                    tolog("!!WARNING!!1300!! Could not create xmldir from updatePandaServer: %d, %s (resetting to site workdir)" % (ec, rv))
                    xmldir = workdir

                # which checksum command should be used? query the site mover
                from SiteMoverFarm import getSiteMover
                sitemover = getSiteMover(readpar('copytool'), "")

                if readpar('region') == 'Nordugrid':
                    fname = os.path.join(self.__pilot_initdir, job.logFile)
                else:
                    fname = os.path.join(workdir, job.logFile)
                if os.path.exists(fname):
                    fnamelog = "%s/logfile.xml" % (xmldir)
                    guids_status = PFCxml(experiment, fnamelog, fntag="lfn", alog=job.logFile, alogguid=job.tarFileGuid, jr=jr)
                    from SiteMover import SiteMover
                    ec, pilotErrorDiag, _fsize, _checksum = SiteMover.getLocalFileInfo(fname, csumtype=sitemover.getChecksumCommand())
                    if ec != 0:
                        tolog("!!WARNING!!1300!! getLocalFileInfo failed: (%d, %s, %s)" % (ec, str(_fsize), str(_checksum)))
                        tolog("!!WARNING!!1300!! Can not set XML (will not be sent to server)")
                        node_xml = ''
                    else:
                        ec, _strXML = updateMetadata(fnamelog, _fsize, _checksum)
                        if ec == 0:
                            tolog("Added (%s, %s) to metadata file (%s)" % (_fsize, _checksum, fnamelog))
                        else:
                            tolog("!!WARNING!!1300!! Could not add (%s, %s) to metadata file (%s). XML will be incomplete: %d" %\
                                  (_fsize, _checksum, fnamelog, ec))

                        # add skipped file info
                        _skippedfname = os.path.join(workdir, "skipped.xml")
                        if os.path.exists(_skippedfname):
                            ec = addSkippedToPFC(fnamelog, _skippedfname)

                        try:
                            f = open(fnamelog)
                        except Exception,e:
                            tolog("!!WARNING!!1300!! Exception caught: Can not open the file %s: %s (will not send XML)" %\
                                  (fnamelog, str(e)))
                            node_xml = ''
                        else:
                            node_xml = ''
                            for line in f:
                                node_xml += line
                            f.close()

                            # transfer logfile.xml to pilot init dir for Nordugrid
                            if readpar('region') == 'Nordugrid':
                                try:
                                    copy2(fnamelog, self.__pilot_initdir)
                                except Exception, e:
                                    tolog("!!WARNING!!1600!! Exception caught: Could not copy NG log metadata file to init dir: %s" % str(e))
                                else:
                                    tolog("Successfully copied NG log metadata file to pilot init dir: %s" % (self.__pilot_initdir))

                else: # log file does not exist anymore
                    if isLogfileCopied(workdir):
                        tolog("Log file has already been copied and removed")
                        if readpar('region') != 'Nordugrid':
                            # only send xml with log info if the log has been transferred
                            if xmlstr:
                                node_xml = xmlstr
                                tolog("Found xml anyway (stored since before)")
                            else:
                                node_xml = ''
                                tolog("!!WARNING!!1300!! XML not found, nothing to send to server")
                    else:
                        tolog("!!WARNING!!1300!! File %s does not exist and transfer lockfile not found (job from old pilot?)" % (fname))
                        node_xml = ''

            elif xmlstr:
                # xmlstr was set in postJobTask for all files
                tolog("XML string set")

                _skippedfname = os.path.join(workdir, "skipped.xml")
                fname = "%s/metadata-%s.xml" % (workdir, str(job.jobId))
                if os.path.exists(fname):
                    if os.path.exists(_skippedfname):
                        # add the skipped file info if needed
                        ec = addSkippedToPFC(fname, _skippedfname)

                    # transfer metadata to pilot init dir for Nordugrid
                    if readpar('region') == 'Nordugrid':
                        try:
                            copy2(fname, self.__pilot_initdir)
                        except Exception, e:
                            tolog("!!WARNING!!1600!! Exception caught: Could not copy metadata file to init dir for NG: %s" % str(e))
                        else:
                            tolog("Successfully copied metadata file to pilot init dir for NG: %s" % (self.__pilot_initdir))
                else:
                    tolog("Warning: Metadata does not exist: %s" % (fname))

                tolog("Will send XML")
                node_xml = xmlstr

            # we don't need the job's log file anymore, delete it (except for NG)
            if (job.result[0] == 'failed' or job.result[0] == 'finished') and readpar('region') != 'Nordugrid':
                try:
                    os.system("rm -rf %s/%s" % (workdir, job.logFile))
                except OSError:
                    tolog("!!WARNING!!1300!! Could not remove %s" % (job.logFile))
                else:
                    tolog("Removed log file")

        return node_xml

    def updateOutputFilesXMLWithSURLs4NG(self, experiment, siteWorkdir, jobId, outputFilesXML):
        """ Update the OutputFiles.xml file with SURLs """

        status = False

        # open and read back the OutputFiles.xml file
        _filename = os.path.join(siteWorkdir, outputFilesXML)
        if os.path.exists(_filename):
            try:
                f = open(_filename, "r")
            except Exception, e:
                tolog("!!WARNING!!1990!! Could not open file %s: %s" % (_filename, e))
            else:
                # get the metadata
                xmlIN = f.read()
                f.close()

                # update the XML
                xmlOUT = updateXMLWithSURLs(experiment, xmlIN, siteWorkdir, jobId, self.__jobrec, format='NG')

                # write the XML
                try:
                    f = open(_filename, "w")
                except OSError, e:
                    tolog("!!WARNING!!1990!! Could not open file %s: %s" % (_filename, e))
                else:
                    # write the XML and close the file
                    f.write(xmlOUT)
                    f.close()

                    tolog("Final XML for Nordugrid / CERNVM:\n%s" % (xmlOUT))
                    status = True
        else:
            tolog("!!WARNING!!1888!! Metadata file does not exist: %s" % (_filename))

        return status

    def updatePandaServer(self, job, site, workerNode, port, xmlstr=None, spaceReport=False, log=None, ra=0, jr=False, useCoPilot=False, stdout_tail="", additionalMetadata=None):
        """
        Update the job status with the jobdispatcher web server.
        State is a tuple of (jobId, ["jobstatus", transExitCode, pilotErrorCode], timestamp)
        log = log extracts
        xmlstr is set in postJobTask for finished jobs (all files). Failed jobs will only send xml for log (created in this function)
        jr = job recovery mode
        """
    
        tolog("Updating job status in updatePandaServer(): PandaId=%d, result=%s, time=%s" % (job.getState()))

        # set any holding job to failed for sites that do not use job recovery (e.g. sites with LSF, that immediately
        # removes any work directory after the LSF job finishes which of course makes job recovery impossible)
        if not self.__jobrec:
            if job.result[0] == 'holding' and site.sitename != "CERNVM":
                job.result[0] = 'failed'
                tolog("This site does not support job recovery: HOLDING state reset to FAILED")

        # note: any changed job state above will be lost for fake server updates, does it matter?

        # get the node structure expected by the server
        node = self.getNodeStructure(job, site, workerNode, spaceReport=spaceReport, log=log)

        # skip the server update (e.g. on NG)
        if not self.__updateServer:
            tolog("(fake server update)")
            return 0, node

        # get the xml
        node['xml'] = self.getXML(job, site.sitename, site.workdir, xmlstr=xmlstr, jr=jr)

        # stdout tail in case job.debug == 'true'
        if job.debug.lower() == "true" and stdout_tail != "":
            # protection for potentially large tails
            stdout_tail = stdout_tail[-2048:]
            node['stdout'] = stdout_tail
            tolog("Will send stdout tail:\n%s (length = %d)" % (stdout_tail, len(stdout_tail)))
        else:
            if job.debug.lower() != "true":
                tolog("Stdout tail will not be sent (debug=False)")
            elif stdout_tail == "":
                tolog("Stdout tail will not be sent (no stdout tail)")
            else:
                tolog("Stdout tail will not be sent (debug=%s, stdout_tail=\'%s\')" % (str(job.debug), stdout_tail))

        # PN fake lostheartbeat
        #    if job.result[0] == "finished":
        #        node['state'] = "holding"
        #        node['xml'] = ""

        # read back node['xml'] from jobState file for CERNVM
        sendXML = True
        if site.sitename == "CERNVM":
            _node = self.getNodeStructureFromFile(site.workdir, repr(job.jobId))
            if _node:
                if _node.has_key('xml'):
                    if _node['xml'] != "":
                        node['xml'] = _node['xml']
                        tolog("Read back metadata xml from job state file (length: %d)" % len(node['xml']))
                    else:
                        tolog("No metadata xml present in current job state file (1 - pilot should not send xml at this time)")
                        sendXML = False
                else:
                    tolog("No xml key in node structure")
                    sendXML = False
            else:
                tolog("No metadata xml present in current job state file (2 - pilot should not send xml at this time)")
                sendXML = False

            # change the state to holding for initial CERNVM job
            if not sendXML and (job.result[0] == "finished" or job.result[0] == "failed"):
                # only set the holding state if the Co-Pilot is used
                if useCoPilot:
                    job.result[0] = "holding"
                    node['state'] = "holding"

        # update job state file
        _retjs = updateJobState(job, site, node, recoveryAttempt=ra)

        # is it the final update?
        if job.result[0] == 'finished' or job.result[0] == 'failed' or job.result[0] == 'holding':
            final = True
        else:
            final = False

        # send the original xml if it exists (end of production job)
        filenameAthenaXML = "%s/metadata-%s.xml.ATHENA" % (site.workdir, repr(job.jobId))
        athenaXMLProblem = False
        if os.path.exists(filenameAthenaXML) and final:

            # get the metadata
            AthenaXML = getMetadata(site.workdir, job.jobId, athena=True)

            # add the metadata to the node
            if AthenaXML != "" and AthenaXML != None:
                tolog("Adding Athena metadata of size %d to node dictionary:\n%s" % (len(AthenaXML), AthenaXML))
                node['metaData'] = AthenaXML
            else:
                pilotErrorDiag = "Empty Athena metadata in file: %s" % (filenameAthenaXML)
                athenaXMLProblem = True
        else:
            # athena XML should exist at the end of the job
            if job.result[0] == 'finished' and 'Install' not in site.sitename and 'ANALY' not in site.sitename and 'DDM' not in site.sitename and 'test' not in site.sitename:
                pilotErrorDiag = "Metadata does not exist: %s" % (filenameAthenaXML)
                athenaXMLProblem = True

        # fail the job if there was a problem with the athena metadata
        # remove the comments below if a certain trf and release should be excluded from sending metadata
        # trf_exclusions = ['merge_trf.py']
        # release_exclusions = ['14.5.2.4']
        # jobAtlasRelease = getAtlasRelease(job.atlasRelease)
        # if athenaXMLProblem and job.trf.split(",")[-1] not in trf_exclusions and jobAtlasRelease[-1] not in release_exclusions:
        if athenaXMLProblem:
            tolog("!!FAILED!!1300!! %s" % (pilotErrorDiag))
            job.result[0] = "failed"
            job.result[2] = self.__error.ERR_NOATHENAMETADATA
            if node.has_key('pilotLog'):
                node['pilotLog'] += "!!FAILED!!1300!! %s" % (pilotErrorDiag)
            else:
                node['pilotLog'] = "!!FAILED!!1300!! %s" % (pilotErrorDiag)
            node['pilotErrorCode'] = job.result[2]
            node['state'] = job.result[0]

        # for backward compatibility
        try:
            experiment = job.experiment
        except:
            experiment = "unknown"

        # do not make the update if Nordugrid (leave for ARC to do)
        if readpar('region') == 'Nordugrid':
            if final:
                # update xml with SURLs stored in special SURL dictionary file
                if self.updateOutputFilesXMLWithSURLs4NG(experiment, site.workdir, job.jobId, job.outputFilesXML):
                    tolog("Successfully added SURLs to %s" % (job.outputFilesXML))

                # update xml with SURLs stored in special SURL dictionary file
                if node.has_key('xml'):
                    tolog("Updating node structure XML with SURLs")
                    node['xml'] = updateXMLWithSURLs(experiment, node['xml'], site.workdir, job.jobId, self.__jobrec) # do not use format 'NG' here
                else:
                    tolog("WARNING: Found no xml entry in the node structure")

                # store final node structure in pilot_initdir (will be sent to server by ARC control tower)
                self.copyNodeStruct4NG(node)
                tolog("Leaving the final update for the control tower")
            return 0, node

        # do not send xml if there was a put error during the log transfer
        _xml = None
        if final and node.has_key('xml'):
            # update xml with SURLs stored in special SURL dictionary file
            tolog("Updating node structure XML with SURLs")
            node['xml'] = updateXMLWithSURLs(experiment, node['xml'], site.workdir, job.jobId, self.__jobrec)

            _xml = node['xml']
            if not isLogfileCopied(site.workdir):
                tolog("Pilot will not send xml about output files since log was not transferred")
                node['xml'] = ""

        # should XML be sent at this time?
        if not sendXML:
            tolog("Metadata xml will not be sent")
            if node.has_key('xml'):
                if node['xml'] != "":
                    _xml = node['xml']
                    node['xml'] = ""

        # add experiment specific metadata
        if final and additionalMetadata != None:
            tolog("Adding additionalMetadata to node")
            if 'metaData' in node:
                node['metaData'] += additionalMetadata
            else:
                node['metaData'] = additionalMetadata

        # make the actual update, repeatedly if necessary (for the final update)
        ret = makeHTTPUpdate(job.result[0], node, port, url=self.__pshttpurl, path=self.__pilot_initdir)
        if not ret[2]: # data is None for a failed update attempt
            tolog("makeHTTPUpdate returned: %s" % str(ret))
            return 1, None

        tolog("ret = %s" % str(ret))
        data = ret[1]
        tolog("data = %s" % str(data))

        if data.has_key("command"):
            job.action = data['command']

        try:
            awk = data['StatusCode']
        except:
            tolog("!!WARNING!!1300!! Having problem updating job status, set the awk to 1 for now, and continue...")
            awk = "1"
        else:
            tolog("jobDispatcher acknowledged with %s" % (awk))

        # need to have a return code so subprocess knows if update goes ok or not
        ecode = int(awk) # use the awk code from jobdispatcher as the exit code

        # PN fake lostheartbeat
        #    if job.result[0] == "finished":
        #        ecode = 1

        # reset xml in case it was overwritten above for failed log transfers
        if final and node.has_key('xml'):
            node['xml'] = _xml

        return ecode, node # ecode=0 : update OK, otherwise something wrong

