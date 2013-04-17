# Mover.py 
# Used by runJob and pilot to transfer input and output files from and to the local SE

import os
import sys
import commands
import re
import urllib

from xml.dom import minidom
from time import time, sleep
from timed_command import timed_command

from pUtil import createPoolFileCatalog, tolog, addToSkipped, removeDuplicates, dumpOrderedItems, dumpFile, getFileAccessInfo,\
     hasBeenTransferred, getLFN, makeTransRegReport, readpar, getMaxInputSize, tailPilotErrorDiag, getCopysetup, getExtension,\
     getCopyprefixLists, getExperiment, getSiteInformation
from FileStateClient import updateFileState, dumpFileStates
from RunJobUtilities import updateCopysetups

# make sure that md5 is properly imported
try:
    from hashlib import md5
except ImportError, err:
    import md5
    tolog("WARNING: imported old md5 (hashlib not available)")
else:
    try:
        func = getattr(md5, "new", None)
    except AttributeError:
        # some hashlib installations (e.g. TECHNION-HEP) are outdated and do not have the new method
        del md5
        import md5
        tolog("(WARNING: Replaced faulty hashlib.md5 with old md5) [AttributeError]")
    else:
        if func:
            tolog("(Verified that hashlib.md5.new exists)")
        else:
            del md5
            import md5
            tolog("(WARNING: Replaced faulty hashlib.md5 with old md5) [new() does not exist]")
            
# Default DQ2 URL
URLBASE = 'http://dms02.usatlas.bnl.gov:8000/dq2/'

# Note: DEFAULT_TIMEOUT and MAX_RETRY are reset in get_data()
MAX_RETRY = 1
MAX_NUMBER_OF_RETRIES = 3
DEFAULT_TIMEOUT = 5*3600/MAX_RETRY # 1h40' if 3 retries # 5 hour total limit on dq2 get/put

from PilotErrors import PilotErrors
from futil import *

import SiteMoverFarm

from config import config_sm
PERMISSIONS_DIR = config_sm.PERMISSIONS_DIR
PERMISSIONS_FILE = config_sm.PERMISSIONS_FILE
CMD_CHECKSUM = config_sm.COMMAND_MD5

#Default archival type
ARCH_DEFAULT = config_sm.ARCH_DEFAULT

class replica:
    """ Replica """

    sfn = None
    setname = None
    fs = None
    filesize = None
    csumvalue = None

def createZippedDictionary(list1, list2):
    """ Created a zipped dictionary from input lists """
    # list1 = [a1, a2, ..]
    # list2 = [b1, b2, ..]
    # -> dict = {a1:b1, a2:b2, ..}

    d = None

    if len(list1) == len(list2):
        try:
            d = dict(zip(list1, list2))
        except Exception,e:
            tolog("Warning: Dictionary creation failed: %s" % str(e))
        else:
            tolog("Created dictionary: %s" % str(d))
    else:
        tolog("Warning: Cannot create zipped dictionary using: list1=%s, list2=%s (different lengths)" % (str(list1), str(list2)))

    return d

def get_data(job, jobSite, ins, stageinTries, analysisJob=False, usect=True, pinitdir="", proxycheck=True, inputDir="", workDir=""):
    """ call the mover and stage-in input files """

    error = PilotErrors()
    pilotErrorDiag = ""
    ec = 0

    # FAX transfer counters (will be reported in jobMetrics, only relevant when FAX was used)
    N_filesWithoutFAX = 0
    N_filesWithFAX = 0

    # FAX control variable, if FAX is used as primary site mover in combination with direct I/O
    usedFAXandDirectIO = False

    # if mover_get_data() fails to create a TURL based PFC, the returned statusPFCTurl will be False, True if succeeded and None if not used
    statusPFCTurl = None

    # create the local access and scope dictionaries
    access_dict = createZippedDictionary(job.inFiles, job.prodDBlockToken)
    scope_dict = createZippedDictionary(job.inFiles, job.scopeIn)

    # create the handler for the potential DBRelease file (the DBRelease file will not be transferred on CVMFS)
    from DBReleaseHandler import DBReleaseHandler
    dbh = DBReleaseHandler(workdir=job.workdir)

    try:
        dsname = ""
        dsdict = {}
        if job.realDatasetsIn and len(job.realDatasetsIn) == 1 and job.realDatasetsIn[0] != 'NULL':
            dsname = job.realDatasetsIn[0]
            if not dsdict.has_key(dsname): dsdict[dsname] = []
            dsdict[dsname].append(job.inFiles[0])
        elif job.realDatasetsIn and len(job.realDatasetsIn) > 1:
            for i in range(len(job.inFiles)):
                inFile = job.inFiles[i]
                dsname = job.realDatasetsIn[i]
                if not dsdict.has_key(dsname): dsdict[dsname] = []
                dsdict[dsname].append(inFile)
        tolog("Calling get function with dsname=%s, dsdict=%s" % (dsname, str(dsdict)))
        rc, pilotErrorDiag, statusPFCTurl, N_filesWithoutFAX, N_filesWithFAX, usedFAXandDirectIO = \
            mover_get_data(ins, job.workdir, jobSite.sitename, stageinTries, ub=jobSite.dq2url, dsname=dsname,\
                           dsdict=dsdict, guids=job.inFilesGuids, analysisJob=analysisJob, usect=usect, pinitdir=pinitdir,\
                           proxycheck=proxycheck, spsetup=job.spsetup, tokens=job.dispatchDBlockToken, userid=job.prodUserID,\
                           access_dict=access_dict, inputDir=inputDir, jobId=job.jobId, DN=job.prodUserID, workDir=workDir,\
                           scope_dict=scope_dict, jobDefId=job.jobDefinitionID, dbh=dbh, jobPars=job.jobPars, cmtconfig=job.cmtconfig,\
                           filesizeIn=job.filesizeIn, checksumIn=job.checksumIn, transferType=job.transferType, experiment=job.experiment)
        tolog("Get function finished with exit code %d" % (rc))

    except SystemError, e:
        pilotErrorDiag = "Get function for input files is interrupted by SystemError: %s" % str(e)
        tolog("!!FAILED!!3000!! Exception caught: %s" % (pilotErrorDiag))
        ec = error.ERR_KILLSIGNAL

    except Exception, e:
        pilotErrorDiag = "Get function can not be called for staging input files: %s" % str(e)
        tolog("!!FAILED!!3000!! Exception caught: %s" % (pilotErrorDiag))
        if str(e).find("No space left on device") >= 0:
            tolog("!!FAILED!!3000!! Get error: No space left on local disk (%s)" % (pinitdir))
            ec = error.ERR_NOLOCALSPACE
        else:
            ec = error.ERR_GETDATAEXC

        # write traceback info to stderr
        import traceback
        exc, msg, tb = sys.exc_info()
        traceback.print_tb(tb)

    else:
        if pilotErrorDiag != "":
            # pilotErrorDiag = 'abcdefghijklmnopqrstuvwxyz0123456789'
            # -> 'Get error: lmnopqrstuvwxyz0123456789'
            pilotErrorDiag = "Get error: " + tailPilotErrorDiag(pilotErrorDiag, size=256-len("Get error: "))

        if rc: # get failed, non-zero return code
            # this error is currently not being sent from Mover (see next error code)
            if rc == error.ERR_FILEONTAPE:
                tolog("!!WARNING!!3000!! Get error: Input file is on tape (will be skipped for analysis job)")
                if analysisJob:
                    tolog("Skipping input file")
                    ec = 0
                else:
                    pass # a prod job should not generate this error
            else:
                ec = rc
                if pilotErrorDiag == "":
                    pilotErrorDiag = error.getPilotErrorDiag(ec)
                tolog("!!FAILED!!3000!! %s" % (pilotErrorDiag))

            if ec:
                tolog("!!FAILED!!3000!! Get returned a non-zero exit code (%d), will now update local pilot TCP server" % (ec))
        else:
            # get_data finished correctly
            tolog("Input file(s):")
            for inputFile in ins:
                try:
                    _ec, rs = commands.getstatusoutput("ls -l %s/%s" % (job.workdir, inputFile))
                except Exception, e:
                    tolog(str(e))
                else:
                    if "No such file or directory" in rs:
                        tolog("File %s was not transferred" % (inputFile))
                    else:
                        tolog(rs)

    return ec, pilotErrorDiag, statusPFCTurl, N_filesWithoutFAX, N_filesWithFAX, usedFAXandDirectIO

def getGuids(fileList):
    """ extracts the guids from the file list """

    guids = []
    # loop over all files
    for thisfile in fileList:
        guids.append(str(thisfile.getAttribute("ID")))

    return guids

def getReplicas(guids):
    """ get the replicas list from the LFC """

    ec = 0
    pilotErrorDiag = ""
    error = PilotErrors()
    replica_list = []

    try:
        import lfc
    except Exception, e:
        pilotErrorDiag = "getReplicas() could not import lfc module: %s" % str(e)
        ec = error.ERR_GETLFCIMPORT
    try:
        ret, replica_list = lfc.lfc_getreplicas(guids, "")
    except Exception, e:
        pilotErrorDiag = "Failed to get LFC replicas: Exception caught: %s" % str(e)
        tolog("!!FAILED!!2999!! %s" % (pilotErrorDiag))
        tolog("getReplicas() finished (failed)")
        ec = error.ERR_FAILEDLFCGETREPS

    if ret != 0:
        err_num = lfc.cvar.serrno
        err_string = lfc.sstrerror(err_num)
        pilotErrorDiag = "Failed to get LFC replicas: %d (lfc_getreplicas failed with: %d, %s)" %\
                         (ret, err_num, err_string)
        tolog("!!FAILED!!2999!! %s" % (pilotErrorDiag))
        tolog("getReplicas() finished (failed)")
        ec = error.ERR_FAILEDLFCGETREPS

    return ec, pilotErrorDiag, replica_list

def getInitialTracingReport(userid, sitename, dsname, eventType, analysisJob, jobId, jobDefId, dn):
    """ setup the dictionary necessary for all instrumentation """

    if analysisJob:
        eventType = eventType + "_a"

    m = md5.new()
    m.update(userid) # anonymise user identity
    hashf = md5.new()
    hashf.update('ppilot_%s' % jobDefId)

    report = {'eventType': eventType, # sitemover
              'eventVersion': 'pilot3', # pilot version
              'protocol': None, # set by specific sitemover
              'clientState': 'INIT_REPORT',
              'localSite': sitename, # localsite
              'remoteSite': sitename, # equals remotesite (pilot does not do remote copy?)
              'timeStart': time(), # time to start
              'catStart': None,
              'relativeStart': None,
              'transferStart': None,
              'validateStart': None,    
              'timeEnd': None,
              'dataset': dsname,
              'version': None,
              'duid': None,
              'filename': None,
              'guid': None,
              'filesize': None,
              'usr': m.hexdigest(),
              'appid': jobId,
              'hostname': '',
              'ip': '',
              'suspicious': '0',
              'usrdn': dn,
              }

    if jobDefId == "":
        report['uuid'] = commands.getoutput('uuidgen -t').replace('-',''), # all LFNs of one request have the same uuid
    else:
        report['uuid'] = hashf.hexdigest()

    if jobDefId != "":
        tolog("Using job definition id: %s" % (jobDefId))

    # add DN etc
    tolog("Trying to add additional info to tracing report")
    try:
        import socket
        report['hostname'] = socket.gethostbyaddr(socket.gethostname())[0]
        report['ip'] = socket.gethostbyaddr(socket.gethostname())[2][0]
    except Exception, e:
        tolog("!!WARNING!!2999!! Tracing report could not add some info: %s" % str(e))

    tolog("Tracing report initialised with: %s" % str(report))
    return report

def getFileInfo(lfchost, region, ub, guids, dsname, dsdict, lfns, pinitdir, analysisJob, tokens, DN, sitemover, error, workdir, dbh, DBReleaseIsAvailable, \
                scope_dict, pfc_name="PoolFileCatalog.xml", filesizeIn=[], checksumIn=[], thisExperiment=None):
    """ Build the file info dictionary """

    fileInfoDic = {}
    replicas_dic = {}
    totalFileSize = 0L    

    tolog("Preparing to build paths for input files")

    # get the site information object
    si = getSiteInformation(thisExperiment.getExperiment())

    # if the pilot is running on a Tier 3 site, then neither LFC nor PFC should be used
    if si.isTier3():
        tolog("Getting file info on a Tier 3 site")

        # create file path to local SE (not used for scope based paths)
        path = sitemover.getTier3Path(dsname, DN) # note: dsname will only be correct for lib files, otherwise fix dsdict, currently empty for single lib file input?
        file_nr = -1
        for lfn in lfns:
            file_nr += 1

            # use scope based path if possible
            if scope_dict and readpar('userucio').lower() == "true":
            #if scope_dict:
                try:
                    spacetoken = tokens[file_nr]
                except:
                    spacetoken = ""
                try:
                    scope = scope_dict[lfn]
                except Exception, e:
                    tolog("!!WARNING!!1232!! Failed to extract scope from scope dictionary for file %s: %s" % (lfn, str(scope_dict)))
                    tolog("Defaulting to old path style (based on dsname)")
                    se_path = os.path.join(path, lfn)
                else:
                    se_path = sitemover.getFullPath(scope, spacetoken, lfn, analysisJob, "")
            else:
                se_path = os.path.join(path, lfn)

            # get the file info
            ec, pilotErrorDiag, fsize, fchecksum = sitemover.getLocalFileInfo(se_path, csumtype="default")
            if ec != 0:
                return ec, pilotErrorDiag, fileInfoDic, totalFileSize, replicas_dic

            # file the file info dictionary
            fileInfoDic[file_nr] = (guids[file_nr], se_path, fsize, fchecksum)

            # check total file sizes to avoid filling up the working dir, add current file size
            try:
                totalFileSize += long(fsize)
            except:
                pass
    else:
        # get the PFC from the proper source
        ec, pilotErrorDiag, xml_from_PFC, xml_source, replicas_dic = \
            getPoolFileCatalog(lfchost, ub, guids, dsname, lfns, pinitdir, analysisJob, tokens, workdir, dbh,\
                               DBReleaseIsAvailable, scope_dict, sitemover, pfc_name=pfc_name, thisExperiment=thisExperiment)
        try:
            if os.path.basename(sitemover.getID()) == 'aria2c':
                sitemover.surls2metalink(replicas_dic, 'AllInput.xml.meta4')
        except Exception, e:
            tolog("!!WARNING!!1232!! Caught exception: %s" % (e))

        if ec != 0:
            return ec, pilotErrorDiag, fileInfoDic, totalFileSize, replicas_dic

        tolog("Using XML source %s" % (xml_source))
        if xml_from_PFC == '':
            pilotErrorDiag = "Failed to get PoolFileCatalog"
            tolog("!!FAILED!!2999!! %s" % (pilotErrorDiag))
            tolog("Mover get_data finished (failed)")
            return error.ERR_NOPFC, pilotErrorDiag, fileInfoDic, totalFileSize, replicas_dic

        xmldoc = minidom.parseString(xml_from_PFC)        
        fileList = xmldoc.getElementsByTagName("File")

        # extracts the guids from the file list
        guids_filelist = getGuids(fileList)
        fileInfoDictionaryFromDispatcher = getFileInfoDictionaryFromDispatcher(lfns, filesizeIn, checksumIn) 
        file_nr = -1
        for thisfile in fileList:
            file_nr += 1
            # get the SURL and GUID from the XML
            gpfn = str(thisfile.getElementsByTagName("pfn")[0].getAttribute("name"))
            guid = guids_filelist[file_nr]

            # get the filesize and checksum from the primary location (the dispatcher)
            _lfn = os.path.basename(gpfn)
            # remove any __DQ2 substring from the LFN if necessary
            if "__DQ2" in _lfn:
                _lfn = stripDQ2FromLFN(_lfn)
            fsize, fchecksum = getFileInfoFromDispatcher(_lfn, fileInfoDictionaryFromDispatcher)
            tolog("lfn=%s, fsize=%s, fchecksum=%s" % (_lfn, fsize, fchecksum))

            # get the file info from the metadata [from LFC]
            if not fsize or not fchecksum:
                ec, pilotErrorDiag, fsize, fchecksum = getFileInfoFromMetadata(thisfile, guid, replicas_dic, lfchost, region, sitemover, error)
                if ec != 0:
                    return ec, pilotErrorDiag, fileInfoDic, totalFileSize, replicas_dic

                # even though checksum and file size is most likely already known from LFC, more reliable file
                # info is stored in DQ2. Try to get it from there unless the dispatcher has already sent it to the pilot
                if dsdict == {}:
                    _dataset = dsname
                else:
                    _dataset = getDataset(os.path.basename(gpfn), dsdict)
                _filesize, _checksum = sitemover.getFileInfoFromDQ2(_dataset, guid)
                if _filesize != "" and _checksum != "":
                    if _filesize != fsize:
                        tolog("!!WARNING!!1001!! LFC file size (%s) not the same as DQ2 file size (%s) (using DQ2 value)" % (fsize, _filesize))
                    if _checksum != fchecksum:
                        tolog("!!WARNING!!1001!! LFC checksum (%s) not the same as DQ2 checksum (%s) (using DQ2 value)" % (fchecksum, _checksum))
                    fsize = _filesize
                    fchecksum = _checksum

            # store in the file info dictionary
            fileInfoDic[file_nr] = (guid, gpfn, fsize, fchecksum)

            # check total file sizes to avoid filling up the working dir, add current file size
            try:
                totalFileSize += long(fsize)
            except:
                pass

    return ec, pilotErrorDiag, fileInfoDic, totalFileSize, replicas_dic

def backupPFC4Mover(pfc_name):
    """ Backup old PFC file used by mover """

    if os.path.exists(pfc_name):
        fname = pfc_name + ".MOVER"
        from shutil import move
        try:
            tolog("Moving %s to %s" % (pfc_name, fname))
            move(pfc_name, fname)
        except Exception, e:
            tolog("!!WARNING!!2999!! Could not move old PFC file: %s" % str(e))
        else:
            tolog("Backed up old PFC file used by mover: %s" % (fname))
    else:
        tolog("Old PFC file does not exist (nothing to backup)")

def createPFC4TRF(pfc_name, guidfname):
    """ Create PFC to be used by trf/runAthena """

    tolog("Creating %s" % (pfc_name))
    try:
        pfc2 = open(pfc_name, 'w')
        pfc2.write('<?xml version="1.0" encoding="UTF-8" standalone="no" ?>\n')
        pfc2.write('<!-- Edited By POOL -->\n')
        pfc2.write('<!DOCTYPE POOLFILECATALOG SYSTEM "InMemory">\n')
        pfc2.write('<POOLFILECATALOG>\n')
        pfc2.write('\n')
        for guid in guidfname.keys():
            fname = guidfname[guid]
            tolog("Processing file: %s" % (fname))
            pfc2.write('  <File ID=\"' + guid + '\">\n')
            pfc2.write('    <physical>\n')
            pfc2.write('      <pfn filetype="ROOT_All" name=\"' + fname + '\"/>\n')
            pfc2.write('    </physical>\n')
            pfc2.write('    <logical/>\n')
            pfc2.write('  </File>\n') 
            pfc2.write('\n')
        pfc2.write('</POOLFILECATALOG>')    
    except Exception, e:
        tolog("!!WARNING!!2999!! Could not create/write to PFC: %s, %s" % (pfc_name, str(e)))    
    else:
        pfc2.close()
        tolog("Created PFC for trf/runAthena: %s" % (pfc_name))
        dumpFile(pfc_name, topilotlog=True)

def isDPMSite(pfn, sitemover):
    """ return True if the site is a DPM site """
    # pfn is the filename of the first file in the file list (enough to test with)

    status = False
    # first get the DQ2 site name, then ask for its setype
    try:
        _DQ2SiteName = sitemover.getDQ2SiteName(surl=pfn)
    except:
        # Note: do not print the exception since it sometimes can not be converted to a string (as seen at Taiwan)
        tolog("WARNING: Failed to get the DQ2 site name (assuming no DPM site)")
    else:
        setype = sitemover.getDQ2SEType(_DQ2SiteName)
        if setype == "dpm":
            status = True
    return status

def getTURLFileInfoDic(output, shortGuidList, useShortTURLs, sitename):
    """ interpret the lcg-getturls stdout and return the TURL file dictionary """

    error = PilotErrors()
    turlFileInfoDic = {}
    ec = 0
    pilotErrorDiag = ""

    # verify that the output does not contain any failures
    if "Invalid argument" in output or "Failed" in output:
        pilotErrorDiag = "lcg-getturls failed: %s" % output[-100:]
        ec = error.ERR_LCGGETTURLS
    else:
        # create a list from the command output
        shortTURLList = output.split('\n')

        # create the short list
        if useShortTURLs:
            _shortTURLList = []
            site_exclusion_list = ['ANALY_TW-FTT_TEST']
            from SiteMover import SiteMover
            s = SiteMover()
            for t in range(0, len(shortTURLList)):
                #_turl = shortTURLList[t]
                turl = shortTURLList[t]
                # only use rfio on the sites that supports it
                #if not sitename in site_exclusion_list:
                #    turl = _turl.replace(s.stripPath(_turl), 'rfio:')
                #tolog("DPM site: Updated %s to %s" % (_turl, turl))
                if turl != "":
                    _shortTURLList.append(turl)
            shortTURLList = _shortTURLList

        i = 0
        for guid in shortGuidList:
            # update PFN if necessary (e.g. for TRIUMF, SFU, LYON) and add it to the dictionary
            turlFileInfoDic[guid] = updatePFN(shortTURLList[i])
            i += 1

    return ec, pilotErrorDiag, turlFileInfoDic

def getDefaultStorage(pfn):
    """ extract default storage from the pfn """

    defaultSE = ""

    # parse
    match = re.findall('^[^:]+://([^:/]+)',pfn)
    if len(match) != 1:
        tolog("!!WARNING!!2990!! Could not parse default storage from %s" % (pfn))
    else:
        defaultSE = match[0]

    return defaultSE

def getTURLs(thinFileInfoDic, sitemover, sitename):
    """ use lcg-getturls to convert the SURLs in the file dictionary to TURLs """

    turlFileInfoDic = {}
    error = PilotErrors()
    ec = 0
    pilotErrorDiag = ""

    # get the setup script
    setup = getCopysetup(mode='get')

    # create the file and guid lists
    fileList = thinFileInfoDic.values()
    guidList = thinFileInfoDic.keys()

    tolog("Will create a PFC with TURL(s) for the following file(s)")
    dumpOrderedItems(fileList)

    # remove any raw, lib, gz and tar files from the file list, and add them to the excluded file dictionary
    from SiteMover import SiteMover
    sitemover = SiteMover()
    _fileList = []
    _guidList = []
    excludedFilesDic = {}
    for i in range(0, len(fileList)):
        if sitemover.isRootFileName(fileList[i]):
            _fileList.append(fileList[i])
            _guidList.append(guidList[i])
        else:
            excludedFilesDic[guidList[i]] = fileList[i]
    fileList = _fileList
    guidList = _guidList
    tolog("Excluded file dictionary (will not be converted to TURLs): %s" % str(excludedFilesDic))

    # is this a dpm site? If so, lcg-getturls must be used for each file or in block calls
    if fileList != []:
        if not isDPMSite(fileList[0], sitemover):
            # for a non DPM site, we only need to use lcg-getturls once, so grab the first element and use it only
# how to add any remaining files to the PFC? check notes from meeting with Johannes
#            fileList = [fileList[0]]
#            guidList = [guidList[0]]
            tolog("Not a DPM site")
            useShortTURLs = False
        else:
            tolog("DPM site")
            useShortTURLs = True

        # loop until the file list is exhausted
        batch = 100
        while fileList != []:
            # reset the short file and guid lists
            shortFileList = []
            shortGuidList = []

            # grab 'batch' number of entries from the list
            if len(fileList) > batch:
                for i in range(batch):
                    shortFileList.append(fileList[0])
                    shortGuidList.append(guidList[0])
                    fileList.remove(fileList[0])
                    guidList.remove(guidList[0])
            else:
                shortFileList = fileList
                shortGuidList = guidList
                fileList = []
                guidList = []

            # now use the short file list in the batch call

            # create a comma separated string
            fileString = '\"' + shortFileList[0] + '\"'
            for i in range(1, len(shortFileList)):
                fileString += ' \"' + shortFileList[i] + '\"'

            # create the command
            if setup == "":
                setup_string = ""
            else:
                setup_string = 'source %s;' % (setup) # already verified
            cmd = '%s lcg-getturls -p dcap,gsidcap,file,root,rfio %s' % (setup_string, fileString)

            s = 0
            output = ""
            maxAttempts = 3
            for attempt in range(maxAttempts):
                timeout = int(60 * 2**attempt) # 120 s, 240 s, 480 s
                tolog("Executing command (%d/%d): %s (with a time-out of %d s)" % (attempt+1, maxAttempts, cmd, timeout))
                try:
                    s, telapsed, cout, cerr = timed_command(cmd, timeout)
                except Exception, e:
                    tolog("!!WARNING!!2999!! timed_command() threw an exception: %s" % str(e))
                    s = 1
                    output = str(e)
                    telapsed = timeout
                else:
                    output = cout + cerr
                    tolog("Elapsed time: %d" % (telapsed))

                # command finished correctly
                if s == 0:
                    break

            # error code handling
            if s != 0:
                tolog("!!WARNING!!2990!! Command failed: %s" % (output))
                if is_timeout(s):
                    pilotErrorDiag = "lcg-getturls get was timed out after %d seconds" % (telapsed)
                    ec = error.ERR_LCGGETTURLSTIMEOUT
                else:
                    pilotErrorDiag = "lcg-getturls failed: %s" % (output)
                    ec = error.ERR_LCGGETTURLS

                # undo copysetup modification
                updateCopysetups('', transferType="undodirect")

                # abort everything, break main loop
                break
            else:
                # interpret the output
                ec, pilotErrorDiag, _turlFileInfoDic = getTURLFileInfoDic(output, shortGuidList, useShortTURLs, sitename)

                # add the returned dictionary to the already existing one
                turlFileInfoDic = dict(turlFileInfoDic.items() + _turlFileInfoDic.items())

    # add the excluded files (if any) to the TURL dictionary
#    turlFileInfoDic = dict(turlFileInfoDic.items() + excludedFilesDic.items())
    turlFileInfoDic = dict(turlFileInfoDic.items())

    return ec, pilotErrorDiag, turlFileInfoDic

def updatePFN(pfn):
    """ update the PFN if necessary (e.g. for TRIUMF, SFU, LYON) """
    # based on /afs/cern.ch/sw/ganga/install/5.5.4/python/GangaAtlas/Lib/Athena/ganga-stage-in-out-dq2.py

    _pfn = pfn

    # get the default SE
    defaultSE = getDefaultStorage(pfn)
    tolog("Got default SE: %s" % (defaultSE))

    # get the used protocol
#    if 'ccsrm.in2p3.fr' in defaultSE or 'srm.triumf.ca' in defaultSE:
    if 'ccsrm.in2p3.fr' in defaultSE or 'triumf.ca' in defaultSE:
        usedProtocol = 'dcap'
    else:
        # get the protocol
        match = re.search('^(\S*)://.*', pfn)
        if match:
            usedProtocol = match.group(1)
        else:
            tolog("!!WARNING!!2990!! Protocol could not be extracted from PFN (cannot not update PFN)")
            return pfn

    tolog("Got protocol: %s" % (usedProtocol))

    # correct PFN for the exceptional sites
    if usedProtocol == "dcap":
        # correct the protocol
        pfn = re.sub('srm://', 'dcap://', pfn)

        # Hack for ccin2p3
        pfn = re.sub('ccsrm', 'ccdcapatlas', pfn)

        # Hack for TRIUMF
#        if 'srm.triumf.ca' in defaultSE:
        if 'triumf.ca' in defaultSE:
            pfn = re.sub('/atlas/dq2/','//pnfs/triumf.ca/data/atlas/dq2/',pfn)
            pfn = re.sub('/atlas/users/','//pnfs/triumf.ca/data/atlas/users/',pfn)
            pfn = re.sub('22125/atlas/','22125//pnfs/triumf.ca/data/atlas/',pfn)

        # Hack for SFU
        if 'wormhole.westgrid.ca' in defaultSE:
            pfn = re.sub('/atlas/dq2/','//pnfs/sfu.ca/data/atlas/dq2/',pfn)
            pfn = re.sub('/atlas/users/','//pnfs/sfu.ca/data/atlas/users/',pfn)
            pfn = re.sub('22125/atlas/','22125//pnfs/sfu.ca/data/atlas/',pfn)
                
    elif usedProtocol in ["root", "Xrootd"]:
        # correct the protocol
        pfn = re.sub('srm://','root://',pfn)

        # Hack for ccin2p3
        pfn = re.sub('ccsrm','ccxroot',pfn)
        pfn = re.sub('ccdcamli01','ccxroot',pfn)
        pfn = re.sub(':1094',':1094/',pfn)

    elif usedProtocol == "gsidcap":
        pfn = re.sub('srm://','gfal:gsidcap://',pfn)
        pfn = re.sub('22128/pnfs','22128//pnfs',pfn)
        pfn = re.sub('gfal:gfal:','gfal:',pfn)

    if _pfn != pfn:
        tolog("Updated PFN from %s to %s" % (_pfn, pfn))
    else:
        tolog("No need to update PFN (not exceptional site)")

    return pfn

def getThinFileInfoDic(fileInfoDic):
    """ create a thinner file dictionary to beused with the TURL PFC """

    thinFileInfoDic = {}

    # fileInfoDic[file_nr] = (guid, gpfn, fsize, fchecksum)
    # thinFileInfoDic[guid] = gpfn
    # turlFileInfoDic[guid] = turl
    for nr in range(len(fileInfoDic.keys())):
        # grab the PFNs and append them to the list
        guid = fileInfoDic[nr][0]
        thinFileInfoDic[guid] = fileInfoDic[nr][1] # PFN

    tolog("Built a thinner file dictionary with %d file(s)" % (len(thinFileInfoDic.keys())))

    return thinFileInfoDic

def createPFC4TURLs(fileInfoDic, pfc_name, sitemover, sitename):
    """ Perform automatic configuration of copysetup[in] (i.e. for direct access/file stager)"""

    # (remember to replace preliminary old/newPrefix)

    error = PilotErrors()

    # note: copysetup fields new/oldPrefix will not be needed any longer
    # the task should be to create the PFC containing TURLs - only

    tolog("Performing automatic configuration")

    # create thinner file dictionary
    thinFileInfoDic = getThinFileInfoDic(fileInfoDic)

    # get the TURLs
    ec, pilotErrorDiag, turlFileInfoDic = getTURLs(thinFileInfoDic, sitemover, sitename)
    if ec == 0:
        tolog("getTURL returned dictionary: %s" % str(turlFileInfoDic))

        if turlFileInfoDic != {}:
            # create a TURL based PFC
            xml_from_PFC = createPoolFileCatalog(turlFileInfoDic, pfc_name=pfc_name, forceLogical=True)
            if xml_from_PFC == "":
                pilotErrorDiag = "PFC creation failed"
                ec = error.ERR_LCGGETTURLS
        else:
            tolog("PFC cannot be created since no TURL based file dictionary was returned by getTURLs() (not needed?)")
            ec = error.ERR_LCGGETTURLS
    else:
        tolog("!!WARNING!!2998!! getTURLs failed: %s" % (pilotErrorDiag))

    return ec, pilotErrorDiag

def shouldPFC4TURLsBeCreated(analysisJob, transferType):
    """ determine whether a TURL based PFC should be created """

    status = False

    allowDirectAccess = readpar('allowdirectaccess').lower()
    if allowDirectAccess.lower() == "true":
        if analysisJob:
            # get the file access info (only useCT is needed here)
            useCT, oldPrefix, newPrefix, useFileStager, directIn = getFileAccessInfo()

            tolog("use copytool = %s (should be false for file stager)" % str(useCT))
            tolog("useFileStager = %s (should be true for file stager)" % str(useFileStager))
            tolog("directIn = %s (should be true for file stager)" % str(directIn))
            tolog("oldPrefix = %s (should be empty if TURL based PFC is required)" % (oldPrefix))
            tolog("newPrefix = %s (should be empty if TURL based PFC is required)" % (newPrefix))

            # PFC should be TURL based for file stager or for direct i/o if old/new prefices are not specified
            if not useCT and useFileStager and directIn and oldPrefix == "" and newPrefix == "":
                # useFileStager thus need not be set (or used here), but directIn must be True
                # if not useCT and directIn and oldPrefix == "" and newPrefix == "":
                status = True
        else:
            if transferType == "direct":
                tolog("Will attempt to create a TURL based PFC (for transferType %s)" % (transferType))
                status = True
    else:
        if allowDirectAccess == "":
            tolog("This site has not set allowDirectAccess - direct access/file stager not allowed")
        else:
            tolog("This site has allowDirectAccess = %s - direct access/file stager not allowed" % (allowDirectAccess))

    if status:
        tolog("TURL based PFC required")
    else:
        tolog("TURL based PFC not required")

    return status

def getDBReleaseVersion(dbh, jobPars):
    """ Get the DBRelease version from the job parameters """

    return dbh.getDBReleaseVersion(jobPars=jobPars)

def isDBReleaseFile(dbh, lfn):
    """ Is the LFN a DBRelease file? """

    return dbh.extractVersion(lfn)

def isDBReleaseAvailable(dbh, version, lfns, jobPars):
    """ Check if the DBRelease file is available locally """

    DBReleaseIsAvailable = False
    if version == "":
        tolog("Job parameters did not specify a DBRelease version (can not verify local availability)")
    else:
        for lfn in lfns:
            if isDBReleaseFile(dbh, lfn):
                tolog("Found a DBRelease file in the input file list (will check local availability)")

                # is the requested DBRelease file available locally?
                if dbh.isDBReleaseAvailable(version):
                    tolog("%s is available locally (will not be staged-in)" % (lfn))
                    DBReleaseIsAvailable = True
                    break

    return DBReleaseIsAvailable

def createdSkeletonDBRelease(dbh, version, path):
    """ Create the skeleton DBRelease file """

    created = False
    tolog("Creating the skeleton DBRelease tarball")
    if dbh.createDBRelease(version, path):
        # managed to create a new DBRelease tarball only containing the setup script
        tolog("Since the DBRelease file is available locally, it will not be transferred")
        created = True
    else:
        tolog("Failed to create the skeleton file")

    return created

def handleDBRelease(dbh, lfns, jobPars, path):
    """ Check if the DBRelease file is locally available, if so, create the skeleton file """

    # get the DBRelease version from the job parameters
    version = getDBReleaseVersion(dbh, jobPars)
    if version != "":
        tolog("DBRelease version from job parameters: %s" % (version))
    else:
        tolog("No DBRelease info found in job parameters")

    # is the DBRelease locally available?
    DBReleaseIsAvailable = isDBReleaseAvailable(dbh, version, lfns, jobPars)

    # create the skeleton DBRelease file in the work directory
    if DBReleaseIsAvailable:
        for lfn in lfns:
            if isDBReleaseFile(dbh, lfn):
                if createdSkeletonDBRelease(dbh, version, path):
                    break
                else:
                    DBReleaseIsAvailable = False

    return DBReleaseIsAvailable

def abortStageIn(dbh, lfns, DBReleaseIsAvailable):
    """ Decide if stage-in should be aborted due to no non-DBRelease input files """

    numberOfFiles = len(lfns)
    numberOfDBReleaseFiles = 0

    if DBReleaseIsAvailable:
        for lfn in lfns:
            if isDBReleaseFile(dbh, lfn): # multi-trf jobs will have more than one DBRelease file
                numberOfDBReleaseFiles += 1

    if numberOfDBReleaseFiles < numberOfFiles:
        tolog("Number of locally available DBRelease files = %d (%d files in total), continue with stage-in" % (numberOfDBReleaseFiles, numberOfFiles))
        status = False # do not abort stage-in
    else:
        tolog("Number of locally available DBRelease files = %d (%d files in total), abort stage-in" % (numberOfDBReleaseFiles, numberOfFiles))
        status = True # abort stage-in

    return status

def sitemover_get_data(sitemover, error, get_RETRY, get_RETRY_replicas, get_attempt, replica_number, N_files_on_tape, N_root_files, N_non_root_files,\
                       gpfn, lfn, path, fsize=None, spsetup=None, fchecksum=None, guid=None, analysisJob=None, usect=None, pinitdir=None, proxycheck=None,\
                       sitename=None, token=None, timeout=None, dsname=None, userid=None, report=None, access=None, inputDir=None, jobId=None,\
                       workDir=None, cmtconfig=None, experiment=None):
    """ Wrapper for the actual stage-in command from the relevant sitemover """

    s = -1
    pErrorText = ""
    replica_transferred = False
    will_use_direct_io = False
    try:
        # Data transfer with test if get_data finishes on time
        # use the time out class to call the get_data function for the appropriate site mover
        # error code, and a direct reading list will be returned
        # (the direct reading list will be None for all site movers except the xrootdSiteMover when it encounters a root file)
        s, pErrorText = sitemover.get_data(gpfn, lfn, path, fsize=fsize, spsetup=spsetup, fchecksum=fchecksum, guid=guid,\
                                           analJob=analysisJob, usect=usect, pinitdir=pinitdir, proxycheck=proxycheck, sitename=sitename,\
                                           token=token, timeout=timeout, dsname=dsname, userid=userid, report=report,\
                                           access=access, inputDir=inputDir, jobId=jobId, workDir=workDir, cmtconfig=cmtconfig, experiment=experiment)
    except Exception, e:
        pilotErrorDiag = "Unexpected exception: %s" % (get_exc_plus())
        tolog("!!FAILED!!2999!! %s" % (pilotErrorDiag))
        s = error.ERR_STAGEINFAILED
        tolog("Mover get_data finished (failed)")
    else:
        # special case (not a real error, so reset the return value s)
        if s == error.ERR_DIRECTIOFILE:
            # reset s to prevent loop from stopping
            s = 0
            tolog("Site mover skipped stage-in in favor of direct i/o")
            will_use_direct_io = True

        if s == error.ERR_FILEONTAPE:
            # reset s to prevent loop from stopping
            s = 0
            tolog("!!WARNING!!2999!! File is on tape (will be skipped): %s" % (gpfn))
            # add metadata to skipped.xml for the last replica retry
            if (replica_number + 1) == get_RETRY_replicas and (get_attempt + 1) == get_RETRY:
                N_files_on_tape += 1
                tolog("Adding replica info to skipped.xml")
                _ec = addToSkipped(lfn, guid)
        elif s != 0:
            # did the get command return error text?
            pilotErrorDiag = pErrorText
            tolog('!!WARNING!!2999!! Error in copying (attempt %s): %s - %s' % (replica_number + 1, s, pilotErrorDiag))
            # add metadata to skipped.xml for the last replica retry
            if (replica_number + 1) == get_RETRY_replicas and (get_attempt + 1) == get_RETRY:
                tolog("Adding replica info to skipped.xml")
                _ec = addToSkipped(lfn, guid)
        else:
            # is the copied file a root file?
            if sitemover.isRootFileName(lfn):
                N_root_files += 1
            else:
                N_non_root_files += 1
            tolog("File transfer finished correctly")
            replica_transferred = True

    return s, pErrorText, N_files_on_tape, N_root_files, N_non_root_files, replica_transferred, will_use_direct_io

def sitemover_get_all_data(sitemover, error, gpfn, lfn, path, fsize=None, spsetup=None, fchecksum=None,\
                           guid=None, analysisJob=None, usect=None, pinitdir=None, proxycheck=None,\
                           sitename=None, token=None, timeout=None, dsname=None, userid=None, report=None, access=None, inputDir=None, jobId=None,\
                           workDir=None, cmtconfig=None, lfns=None, experiment=None, replicas_dic=None, dsdict=None):
    """ Wrapper for the actual stage-in command from the relevant sitemover """

    s = -1
    pilotErrorDiag = ""

    try:
        # Data transfer with test if get_data finishes on time
        # use the time out class to call the get_data function for the appropriate site mover
        # error code, and a direct reading list will be returned
        # (the direct reading list will be None for all site movers except the xrootdSiteMover when it encounters a root file)
        s, pilotErrorDiag = sitemover.get_data(gpfn, lfn, path, fsize=fsize, spsetup=spsetup, fchecksum=fchecksum, guid=guid,\
                                           analJob=analysisJob, usect=usect, pinitdir=pinitdir, proxycheck=proxycheck, sitename=sitename,\
                                           token=token, timeout=timeout, dsname=dsname, userid=userid, report=report, lfns=lfns, replicas_dic=replicas_dic,\
                                           access=access, inputDir=inputDir, jobId=jobId, workDir=workDir, cmtconfig=cmtconfig, experiment=experiment, dsdict=dsdict)
    except Exception, e:
        pilotErrorDiag = "Unexpected exception: %s" % (get_exc_plus())
        tolog("!!FAILED!!2999!! %s" % (pilotErrorDiag))
        s = error.ERR_STAGEINFAILED
        tolog("Mover get_data finished (failed)")
    else:
        if s != 0:
            # did the get command return error text?
            tolog('!!WARNING!!2999!! Error in copying: %s - %s' % (s, pilotErrorDiag))
        else:
            tolog("File transfer finished correctly")

    return s, pilotErrorDiag

def correctTotalFileSize(totalFileSize, fileInfoDic, lfns, dbh, DBReleaseIsAvailable):
    """ Correct the total file size of all input files """

    # correct the totalFileSize for DBRelease file that might be skipped
    for nr in range(len(fileInfoDic.keys())):
        # extract the file info from the dictionary
        gpfn = fileInfoDic[nr][1]
        fsize = fileInfoDic[nr][2]

        # get the corresponding lfn
        lfn = getLFN(gpfn, lfns)

        # already checked availability of DBRelease file above, now correct the total file size
        if isDBReleaseFile(dbh, lfn) and DBReleaseIsAvailable:
            try:
                totalFileSize -= long(fsize)
            except:
                tolog("The requested DBRelease file is available locally, but the file size is not available")
            else:
                tolog("Subtracted the locally available DBRelease file size from the total file size")

    return totalFileSize

def getPFCName(path, inputpoolfcstring):
    """ extract the PFC name """

    _tmp_fcname = inputpoolfcstring.split(':')[-1]
    if os.path.isabs(_tmp_fcname):
        pfc_name = _tmp_fcname
    else:
        pfc_name = os.path.join(os.path.abspath(path), _tmp_fcname)

    return pfc_name

def getNumberOfReplicaRetries(createdPFCTURL, replica_dictionary, guid):
    """ Get the replicas retry number """

    # determine whether alternative replica stage-in should be allowed (not for FileStager and Direct Access jobs)
    if not createdPFCTURL and replica_dictionary != {}:
        # override any get_RETRY in this mode
        get_RETRY_replicas = min(len(replica_dictionary[guid]), MAX_NUMBER_OF_RETRIES)
        tolog("Setting number of replica retries to: %d" % (get_RETRY_replicas))
    else:
        get_RETRY_replicas = 1

    return get_RETRY_replicas

def reportFileCorruption(gpfn, sitemover):
    """ report corrupt file to consistency server """

    # except for lcgcp site mover (since it does not return a proper SURL, the consistency report is done in the site mover)
    _copytool = readpar("copytoolin")
    if _copytool == "":
        _copytool = readpar("copytool")
    if _copytool != "lcgcp" and _copytool != "lcg-cp" and _copytool != "storm":
        if gpfn != "":
            sitemover.reportFileCorruption(gpfn)
        else:
            tolog("!!WARNING!!1990!! Can not report SURL for corrupt file to consistency server since SURL is an empty string")
    else:
        tolog("(Already reported corrupted file)")

def verifyPFCIntegrity(guidfname, lfns, dbh, DBReleaseIsAvailable, error):
    """ Verify the integrity if the PFC """

    # Since there were no transfer errors, guidfname.values() should be the same as the input file list (lfns)
    # although due to bugs in the job definition, the input file list can contain multiple entries of the same file.
    # The server will return xml for unique files. The two lists are always empty or lists of strings, furthermore
    # filelist_fromxml are always included in filelist_fromlfns

    fail = 0
    pilotErrorDiag = ""
    filelist_fromxml = guidfname.values()
    filelist_fromlfns = []
    for lfn in lfns:
        if lfn not in filelist_fromlfns:
            if not (isDBReleaseFile(dbh, lfn) and DBReleaseIsAvailable):
                filelist_fromlfns += [lfn]

    tolog("filelist_fromlfns = %s" % str(filelist_fromlfns))
    tolog("filelist_fromxml = %s" % str(filelist_fromxml))

    # return an error if a file is missing in the PoolFileCatalog.xml
    if len(filelist_fromxml) < len(filelist_fromlfns):
        # which file(s) is/are missing?
        missing = [i for i in filelist_fromlfns if not (i in filelist_fromxml or filelist_fromxml.append(i))]
        # missing = pUtil.diffLists(filelist_fromlfns, filelist_fromxml)
        if missing:
            pilotErrorDiag = "Missing input file(s) in xml: %s" % str(missing)
            tolog("!!FAILED!!2999!! %s" % (pilotErrorDiag))
            fail = error.ERR_MISSFILEXML
        else:
            tolog("Verified: PoolFileCatalog.xml contains all input files")
    else:
        tolog("Verified: PoolFileCatalog.xml contains at least as many files as the initial list")

    return fail, pilotErrorDiag

def createStandardPFC4TRF(createdPFCTURL, pfc_name_turl, pfc_name, guidfname):
    """ Create the default PFC with SURLs if needed """

    # note: in the case FAX was used as a primary site mover in combination with direct I/O
    # the guidfname dictionary will actually contain TURLs and not SURLs. The logic below still
    # holds however, since a TURL based PFC was not created earlier

    # prepare if necessary a PFC containing TURLs (for direct access/file stager)
    # did we create a TURL based PFC earlier?
    createdPFC = False
    if createdPFCTURL:
        # rename the TURL based PFC back to PoolFileCatalog.xml
        try:
            os.rename(pfc_name_turl, pfc_name)
        except Exception, e:
            tolog("!!WARNING!!2997!! Could not rename TURL based PFC: %s" % str(e))
        else:
            tolog("Renamed TURL based PFC from %s to %s" % (pfc_name_turl, pfc_name))
            createdPFC = True
    else:
        tolog("No TURL based PFC was created earlier")

    # create a standard PFC with SURLs if needed (basically this is default)
    # note: the SURLs are actually TURLs if FAX was used as a primary site mover in combination with direct I/O
    if not createdPFC:
        # always write a PoolFileCatalog.xml independently from how many files were transferred succesfully
        # No PFC only if no PFC was returned by DQ2
        createPFC4TRF(pfc_name, guidfname)

def PFC4TURLs(analysisJob, transferType, fileInfoDic, pfc_name_turl, sitemover, sitename, usect):
    """ Create a TURL based PFC if necessary/requested """
    # I.e if copy tool should not be used [useCT=False] and if oldPrefix and newPrefix are not already set in copysetup [useSetPrefixes=False]

    ec = 0
    pilotErrorDiag = ""
    createdPFCTURL = False

    # first check if there is a need to create the PFC
    if shouldPFC4TURLsBeCreated(analysisJob, transferType):
        ec, pilotErrorDiag = createPFC4TURLs(fileInfoDic, pfc_name_turl, sitemover, sitename)
        if ec == 0:
            tolog("PFC created with TURLs")
            createdPFCTURL = True
        elif analysisJob:
            # reset the pilotErrorDiag since it is not needed
            pilotErrorDiag = ""
            tolog("Defaulting to copy-to-scratch")
            statusPFCTurl = False # this will trigger a correction of the setup command (user analysis jobs only, not needed for production jobs)
            usect = True
        else:
            tolog("Will not switch to copy-to-scratch for production job (fail immediately)")

    return ec, pilotErrorDiag, createdPFCTURL, usect

def extractInputFileInfo(fileInfoList_nr, lfns):
    """ Extract the file info for the given input file """

    guid = fileInfoList_nr[0]
    gpfn = fileInfoList_nr[1]
    fsize = fileInfoList_nr[2]
    fchecksum = fileInfoList_nr[3]
    tolog("Extracted (guid, gpfn, fsize, fchecksum) = (%s, %s, %s, %s)" % (guid, gpfn, str(fsize), fchecksum))

    # get the corresponding lfn
    lfn = getLFN(gpfn, lfns)

    if fchecksum == "" or fchecksum == "None":
        fchecksum = 0
    if fsize == "":
        fsize = 0

    return guid, gpfn, lfn, fsize, fchecksum

def getAlternativeReplica(gpfn, guid, replica_number, createdPFCTURL, replica_dictionary):
    """ Grab the gpfn from the replicas dictionary in case alternative replica stage-in is allowed """

    if not createdPFCTURL and replica_dictionary != {}:
        try:
            gpfn = replica_dictionary[guid][replica_number]
        except Exception, e:
            tolog("!!WARNING!!1001!! Could not grab alternative replica from dictionary: %s (using default replica)" % str(e))
        else:
            tolog("Using replica number %d: %s" % (replica_number, gpfn))

    return gpfn

def mover_get_data(lfns,
                   path,
                   sitename,
                   stageinTries,
                   inputpoolfcstring="xmlcatalog_file:PoolFileCatalog.xml",
                   ub=URLBASE,
                   dsname="",
                   dsdict={},
                   guids=[],
                   analysisJob=False,
                   usect=True,
                   pinitdir="",
                   proxycheck=True,
                   spsetup="",
                   tokens=[],
                   userid="",
                   inputDir="",
                   jobId=None,
                   jobDefId="",
                   access_dict=None,
                   scope_dict=None,
                   workDir="",
                   DN=None,
                   dbh=None,
                   jobPars="",
                   cmtconfig="",
                   filesizeIn=[],
                   checksumIn=[],
                   transferType=None,
                   experiment=""):
    """
    This method is called by a job to get the required input data.
    The parameters passed are a list of LFNs, working directory path, site name, 
    and a connection string to the poolfile catalog to fill with
    input data (if none given the default is xmlcatalog_file:PoolFileCatalog.xml).
    The local destination directory (working directory path) should already exist,
    or the copy will fail.

    The program stops at the first failed transfer (after retries) and the PFC
    contains the files that were transferred correctly, an error message is returned.
    """

    tolog("Mover get data started")

    statusPFCTurl = None
    pilotErrorDiag = ""

    # FAX counters (will be reported in jobMetrics)
    N_filesWithoutFAX = 0 # only relevant when FAX has been activated after a stage-in failure
    N_filesWithFAX = 0  # only relevant when FAX has been activated after a stage-in failure

    # FAX control variable, if FAX is used as primary site mover in combination with direct I/O
    usedFAXandDirectIO = False

    # is the DBRelease file available locally?
    DBReleaseIsAvailable = handleDBRelease(dbh, lfns, jobPars, path)

    # should stage-in be aborted? (if there are only locally available DBRelease files in the stage-in list)
    if abortStageIn(dbh, lfns, DBReleaseIsAvailable):
        return 0, pilotErrorDiag, statusPFCTurl, N_filesWithoutFAX, N_filesWithFAX, usedFAXandDirectIO

    # setup the dictionary necessary for all instrumentation
    report = getInitialTracingReport(userid, sitename, dsname, "get_sm", analysisJob, jobId, jobDefId, DN)

    if stageinTries != 0:
        get_RETRY = min(stageinTries, MAX_NUMBER_OF_RETRIES)
    else:
        get_RETRY = MAX_RETRY
    get_TIMEOUT = 5*3600/get_RETRY

    fail = 0
    guidfname = {}
    error = PilotErrors()

    lfchost = readpar('lfchost')
    region = readpar('region')

    # space tokens currently not used for input files
    #    # check if there is are any space tokens
    #    _token = getProperSpaceTokenList(token, listSEs, len(lfns))

    # Select the correct mover
    copycmd, setup = copytool(mode="get")

    # get the sitemover object corresponding to the copy command
    sitemover = getSiteMover(copycmd, setup)

    # get the experiment object
    thisExperiment = getExperiment(experiment)

    # get the name for the PFC file
    pfc_name = getPFCName(path, inputpoolfcstring)

    # build the file info dictionary (use the filesize and checksum from the dispatcher if possible) and create the PFC
    # format: fileInfoDic[file_nr] = (guid, gpfn, fsize, fchecksum)
    ec, pilotErrorDiag, fileInfoDic, totalFileSize, replicas_dic = \
        getFileInfo(lfchost, region, ub, guids, dsname, dsdict, lfns, pinitdir, analysisJob, tokens, DN, sitemover, error, path, dbh, DBReleaseIsAvailable,\
                    scope_dict, pfc_name=pfc_name, filesizeIn=filesizeIn, checksumIn=checksumIn, thisExperiment=thisExperiment)
    if ec != 0:
        return ec, pilotErrorDiag, statusPFCTurl, N_filesWithoutFAX, N_filesWithFAX, usedFAXandDirectIO

    # until the Mover PFC file is no longer needed, call the TURL based PFC "PoolFileCatalogTURL.xml"
    pfc_name_turl = pfc_name.replace(".xml", "TURL.xml")

    # create a TURL based PFC if necessary/requested (i.e. if copy tool should not be used [useCT=False] and
    # if oldPrefix and newPrefix are not already set in copysetup [useSetPrefixes=False])
    ec, pilotErrorDiag, createdPFCTURL, usect = PFC4TURLs(analysisJob, transferType, fileInfoDic, pfc_name_turl, sitemover, sitename, usect)
    if ec != 0:
        return ec, pilotErrorDiag, statusPFCTurl, N_filesWithoutFAX, N_filesWithFAX, usedFAXandDirectIO

    # correct the total file size for the DBRelease file if necessary
    totalFileSize = correctTotalFileSize(totalFileSize, fileInfoDic, lfns, dbh, DBReleaseIsAvailable)

    # only bother with the size checks if the copy tool is to be used (non-direct access mode)
    if usect:
        # get a proper maxinputsize from schedconfig/default 
        _maxinputsize = getMaxInputSize()

        # check the total input file size
        ec, pilotErrorDiag = verifyInputFileSize(totalFileSize, _maxinputsize, error)
        if ec != 0:
            return ec, pilotErrorDiag, statusPFCTurl, N_filesWithoutFAX, N_filesWithFAX, usedFAXandDirectIO

        # do we have enough local space to stage in all data and run the job?
        ec, pilotErrorDiag = verifyAvailableSpace(totalFileSize, path, error)
        if ec != 0:
            return ec, pilotErrorDiag, statusPFCTurl, N_filesWithoutFAX, N_filesWithFAX, usedFAXandDirectIO

    # get the replica dictionary from file (used when the primary replica can not be staged due to some temporary error)
    replica_dictionary = getReplicaDictionary(path)

    # file counters
    N_files_on_tape = 0
    N_root_files = 0
    N_non_root_files = 0

    # FAX counters (will be reported in jobMetrics)
    N_filesWithoutFAX = 0 # only relevant when FAX has been activated after a stage-in failure
    N_filesWithFAX = 0  # only relevant when FAX has been activated after a stage-in failure

    # if FAX is used as a primary site mover then set the default FAX mode to true, otherwise to false (normal mode)
    if copycmd == "fax":
        usedFAXMode = True
    else:
        usedFAXMode = False

    # use isOneByOneFileTransfer() to determine if files should be transferred one by one or all at once
    if not sitemover.isOneByOneFileTransfer():

        # note: this mode is used by the aria2c site mover only
        # normal stage-in is below

        tolog("All files will be transferred at once")

        # extract the file info for the first file in the dictionary
        guid, gpfn, lfn, fsize, fchecksum = extractInputFileInfo(fileInfoDic[0], lfns)
        file_access = getFileAccess(access_dict, lfn)
        dsname = getDataset(lfn, dsdict)

        # perform stage-in using the sitemover wrapper method
        s, pErrorText = sitemover_get_all_data(sitemover, error, gpfn, lfn, path, fsize=fsize, spsetup=spsetup, fchecksum=fchecksum,\
                                               guid=guid, analysisJob=analysisJob, usect=usect, pinitdir=pinitdir, proxycheck=proxycheck,\
                                               sitename=sitename, token=None, timeout=get_TIMEOUT, dsname=dsname, userid=userid, report=report,\
                                               access=file_access, inputDir=inputDir, jobId=jobId, workDir=workDir, cmtconfig=cmtconfig, lfns=lfns,\
                                               experiment=experiment, replicas_dic=replicas_dic, dsdict=dsdict)
        if s != 0:
            tolog('!!WARNING!!2999!! Failed during stage-in of multiple files: %s' % (error.getErrorStr(s)))
            tolog("Exit code: %s" % (s))
            fail = s

    # normal stage-in (one by one file transfers)
    if sitemover.isOneByOneFileTransfer() or fail != 0:
    
        tolog("Files will be transferred one by one")

        # reset any previous failure
        fail = 0

        # loop over all files in the file info dictionary
        number_of_files = len(fileInfoDic.keys())
        tolog("Will process %d file(s)" % (number_of_files))
        for nr in range(number_of_files):
            # extract the file info from the dictionary
            guid, gpfn, lfn, fsize, fchecksum = extractInputFileInfo(fileInfoDic[nr], lfns)

            # update the dataset name
            dsname = getDataset(lfn, dsdict)

            # update the tracing report
            report = updateReport(report, gpfn, dsname, fsize, sitemover)

            # the DBRelease file might already have been handled, go to next file
            if isDBReleaseFile(dbh, lfn) and DBReleaseIsAvailable:
                updateFileState(lfn, workDir, jobId, mode="transfer_mode", state="no_transfer", type="input")
                guidfname[guid] = lfn # needed for verification below
                continue
            else:
                tolog("(Not a DBRelease file)")

            tolog("Mover is preparing to copy file %d/%d (lfn: %s guid: %s dsname: %s)" % (nr+1, number_of_files, lfn, guid, dsname))
            tolog('Copying %s to %s (LFC checksum: \"%s\", fsize: %s) using %s (%s)' %\
                  (gpfn, path, fchecksum, fsize, sitemover.getID(), sitemover.getSetup()))

            # get the number of replica retries
            get_RETRY_replicas = getNumberOfReplicaRetries(createdPFCTURL, replica_dictionary, guid)

            file_access = getFileAccess(access_dict, lfn)

            # loop over get function to allow for multiple get attempts for a file
            will_use_direct_io = False
            get_attempt = 0
            #get_RETRY = 1 #2 #PN
            while get_attempt < get_RETRY:
                if get_attempt > 0:
                    _rest = 5*60
                    tolog("(Waiting %d seconds before next stage-in attempt)" % (_rest))
                    sleep(_rest)
                tolog("Get attempt %d/%d" % (get_attempt + 1, get_RETRY))
                replica_number = 0
                replica_transferred = False
                s = 1

                # loop over replicas
                while s != 0 and replica_number < get_RETRY_replicas:
                    # grab the gpfn from the replicas dictionary in case alternative replica stage-in is allowed
                    gpfn = getAlternativeReplica(gpfn, guid, replica_number, createdPFCTURL, replica_dictionary)

                    #PN
                    #if sitemover:
                    #    del sitemover
                    #if ".lib." in gpfn:
                    #    sitemover = getSiteMover("xrdcp", setup)
                    #else:
                    #    sitemover = getSiteMover("fax", setup)

                    # perform stage-in using the sitemover wrapper method
                    s, pErrorText, N_files_on_tape, N_root_files, N_non_root_files, replica_transferred, will_use_direct_io = sitemover_get_data(sitemover, error,\
                                                                                                                             get_RETRY, get_RETRY_replicas, get_attempt,\
                                                                                                                             replica_number, N_files_on_tape, N_root_files,\
                                                                                                                             N_non_root_files, gpfn, lfn, path,\
                                                                                                                             fsize=fsize, spsetup=spsetup, fchecksum=fchecksum,\
                                                                                                                             guid=guid, analysisJob=analysisJob, usect=usect,\
                                                                                                                             pinitdir=pinitdir, proxycheck=proxycheck,\
                                                                                                                             sitename=sitename, token=None, timeout=get_TIMEOUT,\
                                                                                                                             dsname=dsname, userid=userid, report=report,\
                                                                                                                             access=file_access, inputDir=inputDir, jobId=jobId,\
                                                                                                                             workDir=workDir, cmtconfig=cmtconfig,\
                                                                                                                             experiment=experiment)
                    # get out of the multiple replica loop
                    if replica_transferred:
                        break

                    # increase the replica attempt counter in case the previous replica could not be transferred
                    replica_number += 1

                # get out of the multiple get attempt loop
                if replica_transferred:
                    break

                # increase the get attempt counter in case of failure to transfer the file
                get_attempt += 1

            # increase the successful file transfer counter (used only when reporting FAX transfers)
            if s == 0:
                N_filesWithoutFAX += 1

            if s != 0:
                # normal stage-in failed, now try with FAX if possible
                if error.isPilotResubmissionErrorCode(s):
                    allowfax = readpar('allowfax').lower()
                    if readpar('faxredirector') != "" and (allowfax == "true" or allowfax == "retry"):
                        tolog("Normal stage-in failed, will attempt to use FAX")
                        usedFAXMode = True

                        # get the FAX site mover
                        old_sitemover = sitemover
                        sitemover = getSiteMover("fax", "")

                        # perform stage-in using the sitemover wrapper method
                        s, pErrorText, N_files_on_tape, N_root_files, N_non_root_files, replica_transferred, will_use_direct_io = sitemover_get_data(sitemover, error,\
                                                                                                                                 get_RETRY, get_RETRY_replicas, get_attempt, replica_number,\
                                                                                                                                 N_files_on_tape, N_root_files, N_non_root_files,\
                                                                                                                                 gpfn, lfn, path,\
                                                                                                                                 fsize=fsize, spsetup=spsetup, fchecksum=fchecksum,\
                                                                                                                                 guid=guid, analysisJob=analysisJob, usect=usect,\
                                                                                                                                 pinitdir=pinitdir, proxycheck=proxycheck,\
                                                                                                                                 sitename=sitename, token=None, timeout=get_TIMEOUT,\
                                                                                                                                 dsname=dsname, userid=userid, report=report,\
                                                                                                                                 access=file_access, inputDir=inputDir, jobId=jobId,\
                                                                                                                                 workDir=workDir, cmtconfig=cmtconfig, experiment=experiment)
                        if replica_transferred:
                            tolog("FAX site mover managed to transfer file from remote site (resetting error code to zero)")
                            pilotErrorDiag = ""
                            s = 0

                            # increase the successful FAX transfer counter
                            N_filesWithFAX += 1
                        else:
                            tolog("FAX site mover also failed to transfer file from remote site, giving up")

                        # restore the old sitemover
                        del sitemover
                        sitemover = old_sitemover

            if s != 0:
                tolog('!!FAILED!!2999!! Failed to transfer %s: %s (%s)' % (os.path.basename(gpfn), s, error.getErrorStr(s)))
                tolog("Exit code: %s" % (s))

                # report corrupt file to consistency server if needed
                if s == error.ERR_GETADMISMATCH or s == error.ERR_GETMD5MISMATCH or s == error.ERR_GETWRONGSIZE:
                    reportFileCorruption(gpfn, sitemover)

                fail = s
                break

            # build the dictionary used to create the PFC for the TRF
            # in the case of FAX, use the global paths if direct access is to be used for the particlar file
            if usedFAXMode and will_use_direct_io:
                # the site mover needed here is the FAX site mover since the global file path methods are defined there only
                old_sitemover = sitemover
                sitemover = getSiteMover("fax", "")
                guidfname[guid] = sitemover.findGlobalFilePath(lfn, dsname)

                # restore the old sitemover
                del sitemover
                sitemover = old_sitemover

                # if FAX is used as a primary site mover, in combination with direct access, set the usedFAXandDirectIO flag
                # this will later be used to update the run command (e.g. --lfcHost is not needed etc)
                if copycmd == "fax":
                    usedFAXandDirectIO = True
            else:
                guidfname[guid] = lfn # local_file_name

    if fail == 0:
        # make sure the PFC has the correct number of files
        fail, pilotErrorDiag = verifyPFCIntegrity(guidfname, lfns, dbh, DBReleaseIsAvailable, error)

    # now that the Mover PFC file is no longer needed, back it up and rename the TURL based PFC if it exists
    # (the original PFC is no longer needed. Move it away, and then create the PFC for the trf/runAthena)
    backupPFC4Mover(pfc_name)

    # create a standard PFC with SURLs if needed (basically this is default)
    # note: if FAX was used as a primary site mover in combination with direct I/O, then the SURLs will actually be TURLs
    # but there is no need to use the special TURL creation method PFC4TURL used above (FAX will have returned the TURLs instead)
    createStandardPFC4TRF(createdPFCTURL, pfc_name_turl, pfc_name, guidfname)

    tolog("Number of identified root files     : %d" % (N_root_files))
    tolog("Number of transferred non-root files: %d" % (N_non_root_files))

    if usedFAXMode:
        tolog("Number of files without FAX         : %d (normal transfers)" % (N_filesWithoutFAX))
        tolog("Number of files with FAX            : %d (successful FAX transfers)" % (N_filesWithFAX))

    if N_files_on_tape > 0:
        tolog("!!WARNING!!2999!! Number of skipped files: %d (not staged)" % (N_files_on_tape))
        if N_root_files == 0:
            # this should only happen for user jobs
            tolog("Mover get_data failed since no root files could be transferred")
            fail = error.ERR_NOSTAGEDFILES
        else:
            tolog("Mover get_data finished (partial)")
    else:
        if fail == 0:
            tolog("Get successful")
            tolog("Mover get_data finished")
        else:
            tolog("Mover get_data finished (failed)")
    tolog("Will return exit code = %d, pilotErrorDiag = %s" % (fail, pilotErrorDiag))    

    return fail, pilotErrorDiag, statusPFCTurl, N_filesWithoutFAX, N_filesWithFAX, usedFAXandDirectIO

def performSpecialADRegistration(sitemover, r_fchecksum, r_gpfn):
    """ Perform special adler32 registration """

    tolog("r_gpfn=%s"%r_gpfn)
    # strip protocol and server from the file path
    r_gpfn = sitemover.stripProtocolServer(r_gpfn)
    tolog("r_gpfn=%s"%r_gpfn)

    cmd = "%s %s %s" % (os.environ['SETADLER32'], r_fchecksum, r_gpfn)
    tolog("Executing command: %s" % (cmd))
    rc, rs = commands.getstatusoutput(cmd)
    if rc != 0:
        tolog("!!WARNING!!2995!! Special adler32 command failed: %d, %s" % (rc, rs))
    else:
        if rs != "":
            tolog("Special adler32 command returned: %s" % (rs))
        else:
            tolog("Special adler32 command finished")

def getFileSizeAndChecksum(lfn, outputFileInfo):
    """ get the file size and checksum if possible """

    fsize = 0
    checksum = 0
    tolog("outputFileInfo=%s"%str(outputFileInfo))
    if outputFileInfo:
        try:
            fsize = outputFileInfo[lfn][0]
            checksum = outputFileInfo[lfn][1]
        except Exception, e:
            tolog("Could not extract file size and checksum for file %s: %s" % (lfn, str(e)))
        else:
            tolog("Extracted file size %s and checksum %s for file %s" % (fsize, checksum, lfn))
    else:
        tolog("No output file info available for file %s" % (lfn))

    return fsize, checksum

def chirp_put_data(pfn, ddm_storage, dsname="", sitename="", analysisJob=True, testLevel=0, pinitdir="",\
                   proxycheck=True, token="", timeout=DEFAULT_TIMEOUT, lfn="", guid="", spsetup="", userid="", report=None,\
                   prodSourceLabel="", outputDir="", DN="", dispatchDBlockTokenForOut=None, logFile="", experiment=None):
    """ Special put function for calling chirp site mover """

    ec = 0
    pilotErrorDiag = ""

    # get the sitemover for chirp
    sitemover = getSiteMover("chirp", "")

    # transfer the file
    try:
        if prodSourceLabel == "software":
            analysisJob = False
        # execute put_data and test if it finishes on time
        tolog("Mover put data sending prodSourceLabel=%s to put_data" % (prodSourceLabel))
        ec, pilotErrorDiag, r_gpfn, r_fsize, r_fchecksum, r_farch = sitemover.put_data(pfn, ddm_storage, dsname=dsname, sitename=sitename,\
                                                                analJob=analysisJob, testLevel=testLevel, pinitdir=pinitdir,\
                                                                proxycheck=proxycheck, token=token, timeout=timeout, lfn=lfn,\
                                                                guid=guid, spsetup=spsetup, userid=userid, report=report,\
                                                                prodSourceLabel=prodSourceLabel, outputDir=outputDir, DN=DN,\
                                                                dispatchDBlockTokenForOut=dispatchDBlockTokenForOut, logFile=logFile, experiment=experiment)
        tolog("Site mover put function returned: s=%s, r_gpfn=%s, r_fsize=%s, r_fchecksum=%s, r_farch=%s, pilotErrorDiag=%s" %\
              (ec, r_gpfn, r_fsize, r_fchecksum, r_farch, pilotErrorDiag))
    except:
        ec = -1
        pilotErrorDiag = "Unexpected exception: %s" % (get_exc_plus())
        tolog('!!WARNING!!2999!! %s' % (pilotErrorDiag))
    else:
        if ec != 0:
            tolog('!!WARNING!!2999!! Error in copying: %s - %s' % (ec, pilotErrorDiag))
        else:
            # should the file also be moved to a secondary storage?
            pass

    return ec, pilotErrorDiag

def prepareAlternativeStageOut(sitemover, si, sitename, jobCloud):
    """ Prepare for a stage-out to an alternative SE (Tier-1); download new queuedata, return a corresponding sitemover object """

    alternativeSitemover = None

    # only proceed if we are on a Tier-2 site
    if si.isTier2(sitename):
        # get the corresponding Tier-1 site (where we want to stage-out to)
        queuename = si.getTier1Queue(jobCloud)

        if queuename:
            tolog("Will attempt stage-out to %s" % (queuename))

            # download new queuedata (use alt option to download file queuedata.alt.<extension>) to avoid overwriting the main queuedata file
            ec, hasQueuedata = si.processQueuedata(queuename, alt=True)
            if ec != 0:
                tolog("Failed to download queuedata for queue %s (aborting)")
            else:
                # make sure that both Tier-2 and Tier-1 have lfcregister=server
                if readpar('lfcregister') == "server" and readpar('lfcregister', alt=True) == "server":
                    tolog("Both Tier-1 and Tier-2 sites have lfcregister=server (proceed)")

                    # get the copy tool
                    copycmd = "lcg-cp2"
                    setup = ""
                    tolog("Copy command: %s" % (copycmd))

                    # get the site mover
                    alternativeSitemover = getSiteMover(copycmd, setup)
                    tolog("Got site mover: %s" % str(sitemover))

                    # which space token should be used?
                    # use the requested space token primarily (is it available at the alternative SE?)
                    # otherwise use the default space token of the alternative SE

                    # all is set for a stage-out to an alternative SE, return the alternative sitemover object
                else:
                    tolog("Aborting since Tier-1 lfcregister=\"%s\" and Tier-2 lfcregister=\"%s\" (both must be set to \"server\")" % (readpar('server', alt=True), readpar('server')))
        else:
            tolog("Did not get a queuename for the Tier-1 (aborting)")

    return alternativeSitemover

def getFileList(outputpoolfcstring):
    """ Get the file listfrom the PFC """

    pfc_name = outputpoolfcstring.split(':', 1)[1]        
    xmldoc = minidom.parse(pfc_name)        

    return xmldoc.getElementsByTagName("File")

def getFilenamesAndGuid(thisfile):
    """ Get the file name and guid """

    pfn = str(thisfile.getElementsByTagName("pfn")[0].getAttribute("name"))
    filename = os.path.basename(pfn)

    pfn = os.path.abspath(pfn)
    lfn = ''
    if (thisfile.getElementsByTagName("lfn") != []):
        lfn = str(thisfile.getElementsByTagName("lfn")[0].getAttribute("name"))
    else:
        lfn = filename
    guid = str(thisfile.getAttribute("ID"))

    return filename, lfn, pfn, guid

def getDatasetName(sitemover, datasetDict, lfn, pdsname):
    """ Get the dataset name """
    # (dsname_report is the same as dsname but might contain _subNNN parts)

    # get the dataset name from the dictionary
    if datasetDict:
        try:
            dsname = datasetDict[lfn]
        except Exception, e:
            tolog("!!WARNING!!2999!! Could not get dsname from datasetDict for file %s: %s, %s (using default %s)" % (lfn, e, str(datasetDict), pdsname))
            dsname = pdsname
    else:
        dsname = pdsname

    # save the original dsname for the tracing report
    dsname_report = dsname

    # remove any _subNNN parts from the dataset name (from now on dsname will only be used to create SE destination paths)
    dsname = sitemover.removeSubFromDatasetName(dsname)

    tolog("File %s will go to dataset %s" % (lfn, dsname))

    return dsname, dsname_report

def getSpaceTokenForFile(filename, _token, logFile, file_nr, fileListLength):
    """ Get the currect space token for the given file """

    _token_file = None

    if filename == logFile:
        if _token:
            _token_file = _token[-1]
            if _token_file.upper() != 'NULL' and _token_file != '':
                tolog("Mover is preparing to copy log file to space token: %s" % (_token_file))
            else:
                tolog("Mover is preparing to copy log file (space token not set)")
                _token_file = None
        else:
            tolog("Mover is preparing to copy log file (space token not set)")
    else:
        if _token:
            _token_file = _token[file_nr]
            if _token_file.upper() != 'NULL' and _token_file != '':
                tolog("Mover is preparing to copy file %d/%d to space token: %s" %\
                      (file_nr+1, fileListLength, _token_file))
            else:
                tolog("Mover is preparing to copy file %d/%d (space token not set: %s)" %\
                      (file_nr+1, fileListLength, _token_file))
                _token_file = None
        else:
            tolog("Mover is preparing to copy file %d/%d (space token not set)" % (file_nr+1, fileListLength))

    return _token_file

def sitemover_put_data(sitemover, error, workDir, jobId, pfn, ddm_storage, dsname, sitename, analysisJob, testLevel, pinitdir, proxycheck, _token_file, lfn,\
                       guid, spsetup, userid, report, cmtconfig, prodSourceLabel, outputDir, DN, fsize, checksum, logFile, _attempt, experiment, scope, alt=False):
    """ """

    s = 0
    pilotErrorDiag = ""
    r_gpfn = ""
    r_fsize = ""
    r_fchecksum = ""
    r_farch = ""

    try:
        # do no treat install jobs as an analysis job
        if prodSourceLabel == "software":
            analysisJob = False

        # execute put_data and test if it finishes on time
        s, pilotErrorDiag, r_gpfn, r_fsize, r_fchecksum, r_farch = sitemover.put_data(pfn, ddm_storage, dsname=dsname, sitename=sitename,\
                                                                analJob=analysisJob, testLevel=testLevel, pinitdir=pinitdir, proxycheck=proxycheck,\
                                                                token=_token_file, timeout=DEFAULT_TIMEOUT, lfn=lfn, guid=guid, spsetup=spsetup,\
                                                                userid=userid, report=report, cmtconfig=cmtconfig, prodSourceLabel=prodSourceLabel,\
                                                                outputDir=outputDir, DN=DN, fsize=fsize, fchecksum=checksum, logFile=logFile,\
                                                                attempt=_attempt, experiment=experiment, alt=alt)
        tolog("Stage-out returned: s=%s, r_gpfn=%s, r_fsize=%s, r_fchecksum=%s, r_farch=%s, pilotErrorDiag=%s" %\
              (s, r_gpfn, r_fsize, r_fchecksum, r_farch, pilotErrorDiag))
    except:
        pilotErrorDiag = "Unexpected exception: %s" % (get_exc_plus())
        tolog('!!WARNING!!2999!! %s' % (pilotErrorDiag))
        s = error.ERR_STAGEOUTFAILED

        # write traceback info to stderr
        import traceback
        exc, msg, tb = sys.exc_info()
        traceback.print_tb(tb)
    else:
        # add the guid and surl to the surl dictionary if possible
        if guid != "" and r_gpfn != "":
            if not sitemover.updateSURLDictionary(guid, r_gpfn, workDir, jobId):
                pilotErrorDiag = "Failed to add surl for guid %s to dictionary" % (guid)
                tolog("!!WARNING!!2999!! %s" % (pilotErrorDiag))
        else:
            tolog("!!WARNING!!2998!! Can not update SURL dictionary since guid=%s and r_gpfn=%s" % (guid, r_gpfn))

    return s, pilotErrorDiag, r_gpfn, r_fsize, r_fchecksum, r_farch


def getScope(lfn, file_nr, scopeOut, scopeLog):
    """ Get the scope corresponding to a given LFN """

    # grab the scope from the proper place
    if ".log." in lfn:
        try:
            scope = scopeLog[0]
        except Exception, e:
            scope = ""
            tolog("!!WARNING!!4323!! Failed to extract scope for log file from list: %s, %s" % (str(scopeLog), e))
        else:
            tolog("Using scope \'%s\' for log file" % (scope))
    else:
        try:
            scope = scopeOut[file_nr]
        except Exception, e:
            scope = ""
            tolog("!!WARNING!!4323!! Failed to extract scope for file number %d from list: %s, %s" % (file_nr, str(scopeOut), e))
        else:
            tolog("Using scope \'%s\' for file number %d" % (scope, file_nr))

    return scope

def mover_put_data(outputpoolfcstring,
                   pdsname,
                   sitename,
                   ub=URLBASE,
                   analysisJob=False,
                   testLevel="0",
                   pinitdir="",
                   proxycheck=True,
                   spsetup="",
                   token=[],
                   userid="",
                   prodSourceLabel="",
                   datasetDict=None,
                   outputDir="",
                   jobId=None,
                   jobDefId="",
                   jobWorkDir=None,
                   DN=None,
                   outputFileInfo=None,
                   dispatchDBlockTokenForOut=None,
                   lfcreg=True,
                   jobCloud="",
                   logFile="",
                   cmtconfig="",
                   recoveryWorkDir=None,
                   experiment="ATLAS",
                   stageoutTries=2,
                   scopeOut=None,
                   scopeLog=None,
                   fileDestinationSE=None):
    """
    Move the output files in the pool file catalog to the local storage, change the pfns to grid accessable pfns.
    No DS registration in the central catalog is made. pdsname is used only to define the relative path
    """

    fail = 0
    error = PilotErrors()
    pilotErrorDiag = ""

    # file info field used by job recovery
    fields = ['', '', '', '', '', '', '']

    # Alternative transfer counters (will be reported in jobMetrics, only relevant when alternative stage-out was used)
    N_filesNormalStageOut = 0
    N_filesAltStageOut = 0

    # note: the job work dir does not exist in the case of job recovery
    if recoveryWorkDir:
        workDir = recoveryWorkDir
    else:
        workDir = os.path.dirname(jobWorkDir)

    if fileDestinationSE:
        tolog("Mover is using fileDestinationSE=%s" % (fileDestinationSE))
    else:
        tolog("fileDestinationSE is not set")

    # setup the dictionary necessary for all instrumentation
    report = getInitialTracingReport(userid, sitename, pdsname, "put_sm", analysisJob, jobId, jobDefId, DN)

    # Remember that se can be a list where the first is used for output but any can be used for input
    listSEs = readpar('se').split(',')
    tolog("SE list: %s" % str(listSEs))

    region = readpar('region')
    lfc_host = readpar('lfchost')
    if lfc_host != "":
        os.environ['LFC_HOST'] = lfc_host

    # get the storage path, e.g.
    # ddm_storage = srm://uct2-dc1.uchicago.edu:8443/srm/managerv2?SFN=/pnfs/uchicago.edu/atlasuserdisk
    ddm_storage, pilotErrorDiag = getDDMStorage(ub, analysisJob, region)
    if pilotErrorDiag != "":
        return error.ERR_NOSTORAGE, pilotErrorDiag, fields, None, N_filesNormalStageOut, N_filesAltStageOut

    # get the copy tool
    copycmd, setup = copytool()

    tolog("Copy command: %s" % (copycmd))
    tolog("Setup: %s" % (setup))

    # get the site mover
    sitemover = getSiteMover(copycmd, setup)
    tolog("Got site mover: %s" % str(sitemover))

    # get the file list from the PFC (DOM object list)
    file_list = getFileList(outputpoolfcstring)

    # create the scope dictionary
    # scope_dict = createZippedDictionary(file_list, job.scopeIn)

    # get the site information object
    si = getSiteInformation(experiment)

    # get the list of space tokens
    token_list = getSpaceTokenList(token, listSEs, jobCloud, analysisJob, len(file_list), si)
    alternativeTokenList = None

    # loop over all files to be staged in
    fail = 0
    file_nr = -1
    for thisfile in file_list:
        file_nr += 1

        # get the file names and guid
        filename, lfn, pfn, guid = getFilenamesAndGuid(thisfile)
        # note: pfn is the full path to the local file
        tolog("Preparing copy for %s to %s using %s" % (pfn, ddm_storage, copycmd))

        # get the corresponding scope
        scope = getScope(lfn, file_nr, scopeOut, scopeLog)

        # update the current file state
        updateFileState(filename, workDir, jobId, mode="file_state", state="not_transferred")
        dumpFileStates(workDir, jobId)

        # get the dataset name (dsname_report is the same as dsname but might contain _subNNN parts)
        dsname, dsname_report = getDatasetName(sitemover, datasetDict, lfn, pdsname)

        # update tracing report
        report['dataset'] = dsname_report

        # get the currect space token for the given file
        _token_file = getSpaceTokenForFile(filename, token_list, logFile, file_nr, len(file_list))

        # get the file size and checksum if possible
        fsize, checksum = getFileSizeAndChecksum(lfn, outputFileInfo)

        s = 1
        _attempt = 0
        if stageoutTries > 0 and stageoutTries < 10:
            put_RETRY = stageoutTries
        else:
            tolog("!!WARNING!!1888!! Unreasonable number of stage-out tries: %d (reset to default)" % (stageoutTries))
            put_RETRY = 2

        #PN
        #put_RETRY = 1
        tolog("Number of stage-out tries: %d" % (stageoutTries))

        # loop over put_data() to allow for multple stage-out attempts
        while s != 0 and _attempt < put_RETRY:
            _attempt += 1

            # if not first stage-out attempt, take a nap before next attempt
            if _attempt > 1:
                _rest = 10*60
                tolog("(Waiting %d seconds before next stage-out attempt)" % (_rest))
                sleep(_rest)

            tolog("Put attempt %d/%d" % (_attempt, put_RETRY))

            # perform the stage-out
            s, pilotErrorDiag, r_gpfn, r_fsize, r_fchecksum, r_farch = sitemover_put_data(sitemover, error, workDir, jobId, pfn, ddm_storage, dsname,\
                                                                                          sitename, analysisJob, testLevel, pinitdir, proxycheck,\
                                                                                          _token_file, lfn, guid, spsetup, userid, report, cmtconfig,\
                                                                                          prodSourceLabel, outputDir, DN, fsize, checksum, logFile,\
                                                                                          _attempt, experiment, scope)
            # increase normal stage-out counter if file was staged out
            if s == 0:
                N_filesNormalStageOut += 1

            # attempt alternative stage-out if required
            if s != 0:
                tolog('!!WARNING!!2999!! Error in copying (attempt %s): %s - %s' % (_attempt, s, pilotErrorDiag))

                # should alternative stage-out be attempted?
                if _attempt > 1:
                    useAlternativeStageOut = si.allowAlternativeStageOut()
                else:
                    useAlternativeStageOut = False

                #PN disabled for now
                useAlternativeStageOut = False

                if "failed to remove file" in pilotErrorDiag and not useAlternativeStageOut:
                    tolog("Aborting stage-out retry since file could not be removed from storage/catalog")
                    break

                # perform alternative stage-out if required
                if useAlternativeStageOut:
                    tolog("Attempting alternative stage-out (to the Tier-1 of the cloud the job belongs to)")

                    # download new queuedata and return the corresponding sitemover object
                    alternativeSitemover = prepareAlternativeStageOut(sitemover, si, sitename, jobCloud)

                    if alternativeSitemover:
                        # update the space token?
                        if not alternativeTokenList:
                            alternativeListSEs = readpar('se', alt=True).split(',')
                            tolog("Alternative SE list: %s" % str(alternativeListSEs))

                        # which space token should be used? Primarily use the requested space token primarily (is it available at the alternative SE?)
                        # otherwise use the default space token of the alternative SE
                        alternativeTokenList = getSpaceTokenList(token, alternativeListSEs, jobCloud, analysisJob, len(file_list), si, alt=True)
                        tolog("Created alternative space token list: %s" % str(alternativeTokenList))

                        # get the currect space token for the given file
                        _token_file = getSpaceTokenForFile(filename, alternativeTokenList, logFile, file_nr, len(file_list))
                        tolog("Using alternative space token: %s" % (_token_file))

                        # perform the stage-out
                        tolog("Attempting stage-out to alternative SE")
                        _s, _pilotErrorDiag, r_gpfn, r_fsize, r_fchecksum, r_farch = sitemover_put_data(alternativeSitemover, error, workDir, jobId, pfn, ddm_storage, dsname,\
                                                                                                      sitename, analysisJob, testLevel, pinitdir, proxycheck,\
                                                                                                      _token_file, lfn, guid, spsetup, userid, report, cmtconfig,\
                                                                                                      prodSourceLabel, outputDir, DN, fsize, checksum, logFile,\
                                                                                                      _attempt, experiment, scope)
                        if _s == 0:
                            # do no further stage-out (prevent another attempt, but allow e.g. chirp transfer below, as well as
                            # return a zero error code from this function, also for job recovery returning s=0 will not cause job recovery
                            # to think that the file has not been transferred)
                            _attempt = 999
                            s = 0

                            # increase alternative stage-out counter
                            N_filesAltStageOut += 1

                elif _attempt > 1:
                    tolog("Stage-out to alternative SE not allowed")

            if s == 0:
                # use special tool for ADLER32 registration if found
                if os.environ.has_key('SETADLER32'):
                    performSpecialADRegistration(sitemover, r_fchecksum, r_gpfn)

                # should the file also be moved to a secondary storage?
                if dispatchDBlockTokenForOut:
                    # verify that list has correct length

                    s2 = None
                    try:
                        if filename == logFile:
                            dDBlockTokenForOut = dispatchDBlockTokenForOut[-1]
                        else:
                            dDBlockTokenForOut = dispatchDBlockTokenForOut[file_nr]
                        if dDBlockTokenForOut.startswith('chirp'):
                            s2, pilotErrorDiag2 = chirp_put_data(pfn, ddm_storage, dsname=dsname, sitename=sitename,\
                                                         analysisJob=analysisJob, testLevel=testLevel, pinitdir=pinitdir,\
                                                         proxycheck=proxycheck, token=_token_file, timeout=DEFAULT_TIMEOUT, lfn=lfn,\
                                                         guid=guid, spsetup=spsetup, userid=userid, report=report,\
                                                         prodSourceLabel=prodSourceLabel, outputDir=outputDir, DN=DN,\
                                                         dispatchDBlockTokenForOut=dDBlockTokenForOut, logFile=logFile, experiment=experiment)
                    except Exception, e:
                        tolog("!!WARNING!!2998!! Exception caught in mover chirp put: %s" % str(e))
                    else:
                        if s2:
                            tolog("Mover chirp put finished successfully")
                        elif s2 == None:
                            tolog("Skipped chirp transfer")
                        else:
                            tolog("chirp transfer returned exit code: %d" % (s2))
                else:
                    tolog("dispatchDBlockTokenForOut not set (no chirp transfer)")

            tolog("Return code: %d" % (s))

        if s != 0:
            tolog('!!WARNING!!2999!! Failed to transfer %s: %s (%s)' % (os.path.basename(pfn), s, error.getErrorStr(s)))
            fail = s 
            break

        # gpfn = ftpserv + destpfn
        # keep track of which files have been copied
        fields[0] += '%s+' % r_gpfn    
        fields[1] += '%s+' % lfn
        fields[2] += '%s+' % guid
        fields[3] += '%s+' % r_fsize
        fields[4] += '%s+' % r_fchecksum
        fields[5] += '%s+' % r_farch
        fields[6] += '%s+' % pfn

        if fail == 0:
            # should the file be registered in LFC [in the US]?
            _copytool = readpar("copytool")
            if (region == "US" or _copytool == "dq2" or (region == "CERN" and _copytool == "xrdcp")) \
                   and lfc_host != "" and _copytool != 'lcg-cp' and _copytool != 'lcgcp' and _copytool != "mv" and lfcreg:
                # update the current file state since the transfer was ok
                updateFileState(filename, workDir, jobId, mode="file_state", state="transferred")
                dumpFileStates(workDir, jobId)

                # perform LFC registration for the current file
                ec, pilotErrorDiag = sitemover.registerFileLFC(analysisJob, ddm_storage, dsname, lfn, r_gpfn, guid, r_fchecksum, r_fsize)
                if ec != 0:
                    return ec, pilotErrorDiag, fields, None, N_filesNormalStageOut, N_filesAltStageOut
                else:
                    # update the current file state
                    updateFileState(filename, workDir, jobId, mode="reg_state", state="registered")
                    dumpFileStates(workDir, jobId)
                    tolog("LFC registration finished")
            else:
                # update the current file states
                updateFileState(filename, workDir, jobId, mode="file_state", state="transferred")
                if _copytool != "chirp":
                    updateFileState(filename, workDir, jobId, mode="reg_state", state="registered")
                dumpFileStates(workDir, jobId)
                tolog("No LFC registration by the pilot")

    if fail != 0:
        return fail, pilotErrorDiag, fields, None, N_filesNormalStageOut, N_filesAltStageOut

    # remove the trailing '+'-sign in each field
    fields[0] = fields[0][:-1]
    fields[1] = fields[1][:-1]
    fields[2] = fields[2][:-1]
    fields[3] = fields[3][:-1]
    fields[4] = fields[4][:-1]
    fields[5] = fields[5][:-1]

    tolog("Put successful")
    return 0, pilotErrorDiag, fields, '1', N_filesNormalStageOut, N_filesAltStageOut

def getSpaceTokenList(token, listSEs, jobCloud, analysisJob, fileListLength, si, alt=False):
    """ Get the list of space tokens """

    # check if there are any space tokens
    if token == [''] and fileListLength > 1:
        # correct length of empty list
        token = token*fileListLength
        tolog("Corrected length of empty space token list from 0 to %d" % len(token))

    # no space tokens for tier 3s
    if not si.isTier3():
        token_list = getProperSpaceTokenList(token, listSEs, jobCloud, analysisJob, alt=alt)
    else:
        token_list = None

    return token_list

def getProperSpaceTokenList(spacetokenlist, listSEs, jobCloud, analysisJob, alt=False):
    """ return a proper space token list and perform space token verification """
    # a space token list from destinationDBlockToken (spacetokenlist) will always have length N >= 2
    # token[0 - N-2] is for output files while token[N-1] is for log files.
    # N is the number of files to transfer (used to ensure that the returned token list has the
    # same number of items as there are files, in case space token is read from schedconfig.se).
    # space token verification will be performed for non-NULL values in spacetokenlist.
    # alt option is used during stage-out to alternative SE to distinguish the two queuedata files

#    _type = str(token.__class__)
#    if _type.find('list') >= 0:

    from SiteMover import SiteMover
    properspacetokenlist = None

    tolog("Initial space token list:")
    dumpOrderedItems(spacetokenlist)

    # first check if there's a space token defined in schedconfig.se
    # and make it default (if available)
    _defaulttoken, _dummypath = SiteMover.extractSE(listSEs[0])
    if _defaulttoken:
        tolog("Found default space token in schedconfig.se: %s" % (_defaulttoken))

    # check if destinationDBlockToken contains valid space tokens
    # and replace any NULL values with the default
    # if there is no default space token, then keep the NULL value
    # (will be skipped later)
    foundnonnull = False
    length = len(spacetokenlist)
    if length > 0:
        # replace NULL values if needed
        for t in range(length):
            # first check output file space tokens
            if (spacetokenlist[t].upper() == 'NULL' or spacetokenlist[t] == '') and t < length-1:
                if _defaulttoken:
                    # replace NULL value with default output space token
                    spacetokenlist[t] = _defaulttoken
                    tolog("Replaced NULL value with default space token (%s) for output file %d" % (spacetokenlist[t], t+1))
            elif (spacetokenlist[t].upper() == 'NULL' or spacetokenlist[t] == '') and t == length-1:
                if _defaulttoken:
                    # replace NULL value with default log space token
                    spacetokenlist[t] = _defaulttoken
                    tolog("Replaced NULL value with default space token (%s) for log file" % (spacetokenlist[t]))
            elif spacetokenlist[t].upper() != 'NULL':
                # used below in the verification step
                foundnonnull = True
        # create the proper space token list
        properspacetokenlist = spacetokenlist
    else:
        # no token list from destinationDBlockToken, create one from schedconfig.se if possible
        if _defaulttoken:
            # token = [output space tokens] + [log space token] (currently the same) 
            properspacetokenlist = [_defaulttoken]*(length-1) + [_defaulttoken]
        else:
            # default to the original NULL list (will be skipped later)
            properspacetokenlist = spacetokenlist

    # Now verify the space tokens (only if setokens is set)
    setokens = readpar("setokens", alt=alt)
    try:
        cloud = readpar("cloud", alt=alt).split(",")[0]
    except Exception, e:
        tolog("!!WARNING!!2991!! No cloud field in queuedata: %s" % str(e))
        cloud = jobCloud

    if setokens != "" and setokens != None: 
        for i in range(len(properspacetokenlist)):
            if not verifySpaceToken(properspacetokenlist[i], setokens) or cloud != jobCloud:
                __defaulttoken = _defaulttoken
                if properspacetokenlist[i] == "":
                    tolog("!!WARNING!!2999!! Space token not set (reset to NULL)")
                else:
                    if cloud == jobCloud:
                        if _defaulttoken:
                            tolog("!!WARNING!!2999!! Space token %s is not allowed (reset to %s)" % (properspacetokenlist[i], _defaulttoken))
                        else:
                            tolog("!!WARNING!!2999!! Space token %s is not allowed (reset to NULL)" % (properspacetokenlist[i]))
                    else:
                        if not analysisJob:

                            # if this line is to be removed: should new schedconfig.alttoken be created and used instead?

                            __defaulttoken = "ATLASPRODDISK"
                            tolog("Note: schedconfig.cloud != job.cloud: Space token set to %s" % (__defaulttoken))
                        else:
                            tolog("schedconfig.cloud = %s" % str(cloud))
                            tolog("job.cloud = %s" % str(jobCloud))
                if __defaulttoken:
                    properspacetokenlist[i] = __defaulttoken
                else:
                    properspacetokenlist[i] = "NULL"
    else:
        # does the properspacetokenlist have non-NULL values?
        if foundnonnull:
            # reset the list since the space tokens can't be verified against setokens
            # or change it to the default space token if set
            for i in range(len(properspacetokenlist)):
                if properspacetokenlist[i].upper() != "NULL":
                    if _defaulttoken:
                        if properspacetokenlist[i].upper() == _defaulttoken:
                            tolog("!!WARNING!!2999!! Empty schedconfig.setokens (must be set for space token verification)")
                            tolog("!!WARNING!!2999!! but requested space token (%s) is same as default space token" %\
                                  (properspacetokenlist[i]))
                        else:
                            tolog("!!WARNING!!2999!! Resetting space token %s to default space token %s since it can not be verified (empty schedconfig.setokens)" %\
                                  (properspacetokenlist[i], _defaulttoken))
                        properspacetokenlist[i] = _defaulttoken
                    else:
                        tolog("!!WARNING!!2999!! Resetting space token %s to NULL since it can not be verified (empty schedconfig.setokens)" %\
                              (properspacetokenlist[i]))
                        properspacetokenlist[i] = "NULL"
        else:
            tolog("Skipping space token verification since setokens not set")
    tolog("Proper space token list:")
    dumpOrderedItems(properspacetokenlist)

    return properspacetokenlist

def verifySpaceToken(spacetoken, setokens):
    """ verify space token against setokens list """

    status = False

    setokenslist = setokens.split(",")
    if spacetoken in setokenslist:
        tolog("Verified space token: %s" % (spacetoken))
        status = True
    else:
        if spacetoken == "":
            tolog("Warning: ended up with empty space token")
        else:
            tolog("Warning: Space token %s is not among allowed values: %s" % (spacetoken, str(setokenslist)))

    return status

def getDDMStorage(ub, analysisJob, region):
    """ return the DDM storage (http version) """

    pilotErrorDiag = ""
    useHTTP = True
    ddm_storage = ""

    # skip this function unless we are running in the US or on NG
    if not (region == 'US' or region == 'Nordugrid'):
        return ddm_storage, pilotErrorDiag

    # get the storage paths
    try:
        ddm_storage = getDestinationDDMStorage(analysisJob)
    except Exception, e:
        pilotErrorDiag = "Exception thrown in put function: %s" % str(e)
        tolog("!!WARNING!!2999!! %s" % (pilotErrorDiag))
        return "", pilotErrorDiag
    else:
        tolog("Got ddm_storage from queuedata file: %s" % (ddm_storage))

    # use HTTP I/F to retrieve storage path
    # check if storage path has a proper protocol
    if not ddm_storage.startswith('http://'):
        useHTTP = False

    if useHTTP and ub == "":
        useHTTP = False

    if useHTTP:
        try:
            if ub[-1] == "/":
                # prevent a double slash
                url = ub + 'storages/default'
            else:
                url = ub + '/storages/default'
            tolog("Trying urlopen with: %s" % (url))
            f = urllib.urlopen(url)
        except Exception, e:
            pilotErrorDiag = "Connection to DDM http server failed (%s)" % (get_exc_short())
            tolog("!!WARNING!!2999!! %s" % (pilotErrorDiag))
            return "", pilotErrorDiag
        else:
            ret = f.read()
            if ret.find('HTML') >= 0:
                # most likely returned html with 'The requested URL could not be retrieved'
                # ret should be on the form srm://iut2-dc1.iu.edu/pnfs/iu.edu/data/ddm1/ or similar (other protocol)
                pilotErrorDiag = "Fetching default storage failed: %s" % (ret)
                tolog("!!WARNING!!2999!! %s" % (pilotErrorDiag))
                return "", pilotErrorDiag
            else:
                tolog('Fetched default storage (%d bytes) from url: %s' % (len(ret), url))

        # Entry returned by DDM is supposed to be an URL (of the form method://host:port/full_path_to_se/ )
        ddm_storage = ret.strip()

    tolog("Put function using storage: %s" % (ddm_storage))

    return ddm_storage, pilotErrorDiag

def getDestinationDDMStorage(analysisJob):
    """ return the DDM storage, i.e. the storage destination path """

    # use static info if they are defined
    seName = readpar('se')

    # remove any secondary SE's
    if seName.find(",") > 0:
        try:
            # Remember that se can be a list where the first is used for output but any can be used for input
            seNameTmp = seName.split(",")[0]
        except Exception, e:
            tolog("!!WARNING!!2999!! Could not separate SE in string: %s, %s" % (seName, e))
        else:
            tolog("Extracted primary SE from list: %s (%s)" % (seNameTmp, seName))
            seName = seNameTmp

    # separate se info from lrc info (US)
    if seName.find("^") > 0:
        seNameTmpList = seName.split("^")
        # ddm info will be in the second list item, space token info in the first
        seName = seNameTmpList[1]
        tolog("Selected seName=%s from list %s" % (seName, str(seNameTmpList)))

    # get rid off any space tokens if present
    from SiteMover import SiteMover
    _dummytoken, seName = SiteMover.extractSE(seName)
    if analysisJob:
        # convert seprodpath to a dirlist since it might have a complex structure
        seList = SiteMover.getDirList(readpar('sepath'))
    else:
        # note that seprodpath can be a list if space tokens are used
        # if that is the case, the select the proper seprodpath.
        # also note that the returned path is only preliminary
        # if space tokens are used, then the final path should be set
        # in the site mover since the space token might depend on the file

        # convert seprodpath to a dirlist since it might have a complex structure
        seList = SiteMover.getDirList(readpar('seprodpath'))
        if seList[0] == "":
            seList = SiteMover.getDirList(readpar('sepath'))

    # use the first seprodpath for now (should be set properly in the site mover)
    sePath = seList[0]

    if seName != "" and sePath != "":        
        if seName[-1] == "/" and sePath[0] == "/":
            # prevent paths from containing double slashes
            path = seName[:-1] + sePath
        else:
            path = seName + sePath
    else:
        path = seName + sePath
    tolog("getDDMStorage() will return: %s" % (path))
    return path

def getReplicasDictionary(replicas_list, lfn_dict):
    """ Create a dictionary of the guids and replica objects """

    error = PilotErrors()
    pilotErrorDiag = ""
    ec = 0
    replicas_dic = {}

    # replicas_list is a linear list of all guids and sfns
    for _replica in replicas_list:
        guid = _replica.guid
        rep = replica()
        rep.sfn = _replica.sfn
        rep.filesize = _replica.filesize
        rep.csumvalue = _replica.csumvalue

        # empty sfn's should only happen if there was a timeout in the lfc_getreplicas call, which in
        # turn could be caused by a large distance between the client and server, or if there were many
        # guids in the batch call. should be fixed in later LFC server versions (from 1.7.0)
        if rep.sfn == "":
            if lfn_dict.has_key(guid):
                filename = lfn_dict[guid]
            else:
                tolog("No such guid, continue")
                continue
            if "DBRelease" in filename:
                ec = error.ERR_DBRELNOTYETTRANSFERRED
                pilotErrorDiag = "%s has not been transferred yet (%s)" % (filename, guid)
            else:
                ec = error.ERR_NOLFCSFN
                pilotErrorDiag = "SFN not set in LFC for guid %s (%s, LFC entry erased or file not yet transferred)" % (guid, filename)
            break

        # setname and fs might not exist
        try:
            rep.setname = _replica.setname
        except:
            rep.setname = None
        try:
            rep.fs = _replica.fs
        except:
            rep.fs = None

        # add the replica object to the dictionary for the corresponding guid
        if replicas_dic.has_key(guid):
            replicas_dic[guid].append(rep)
        else:
            replicas_dic[guid] = [rep]

    return ec, pilotErrorDiag, replicas_dic

def getMatchedReplicasFileName(workdir):
    """ return the matched replicas file name """

    return os.path.join(workdir, "matched_replicas.%s" % (getExtension()))

def storeMatchedReplicas(guid, matched_replicas, workdir):
    """ store the matched replicas to file for a given guid in case the primary replica might not be available """

    fileName = getMatchedReplicasFileName(workdir)

    # start by reading back the file content if it already exists
    # (empty dictionary in case of failure or for initial creation)
    replica_dictionary = getReplicaDictionary(workdir)

    # add matched replicas to the replica dictionary
    replica_dictionary[guid] = matched_replicas

    # store the matched replicas
    try:
        # overwrite any existing file
        f = open(fileName, "w")
    except Exception, e:
        tolog("!!WARNING!!1001!! Could not open file: %s, %s" % (fileName, str(e)))
    else:
        # is the file a pickle or a json file?
        if fileName.endswith('json'):
            from json import dump
        else:
            from pickle import dump

        # dump the dictionary to file
        try:
            dump(replica_dictionary, f)
        except Exception, e:
            tolog("!!WARNING!!1001!! Could not dump replica dictionary to file: %s" % str(e))
        else:
            tolog("Successfully dumped replica dictionary to file %s for guid: %s" % (fileName, guid))

        # done with the file
        f.close()

def getReplicaDictionary(workdir):
    """ Get the replica dictionary from file (used when the primary replica can not be staged due to some temporary error) """

    fileName = getMatchedReplicasFileName(workdir)

    if os.path.exists(fileName):
        # matched replicas dictionary file already exists, read it back
        try:
            f = open(fileName, "r")
        except Exception, e:
            tolog("!!WARNING!!1001!! Could not open file: %s, %s" % (fileName, str(e)))
            replica_dictionary = {}
        else:
            # is the file a pickle or a json file?
            if fileName.endswith('json'):
                from json import load
            else:
                from pickle import load
            try:
                # load the dictionary
                replica_dictionary = load(f)
            except Exception, e:
                tolog("!!WARNING!!1001!! Could not read back replica dictionary: %s" % str(e))
                replica_dictionary = {}
            else:
                # tolog("Successfully read back replica dictionary containing %d key(s)" % len(replica_dictionary.keys()))
                pass

            # done with the file
            f.close()
    else:
        tolog("Creating initial replica dictionary")
        replica_dictionary = {}

    return replica_dictionary

def foundMatchedCopyprefixReplica(sfn, pfroms, ptos):
    """ Match replicas using copyprefix """

    found_match = False

    # loop over all possible pfroms and ptos
    for (pfrom, pto) in map(None, pfroms, ptos):
        if pfrom:
            if sfn[:len(pfrom)] == pfrom:
                tolog("Copyprefix matched replica %s (SURL) using pfrom" % (sfn))
                found_match = True
                break
            else:
                tolog("Did not find it there either (pfrom)")
        if pto and not found_match:
            if sfn[:len(pto)] == pto:
                tolog("Copyprefix matched replica %s (SURL) using pto" % (sfn))
                found_match = True
                break
            else:
                tolog("Did not find it there either (pto)")

    return found_match

def getLFCFileList(guid_token_dict, lfchost, analysisJob, workdir, lfn_dict=None):
    """ Build the LFC file list using lfc_getreplicas """

    from SiteMover import SiteMover

    ec = 0
    pilotErrorDiag = ""
    xml_source = "LFC"
    file_dict = {}
    replicas_dict = None

    error = PilotErrors()

    try:
        import lfc
    except Exception, e:
        pilotErrorDiag = "getLFCFileList() could not import lfc module: %s" % str(e)
        return error.ERR_GETLFCIMPORT, pilotErrorDiag, file_dict, xml_source, replicas_dict

    tolog("Successfully imported LFC module")
    tolog("Get function using LFC_HOST: %s" % (lfchost))

    os.environ['LFC_HOST'] = lfchost
    os.environ['LFC_CONNTIMEOUT'] = '60'
    os.environ['LFC_CONRETRY'] = '2'
    os.environ['LFC_CONRETRYINT'] = '60'

    copyprefix = readpar('copyprefixin')
    if copyprefix == "":
        tolog("copyprefixin is not set, trying to use copyprefix instead")
        copyprefix = readpar('copyprefix')
    
    if copyprefix == "":
        pilotErrorDiag = "Get data failed: copyprefix not set"
        tolog("!!FAILED!!2999!! %s" % (pilotErrorDiag))
        tolog("Site mover get_data finished (failed)")
        return error.ERR_STAGEINFAILED, pilotErrorDiag, file_dict, xml_source, replicas_dict
    else:
        tolog("Read copyprefix: %s" % (copyprefix))

    # Remember that se can be a list where the first is used for output but any can be used for input
    listSEs = readpar('se').split(',')
    tolog("SE list:")
    dumpOrderedItems(listSEs)

    # strip listSEs from space token info if needed
    _listSEs = SiteMover.stripListSEs(listSEs)
    if listSEs != _listSEs:
        tolog("SE list has been stripped to:")
        dumpOrderedItems(_listSEs)
    listSEs = _listSEs

    # add seprodpaths to all listSESs entries (for production jobs)
    if not analysisJob:
        listSEs = SiteMover.addPaths(listSEs)

    # get the guids list
    guids = guid_token_dict.keys()

    # get the replicas list for all guids
    try:
        ec, pilotErrorDiag, replicas_list = getReplicas(guids)
    except Exception, e:
        pilotErrorDiag = "getReplicas threw an exception: %s" % str(e)
        tolog("!!FAILED!!2999!! %s" % (pilotErrorDiag))
        ec = -1

    if ec != 0:
        return ec, pilotErrorDiag, file_dict, xml_source, replicas_dict
    else:
        # the replicas_list is a condense list containing all guids and all sfns, one after the other.
        # there might be several identical guids followed by the corresponding sfns. we will not reformat
        # this list into a sorted dictionary
        # replicas_dict[guid] = [rep1, rep2] where repN is an object of class replica (defined above)
        try:
            ec, pilotErrorDiag, replicas_dict = getReplicasDictionary(replicas_list, lfn_dict)
        except Exception, e:
            pilotErrorDiag = "%s" % str(e)
            tolog("!!FAILED!!2999!! %s" % (pilotErrorDiag))
            return error.ERR_FAILEDLFCGETREP, pilotErrorDiag, file_dict, xml_source, replicas_dict
        else:
            if ec != 0:
                tolog("!!FAILED!!2999!! %s" % (pilotErrorDiag))
                return ec, pilotErrorDiag, file_dict, xml_source, replicas_dict

        # handle copyprefix lists
        pfroms, ptos = getCopyprefixLists(copyprefix)
        tolog("Copyprefix lists: %s, %s" % (str(pfroms), str(ptos)))

        # loop over all guids and correct empty values
        for guid in replicas_dict.keys():
            tolog("Processing guid %s" % (guid))

            # get the space token if available
            if guid_token_dict:
                if guid_token_dict[guid] == "" or guid_token_dict[guid] == "NULL" or guid_token_dict[guid] == None:
                    _token = None
                else:
                    _token = guid_token_dict[guid]
            else:
                _token = None
            if _token:
                tolog("Will try to match token: %s" % (_token))
                # if there is a token, pull forward the corresponding area in the se_list
                _listSEs = SiteMover.pullMatchedAreas(listSEs, _token.lower())
                tolog("Re-sorted list of SEs for area: %s" % (_token.lower()))
            else:
                tolog("No space token matching")
                _listSEs = listSEs

            # put any tape resident replicas at the end of the list unless requested with the dispatchDBlockToken
            replicas = replicas_dict[guid]
            if len(replicas) > 1 or _token:
                replicas = sortReplicas(replicas, _token)
            tolog("Sorted replica list:")
            _j = 0
            for replica in replicas:
                _j += 1
                tolog("%d. %s (fsize: %s fchecksum: %s)" % (_j, replica.sfn, replica.filesize, replica.csumvalue))

            # find the replica at the correct host
            matched_replicas = []
            DBRelease = False

            # now loop over the replicas list
            for replica in replicas:
                sfn = replica.sfn
                tolog("Checking replica %s for guid %s" % (sfn, guid))
                if "DBRelease" in sfn:
                    # either all replicas in the list are DBReleases or none of them
                    DBRelease = True

                # for dCache systems the PNFSID and ToA end point is/can be stored in replica.setname/fs
                # in those cases the PNFSID should be used instead of the filename
                PNFSID = replica.setname
                if PNFSID == "":
                    PNFSID = None
                elif PNFSID:
                    tolog("Found PNFSID: %s" % (PNFSID))
                # TOAENDPOINT = replica.fs
                # if TOAENDPOINT == "":
                #     TOAENDPOINT = None
                # elif TOAENDPOINT:
                #    tolog("Found TOAENDPOINT: %s" % (TOAENDPOINT))

                #if PNFSID:
                # # shuffle between dcdcap02/03?
                #    sfn = "pnfs://dcdcap02.usatlas.bnl.gov:22125/%s" % (PNFSID)
                #    tolog("Using PNFSID replica: %s" % (sfn))
                #    matched_replicas.append(sfn)
                #    break

                # SURL: e.g. replica.sfn =
                # srm://ccsrm.in2p3.fr/pnfs/in2p3.fr/data/atlas/disk/dq2/misal1_mc13.009036/
                # misal1_mc13.009036.PythiattH190WW3l3v4j_neglep_H1T2.simul.HITS.v12000605_tid018721_sub01129238/
                # RDO.018721._00117.pool.root.2
                # replica host = ccsrm.in2p3.fr
                # copyprefix = dcap://ccdcache.in2p3.fr:22125^srm://ccsrm.in2p3.fr
                #
                # SRMv2 sites should still have defined copyprefix. replica.sfn's will never be on the form
                # srm://gridka-dcache.fzk.de:8443/srm/managerv2?SFN=/pnfs/gridka.de/atlas/disk-only/mc/simone_test/test.1
                # since they are already stripped of the port and version info

                tolog("Checking list of SEs")
                found = False
                for se in _listSEs:
                    if sfn[:len(se)] == se:
                        tolog("Found matched replica: %s at %s" % (sfn, se))
                        matched_replicas.append(sfn)
                        found = True
                if not found:
                    tolog("Could not find any matching se, try to use copyprefix instead")
                    if foundMatchedCopyprefixReplica(sfn, pfroms, ptos):
                        matched_replicas.append(sfn)
                    else:
                        tolog("Found no matched replicas using copyprefix")

            if len(matched_replicas) > 0:
                # remove any duplicates
                matched_replicas = removeDuplicates(matched_replicas)

                tolog("Matched replica list:")
                dumpOrderedItems(matched_replicas)

                if DBRelease and not _token:
                    # randomize DBRelease replicas residing on disk
                    matched_replicas = randomizeReplicas(matched_replicas)
                    tolog("Randomized replica list:")
                    dumpOrderedItems(matched_replicas)

                # always select the first match which might be from the randomized list (DBRelease files only)
                file_dict[guid] = matched_replicas[0]
                tolog("Will use primary replica: %s" % file_dict[guid])

                # backup the matched replicas to file since the primary replica might not be available
                # in that case, switch to the secondary replica, and so on
                storeMatchedReplicas(guid, matched_replicas, workdir)

            else:
                if lfn_dict.has_key(guid):
                    if "DBRelease" in lfn_dict[guid]:
                        pilotErrorCode = "%s has not been transferred yet (guid=%s)" % (lfn_dict[guid], guid)
                        ec = error.ERR_DBRELNOTYETTRANSFERRED
                    else:
                        if (readpar('copytool').lower() == "fax" and readpar('copytoolin') == "") or readpar('copytoolin').lower() == "fax":
                            # special use case for FAX; the file might not be available locally, so do not fail here because no local copy can be found
                            # use a 'fake' replica for now, ie use the last known SURL, irrelevant anyway since the SURL will eventually come from FAX
                            file_dict[guid] = sfn
                            tolog("Will use SURL=%s for the replica dictionary (will be overwritten later by FAX once it is known)" % (sfn))
                            matched_replicas.append(sfn)
                            matched_replicas = removeDuplicates(matched_replicas)
                            storeMatchedReplicas(guid, matched_replicas, workdir)
                            ec = 0
                        else:
                            pilotErrorDiag = "(1) Replica with guid %s not found at %s (or in the se list: %s)" % (guid, copyprefix, str(_listSEs))
                            ec = error.ERR_REPNOTFOUND
                else:
                    pilotErrorDiag = "(2) Replica with guid %s not found at %s (or in the se list: %s)" % (guid, copyprefix, str(_listSEs))
                    ec = error.ERR_REPNOTFOUND
                tolog("!!FAILED!!2999!! %s" % (pilotErrorDiag))
                tolog("Mover getLFCFileList finished (failed): replica not found")
                return ec, pilotErrorDiag, file_dict, xml_source, replicas_dict

    return ec, pilotErrorDiag, file_dict, xml_source, replicas_dict

def getRucioFileList(scope_dict, guid_token_dict, lfn_dict, analysisJob, sitemover):
    """ Building the file list using scope information """

    ec = 0
    pilotErrorDiag = ""
    xml_source = "Rucio"
    file_dict = {}
    replicas_dict = {}

    error = PilotErrors()

    # Formats (input):
    # scope_dict = { "lfn1": "scope1", .. }
    # guid_token_dict = { "guid1": "spacetoken1", .. }
    # lfn_dict = { "guid1": "lfn1", .. }
    # Formats (output):
    # file_dict = { "guid1": "pfn1", .. }
    # replicas_dict = { "guid1": ["pfn-rep1"], .. }
    # Note: since a predeterministic path is used, and no LFC file lookup, there is only one replica per guid in the replicas dictionary,
    # i.e. the replicas dictionary (replicas_dict) can be constructed from the file dictionary (file_dict)

    # Get the guids list and loop over it
    guid_list = guid_token_dict.keys()
    tolog("file_dict=%s"%str(file_dict))
    tolog("replicas_dict=%s"%str(replicas_dict))
    tolog("guid_list=%s"%str(guid_list))
    for guid in guid_list:
        try:
            # Get the LFN
            lfn = lfn_dict[guid]

            # Get the scope
            scope = scope_dict[lfn]

            # Get the space token descriptor (if any)
            spacetoken = guid_token_dict[guid]

            # Construct the PFN
            pfn = sitemover.getFullPath(scope, spacetoken, lfn, analysisJob, "")
        except Exception, e:
            ec = error.ERR_NOPFC
            pilotErrorDiag = "Exception caught while trying to build Rucio based file dictionaries: %s" % (e)
            tolog("!!WARNING!!2332!! %s" % (pilotErrorDiag))
        else:
            # Build the dictionaries
            tolog("guid=%s" % str(guid))
            try:
                file_dict[guid] = pfn
            except Exception, e:
                tolog("1:%s" % (e))
            try:
                replicas_dict[guid] = [pfn]
            except Exception, e:
                tolog("2:%s" % (e))

    tolog("done xxx")
    return ec, pilotErrorDiag, file_dict, xml_source, replicas_dict

def createFileDictionaries(guids, lfns, tokens, DBReleaseIsAvailable, dbh):
    """ Create proper file dictionaries """
    # do not include the DBRelease if not needed

    # lfn dictionary with guids as keys
    lfn_dict = dict(zip(guids, lfns))

    # token dictionary with guids as keys
    if len(guids) == len(tokens):
        guid_token_dict = dict(zip(guids, tokens))
    else:
        guid_token_dict = {}
        if len(tokens) > 1:
            tolog("Warning: len(guids) = %d ne len(tokens) = %d, guids = %s, tokens = %s" %\
                  (len(guids), len(tokens), str(guids), str(tokens)))

    # should the DBRelease file be removed?
    if DBReleaseIsAvailable:
        for guid in guids:
            if lfn_dict.has_key(guid):
                lfn = lfn_dict[guid]
                if isDBReleaseFile(dbh, lfn):
                    # remove the DBRelease file from the dictionaries
                    del lfn_dict[guid]
                    del guid_token_dict[guid]
                    tolog("Removed locally available DBRelease file %s / %s from file dictionaries" % (lfn, guid))
                    # no break here since the file list can contain duplicate DBRelease
            else:
                tolog("Encountered deleted key: %s" % (guid))
    return lfn_dict, guid_token_dict

def getPoolFileCatalog(lfchost, ub, guids, dsname, lfns, pinitdir, analysisJob, tokens, workdir, dbh, DBReleaseIsAvailable, scope_dict, sitemover,\
                       pfc_name="PoolFileCatalog.xml", thisExperiment=None):
    """ get the PFC from the proper source """
    # In LCG land use lfc_getreplica to get the pfn in order to create the PoolFileCatalog, unless scope is used to create predeterministic path

    xml_from_PFC = ""
    pilotErrorDiag = ""
    ec = 0
    replicas_dic = None

    xml_source = "[undefined]"
    region = readpar('region')
    tolog("Guids:")
    dumpOrderedItems(guids)

    # get proper file dictionaries (do not include the DBRelease if not needed)
    lfn_dict, guid_token_dict = createFileDictionaries(guids, lfns, tokens, DBReleaseIsAvailable, dbh)

    # update booleans if Rucio is used and scope dictionary is set
    if scope_dict and readpar('userucio').lower() == "true":
#    if scope_dict:
        tolog("Resetting file lookup boolean (LFC will not be queried)")
        use_rucio = True
        thisExperiment.doFileLookups(False)
    else:
        use_rucio = False
        thisExperiment.doFileLookups(True)

    if thisExperiment.willDoFileLookups():
        # get the replica dictionary from the LFC
        ec, pilotErrorDiag, file_dic, xml_source, replicas_dic = getLFCFileList(guid_token_dict, lfchost, analysisJob, workdir, lfn_dict=lfn_dict)
        if ec != 0:
            return ec, pilotErrorDiag, xml_from_PFC, xml_source, replicas_dic
        tolog("file_dic = %s" % str(file_dic))

        # create a pool file catalog
        xml_from_PFC = createPoolFileCatalog(file_dic, pfc_name=pfc_name)
    elif use_rucio:
        tolog("Replica dictionaries will be prepared for Rucio")
        # get the replica dictionary etc using predeterministic paths method
        ec, pilotErrorDiag, file_dic, xml_source, replicas_dic = getRucioFileList(scope_dict, guid_token_dict, lfn_dict, analysisJob, sitemover)
        if ec != 0:
            return ec, pilotErrorDiag, xml_from_PFC, xml_source, replicas_dic
        tolog("file_dic = %s" % str(file_dic))

        # create a pool file catalog
        xml_from_PFC = createPoolFileCatalog(file_dic, pfc_name=pfc_name)

    if xml_from_PFC == '' and region != 'Nordugrid':
        # fetch the input file xml from the dq2 server
        xml_from_PFC, xml_source = getPoolFileCatalogDQ2(ub, guids)

    if region == 'Nordugrid' or region == 'testregion':
        # build a new PFC for NG
        ec, pilotErrorDiag, xml_from_PFC, xml_source = getPoolFileCatalogNG(guids, lfns, pinitdir)

    return ec, pilotErrorDiag, xml_from_PFC, xml_source, replicas_dic

def getPoolFileCatalogNG(guids, lfns, pinitdir):
    """ build a new PFC for NG """

    xml_from_PFC = ""
    pilotErrorDiag = ""
    error = PilotErrors()
    xml_source = "NG Panda server"
    ec = 0
    if guids and lfns:
        file_dic = {}
        lfn_id = 0
        # build the file list
        for guid in guids:
            if guid == "NULL" or guid == None or guid == "None" or guid == "":
                pilotErrorDiag = "Guid (%s) was not provided by server for file %s" % (str(guid), lfns[lfn_id])
                tolog("!!FAILED!!2999!! %s" % (pilotErrorDiag))
                return error.ERR_NOPFC, pilotErrorDiag, xml_from_PFC, xml_source
            file_dic[guid] = os.path.join(pinitdir, lfns[lfn_id])
            lfn_id += 1
        tolog("File list generated with %d entries" % len(file_dic))

        # create a pool file catalog
        xml_from_PFC = createPoolFileCatalog(file_dic)
    else:
        pilotErrorDiag = "Guids were not provided by server"
        tolog("!!FAILED!!2999!! %s" % (pilotErrorDiag))
        ec = error.ERR_NOPFC

    return ec, pilotErrorDiag, xml_from_PFC, xml_source

def getPoolFileCatalogDQ2(baseURL, guids):
    """
    Returns a PoolFileCatalog XML file.
    If the connection to the DQ2 server (LRC http wrapper) fails or
    the server returns a malformed output, an error message is printed
    and and empty PFC is returned
    If some files are missing, a PFC with the other files is
    returned and no error is printed or risen (this is coherent with
    the LRC wrapper behavior).
    This list list should be checked by the caller to see if some
    of the LFNs are missing
    baseURL - URL of the LRC http wrapper
    guids - list of the desired GUIDs

    This function is actually writing on disk and then removing a file
    called PoolFileCatalog.xml
    Beware, existing files with that name may be removed and interfere
    with this function output.
    """

    xml_source = "DQ2"

    # get PoolFileCatalog
    iGUID = 0
    xml_from_PFC =''    

    # In LCG land use dq2_poolFCjobO
    region = readpar('region')
    if region != 'US' and region != 'Nordugrid':
        tolog("!!FAILED!!2999!! Can not get PFC with LRC method for region %s" % (region))
        return '', xml_source

    strGUIDs = 'guids='
    # if no site service
    if baseURL == '':
        tolog('!!FAILED!!2999!! DQ2 URL not set')
        return xml_from_PFC, xml_source
    tolog("Number of GUIDs: %d" % len(guids))
    for guid in guids:
        iGUID += 1
        # make argument
        strGUIDs += '%s+' % guid
        if iGUID % 40 == 0 or iGUID == len(guids):
            # get PoolFileCatalog
            strGUIDs = strGUIDs[:-1]
            try:
                f = urllib.urlopen( baseURL+'/lrc/PoolFileCatalog?'+strGUIDs )
            except:
                tolog('!!FAILED!!2999!! Error opening DDM URL (%s)' % (get_exc_short()))
                return xml_from_PFC, xml_source
            ret = f.read()
            if ret.find('POOLFILECATALOG') == -1:
                tolog('!!FAILED!!2999!! Getting POOL FileCatalog failed: could not find the file/s in LRC!')
                tolog('Error returned from LRC: %s' % (ret))
                continue
            # append
            xml_from_PFC += ret
            strGUIDs = 'guids='
    # remove redundant trailer and header
    th = \
"""
</POOLFILECATALOG>
<\?xml version="1.0" encoding="UTF-8" standalone="no" \?>
<!-- Edited By POOL -->
<!DOCTYPE POOLFILECATALOG SYSTEM "InMemory">
<POOLFILECATALOG>

\s*<META name="fsize" type="string"/>

\s*<META name="md5sum" type="string"/>

\s*<META name="lastmodified" type="string"/>

\s*<META name="archival" type="string"/>
"""
    xml_from_PFC = re.sub(th, '', xml_from_PFC)
    # return XML
    return xml_from_PFC, xml_source

def checkLocalSE(analyJob):
    """ Make sure that the local SE is responding """

    status = False

    # Select the correct mover
    (copycmd, setup) = copytool()

    tolog("Copy command: %s" % (copycmd))
    tolog("Setup: %s" % (setup))
    sitemover = getSiteMover(copycmd, setup)
    tolog("Got site mover: %s" % str(sitemover))
    tolog("Checking local SE...")

    # move code to site mover, SiteMover to contain default function returning "NotSupported" message

    # determine which timeout option to use
    timeout = 120
    if sitemover.isNewLCGVersion("%s lcg-ls" % (setup)):
        timeout_option = "--connect-timeout=300 --sendreceive-timeout=%d" % (timeout)
    else:
        timeout_option = "-t %d" % (timeout)

    _se = readpar('se').split(",")[0]
    token, se = sitemover.extractSE(_se)

    # build a proper path
    if analyJob:
        sepath = sitemover.filterSE(readpar('sepath'))
    else:
        sepath = sitemover.filterSE(readpar('seprodpath'))
    destinationList = sitemover.getDirList(sepath)
    destination = sitemover.getMatchingDestinationPath(token, destinationList)
    path = se + destination

    cmd = "%s lcg-ls -l -b %s -T srmv2 %s" % (setup, timeout_option, path)
    tolog("Executing command: %s" % (cmd))

    try:
        ec, rs = commands.getstatusoutput(cmd)
    except Exception, e:
        tolog("!!WARNING!!1111!! Command failed with exception: %s" % (e))
    else:
        if ec != 0:
            tolog("!!WARNING!!1111!! Command failed: %d, %s" % (ec, rs))
        else:
            # tolog("SE responded with: %s" % (rs))
            status = True

    return status

def check_localSE_space(sitename, ub):
    """ Returns the amount of available space on the SE """

    # Select the correct mover
    (copycmd, setup) = copytool()

    tolog("Calling getSiteMover from check_localSE_space")
    tolog("Copy command: %s" % (copycmd))
    tolog("Setup: %s" % (setup))
    sitemover = getSiteMover(copycmd, setup)
    tolog("Got site mover: %s" % str(sitemover))
    tolog("Checking SE space...")
    try:
        retval = int(sitemover.check_space(ub))
        if retval == 0:
            retval = 999995
            tolog("0 available space reported, returning %d" % (retval))
        else:
            tolog("check_localSE_space will return %d" % (retval))
    except:
        retval = 999999
        tolog("!!WARNING!!2999!! Exception (%s) in checking available space, returning %d" % (get_exc_short(), retval))
    return retval

def getSiteMover(sitemover, setup, *args, **kwrds):
    return SiteMoverFarm.getSiteMover(sitemover, setup, *args, **kwrds)

def copytool(mode="put"):
    """
    Selects the correct copy tool (SiteMover id) given a site name
    'mode' is used to distinguish between different copy commands
    """

    # if queuedata.dat exists, get the copy tool from the DB (via http)
    try:
        # get the copytool
        copytool_tmp = readpar('copytool')
    except Exception, e:
        tolog("!!WARNING!!1113!! Caught exception (failed to read queuedata file): %s" % (e))
        tolog('No special copytool found, using cp')
        return ('cp', '')
    else:
        if mode == "get":
            copytoolname = readpar('copytoolin')
            if copytoolname == "":
                # not set, use same copytool for stage-in as for stage-out
                copytoolname = copytool_tmp
            if copytoolname.find('^') > -1:
                copytoolname, pstage = copytoolname.split('^')
        else:
            copytoolname = copytool_tmp

        if copytoolname != '':
            if copytoolname == 'lcgcp':
                copytoolname = 'lcg-cp'
                tolog("(Renamed copytool lcgcp to lcg-cp)")
            tolog("Got copytool %s from queuedata file" % (copytoolname))
        else:
            tolog("!!WARNING!!2999!! copytool not found (using default cp)")
            copytoolname = 'cp'

        # get the copysetup
        copysetup_tmp = readpar('copysetup')
        if mode == "get":
            copysetup = readpar('copysetupin')
            if copysetup == "":
                # not set, use same copysetup for stage-in as for stage-out
                copysetup = copysetup_tmp
        else:
            copysetup = copysetup_tmp

        if copysetup != '':
            # discard the directAccess info also stored in this variable
            _count = copysetup.count('^')
            if _count > 0:
                # make sure the DB entry doesn't start with directAccess info
                if _count == 2 or _count == 4 or _count == 5:
                    copysetup = copysetup.split('^')[0]
                else:
                    tolog('!!WARNING!!2999!! Could not figure out copysetup: %s' % (copysetup))
                    tolog('!!WARNING!!2999!! Resetting copysetup to an empty string')
                    copysetup = ''
            # Check copysetup actually exists!
            if copysetup != '' and os.access(copysetup, os.R_OK) == False:
                tolog('WARNING: copysetup %s is not readable - resetting to empty string' % (copysetup))
                copysetup = ''
            else:
                tolog("copysetup is: %s (file access verified)" % (copysetup))
        else:
            tolog("No copysetup found in queuedata")
        return (copytoolname, copysetup)

def randomizeReplicas(replicaList):
    """ create a randomized replica list that leaves tape areas at the end of the list """

    from SiteMover import SiteMover
    _randomized_replicas = []
    _replicas_hotdisk = []
    _replicas_tape = []

    if len(replicaList) > 1:
        # first remove tape residing replicas from list
        for _replica in replicaList:
            if "hotdisk" in _replica.lower():
                _replicas_hotdisk.append(_replica)
            elif "tape" in _replica.lower() or "t1d0" in _replica.lower():
                _replicas_tape.append(_replica)
            else:
                # look up the DQ2 site using the surl in case of obscured tape site
                if SiteMover.isFileOnTape(_replica):
                    _replicas_tape.append(_replica)
                else:
                    _randomized_replicas.append(_replica)

        if len(_randomized_replicas) > 0:
            # randomize the list
            from random import shuffle
            shuffle(_randomized_replicas)
        # create the final list with any hotdisk replicas in the beginning and tape replicas at the end
        _randomized_replicas = _replicas_hotdisk + _randomized_replicas + _replicas_tape
    else:
        _randomized_replicas = replicaList

    return _randomized_replicas

def sortReplicas(replicas, token):
    """
    Push tape resident replicas to the end of the list and pull mcdisk resident replicas to the
    beginning of the list
    """
    # if token exists, insist on using it

    from SiteMover import SiteMover
    _replicas_mcdisk_datadisk = []
    _replicas_hotdisk = []
    _replicas_disk = []
    _replicas_tape = []

    # put any tape resident replicas at the end of the list unless requested with the dispatchDBlockToken
    if token:
        # only keep replicas matching the given space token (tape should be first)
        for _replica in replicas:
            if token.lower() in _replica.sfn.lower():
                _replicas_tape.append(_replica)
            else:
                # look up the DQ2 site using the surl in case of obscured tape site
                if SiteMover.isFileOnTape(_replica.sfn):
                    _replicas_tape.append(_replica)
                else:
                    _replicas_disk.append(_replica)
        replicas = _replicas_tape + _replicas_disk
    else:
        # separate tape and mcdisk/datadisk replicas from other disk replicas
        for _replica in replicas:
            if "tape" in _replica.sfn.lower() or "t1d0" in _replica.sfn.lower():
                _replicas_tape.append(_replica)
            elif "hotdisk" in _replica.sfn.lower():
                _replicas_hotdisk.append(_replica)
            elif "mcdisk" in _replica.sfn.lower() or "datadisk" in _replica.sfn.lower() or "bnlt0d1" in _replica.sfn.lower():
                _replicas_mcdisk_datadisk.append(_replica)
            else:
                # look up the DQ2 site using the surl in case of obscured tape site
                tolog("Replica: %s" % (_replica.sfn))
                if SiteMover.isFileOnTape(_replica.sfn):
                    _replicas_tape.append(_replica)
                else:
                    _replicas_disk.append(_replica)
        replicas = _replicas_hotdisk + _replicas_mcdisk_datadisk + _replicas_disk + _replicas_tape

    return replicas

def getLocalSpace(path):
    """ Return the current remaining local space (B) """

    import Node
    thisWorkNode = Node.Node()
    thisWorkNode.collectWNInfo(path)
    return int(thisWorkNode.disk)*1024**2 # convert from MB to B

def getLocalSpaceLimit(_maxinputsize):
    """ Return the minimum local space needed to run a job """

    from pilot import localspacelimit0, localsizelimit_stdout
    # convert from kB to B
    _localspacelimit0 = localspacelimit0*1024           # 5 GB, max output file size
    _localsizelimit_stdout = localsizelimit_stdout*1024 # 2 GB, max job stdout/log size
    # _maxinputsize = getMaxInputSize() # typically 14 GB, max total size of input files

    return _maxinputsize + _localspacelimit0 + _localsizelimit_stdout

def verifyInputFileSize(totalFileSize, _maxinputsize, error):
    """ Verify that the total input file size is within the allowed limit """

    ec = 0
    pilotErrorDiag = ""

    if totalFileSize > _maxinputsize and _maxinputsize != 0:
        pilotErrorDiag = "Too many/too large input files. Total file size %d B > %d B" % (totalFileSize, _maxinputsize)
        tolog("!!FAILED!!2999!! %s" % (pilotErrorDiag))
        tolog("Mover get_data finished (failed)")
        ec = error.ERR_SIZETOOLARGE
    else:
        if _maxinputsize != 0:
            tolog("Total input file size %d B within allowed limit %d B" % (totalFileSize, _maxinputsize))
        else:
            tolog("Total input file size %d B within allowed limit (not set)" % (totalFileSize))
    return ec, pilotErrorDiag

def verifyAvailableSpace(totalFileSize, path, error):
    """ Verify that enough local space is available to stage in and run the job """

    ec = 0
    pilotErrorDiag = ""

    # skip for now: add the 5 GB + 2 GB limits for output and log files to the total input file size
    _neededSpace = totalFileSize # getLocalSpaceLimit(totalFileSize)
    tolog("Needed space: %d B" % (_neededSpace))
    # get the locally available space
    _availableSpace = getLocalSpace(path)
    tolog("Locally available space: %d B" % (_availableSpace))
    # are we wihin the limit?
    if _neededSpace > _availableSpace:
        pilotErrorDiag = "Not enough local space for staging input files and run the job (need %d B, but only have %d B)" %\
                         (_neededSpace, _availableSpace)
        tolog("!!FAILED!!2999!! %s" % (pilotErrorDiag))
        ec = error.ERR_NOLOCALSPACE

    return ec, pilotErrorDiag

def getFileAccess(access_dict, lfn):
    """ Get the special file access info if needed """

    if access_dict:
        try:
            file_access = access_dict[lfn]
        except Exception, e:
            tolog("No special file access: %s" % str(e))
            file_access = None
        else:
            if file_access == "" or file_access == "NULL" or file_access == None:
                tolog("No special file access")
                file_access = None
            else:
                tolog("Special file access: %s" % str(file_access))
    else:
        file_access = None

    return file_access

def updateReport(report, gpfn, dsname, fsize, sitemover):
    """ Update the tracing report with the DQ2 site name """

    # gpfn is the SURL
    # get the DQ2 site name from ToA
    try:
        _dq2SiteName = sitemover.getDQ2SiteName(surl=gpfn)
    except Exception, e:
        tolog("Warning: Failed to get the DQ2 site name: %s (tracing report will have the wrong site name)" % str(e))
    else:
        report['localSite'], report['remoteSite'] = (_dq2SiteName, _dq2SiteName)
        tolog("DQ2 site name: %s" % (_dq2SiteName))

    # update the tracing report with the correct dataset for this file
    report['dataset'] = dsname
    report['filesize'] = str(fsize)

    return report

def getFileInfoDictionaryFromDispatcher(lfns, filesizeIn, checksumIn):
    """ Get the filesize and checksum dictionary from the dispatcher lists """

    fileInfoDictionary = {}

    if len(lfns) == len(filesizeIn) and len(filesizeIn) == len(checksumIn):
        lfn_nr = -1
        for lfn in lfns:
            lfn_nr += 1
            fileInfoDictionary[lfn] = (filesizeIn[lfn_nr], checksumIn[lfn_nr])
    else:
        tolog("!!WARNING!!1222!! List have different lengths: %s, %s, %s" % (str(lfns), str(filesizeIn), str(checksumIn)))
        tolog("!!WARNING!!1222!! Can not use file info from dispatcher")

    return fileInfoDictionary

def stripDQ2FromLFN(lfn):
    """ Remove any __DQ2 part of an LFN """
    # E.g. LFN = AOD.505307._000001.pool.root.9__DQ2-1315236060
    # -> AOD.505307._000001.pool.root.9
    # This method assumes that the LFN contains the __DQ2-<nr> substring

    pattern = "(\s*)\_\_DQ2\-[0-9]+"

    found = re.search(pattern, lfn)    
    if found:
        try:
            __DQ2 = found.group(0)
        except Exception, e:
            tolog("!!WARNING!!1112!! Failed to identify __DQ2 substring: %s" % (e))
        else:
            lfn = lfn.replace(__DQ2, "")

    return lfn

def getFileInfoFromDispatcher(lfn, fileInfoDictionary):
    """ Get the filesize and checksum from the dispatcher lists """

    filesize = ''
    checksum = ''

    try:
        filesize, checksum = fileInfoDictionary[lfn]
    except Exception, e:
        tolog("!!WARNING!! Entry does not exist in dispatcher file info dictionary: %s" % (e))
        tolog("LFN = %s" % (lfn))
        tolog("File info dictionary = %s" % str(fileInfoDictionary))
    else:
        # remove checksum type if present
        if ":" in checksum:
            checksum = checksum.split(":")[1]

        if filesize == '' or filesize == '0' or filesize == 'NULL' or not filesize.isdigit():
            tolog("WARNING: Bad file size for lfn=%s from dispatcher: %s (reset to empty string)" % (lfn, filesize))
            filesize = ''
        if checksum == '' or checksum == 'NULL' or not checksum.isalnum() or len(checksum) != 8:
            tolog("WARNING: Bad checksum for lfn=%s from dispatcher: %s (reset to empty string)" % (lfn, checksum))
            checksum = ''
            
    return filesize, checksum

def getFileInfoFromMetadata(thisfile, guid, replicas_dic, lfchost, region, sitemover, error):
    """ Get the file info from the metadata """

    ec = 0
    pilotErrorDiag = ""

    # create a dictionary for the metadata tags (which includes the md5sum/adler32 value)
    dic = {}
    dic['md5sum'] = ""
    dic['adler32'] = ""
    dic['fsize'] = ""
    csumtype = "unknown"
    if lfchost != "" and region != "Nordugrid":
        # extract the filesize and checksum
        try:
            # always use the first replica (they are all supposed to have the same file sizes and checksums)
            _fsize = replicas_dic[guid][0].filesize
            _fchecksum = replicas_dic[guid][0].csumvalue
        except Exception, e:
            pilotErrorDiag = "filesize/checksum could not be extracted for guid: %s, %s" % (guid, str(e))
            tolog("!!FAILED!!2999!! %s" % (pilotErrorDiag))
            tolog("Mover get_data finished (failed) [getFileInfoFromMetadata]")
            return error.ERR_FAILEDLFCGETREPS, pilotErrorDiag, None, None
        else:
            tolog("Extracted fsize: %s fchecksum: %s for guid: %s" % (str(_fsize), _fchecksum, guid))

        csumtype = sitemover.getChecksumType(_fchecksum)
        if _fchecksum == "":
            dic['md5sum'] = 0
            dic['adler32'] = 0
        else:
            if csumtype == "adler32":
                dic['adler32'] = _fchecksum
                dic['md5sum'] = 0
            else:
                dic['adler32'] = 0
                dic['md5sum'] = _fchecksum
                
        dic['fsize'] = str(_fsize)
    else:
        for i in range(len(thisfile.getElementsByTagName("metadata"))):
            key = str(thisfile.getElementsByTagName("metadata")[i].getAttribute("att_name"))
            dic[key] = str(thisfile.getElementsByTagName("metadata")[i].getAttribute("att_value"))
            # eg. dic = {'lastmodified': '1178904328', 'md5sum': 'fa035fc0a92066a5373ff9580e3d9862',
            #            'fsize': '33200853', 'archival': 'P'}
            # Note: md5sum/adler32 can assume values <32/8strings>, "NULL", "", 0
        tolog("dic = %s" % str(dic))    
        if dic['adler32'] != 0 and dic['adler32'] != "" and dic['adler32'] != "NULL":
            csumtype = "adler32"
        elif dic['md5sum'] != 0 and dic['md5sum'] != "" and dic['md5sum'] != "NULL":
            csumtype = "md5sum"
        else:
            csumtype = CMD_CHECKSUM

    if csumtype == "adler32":
        fchecksum = dic['adler32']
    else: # pass a 0 if md5sum was actually not set
        fchecksum = dic['md5sum']
    fsize = dic['fsize']
    tolog("csumtype: %s, checksum: %s, fsize: %s" % (csumtype, str(fchecksum), str(fsize)))

    return ec, pilotErrorDiag, fsize, fchecksum

def getDataset(filename, dsdict):
    """ Get the dataset for a given file """

    dataset = ""
    for ds in dsdict.keys():
        if filename in dsdict[ds]:
            dataset = ds
            break

    if dataset == "":
        tolog("!!WARNING!!2999!! Dataset not found for file %s" % (filename))
    else:
        tolog("File %s belongs to dataset/container %s" % (filename, dataset))

    return dataset

# is a [completed] function like this needed?
#def addSURLsToXML():
#    """ Add SURLs to metadata file to allow server to perform LFC registration """
#
#    status = False
#
#    return status
