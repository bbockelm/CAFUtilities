import sys, os, signal, time, shutil, cgi
import commands, re
import urllib

from xml.dom import minidom
from xml.dom.minidom import Document
from xml.dom.minidom import parse, parseString

# exit code
EC_Failed = 255

try:
    import datetime
except:
    pass

try:
    from PilotErrors import PilotErrors
    from config import config_sm
    from timed_command import timed_command
    
    CMD_CHECKSUM = config_sm.COMMAND_MD5
except:
    pass

# all files that need to be copied to the workdir
fileList = commands.getoutput('ls *.py').split()

# default pilot log files
pilotlogFilename = "pilotlog.out"
pilotstderrFilename = "pilot.stderr"

def setPilotlogFilename(filename):
    """ set the pilot log file name"""
    global pilotlogFilename
    if len(filename) > 0:
        pilotlogFilename = filename

def getPilotlogFilename():
    """ return the pilot log file name"""
    return pilotlogFilename

def setPilotstderrFilename(filename):
    """ set the pilot stderr file name"""
    global pilotstderrFilename
    if len(filename) > 0:
        pilotstderrFilename = filename

def getPilotstderrFilename():
    """ return the pilot stderr file name"""
    return pilotstderrFilename

def tolog_file(msg):
    """ write date+msg to pilot log only """
    # t = time.strftime("%d %b %Y %H:%M:%S", time.localtime())
    t = time.strftime("%d %b %Y %H:%M:%S", time.gmtime(time.time()))
    appendToLog("%s| %s\n" % (t, msg))

def appendToLog(txt):
    """ append txt to file """

    try:
        f = open(pilotlogFilename, 'a')
        f.write(txt)
        f.close()
    except Exception, e:
        if "No such file" in str(e):
            pass
        else:
            print "WARNING: Exception caught: %s" % str(e)

def tolog(msg, tofile=True):
    """ write date+msg to pilot log and to stdout """

    import inspect

    MAXLENGTH = 12
    # getting the name of the module that is invoking tolog() and adjust the length
    try:
        module_name = os.path.basename(inspect.stack()[1][1])
    except Exception, e:
        module_name = "unknown"
        print "Exception caught by tolog():", e
    module_name_cut = module_name[0:MAXLENGTH].ljust(MAXLENGTH)
    msg = "%s| %s" % (module_name_cut, msg)

    # t = time.strftime("%d %b %Y %H:%M:%S", time.localtime())
    # t = time.strftime("%d %b %Y %H:%M:%S", time.gmtime(time.time()))
    t = timeStampUTC()
    if tofile:
        appendToLog("%s|%s\n" % (t, msg))

    # remove backquotes from the msg since they cause problems with batch submission of pilot
    # (might be present in error messages from the OS)
    msg = msg.replace("`","'")
    msg = msg.replace('"','\\"')
    print "%s| %s" % (t, msg)

    # write any FAILED messages to stderr
    if "!!FAILED!!" in msg:
        print >> sys.stderr, "%s| %s" % (t, msg)

def tolog_err(msg):
    """ write error string to log """
    tolog("!!WARNING!!4000!! %s" % str(msg))
    
def tolog_warn(msg):
    """ write warning string to log """
    tolog("!!WARNING!!4000!! %s" % str(msg))

def makeHTTPUpdate(state, node, port, url='pandaserver.cern.ch', path=None):
    """ make http connection to jobdispatcher """

    if state == 'finished' or state == 'failed' or state == 'holding':
        tolog("Preparing for final Panda server update")
        trial = 1
        max_trials = 10
        delay = 2*60 # seconds
        tolog("Max number of trials: %d, separated by delays of %d seconds" % (max_trials, delay))
    else:
        # standard non final update
        trial = 1
        max_trials = 1
        delay = None

    # make http connection to jobdispatcher        
    while trial <= max_trials:
        # draw a random server URL
        _url = '%s:%s/server/panda' % (url, port)
        tolog("HTTP connect using server: %s" % (_url))
        ret = httpConnect(node, _url, path=path)
        if ret[0] and trial == max_trials: # non-zero exit code
            if delay: # final update 
                tolog("!!FAILED!!4000!! [Trial %d/%d] Could not update Panda server (putting job in holding state if possible)" %\
                      (trial, max_trials))
                # state change will take place in postJobTask
                # (the internal pilot state will not be holding but lostheartbeat)
            else:
                tolog("!!WARNING!!4000!! [Trial %d/%d] Could not update Panda server, EC = %d" %\
                      (trial, max_trials, ret[0]))
            break
        elif ret[0]: # non-zero exit code
            tolog("!!WARNING!!4000!! [Trial %d/%d] Could not update Panda server, EC = %d" %\
                  (trial, max_trials, ret[0]))
            if delay: # final update
                tolog("Can not miss the final update. Will take a nap for %d seconds and then try again.." % (delay))
                trial += 1
                time.sleep(delay)
            else: # try again later
                tolog("Panda server update postponed..")
                break
        else:
            break
    return ret

def httpConnect(data, url, mode="UPDATE", sendproxy=False, path=None): # default mode allows exceptions to occur w/o interrupting the program
    """ function to handle the http connection """

    # check if a job should be downloaded or if it's a server update
    if mode == "GETJOB":
        cmd = 'getJob'
    elif mode == "ISTHEREANALYSISJOB":
        cmd = 'isThereAnalysisJob'
    elif mode == "GETSTATUS":
        cmd = 'getStatus'
    else:
        cmd = 'updateJob'

    # only send attemptNr with updateJob
    if cmd != 'updateJob' and data.has_key('attemptNr'):
        tolog("Removing attemptNr from node structure since it is not needed for command %s" % (cmd))
        del data['attemptNr']
    else:
        if data.has_key('attemptNr'):
            tolog("Sending attemptNr=%d for cmd=%s" % (data['attemptNr'], cmd))
        else:
            tolog("Will not send attemptNr for cmd=%s" % (cmd))

    # send the data dictionary to the dispatcher using command cmd
    # return format: status, parsed data, response
    return toServer(url, cmd, data, path)

def returnLogMsg(logf=None, linenum=20):
    ''' return the last N lines of log files into a string'''
    thisLog = ''
    if logf:
        if not os.path.isfile(logf):
            thisLog = "\n- No log file %s found -" % (logf)
        else:
            thisLog = "\n- Log from %s -" % (logf)
            f = open(logf)
            lines = f.readlines()
            f.close()

            if len(lines) <= linenum:
                ln = len(lines)
            else:
                ln = linenum
            
            for i in range(-ln,0):
                thisLog += lines[i]

    return thisLog

def findGuid(analJob, metadata_filename, filename):
    """ find guid in alternative file or generate it """

    guid = None
    metadata_path = os.path.dirname(metadata_filename)
    if os.path.exists(metadata_filename):
        # now grab the guids from the preprocessed metadata
        _guid = getGuidsFromXML(metadata_path, filename=filename, metadata=metadata_filename)
        if _guid != []:
            if _guid[0] != "":
                tolog("Found guid %s in %s (missing in PFC)" % (_guid[0], metadata_filename))
                guid = _guid[0]
            else:
                guid = None
        else:
            guid = None
    else:
        tolog("Could not locate file: %s" % (metadata_filename))

    if not guid:
        if analJob:
            guid = commands.getoutput('uuidgen')
            tolog("Generated guid: %s" % (guid))
        else:
            tolog("Guid missing for file: %s (b)" % (filename))
            guid = None
    else:
        tolog("Guid identified")

    return guid

def preprocessMetadata(filename):
    """ remove META tags from metadata since they can contain value that minidom can not chandle """

    status = True

    # loop over the file and remove the META tags
    try:
        f = open(filename, "r")
    except Exception, e:
        tolog("!!WARNING!!2999!! Could not open file: %s (%s)" % (filename, str(e)))
        status = False
    else:
        lines = f.readlines()
        f.close()

        # remove the META tags
        new_lines = ""
        for line in lines:
            if not "<META" in line and not "<metadata" in line:
                new_lines += line

        # remove the old file before recreating it
        try:
            os.remove(filename)
        except Exception, e:
            tolog("!!WARNING!!2999!! Could not remove file: %s (%s)" % (filename, str(e)))
            status = False
        else:
            try:
                f = open(filename, "w")
            except Exception, e:
                tolog("!!WARNING!!2999!! Could not recreate file: %s (%s)" % (filename, str(e)))
                status = False
            else:
                f.writelines(new_lines)
                f.close()
                tolog("New temporary metadata file:\n" + new_lines)

    return status

def prepareMetadata(metadata_filename):
    """ prepare the metadata for potential guid rescue """

    metadata_filename_BAK = metadata_filename + ".BAK"
    metadata_filename_ORG = metadata_filename

    if os.path.exists(metadata_filename):
        # first create a copy of the metadata
        try:
            shutil.copy2(metadata_filename, metadata_filename_BAK)
        except Exception, e:
            tolog("!!WARNING!!2999!! Could not copy metadata: %s" % str(e))
        else:
            metadata_filename = metadata_filename_BAK
            tolog("Created file: %s" % (metadata_filename))

        # remove junk metadata
        try:
            status = preprocessMetadata(metadata_filename)
        except Exception, e:
            tolog("!!WARNING!!2999!! Could not preprocess metadata: %s" % str(e))
            metadata_filename = metadata_filename_ORG
        else:
            if status:
                tolog("Successfully updated %s" % (metadata_filename))
            else:
                tolog("Could not update %s" % (metadata_filename))
                metadata_filename = metadata_filename_ORG
    else:
        tolog("Nothing for prepareMetadata() to do")

    return metadata_filename

def PFCxml(experiment, fname, fnlist=[], fguids=[], fntag=None, alog=None, alogguid=None, fsize=[], checksum=[], analJob=False, jr=False, additionalOutputFile=None, additionalOutputFileGuid=None):
    """ fguids list will be changed in the caller as well, since list is mutable object
    fguids and fnlist are for output files, alog and alogguid are for workdir tarball log
    files, which are not mutable so don't expect the value changed inside this function
    will change in the caller as well !!! """
    # fnlist = output file list
    # fguids = output file guid list
    # fntag = pfn/lfn identifier
    # alog = name of log file
    # alogguid = guid of log file
    # fsize = file size list
    # checksum = checksum list
    # analJob = analysis job
    # jr = job recovery mode, guid generation by pilot not allowed

    # fntag = lfn is used for the metadata-<jobId>.xml that is sent to the server
    # fntag = pfn is used for OutPutFileCatalog.xml that is used by the mover for the stage out
    # from pilot v 46aRC3, the SURL will be added to the metadata file for fntag = lfn to allow for server side LFC registration

    tolog("PFCxml started")
    status = True
    flist = []
    glist = []
    from SiteMover import SiteMover

    # get the experiment object
    thisExperiment = getExperiment(experiment)

    # for metadata.xml prepare the file for potential guid grabbing
    if "metadata" in fname and None in fguids:
        try:
            metadata_filename = prepareMetadata(fname + ".ATHENA")
        except Exception, e:
            tolog("!!WARNING!!2999!! Failed to prepare metadata: %s" % str(e))
            metadata_filename = fname
    else:
        metadata_filename = fname

    # add log file
    if alog:
        flist.append(alog)
        if not alogguid:
            if not jr:
                alogguid = commands.getoutput('uuidgen')
                tolog("Generated log guid: %s" % (alogguid))
            else:
                tolog("!!WARNING!!2999!! Log guid generation not allowed in recovery mode")
                alogguid = ''
                status = False
        glist.append(alogguid)

    # add additional output files (only for CERNVM, not NG or any other sites)
    if additionalOutputFile:
        flist.append(additionalOutputFile)
        if not additionalOutputFileGuid:
            additionalOutputFileGuid = commands.getoutput('uuidgen')
        glist.append(additionalOutputFileGuid)

    if fnlist:
        flist = flist + fnlist
        tolog("fnlist = %s" % str(fnlist))
        tolog("fguids = %s" % str(fguids))
        for i in range(0, len(fnlist)):
            # check for guid
            try:
                _dummy = fguids[i]
                del _dummy
            except IndexError, e:
                guid = findGuid(analJob, metadata_filename, fnlist[i])
                if guid and guid != "":
                    tolog("Found guid for file: %s (%s)" % (fnlist[i], guid))
                else:
                    if not jr:
                        guid = commands.getoutput('uuidgen')
                        tolog("Generated guid for file (%d): %s (%s)" % (i, fnlist[i], guid))
                    else:
                        tolog("!!WARNING!!2999!! Guid generation not allowed in recovery mode (file: %s)" % (fnlist[i]))
                        guid = ''
                        status = False
                fguids.insert(i, guid)
            else:
                if not fguids[i]:
                    guid = findGuid(analJob, metadata_filename, fnlist[i])
                    if guid and guid != "":
                        tolog("Found guid for file: %s (%s)" % (fnlist[i], guid))
                    else:
                        if not jr:
                            guid = commands.getoutput('uuidgen')
                            tolog("Generated guid for file (%d): %s (%s)" % (i, fnlist[i], guid))
                        else:
                            tolog("!!WARNING!!2999!! Guid generation not allowed in recovery mode (file: %s)" % (fnlist[i]))
                            guid = ''
                            status = False
                    try:
                        fguids[i] = guid
                    except:
                        fguids.insert(i, guid)

            if fntag == "lfn":
                # check for file size
                try:
                    _dummy = fsize[i]
                    del _dummy
                except IndexError, e:
                    #print "This item doesn't exist"
                    fsize.insert(i, "")
                #else:
                #    if not fsize[i]: # this fsize doesn't exist
                #        fsize[i] = ""
                # check for checksum
                try:
                    _dummy = checksum[i]
                    del _dummy
                except IndexError, e:
                    #print "This item doesn't exist"
                    checksum.insert(i, "")
                #else:
                #    if not checksum[i]: # this checksum doesn't exist
                #        checksum[i] = ""
        glist = glist + fguids

    if fntag == "pfn":
        #create the PoolFileCatalog.xml-like file in the workdir
        fd = open(fname, "w")
        fd.write('<?xml version="1.0" encoding="UTF-8" standalone="no" ?>\n')
        fd.write("<!-- Edited By POOL -->\n")
        fd.write('<!DOCTYPE POOLFILECATALOG SYSTEM "InMemory">\n')
        fd.write("<POOLFILECATALOG>\n")
        for i in range(0,len(flist)):
            fd.write('  <File ID="%s">\n' % (glist[i]))
            fd.write("    <physical>\n")
            fd.write('      <pfn filetype="ROOT_All" name="%s"/>\n' % (flist[i]))
            fd.write("    </physical>\n")
            fd.write("  </File>\n")
        fd.write("</POOLFILECATALOG>\n")
        fd.close()
    elif fntag == "lfn":
        # create the metadata.xml-like file that's needed by dispatcher jobs
        fd=open(fname, "w")
        fd.write('<?xml version="1.0" encoding="UTF-8" standalone="no" ?>\n')
        fd.write("<!-- ATLAS file meta-data catalog -->\n")
        fd.write('<!DOCTYPE POOLFILECATALOG SYSTEM "InMemory">\n')
        fd.write("<POOLFILECATALOG>\n")
        for i in range(0, len(flist)):
            fd.write('  <File ID="%s">\n' % (glist[i]))
            fd.write("    <logical>\n")
            fd.write('      <lfn name="%s"/>\n' % (flist[i]))
            fd.write("    </logical>\n")

            # add SURL metadata (not known yet) for server LFC registration
            # use the GUID as identifier (the string "<GUID>-surltobeset" will later be replaced with the SURL)
            if thisExperiment:
                special_xml = thisExperiment.getMetadataForRegistration(glist[i])
                if special_xml != "":
                    fd.write(special_xml)

            # add log file metadata later (not known yet)
            if flist[i] == alog:
                fd.write('    <metadata att_name="fsize" att_value=""/>\n')
                fd.write('    <metadata att_name="csumtypetobeset" att_value=""/>\n')
            elif (additionalOutputFile and flist[i] == additionalOutputFile):
                if ".xml" in additionalOutputFile:
                    fd.write('    <metadata att_name="fsizeXML" att_value=""/>\n')
                    fd.write('    <metadata att_name="csumtypetobesetXML" att_value=""/>\n')
                else:
                    fd.write('    <metadata att_name="fsizeAdditional" att_value=""/>\n')
                    fd.write('    <metadata att_name="csumtypetobesetAdditional" att_value=""/>\n')
            else:
                if len(fsize) != 0:
                    fd.write('    <metadata att_name="fsize" att_value="%s"/>\n' % (fsize[i]))
                if len(checksum) != 0:
                    fd.write('    <metadata att_name="%s" att_value="%s"/>\n' %\
                             (SiteMover.getChecksumType(checksum[i]), checksum[i]))
            fd.write("  </File>\n")
        fd.write("</POOLFILECATALOG>\n")
        fd.close()
    else:
        tolog("fntag is neither lfn nor pfn, didn't create the XML file for output files")

    #PN
    dumpFile(fname)

    return status

def stageInPyModules(initdir, workdir):
    """ copy pilot python modules into pilot workdir from condor initial dir """

    status = True
    ec = 0

    if workdir and initdir:
        for k in fileList:
            if os.path.isfile("%s/%s" % (initdir, k)):
                try:
                    shutil.copy2("%s/%s" % (initdir, k), workdir)
                except Exception, e:
                    tolog("!!WARNING!!2999!! stageInPyModules failed to copy file %s/%s to %s: %s" % (initdir, k, workdir, str(e)))
                    status = False
                    break
            else:
                tolog("!!WARNING!!2999!! File missing during stage in: %s/%s" % (initdir, k))

    if status:
        tolog("Pilot modules have been copied to %s" % (workdir))
    else:
        # get error handler
        error = PilotErrors()
        ec = error.ERR_GENERALERROR

    return ec

def removePyModules(dir):
    """ copy pilot python modules into pilot workdir
    from condor initial dir """

    if dir:
        for k in fileList:
            try:
                os.system("rm -rf %s/%s %s/*.pyc"%(dir,k,dir))
            except:
                pass

def setTimeConsumed(t_tuple):
    """ set the system+user time spent by the job """

    # The cpuConsumptionTime is the system+user time while wall time is encoded in pilotTiming, third number.
    # Previously the cpuConsumptionTime was "corrected" with a scaling factor but this was deemed outdated and is now set to 1.
    
    t_tot = reduce(lambda x, y:x+y, t_tuple[2:3])
    conversionFactor = 1.0
    cpuCU = "kSI2kseconds"
    cpuCT = int(t_tot*conversionFactor)

    return cpuCU, cpuCT, conversionFactor

def timeStamp(): 
    """ return ISO-8601 compliant date/time format """

    tmptz = time.timezone
    if tmptz > 0:
        signstr = '-'
    else:
        signstr = '+'
    tmptz_hours = int(tmptz/3600)

    return str("%s%s%02d%02d" % (time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime()), signstr, tmptz_hours, int(tmptz/60-tmptz_hours*60)))

def timeStampUTC(t=None):
    """ return UTC time stamp """

    if not t:
        t = time.time()
    return time.strftime("%d %b %H:%M:%S", time.gmtime(t))

def getJobStatus(jobId, pshttpurl, psport, path):
    """
    Return the current status of job <jobId> from the dispatcher 
    typical dispatcher response: 'status=finished&StatusCode=0'
    StatusCode  0: succeeded
               10: time-out
               20: general error
               30: failed
    In the case of time-out, the dispatcher will be asked one more time after 10s
    """

    status = 'unknown'
    StatusCode = -1
    nod = {}
    nod['ids'] = jobId
    url = "%s:%s/server/panda/getStatus" % (pshttpurl, repr(psport))
    # ask dispatcher about lost job status
    trial = 1
    max_trials = 2
    while trial <= max_trials:
        try:
            # open connection
            ret = httpConnect(nod, url, path=path, mode="GETSTATUS")
            response = ret[1]
            tolog("response: %s" % str(response))
            if response:
                try:
                    # decode the response
                    # eg. var = ['status=notfound', 'attemptNr=0', 'StatusCode=0']
                    # = response

                    # create a dictionary of the response (protects against future updates)
                    # eg. dic = {'status': 'activated', 'attemptNr': '0', 'StatusCode': '0'}
#                    dic = {}
#                    for i in range(len(var)):
#                        key = var[i].split('=')[0]
#                        dic[key] = var[i].split('=')[1]

                    status = response['status']              # e.g. 'holding'
                    attemptNr = int(response['attemptNr'])   # e.g. '0'
                    StatusCode = int(response['StatusCode']) # e.g. '0'
                except Exception, e:
                    tolog("!!WARNING!!2997!! Exception: Dispatcher did not return allowed values: %s, %s" % (str(ret), str(e)))
                    status = "unknown"
                    attemptNr = -1
                    StatusCode = 20
            else:
                tolog("!!WARNING!!2998!! Dispatcher did not return allowed values: %s" % str(ret))
                status = "unknown"
                attemptNr = -1
                StatusCode = 20
        except Exception,e:
            tolog("Could not interpret job status from dispatcher: %s, %s" % (response, str(e)))
            status = 'unknown'
            attemptNr = -1
            StatusCode = -1
            break
        else:
            if StatusCode == 0: # success
                break
            elif StatusCode == 10: # time-out
                trial = trial + 1
                time.sleep(10)
                continue
            elif StatusCode == 20: # other error
                if ret[0] == 13056 or ret[0] == '13056':
                    tolog("Wrong certificate used with curl operation? (encountered error 13056)")
                break
            else: # general error
                break

    return status, attemptNr, StatusCode

def getExitCode(path, filename):
    """ Try to read the exit code from the pilot stdout log """

    ec = -1

    # first create a tmp file with only the last few lines of the status file to avoid
    # reading a potentially long status file
    tmp_file_name = "tmp-tail-dump-file"
    try:
        os.system("tail %s/%s >%s/%s" % (path, filename, path, tmp_file_name))
    except Exception, e:
        tolog("Job Recovery could not create tmp file %s: %s" % (tmp_file_name, str(e)))
    else:
        # open the tmp file and look for the pilot exit info
        try:
            tmp_file = open(tmp_file_name, 'r')
        except IOError:
            tolog("Job Recovery could not open tmp file")
        else:
            try:
                all_lines = tmp_file.readlines()
            except Exception, e:
                tolog("Job Recovery could not read tmp file %s: %s" % (tmp_file_name, str(e)))

            tmp_file.close()

            # remove the tmp file
            try:
                os.remove(tmp_file_name)
            except OSError:
                tolog("Job Recovery could not remove tmp file: %s" % tmp_file)

            # now check the pilot exit info, if is has an exit code - remove the directory
            exit_info = re.compile(r"job ended with \(trf,pilot\) exit code of \(\d+,\d+\)")
            exitinfo_has_exit_code = False
            for line in all_lines:
                if re.findall(exit_info, line):
                    exitinfo_has_exit_code = True

            if exitinfo_has_exit_code:
                tolog("Job had an exit code")

                # ...
            else:
                tolog("Job had no exit code")

    return ec

def getRemainingOutputFiles(outFiles):
    """
    Return list of files if there are remaining output files in the lost job data directory
    """
    remaining_files = []

    for file_name in outFiles:
        if os.path.exists(file_name):
            remaining_files.append(file_name)

    return remaining_files

def remove(entries):
    """
    Remove files and directories
    entries should be a list
    """
    status = True

    # protect against wrong usage
    if type(entries) == list:
        if len(entries) > 0:
            for entry in entries:
                try:
                    os.system("rm -rf %s" %entry)
                except OSError:
                    tolog("Could not remove %s" % entry)
                    status = False
    else:
        tolog("Argument has wrong type, expected list: %s" % str(type(entries)))
        status = False
    return status

def getCPUmodel():
    """ Get cpu model and cache size from /proc/cpuinfo """
    # model name      : Intel(R) Xeon(TM) CPU 2.40GHz
    # cache size      : 512 KB
    # gives the return string "Intel(R) Xeon(TM) CPU 2.40GHz 512 KB"

    cpumodel = ''
    cpucache = ''
    modelstring = ''
    try:
        f = open('/proc/cpuinfo', 'r')
    except Exception, e:
        tolog("Could not open /proc/cpuinfo: %s" % str(e))
    else:
        re_model = re.compile('^model name\s+:\s+(\w.+)')
        re_cache = re.compile('^cache size\s+:\s+(\d+ KB)')

        # loop over all lines in cpuinfo
        for line in f.readlines():
            # try to grab cpumodel from current line
            model = re_model.search(line)
            if model:
                # found cpu model
                cpumodel = model.group(1)

            # try to grab cache size from current line
            cache = re_cache.search(line)
            if cache:
                # found cache size
                cpucache = cache.group(1)

            # stop after 1st pair found - can be multiple cpus
            if cpumodel and cpucache:
                # create return string
                modelstring = cpumodel + " " + cpucache
                break
                
        f.close()

    # default return string if no info was found
    if not modelstring:
        modelstring = "UNKNOWN"

    return modelstring

def getExeErrors(startdir, fileName):
    """ Extract exeErrorCode and exeErrorDiag from jobInfo.xml """

    exeErrorCode = 0
    exeErrorDiag = ""

    # first check if the xml file exists (e.g. it doesn't exist for a test job)
    findFlag = False
    line = ""
    # try to locate the file
    out = commands.getoutput("find %s -name %s" % (startdir, fileName))
    if out != "":
        for line in out.split('\n'):
            tolog("Found trf error file at: %s" % (line))
            findFlag = True
            break # just in case, but there should only be one trf error file
    if findFlag:
        if os.path.isfile(line):
            # import the xml functions
            from xml.sax import make_parser
            from xml.sax.handler import feature_namespaces
            import JobInfoXML

            # Create a parser
            parser = make_parser()

            # Tell the parser we are not interested in XML namespaces
            parser.setFeature(feature_namespaces, 0)

            # Create the handler
            dh = JobInfoXML.JobInfoXML()

            # Tell the parser to use our handler
            parser.setContentHandler(dh)

            # Parse the input
            parser.parse(line)

            # get the error code and the error message(s)
            exeErrorCode = dh.getCode()
            exeErrorDiag = dh.getMessage()
        else:
            tolog("Could not read trf error file: %s" % (line))
    else:
        tolog("Could not find trf error file %s in search path %s" % (fileName, startdir))

    # only return a maximum of 250 characters in the error message (as defined in PandaDB)
    return exeErrorCode, exeErrorDiag[:250]

def debugInfo(_str, tofile=True):
    """ print the debug string to stdout"""

    tolog("DEBUG: %s" % (_str), tofile=tofile)

def isBuildJob(outFiles):
    """
    Check if the job is a build job
    (i.e. check if the job only has one output file that is a lib file)
    """

    isABuildJob = False
    # outFiles only contains a single file for build jobs, the lib file
    if len(outFiles) == 1:
        # e.g. outFiles[0] = user.paulnilsson.lxplus001.lib._001.log.tgz
        if outFiles[0].find(".lib.") > 0:
            isABuildJob = True

    return isABuildJob

def OSBitsCheck():
    """ determine whether the platform is a 32 or 64-bit OS """

    b = -1
    try:
        a = commands.getoutput('uname -a')
        b = a.find('x86_64')
    except:
        return 32 # default
    else:
        if b == -1 : # 32 bit OS
            return 32
        else: # 64 bits OS
            return 64

def uniqueList(input_list):
    """ 
    return a list of unique entries
    input_list = ['1', '1', '2'] -> ['1', '2']
    """

    u = {}
    for x in input_list:
        u[x] = 1
    return u.keys()

def diffLists(list1, list2):
    """
    compare the input lists (len(list1) must be > len(list2))
    and return the difference
    """

    d = {}
    for x in list1:
        d[x] = 1
    for x in list2:
        if d.has_key(x):
            del d[x]
    return d.keys()

def getOutputFileInfo(outputFiles, checksum_cmd, skiplog=False, logFile=""):
    """
    Return lists with file sizes and checksums for the given output files
    """

    ec = 0
    fsize = []
    checksum = []

    # get error handler
    error = PilotErrors()
    pilotErrorDiag = ""

    # add the log file if necessary (when this function is called from runJob)
    # WARNING: temporary redundancy. fsize and checksum is checked again in mover code, merge later
    if logFile != "":
        outputFiles.insert(0, logFile)

    for filename in outputFiles:
        # add "" for the log metadata since it has not been created yet
        if filename == logFile and skiplog:
            ec = -1
        else:
            from SiteMover import SiteMover
            ec, pilotErrorDiag, _fsize, _checksum = SiteMover.getLocalFileInfo(filename, csumtype=checksum_cmd)
            tolog("Adding %s,%s for file %s using %s" % (_fsize, _checksum, filename, checksum_cmd))
        if ec == 0:
            fsize.append(_fsize)
            checksum.append(_checksum)
        else:
            if ec == error.ERR_FAILEDMD5LOCAL or ec == error.ERR_FAILEDADLOCAL:
                fsize.append(_fsize)
                checksum.append("")
            else:
                fsize.append("")
                checksum.append("")
            if ec != -1: # skip error message for log
                tolog("!!WARNING!!4000!! getFileInfo received an error from getLocalFileInfo for file: %s" % (filename))
                tolog("!!WARNING!!4000!! ec = %d, pilotErrorDiag = %s, fsize = %s, checksum = %s" %\
                      (ec, pilotErrorDiag, str(_fsize), str(_checksum)))
            else:
                tolog("setting ec=0 (%d)" % (ec))
                # do not return -1 as en error message since it only applies to log files
                ec = 0
    if logFile != "":
        outputFiles.remove(logFile)

    tolog("going to return %d,%s,%s,%s" % (ec, pilotErrorDiag, fsize, checksum))
    return ec, pilotErrorDiag, fsize, checksum

def updateMetadata(fname, fsize, checksum, format=None, fsizeXML=None, checksumXML=None, fsizeAdditional=None, checksumAdditional=None):
    """
    Add the fsize and checksum values for the log (left empty until this point)
    Return exit code and xml
    If format = NG, then the NorduGrid format of the metadata will be assumed
    fsizeXML and checksumXML are extra attributes needed for CERNVM xml file handling
    """
    ec = 0
    lines = ""
    try:
        f = open(fname, 'r')
    except Exception, e:
        tolog("Failed to open metadata file: %s" % str(e))
        ec = -1
    else:
        if format == 'NG':
            metadata1 = '<size></size>'
            new_metadata1 = '<size>%s</size>' % (fsize)
        else:
            metadata1 = '<metadata att_name="fsize" att_value=""/>'
            new_metadata1 = '<metadata att_name="fsize" att_value="%s"/>' % (fsize)

        # find out if checksum or adler32 should be added
        from SiteMover import SiteMover
        csumtype = SiteMover.getChecksumType(checksum)

        # special handling for CERNVM metadata
        if checksumXML:
            metadata4 = '<metadata att_name="csumtypetobesetXML" att_value=""/>'
        else:
            metadata4 = 'notused'
        if fsizeXML:
            metadata5 = '<metadata att_name="fsizeXML" att_value=""/>'
            new_metadata5 = '<metadata att_name="fsize" att_value="%s"/>' % (fsizeXML)
        else:
            metadata5 = 'notused'
            new_metadata5 = ''
        if checksumAdditional:
            metadata6 = '<metadata att_name="csumtypetobesetXML" att_value=""/>'
            if csumtype == "adler32":
                new_metadata6 = '<ad32>%s</ad32>' % (checksumAdditional)
            else:
                new_metadata6 = '<md5sum>%s</md5sum>' % (checksumAdditional)
        else:
            metadata6 = 'notused'
            new_metadata6 = ''
        if fsizeAdditional:
            metadata7 = '<metadata att_name="fsizeAdditional" att_value=""/>'
            new_metadata7 = '<metadata att_name="fsize" att_value="%s"/>' % (fsizeAdditional)
        else:
            metadata7 = 'notused'
            new_metadata7 = ''

        # for NG and CERNVM
        if format == 'NG':
            if csumtype == "adler32":
                metadata2 = '<ad32></ad32>'
                new_metadata2 = '<ad32>%s</ad32>' % (checksum)
            else:
                metadata2 = '<md5sum></md5sum>'
                new_metadata2 = '<md5sum>%s</md5sum>' % (checksum)
        else:
            if csumtype == "adler32":
                metadata2 = '<metadata att_name="adler32" att_value=""/>'
                new_metadata2 = '<metadata att_name="adler32" att_value="%s"/>' % (checksum)
            else:
                metadata2 = '<metadata att_name="md5sum" att_value=""/>'
                new_metadata2 = '<metadata att_name="md5sum" att_value="%s"/>' % (checksum)
        metadata3 = '<metadata att_name="csumtypetobeset" att_value=""/>'

        for line in f.readlines():
            newline = ""
            if line.find(metadata1) != -1:
                newline = line.replace(metadata1, new_metadata1)
                lines += newline
            elif line.find(metadata2) != -1:
                newline = line.replace(metadata2, new_metadata2)
                lines += newline
            elif line.find(metadata3) != -1:
                newline = line.replace(metadata3, new_metadata2)
                lines += newline
            elif line.find(metadata4) != -1:
                newline = line.replace(metadata4, new_metadata2)
                lines += newline
            elif line.find(metadata5) != -1:
                newline = line.replace(metadata5, new_metadata5)
                lines += newline
            elif line.find(metadata6) != -1:
                newline = line.replace(metadata6, new_metadata6)
                lines += newline
            elif line.find(metadata7) != -1:
                newline = line.replace(metadata7, new_metadata7)
                lines += newline
            elif line.find('csumtypetobeset') != -1:
                newline = line.replace()
            else:
                lines += line
            
        f.close()
        try:    
            f = open(fname, 'w')
            f.write(lines)
            f.close()
        except Exception, e:
            tolog("Failed to write new metadata for log: %s" % str(e))
            ec = -1

    return ec, lines

def removeFiles(dir, _fileList):
    """
    Remove files from the work dir
    """

    ec = 0
    found = 0
    for _file in _fileList:
        if os.path.isfile("%s/%s" % (dir, _file)):
            try:
                os.remove("%s/%s" % (dir, _file))
            except Exception, e:
                tolog("Failed to remove file: %s/%s, %s" % (dir, _file, e))
                ec = 1
            else:
                tolog("Removed file: %s/%s" % (dir, _file))
                found += 1
    if found > 0:
        tolog("Removed %d/%d file(s)" % (found, len(_fileList)))

    return ec

def createPoolFileCatalog(file_list, pfc_name="PoolFileCatalog.xml", forceLogical=False):
    """
    Create the PoolFileCatalog.xml
    file_list = { guid1 : sfn1, ... }
    Adapted from R. Walker's code
    """

    outxml = ''
    if len(file_list) == 0:
        tolog('No input files so no PFC created')
    else:
        dom = minidom.getDOMImplementation()
        doctype = dom.createDocumentType("POOLFILECATALOG","","InMemory")
        doc = dom.createDocument(None, "POOLFILECATALOG", doctype)
        root = doc.documentElement
        doc.appendChild(root)

        # Prepare plain text as can`t trust minidom on python <2.3
        pfc_text = '<?xml version="1.0" ?>\n'
        pfc_text += '<!-- Edited By Panda Pilot3 -->\n'
        pfc_text += '<!DOCTYPE POOLFILECATALOG  SYSTEM "InMemory">\n'
        pfc_text += '<POOLFILECATALOG>\n'

        # Strip .N because stagein makes soft link, and params have no .N
        for guid in file_list.keys():
            sfn = file_list[guid]
            ftype='ROOT_All'

            _file = doc.createElement("File")
            _file.setAttribute('ID', guid)
            root.appendChild(_file)

            # physical element - file in local directory without .N extension
            physical = doc.createElement("physical")
            _file.appendChild(physical)

            pfn = doc.createElement('pfn')
            pfn.setAttribute('filetype', ftype)
            pfn.setAttribute('name', sfn)
            physical.appendChild(pfn)

            if forceLogical:
                logical = doc.createElement('logical')
                logical.setAttribute('name', os.path.basename(sfn))
                _file.appendChild(logical)
                pfc_text += '  <File ID="%s">\n    <physical>\n      <pfn filetype="%s" name="%s"/>\n    </physical>\n    <logical>\n      <lfn name="%s"/>\n    </logical>\n  </File>\n' % (guid, ftype, sfn, os.path.basename(sfn))
            else:
                logical = doc.createElement('logical')
                _file.appendChild(logical)
                pfc_text += '  <File ID="%s">\n    <physical>\n      <pfn filetype="%s" name="%s"/>\n    </physical>\n    <logical/>\n  </File>\n' %\
                            (guid, ftype, sfn)

        pfc_text += '</POOLFILECATALOG>\n'
        tolog(str(doc.toxml()))
        tolog(pfc_text)

        try:
            f = open(pfc_name, 'w')
            f.write(pfc_text)
        except Exception, e:
            tolog("!!WARNING!!2999!! Could not create file %s: %s" % (pfc_name, e))
        else:
            tolog("Created %s" % (pfc_name))
            f.close()

        outxml = pfc_text

    return outxml

def replace(filename, stext, rtext):
    """ replace string stext with rtext in file filename """

    status = True
    try:
        _input = open(filename, "r")
    except Exception, e:
        tolog("!!WARNING!!4000!! Open failed with %s" % str(e))
        status = False
    else:
        try:
            output = open(filename + "_tmp", "w")
        except Exception, e:
            tolog("!!WARNING!!4000!! Open failed with %s" % str(e))
            status = False
            _input.close()
        else:
            for s in _input.xreadlines():
                output.write(s.replace(stext, rtext))
            _input.close()
            # rename tmp file and overwrite original file    
            try:
                os.rename(filename + "_tmp", filename)
            except Exception, e:
                tolog("!!WARNING!!4000!! Rename failed with %s" % str(e))
                status = False
            output.close()

    return status                

def dumpFile(filename, topilotlog=False):
    """ dump a given file to stdout or to pilotlog """

    if os.path.exists(filename):
        tolog("Dumping file: %s" % (filename))
        try:
            f = open(filename, "r")
        except Exception, e:
            tolog("!!WARNING!!4000!! Exception caught: %s" % str(e))
        else:
            i = 0
            for line in f.readlines():
                i += 1
                line = line.rstrip()
                if topilotlog:
                    tolog("%s" % (line))
                else:
                    print "%s" % (line)
            f.close()
            tolog("Dumped %d lines from file %s" % (i, filename))
    else:
        tolog("!!WARNING!!4000!! %s does not exist" % (filename))

def getDirectAccessDic(qdata):
    """ return the directAccess dictionary in case the site supports direct access / file stager """
    # task: create structure
    # directAccess = {
    #                  'oldPrefix' : 'gsiftp://osgserv04.slac.stanford.edu/xrootd/atlas',
    #                  'newPrefix' : 'root://atl-xrdr.slac.stanford.edu:1094//atlas/xrootd',
    #                  'useCopyTool' : False,
    #                  'directIn' : True
    #                  'useFileStager' : True
    #                }
    # from queuedata variable copysetup
    # copysetup = setup_string^oldPrefix^newPrefix^useFileStager^directIn
    # example:
    # copysetup=^gsiftp://osgserv04.slac.stanford.edu/xrootd/atlas^root://atl-xrdr.slac.stanford.edu:1094//atlas/xrootd^False^True
    # (setup_string not used)
    # (all cases tested)
    # qdata = 'whatever^gsiftp://osgserv04.slac.stanford.edu/xrootd/atlas^root://atl-xrdr.slac.stanford.edu:1094//atlas/xrootd^False^True'
    # qdata = '^gsiftp://osgserv04.slac.stanford.edu/xrootd/atlas^root://atl-xrdr.slac.stanford.edu:1094//atlas/xrootd^False^True'
    # qdata = 'gsiftp://osgserv04.slac.stanford.edu/xrootd/atlas^root://atl-xrdr.slac.stanford.edu:1094//atlas/xrootd^False^True'
    # qdata = 'setup^gsiftp://osgserv04.slac.stanford.edu/xrootd/atlas^root://atl-xrdr.slac.stanford.edu:1094//atlas/xrootd^False^False^False'
    # qdata = '' or 'whatever'
    # For TURL PFC creation, the copysetup has the following structure
    # copysetup = setup_string^useFileStager^directIn
    directAccess = None

    if qdata.find('^') > -1:
        n = qdata.count('^')
        i = 0
        # protect against a forgotten inital ^ in case the setup_string is empty!
        if n >= 2 and n <= 5:
            # read data
            data = qdata.split('^')

            # get the setup file (actually not used here)
            # _setup = data[0]

            if n != 2:
                # get file transfer prefices
                i += 1
                oldPrefix = data[i]
                i += 1
                newPrefix = data[i]
            else:
                oldPrefix = ""
                newPrefix = ""

            # get file stager mode
            i += 1
            if data[i].lower() == 'true':
                useFileStager = True
                useCopyTool = False
            else:
                useFileStager = False
                useCopyTool = True

            # get direct access mode
            i += 1
            if data[i].lower() == 'true':
                directIn = True
                useCopyTool = False
            else:
                directIn = False
                if not useFileStager:
                    useCopyTool = True
                else:
                    useCopyTool = False
#                if useFileStager:
#                    tolog("!!WARNING!!2999!! direct access mode reset to True (can not be False in combination with file stager mode)")
#                    directIn = True
#                    useCopyTool = False
#                else:
#                    directIn = False
#                    useCopyTool = True

            # in case copysetup contains a third boolean (old)
            if n == 5:
                tolog("!!WARNING!!2999!! Update schedconfig to use new direct access format: copysetup = setup_string^oldPrefix^newPrefix^useFileStager^directIn")
                if data[n].lower() == 'true':
                    useFileStager = True
                    directIn = True
                    useCopyTool = False
                else:
                    useFileStager = False
                    directIn = False
                    useCopyTool = True

            # create structure
            directAccess = {
                'oldPrefix': oldPrefix,
                'newPrefix': newPrefix,
                'useCopyTool': useCopyTool,
                'directIn': directIn,
                'useFileStager': useFileStager
                }
            tolog("directAccess: %s" % str(directAccess))
        else:
            tolog("!!WARNING!!4000!! copysetup has wrong format: %s" % (qdata))
    else:
        # do nothing, don't care about the copysetup right now (only later in Mover)
        pass

    return directAccess

def getErrors(filename):
    """ get all !!TEXT!!NUMBER!!... errors from file """

    ret = ""
    try:
        f = open(filename)
        lines = f.readlines()
        f.close()
    except Exception, e:
        tolog("!!WARNING!!4000!! could not open/read file: %s" % str(e))
    else:
        p = r"!!(\S+)!!\d+!!"
        pattern = re.compile(p)
        for line in lines:
            if re.findall(pattern, line):
                ret += line

    return ret            

def getLFN(pfn, lfnlist):
    """
    get the local file name from the xml, and ignore any trailing __DQ2-parts
    e.g. HITS.017771._00188.pool.root__DQ2-1200097946 -> HITS.017771._00188.pool.root
    """

    lfn = ""
    for lfn in lfnlist:
        bpfn = os.path.basename(pfn)
        if bpfn[:len(lfn)] == lfn:
            break
    return lfn

def makeTransRegReport(all_transferred, some_transferred, latereg, nr_transferred, nr_files, fail, ec, ret, fields):
    """ make the transfer and registration report """

    error = PilotErrors()

    tolog("")
    tolog("..Transfer and registration report.........................................................................")
    tolog(". Mover has finished")

    if all_transferred and not latereg:
        if nr_files > 1:
            tolog(". All (%d) files have been transferred and registered" % (nr_files))
        else:
            tolog(". The single file has been transferred and registered")
    elif all_transferred and latereg:
        if nr_files > 1:
            tolog(". All (%d) files have been transferred but not registered" % (nr_files))
            tolog(". The files will be registrered by a later pilot if job recovery is supported,")
        else:
            tolog(". The single file has been transferred but not registered")
            tolog(". The file will be registrered by a later pilot if job recovery is supported,")
        tolog(". otherwise this job will fail")
    elif some_transferred and latereg:
        tolog(". Some files (%d/%d) were transferred but no file was registered" % (nr_transferred, nr_files))
        tolog(". The remaining files will be transferred and registrered by a later pilot if job recovery is supported,")
        tolog(". otherwise this job will fail")
    elif some_transferred and not latereg:
        tolog(". Some files (%d/%d) were transferred and registered" % (nr_transferred, nr_files))
        tolog(". The remaining files will be transferred and registrered by a later pilot if job recovery is supported,")
        tolog(". otherwise this job will fail")
    elif not some_transferred:
        tolog(". No files (%d/%d) were transferred or registered" % (nr_transferred, nr_files))
        if nr_files > 1:
            tolog(". The files will be transferred and registrered by a later pilot if job recovery is supported,")
        else:
            tolog(". The file will be transferred and registrered by a later pilot if job recovery is supported,")
            tolog(". otherwise this job will fail")
    else:
        tolog(". Mover has finished")

    if fail != 0:
        tolog(". File transfer exit code                       : (%d, %s)" % (fail, error.getErrorStr(fail)))
    else:
        tolog(". File transfer exit code                       : (%d, <no error>)" % (fail))
        
    if some_transferred:
        tolog(". File registration return values               : (%d, %s, %s)" %\
              (ec, error.getErrorStr(ec), str(ret)))

    tolog(". Put function will return fields               : %s" % str(fields))
    tolog(". Transfer and registration report produced at  : %s" % timeStamp())
    tolog("...........................................................................................................")
    tolog("")

def hasBeenTransferred(fields):
    """ determine whether files were successfully transferred """

    status = False
    s = 0
    # the fields will all be empty if no files were transferred
    for field in fields:
        s += len(field)
    if s > 0:
        status = True
    return status

def removeSRMInfo(f0):
    """ remove any SRM info from the f0 string """

    from SiteMover import SiteMover
    fields0 = ""
    for pfns in f0.split("+"):
        if pfns != "":
            pfns = SiteMover.stripPortAndVersion(pfns)
            fields0 += "%s+" % (pfns)

    # remove any trailing +-sign
    if fields0[-1] == "+":
        fields0 = fields0[:-1]

    if fields0 == "":
        fields0 = f0
    if f0 != fields0:
        tolog("removeSRMInfo() has updated %s to %s" % (f0, fields0))

    return fields0

def lateRegistration(ub, job, type="unknown"):
    """ late registration used by the job recovery """

    # function will return True if late registration has been performed, False if it failed
    # and None if there is nothing to do
    status = False
    latereg = False
    fields = None

    # protect against old jobState files which may not have the new variables
    try:
        tolog("type: %s" % (type))
        if type == "output":
            if job.output_latereg == "False":
                latereg = False
            else:
                latereg = True
            fields = job.output_fields
        elif type == "log":
            if job.log_latereg == "False":
                latereg = False
            else:
                latereg = True
            fields = job.log_field
        else:
            tolog("!!WARNING!!4000!! Unknown id type for registration: %s" % (type))
            tolog("!!WARNING!!4000!! Skipping late registration step")
            pass
    except Exception, e:
        tolog("!!WARNING!!4000!! Late registration has come upon an old jobState file - can not perform this step: %s" % str(e))
        pass
    else:
        tolog("latereg: %s" % str(latereg))
        tolog("fields: %s" % str(fields))
        # should late registration be performed?
#        if latereg:
#            ec, ret = registerFiles(fields, ub=ub)
#            if ec == 0:
#                tolog("registerFiles done")
#                status = True
#            else:
#                tolog("!!WARNING!!4000!! File registration returned: (%d, %s)" % (ec, ret))

    if not latereg:
        tolog("Nothing to register (%s)" % (type))
        return None
    else:
        return status

def isAnalysisJob(trf):
    """ Determine whether the job is an analysis job or not """

    if (trf.startswith('https://') or trf.startswith('http://')):
        analysisJob = True
    else:
        analysisJob = False

    return analysisJob

def timedCommand(cmd, timeout=300):
    """ Protect cmd with timed_command """

    tolog("Executing command: %s (protected by timed_command, timeout: %d s)" % (cmd, timeout))
    t0 = os.times()
    try:
        exitcode, telapsed, cout, cerr = timed_command(cmd, timeout)
    except Exception, e:
        pilotErrorDiag = 'timed_command() threw an exception: %s' % str(e)
        tolog("!!WARNING!!2220!! %s" % pilotErrorDiag)            
        exitcode = 1
        output = str(e)
        t1 = os.times()
        telapsed = int(round(t1[4] - t0[4]))
    else:
        if cerr != "":
            tolog("!!WARNING!!2220!! Timed command stderr: %s" % (cerr))
        output = cout
    tolog("Elapsed time: %d" % (telapsed))

    if telapsed >= timeout:
        tolog("!!WARNING!!2220!! Command timed out")
        output += " (timed out)"

    # timed_command adds a trailing \n, remove it
    if output.endswith('\n'):
        output = output[:-1]

    return exitcode, output

def stringToFields(jobFields):
    """ Convert a jobState string to a fields array """

    jobFields = jobFields.replace('[','').replace(']','')
    jobFields = jobFields.replace("\'","")
    rf = jobFields.split(',')
    fields = []
    for f in rf:
        fields += [f.strip()]

    return fields

def readpar(parameter, alt=False):
    """ Read 'parameter' from queuedata via SiteInformation class """

    from SiteInformation import SiteInformation
    si = SiteInformation()

    return si.readpar(parameter, alt=alt)

def getBatchSystemJobID():
    """ return the batch system job id (will be reported to the server) """

    # BQS (e.g. LYON)
    if os.environ.has_key("QSUB_REQNAME"):
        return "BQS", os.environ["QSUB_REQNAME"]
    # BQS alternative
    if os.environ.has_key("BQSCLUSTER"):
        return "BQS", os.environ["BQSCLUSTER"]
    # Torque
    if os.environ.has_key("PBS_JOBID"):
        return "Torque", os.environ["PBS_JOBID"]
    # LSF
    if os.environ.has_key("LSB_JOBID"):
        return "LSF", os.environ["LSB_JOBID"]
    # Sun's Grid Engine 
    if os.environ.has_key("JOB_ID"):
        return "Grid Engine", os.environ["JOB_ID"]
    # Condor (variable sent through job submit file)
    if os.environ.has_key("clusterid"):
        return "Condor", os.environ["clusterid"]
    # SLURM
    if os.environ.has_key("SLURM_JOB_ID"):
        return "SLURM", os.environ["SLURM_JOB_ID"]
    
#    # Condor (id unknown)
#    if os.environ.has_key("_CONDOR_SCRATCH_DIR"):
#        return "Condor", "(unknown clusterid)"
    
    return None, ""

def touch(filename):
    """ touch a file """

    if not os.path.isfile(filename):
        try:
            os.system("touch %s" % (filename))
        except Exception, e:
            tolog("!!WARNING!!1000!! Failed to touch file: %s" % str(e))
        else:
            tolog("Lock file created: %s" % (filename))

def createLockFile(jobrec, workdir, lockfile="LOCKFILE"):
    """
    Site workdir protection to prevent the work dir from being deleted by the cleanup
    function if pilot fails to register the log
    """

    # only try to create a lock file if it doesn't exist already
    # do not bother to create it if the site doesn't allow for job recovery
    f = "%s/%s" % (workdir, lockfile)
    if lockfile == "LOCKFILE":
        if jobrec:
            touch(f)
    else:
        touch(f)

def checkLockFile(workdir, lockfile):
    """checks if a lockfile exists in path
    workdir/lockfile
    """
    f = '%s/%s' % (workdir, lockfile)
    return os.path.isfile(f)


def verifyTransfer(workdir, verbose=True):
    """ verify that all files were transferred by checking the existance of the ALLFILESTRANSFERRED lockfile """

    status = False
    fname = "%s/ALLFILESTRANSFERRED" % (workdir)
    if os.path.exists(fname):
        if verbose:
            tolog("Verified: %s" % (fname))
        status = True
    else:
        if verbose:
            tolog("Transfer verification failed: %s (file does not exist)" % (fname))

    return status

def removeLEDuplicates(logMsg):
    """ identify duplicated messages in the log extracts and remove them """

    # first create a new log extracts list that does not have the time stamps
    # (which will be different for the otherwise different messages)
    # E.g.:
    # 31 Mar 2008 01:32:37| !!WARNING!!1999!! Could not read modification time of ESD.020072._04023.pool.root.9
    # 31 Mar 2008 02:03:08| !!WARNING!!1999!! Could not read modification time of ESD.020072._04023.pool.root.9
    # should only be printed once,
    # 31 Mar 2008 01:32:37| !!WARNING!!1999!! Could not read modification time of ESD.020072._04023.pool.root.9

    log_extracts_list = logMsg.split('\n')

    # create a temporary list with stripped timestamp fields
    log_extracts_tmp = []
    pattern = re.compile(r"(\d+ [A-Za-z]+ \d+ \d+:\d+:\d+\|)")
    for line in log_extracts_list:
        # id the time stamp
        found = re.findall(pattern, line)
        if len(found) > 0:
            # remove any time stamp
            line = line.replace(found[0], '')
        log_extracts_tmp.append(line)

    # remove duplicate lines and create an index list to know where the original line was
    # (we want to bring back the timestamp)
    # do not use dictionaries since they are not sorted
    i = 0
    log_extracts_index = []
    log_extracts_tmp2 = []
    for line in log_extracts_tmp:
        if line not in log_extracts_tmp2:
            log_extracts_index.append(i)
            log_extracts_tmp2.append(line)
        i += 1

    # create the final list
    log_extracts_tmp = []
    for index in log_extracts_index:
        log_extracts_tmp.append(log_extracts_list[index])

    # return the stripped logMsg
    return "\n".join(log_extracts_tmp)

def writeToFile(filename, s):
    """ write string s to file """

    try:
        f = open(filename, "w")
    except Exception, e:
        tolog("!!WARNING!!2990!! Could not open: %s, %s" % (filename, str(e)))
    else:
        f.write("%s" % (s))
        f.close()
        tolog('Wrote string "%s" to file: %s' % (s, filename))

def readCodeFromFile(filename):
    """ read exit code from file <workdir>/EXITCODE """

    ec = 0
    if os.path.exists(filename):
        try:
            f = open(filename, "r")
        except Exception, e:
            tolog("Failed to open %s: %s" % (filename, str(e)))
        else:
            ec = int(f.read())
            tolog("Found code %d in file %s" % (ec, filename))
            f.close()
    else:
        tolog("No code to report (file %s does not exist)" % (filename))
    return ec

def readStringFromFile(filename):
    """ read exit code from file <workdir>/EXITCODE """

    s = ""
    if os.path.exists(filename):
        try:
            f = open(filename, "r")
        except Exception, e:
            tolog("Failed to open %s: %s" % (filename, str(e)))
        else:
            s = f.read()
            tolog("Found string %s in file %s" % (s, filename))
            f.close()
    else:
        tolog("No string to report (file %s does not exist)" % (filename))
    return s

def verifyQueuedata(queuename, filename, _i, _N, url):
    """ check if the downloaded queuedata has the proper format """

    hasQueuedata = False
    try:
        f = open(filename, "r")
    except Exception, e:
        tolog("!!WARNING!!1999!! Open failed with %s" % str(e))
    else:
        output = f.read()
        f.close()
        if not ('appdir' in output and 'copytool' in output):
            if len(output) == 0:
                tolog("!!WARNING!!1999!! curl command returned empty queuedata (wrong queuename %s?)" % (queuename))
            else:
                tolog("!!WARNING!!1999!! Attempt %d/%d: curl command did not return valid queuedata from config DB server %s" %\
                      (_i, _N, url))
                output = output.replace('\n', '')
                output = output.replace(' ', '')
                tolog("!!WARNING!!1999!! Output begins with: %s" % (output[:64]))
            try:
                os.remove(filename)
            except Exception, e:
                tolog("!!WARNING!!1999!! Failed to remove file %s: %s" % (filename, str(e)))
        else:
            # found valid queuedata info, break the for-loop
            tolog("schedconfigDB returned: %s" % (output))
            hasQueuedata = True

    return hasQueuedata

def isSameType(trf, userflag):
    """ is the lost job of same type as the current pilot? """

    # treat userflag 'self' as 'user'
    if userflag == 'self':
        userflag = 'user'

    if (isAnalysisJob(trf) and userflag == 'user') or \
           (not isAnalysisJob(trf) and userflag != 'user'):
        sametype = True
        if userflag == 'user':
            tolog("Lost job is of same type as current pilot (analysis pilot, lost analysis job trf: %s)" % (trf))
        else:
            tolog("Lost job is of same type as current pilot (production pilot, lost production job trf: %s)" % (trf))
    else:
        sametype = False
        if userflag == 'user':
            tolog("Lost job is not of same type as current pilot (analysis pilot, lost production job trf: %s)" % (trf))
        else:
            tolog("Lost job is not of same type as current pilot (production pilot, lost analysis job trf: %s)" % (trf))

    return sametype

def verifyProxyValidity(limit):
    """ make sure that we have a long lasting proxy before asking for a job """

    # (envsetup will be set in verifyProxy in this case)
    from SiteMover import SiteMover
    return SiteMover.verifyProxy(envsetup="", limit=limit)

def getGuidsFromXML(dir, id=None, filename=None, metadata=""):
    """ extract the guid matching the filename from the xml, or all guids if filename not set """

    guids = []

    if metadata != "":
        metadata_filename = metadata
    else:
        # are we in recovery mode? then id is set
        if id:
            metadata_filename = "%s/metadata-%s.xml" % (dir, repr(id))
        else:
            metadata_filename = "%s/metadata.xml" % (dir)

    xmldoc = minidom.parse(metadata_filename)
    _fileList = xmldoc.getElementsByTagName("File")
    for thisfile in _fileList:
        gpfn = str(thisfile.getElementsByTagName("lfn")[0].getAttribute("name"))
        if (filename and gpfn == filename) or (not filename):
            guid = str(thisfile.getAttribute("ID"))
            guids.append(guid)

    return guids

def addToSkipped(lfn, guid):
    """ add metadata for skipped file """

    ec = 0
    try:
        # append to skipped.xml file
        fd = open("skipped.xml", "a")
    except Exception, e:
        tolog("!!WARNING!!2999!! Exception caught: %s" % str(e))
        ec = -1
    else:
        fd.write('  <File ID="%s">\n' % (guid))
        fd.write("    <logical>\n")
        fd.write('      <lfn name="%s"/>\n' % (lfn))
        fd.write("    </logical>\n")
        fd.write("  </File>\n")
        fd.close()
    return ec

def addSkippedToPFC(fname, skippedfname):
    """ add skipped input file info to metadata.xml """

    ec = 0
    try:
        fd = open(skippedfname, "r")
    except Exception, e:
        tolog("!!WARNING!!2999!! %s" % str(e))
        ec = -1
    else:
        skippedXML = fd.read()
        fd.close()
        try:
            fdPFC = open(fname, "r")
        except Exception, e:
            tolog("!!WARNING!!2999!! %s" % str(e))
            ec = -1
        else:
            PFCXML = fdPFC.read()
            fdPFC.close()

    if ec == 0:
        # add the skipped file info to the end of the PFC
        PFCXML = PFCXML.replace("</POOLFILECATALOG>", skippedXML)
        PFCXML += "</POOLFILECATALOG>\n"

        # move the old PFC and create a new PFC
        try:
            os.system("mv %s %s.BAK2" % (fname, fname))
        except Exception, e:
            tolog("!!WARNING!!2999!! %s" % str(e))
            ec = -1
        else:
            try:
                fdNEW = open(fname, "w")
            except Exception, e:
                tolog("!!WARNING!!2999!! %s" % str(e))
                ec = -1
            else:
                fdNEW.write(PFCXML)
                fdNEW.close()
                tolog("Wrote updated XML with skipped file info:\n%s" % (PFCXML))
    return ec

def verifyReleaseString(release):
    """ Verify that the release (or homepackage) string is set """

    if release == None:
        release = ""
    release = release.upper()
    if release == "":
        release = "NULL"
    if release == "NULL":
        tolog("Detected unset (NULL) release/homepackage string")
    return release

def getSwbase(appdir, release, homePackage, processingType, cmtconfig):
    """ return the swbase variable """
    # appdir comes from thisSite.appdir (might not be set)
    # release info is needed to figure out the correct path to use when schedconfig.appdir is set

    swbase = ""

    # verify the validity of the release string in case it is not set (as can be the case for prun jobs)
    release = verifyReleaseString(release)

    region = readpar('region')
    if region == 'Nordugrid':
        if os.environ.has_key('RUNTIME_CONFIG_DIR'):
            _swbase = os.environ['RUNTIME_CONFIG_DIR']
            if os.path.exists(_swbase):
                swbase = _swbase
    elif os.environ.has_key('VO_ATLAS_SW_DIR'):
        # use the appdir from queuedata if available
        scappdir = readpar('appdir')
        # protect against complex appdir form
        if "|" in scappdir and appdir != "":
            from SiteInformation import SiteInformation
            si = SiteInformation()
            ec, _scappdir = si.extractAppdir(scappdir, processingType, homePackage)
            if ec != 0:
                tolog("!!WARNING!!2222!! Failed to extract complex appdir: %d, %s, %s, %s" % (ec, scappdir, processingType, homePackage))
            else:
                scappdir = _scappdir
            tolog("Using alternative appdir=%s" % (scappdir))
#            scappdir = appdir
        elif scappdir != "":
            tolog("Got a plain appdir from queuedata: %s" % (scappdir))
        else:
            tolog("Appdir not set in queuedata")

        if scappdir != "": # as of Dec 2009 appdir != '' only for CERN sites
            # CERN-RELEASE:
            # appdir=/afs/cern.ch/atlas/software/releases (full path to releases)
            # CERN-UNVALID:
            # appdir=/afs/cern.ch/atlas/software/unvalidated/caches (full path to releases)
            # CERN-BUILDS:
            # appdir=/afs/cern.ch/atlas/software/builds (already points to the latest release, do not add release)
            # CERN-PROD:
            # appdir=/afs/cern.ch/atlas/software/releases (full path to releases)
            # Release can be added to appdir for CERN-RELEASE, CERN-UNVALID, CERN-PROD, but not to CERN-BUILDS
            if os.path.exists(os.path.join(scappdir, release)):
                swbase = scappdir
            else:
                # the build queue is special
                if scappdir[-len('builds'):] == 'builds':
                    swbase = scappdir
                # backup, old cases
                elif os.path.exists(os.path.join(scappdir, 'software/releases')):
                    swbase = os.path.join(scappdir, 'software/releases')
                # backup, for remaining LCG sites, only 'software' needs to be added
                else:
                    swbase = os.path.join(scappdir, 'software')
                    if not os.path.exists(swbase):
                        swbase = scappdir
        else:
            tolog("VO_ATLAS_SW_DIR=%s" % (os.environ['VO_ATLAS_SW_DIR']))

            # primary software base (search appdir for alternatives)
            swbase = os.environ['VO_ATLAS_SW_DIR'] + '/software'
    else:
        # for non-LCG sites
        if appdir.find('atlas_app/atlas_rel') < 0:
            swbase = appdir + '/atlas_app/atlas_rel'
        else:
            swbase = appdir

    # add cmtconfig sub dir for CERNVM and for cvmfs systems
    _cmtconfig = cmtconfig.replace("-", "_")
    _swbase = os.path.join(swbase, _cmtconfig)
    if os.path.exists(_swbase) and release != "" and release.upper() != "NULL":
        swbase = _swbase

    # uncomment if testing interactively at lxplus
    # swbase = appdir
    return swbase.replace('//','/')

class _Curl:
    """ curl class """

    # constructor
    def __init__(self):
        # path to curl
        self.path = 'curl'
        # verification of the host certificate
        self._verifyHost = True
        # request a compressed response
        self.compress = True
        # SSL cert/key
        from SiteInformation import SiteInformation
        si = SiteInformation()
        self.sslCert = si.getSSLCertificate()
        self.sslKey = self.sslCert
        # CA cert dir
        self.sslCertDir = si.getSSLCertificatesDirectory()

    # GET method
    def get(self, url, data, path):
        # make command
        com = '%s --silent --get' % self.path
        com += ' --connect-timeout 100 --max-time 120'
        if not self._verifyHost:
            com += ' --insecure'
        if self.compress:
            com += ' --compressed'
        if self.sslCertDir != '':
            com += ' --capath %s' % self.sslCertDir
        if self.sslCert != '':
            com += ' --cert %s --cacert %s' % (self.sslCert, self.sslCert)
        if self.sslKey != '':
            com += ' --key %s' % self.sslKey
        #com += ' --verbose'
        # data
        strData = ''
        for key in data.keys():
            strData += 'data="%s"\n' % urllib.urlencode({key:data[key]})
        # write data to temporary config file
        # tmpName = commands.getoutput('uuidgen')
        tmpName = '%s/curl.config' % (path)
        try:
            tmpFile = open(tmpName, 'w')
            tmpFile.write(strData)
            tmpFile.close()
        except IOError, e:
            tolog("!!WARNING!!2999!! %s" % str(e))
        if os.path.exists(tmpName):
            com += ' --config %s' % tmpName
        else:
            tolog("!!WARNING!!2999!! Can not set --config option since curl.config could not be created, curl will fail")
        com += ' %s' % url
        # execute
        tolog("Executing command: %s" % (com))
        try:
            ret = commands.getstatusoutput(com)
        except Exception, e:
            tolog("!!WARNING!!1111!! Caught exception from curl command: %s" % (e))
            ret = [-1, e]
        # remove temporary file
        #os.remove(tmpName)
        return ret

    # POST method
    def post(self, url, data, path):
        # make command
        com = '%s --silent --show-error' % self.path
        com += ' --connect-timeout 100 --max-time 120'
        if not self._verifyHost:
            com += ' --insecure'
        if self.compress:
            com += ' --compressed'
        if self.sslCertDir != '':
            com += ' --capath %s' % self.sslCertDir
        if self.sslCert != '':
            com += ' --cert %s --cacert %s' % (self.sslCert, self.sslCert)
        if self.sslKey != '':
            com += ' --key %s' % self.sslKey
        #com += ' --verbose'
        # data
        strData = ''
        for key in data.keys():
            strData += 'data="%s"\n' % urllib.urlencode({key:data[key]})
        # write data to temporary config file
        tmpName = '%s/curl.config' % (path)
        try:
            tmpFile = open(tmpName,'w')
            tmpFile.write(strData)
            tmpFile.close()
        except IOError, e:
            tolog("!!WARNING!!2999!! %s" % str(e))
        if os.path.exists(tmpName):
            com += ' --config %s' % tmpName
        else:
            tolog("!!WARNING!!2999!! Can not set --config option since curl.config could not be created, curl will fail")
        com += ' %s' % url
        # execute
        tolog("Executing command: %s" % (com))
        try:
            ret = commands.getstatusoutput(com)
        except Exception, e:
            tolog("!!WARNING!!1111!! Caught exception from curl command: %s" % (e))
            ret = [-1, e]
        # remove temporary file
        #os.remove(tmpName)        
        return ret

    # PUT method
    def put(self, url, data):
        # make command
        com = '%s --silent' % self.path
        if not self._verifyHost:
            com += ' --insecure'
        if self.compress:
            com += ' --compressed'
        if self.sslCertDir != '':
            com += ' --capath %s' % self.sslCertDir
        if self.sslCert != '':
            com += ' --cert %s --cacert %s' % (self.sslCert, self.sslCert)
        if self.sslKey != '':
            com += ' --key %s' % self.sslKey
        #com += ' --verbose'
        # emulate PUT 
        for key in data.keys():
            com += ' -F "%s=@%s"' % (key,data[key])
        com += ' %s' % url
        # execute
        tolog("Executing command: %s" % (com))
        try:
            ret = commands.getstatusoutput(com)
        except Exception, e:
            tolog("!!WARNING!!1111!! Caught exception from curl command: %s" % (e))
            ret = [-1, e]
        return ret

    def verifyHost(self, verify):
        # set _verifyHost
        self._verifyHost = verify

# send message to dispatcher
def toServer(baseURL, cmd, data, path):
    """ sends 'data' using command 'cmd' to the dispatcher """

    try:
        tpre = datetime.datetime.utcnow()
    except:
        pass
    tolog("toServer: cmd = %s" % (cmd))
    tolog("toServer: len(data) = %d" % len(data))
    tolog("data = %s" % str(data))

    # instantiate curl
    curl = _Curl()

    # use insecure for dev server
    #if 'voatlas220' in baseURL:
    #    curl.verifyHost(False)

    # execute
    if cmd == "getStatus":
        url = baseURL
    else:
        url = baseURL + '/' + cmd
    curlstat, response = curl.post(url, data, path)
    try:
        tpost = datetime.datetime.utcnow()
        tolog("Elapsed seconds: %d" % ((tpost-tpre).seconds))
    except:
        pass
    try:
        if curlstat == 0:
            # parse response message
            outtxt = response.lower()
            if outtxt.find('<html>') > 0:
                if outtxt.find('read timeout') > 0:                   
                    tolog("!!WARNING!!2999!! Timeout on dispatcher exchange")
                else:
                    tolog("!!WARNING!!2999!! HTTP error on dispatcher exchange")
                tolog("HTTP output: %s" % (response))
                return EC_Failed, None, None

            # create the parameter list from the dispatcher response
            data, response = parseDispatcherResponse(response)

            status = int(data['StatusCode'])
            if status != 0:
                # pilotErrorDiag = getDispatcherErrorDiag(status)
                tolog("Dumping %s/curl.config file:" % (path))
                dumpFile('%s/curl.config' % (path), topilotlog=True)
        else:
            tolog("!!WARNING!!2999!! Dispatcher message curl error: %d " % (curlstat))
            tolog("Response = %s" % (response))
            tolog("Dumping curl.config file:")
            dumpFile('%s/curl.config' % (path), topilotlog=True)
            return curlstat, None, None
        if status == 0:
            return status, data, response
        else:
            return status, None, None
    except:
        _type, value, traceBack = sys.exc_info()
        tolog("ERROR %s : %s %s" % (cmd, _type, value))
        return EC_Failed, None, None

def getPilotToken(tofile=False):
    """ read the pilot token from file """

    pilottoken = None
    filename = "pilottoken.txt"
    if os.path.exists(filename):
        try:
            f = open(filename, "r")
        except Exception, e:
            tolog("!!WARNING!!2999!! Could not open pilot token file: %s" % str(e), tofile=tofile)
        else:
            try:
                pilottoken = f.read()
            except Exception, e:
                tolog("!!WARNING!!2999!! Could not read pilot token: %s" % str(e), tofile=tofile)
            else:
                f.close()
                tolog("Successfully read pilot token", tofile=tofile)
                try:
                    os.remove(filename)
                except Exception, e:
                    tolog("!!WARNING!!2999!! Could not remove pilot token file: %s" % str(e), tofile=tofile)
                else:
                    tolog("Pilot token file has been removed", tofile=tofile)

    return pilottoken

def removeSubFromResponse(response):
    """ Remove any _subNNN strings from the dataset variables (realDatasets and destinationDblock) """

    tolog("response='%s'"%(response))
    pattern = re.compile('\S+(\_sub[0-9]+)')
    match = pattern.match(response)
    if match:
        # strip away the _subNNN string
        try:
            response = response.replace(match.group(1), '')
        except Exception, e:
            tolog("!!WARNING!!1119!! Failed to remove _sub string (%s) from dispatcher response: %s" % (match.group(1), e))
        else:
            tolog("Updated dispatcher response (removed %s): %s" % (match.group(1), response))
    else:
        tolog("Found no _subNNN string in the dispatcher response")

    return response

def parseDispatcherResponse(response):
    """ Create the parameter list from the dispatcher response """

# use this when listFilesInDataset usage is not needed any more (v 51b)
#    # remove any _subNNN strings if necessary (from dataset names)
#    if "_sub" in response:
#        response = removeSubFromResponse(response)

    parList = cgi.parse_qsl(response, keep_blank_values=True)
    tolog("Dispatcher response: %s" % str(parList))

    data = {}
    for p in parList:
        data[p[0]] = p[1]

    return data, response

def grep(patterns, file_name):
    """ Search for the patterns in the given list in a file """
    # Example:
    # grep(["St9bad_alloc", "FATAL"], "athena_stdout.txt")
    # -> [list containing the lines below]
    #   CaloTrkMuIdAlg2.sysExecute()             ERROR St9bad_alloc
    #   AthAlgSeq.sysExecute()                   FATAL  Standard std::exception is caught

    matched_lines = []
    p = []
    for pattern in patterns:
        p.append(re.compile(pattern))

    try:
        f = open(file_name, "r")
    except IOError, e:
        tolog("!!WARNING!!2999!! %s" % str(e))
    else:
        while True:
            # get the next line in the file
            line = f.readline()
            if not line:
                break

            # can the search pattern be found
            for cp in p:
                if re.search(cp, line):
                    matched_lines.append(line)
        f.close()
    return matched_lines

def getJobReport(filename):
    """ Extract the job report from the stdout, or the last N lines """

    report = ""
    if os.path.exists(filename):
        pattern = re.compile("Job Report produced by")
        try:
            f = open(filename, "r")
        except IOError, e:
            tolog("!!WARNING!!1299!! %s" % str(e))
        else:
            matched_lines = []
            status = True
            first_report = True
            while status:
                # get the next line in the file
                line = f.readline()
                if not line:
                    break

                # find the start position of the job report and grab all remaining lines
                if re.search(pattern, line) and first_report:
                    # the job report is repeated, only grab it the second time it appears
                    first_report = False
                elif re.search(pattern, line) and not first_report:
                    # save the job report title line
                    line = line.replace("=====", "-")
                    matched_lines.append(line)
                    while True:
                        line = f.readline()
                        if not line:
                            status = False
                            break
                        matched_lines.append(line)

            # grab the last couple of lines in case the trf failed before the job report was printed
            if len(matched_lines) == 0:
                N = 10
                tolog("Job report could not be found in the payload stdout, will add the last %d lines instead for the log extracts" % (N))
                report = "- Last %d lines from %s -\n" % (N, filename)
                report = report + tail(filename, N)
            else:
                report = "".join(matched_lines)
            f.close()
    else:
        tolog("!!WARNING!!1299!! File %s does not exist" % (filename))

    return report

def tail(filename, number_of_lines):
    """ Grab the last N lines from a file """

    report = ""
    if os.path.exists(filename):
        try:
            # U is to open it with Universal newline support
            f = open(filename, "rU")
        except IOError, e:
            tolog("!!WARNING!!1299!! %s" % str(e))
        else:
            read_size = 1024
            offset = read_size
            f.seek(0, 2)
            file_size = f.tell()

            # abort if zero file size
            if file_size == 0:
                tolog("!!WARNING!!1299!! File %s has zero size" % (filename))
            else:
                # loop over file
                while True:
                    if file_size < offset:
                        offset = file_size
                    f.seek(-offset, 2)
                    read_str = f.read(offset)

                    try:
                        # Remove newline at the end
                        if read_str[offset - 1] == '\n':
                            read_str = read_str[:-1]
                        lines = read_str.split('\n')

                        # Got number_of_lines lines
                        if len(lines) >= number_of_lines:
                            report = "\n".join(lines[-number_of_lines:])
                            break
                    except Exception, e:
                        # the following message will be visible in the log extracts
                        report = "!!WARNING!!1299!! tail command caught an exception when reading payload stdout: %s" % str(e)
                        tolog(report)
                        break

                    # Reached the beginning
                    if offset == file_size:
                        report = read_str
                        break
                    offset += read_size
            f.close()
    else:
        tolog("!!WARNING!!1299!! File %s does not exist" % (filename))

    return report

def filterJobReport(report):
    """ Extract the exit and error code from the job report """

    filtered_report = ""
    filters = ["ExitCode", "ErrorCode"]
    patterns = []
    for _filter in filters:
        patterns.append(re.compile(_filter))

    # loop over the full report
    if report != "":
        header = True
        for line in report.split("\n"):
            # grab the header line
            if header:
                filtered_report += line + "\n"
                header = False
            # match the exit and error code lines
            for pattern in patterns:
                if re.search(pattern, line):
                    filtered_report += line + "\n"
    else:
        tolog("!!WARNING!!2999!! Found empty job report")

    return filtered_report

def removeDuplicates(seq):
    """ Order preserving duplicate removal """

    checked = []
    for entry in seq:
        if entry not in checked:
            checked.append(entry)
    return checked

def dumpOrderedItems(l):
    """ dump list l """

    _i = 0
    for item in l:
        _i += 1
        if item == "":
            tolog("%d. <empty>" % (_i))
        else:
            tolog("%d. %s" % (_i, item))

def getDatasetDict(outputFiles, destinationDblock, logFile, logFileDblock):
    """ Create a dataset dictionary """

    datasetDict = None

    # verify that the lists are of equal size
    if len(outputFiles) != len(destinationDblock):
        tolog("WARNING: Lists are not of same length: %s, %s" % (str(outputFiles), str(destinationDblock)))
    elif len(outputFiles) == 0:
        tolog("No output files for this job (outputFiles has zero length)")
    elif len(destinationDblock) == 0:
        tolog("WARNING: destinationDblock has zero length")        
    else:
        # verify that lists contains valid entries
        _l = [outputFiles, destinationDblock]
        ok = True
        for _list in _l:
            for _entry in _list:
                if _entry == "NULL" or _entry == "" or _entry == " " or _entry == None:
                    tolog("!!WARNING!!2999!! Found non-valid entry in list: %s" % str(_list))
                    ok = False
                    break

        if ok:
            # build the dictionary
            try:
                datasetDict = dict(zip(outputFiles, destinationDblock))
            except Exception, e:
                tolog("!!WARNING!!2999!! Exception caught in getDatasetDict(): %s" % str(e))
                datasetDict = None
            else:
                # add the log file info
                datasetDict[logFile] = logFileDblock

    return datasetDict

def getAtlasRelease(atlasRelease):
    """ return a list of the jobs' release(s) """

    if readpar('region') == 'Nordugrid':
        return os.environ['ATLAS_RELEASE'].split(",")
    else:
        return atlasRelease.split("\n")

def getFileGuid(metadata_filename, guid_filename):
    """ read the log guid from metadata """

    logFileGuid = ""

    if os.path.exists(metadata_filename):
        try:
            xmldoc = minidom.parse(metadata_filename)
            _fileList = xmldoc.getElementsByTagName("File")
            for thisfile in _fileList:
                lfn = str(thisfile.getElementsByTagName("lfn")[0].getAttribute("name"))
                _guid = str(thisfile.getAttribute("ID"))
                if guid_filename == lfn:
                    logFileGuid = _guid
                    tolog("Guid %s belongs to file %s" % (_guid, lfn))
        except Exception, e:
            tolog("!!WARNING!!2999!! Could not parse the metadata - guids unknown: %s" % (e))
    else:
        tolog("!!WARNING!!2999!! Could not locate %s, log file guid can not be verified" % (metadata_filename))

    return logFileGuid

def getChecksumCommand():
    """ return the site mover checksum command """

    # which checksum command should be used? query the site mover
    from SiteMoverFarm import getSiteMover
    sitemover = getSiteMover(readpar('copytool'), "")
    return sitemover.getChecksumCommand()

def tailPilotErrorDiag(pilotErrorDiag, size=256):
    """ Return the last 256 characters of pilotErrorDiag """

    try:
        return pilotErrorDiag[-size:]
    except Exception, e:
        tolog("Warning: tailPilotErrorDiag caught exception: %s" % str(e))
        return pilotErrorDiag

def getMaxInputSize(MB=False):
    """ Return a proper maxinputsize value """

    _maxinputsize = readpar('maxinputsize') # normally 14336 MB
    MAX_INPUT_FILESIZES = 14*1024*1024*1024 # 14 GB, 14336 MB (pilot default)
    MAX_INPUT_FILESIZES_MB = 14*1024 # 14336 MB (pilot default)
    if _maxinputsize != "":
        try:
            if MB: # convert to MB int
                _maxinputsize = int(_maxinputsize) # MB
            else: # convert to B int
                _maxinputsize = int(_maxinputsize)*1024*1024 # MB -> B
        except Exception, e:
            tolog("!!WARNING!!2999!! schedconfig.maxinputsize: %s" % str(e))
            if MB:
                _maxinputsize = MAX_INPUT_FILESIZES_MB
            else:
                _maxinputsize = MAX_INPUT_FILESIZES
    else:
        if MB:
            _maxinputsize = MAX_INPUT_FILESIZES_MB
        else:
            _maxinputsize = MAX_INPUT_FILESIZES

    if MB:
        tolog("Max input size = %d MB (pilot default)" % (_maxinputsize))
    else:
        tolog("Max input size = %d B (pilot default)" % (_maxinputsize))

    return _maxinputsize

def setHTTPProxies(sitename):
    """ Set up proxy env. variables for BNL and SLAC sites """

    if sitename.upper().find("BNL") != -1:
        os.environ["http_proxy"] = "http://squid.sec.bnl.local:3128/"
        os.environ["https_proxy"] = "http://squid.sec.bnl.local:3128/"
    elif sitename.upper().find("SLAC") != -1:
        os.environ["http_proxy"] = "http://atl-proxy.slac.stanford.edu:3128/"
        os.environ["https_proxy"] = "http://atl-proxy.slac.stanford.edu:3128/"

def checkSpecialEnvVars(sitename):
    """ Check special environment variables """
    # add further checks to this function if needed

    ec = 0

    # get error handler
    error = PilotErrors()

    # check if ATLAS_POOLCOND_PATH is set
    try:
        if os.environ.has_key('ATLAS_POOLCOND_PATH'):
            tolog("ATLAS_POOLCOND_PATH = %s" % (os.environ['ATLAS_POOLCOND_PATH']))
        else:
            tolog("ATLAS_POOLCOND_PATH not set by wrapper")
    except Exception, e:
        tolog("WARNING: os.environ.has_key failed: %s" % str(e))

    if os.environ.has_key("VO_ATLAS_SW_DIR") and not "CERNVM" in sitename and readpar('region') != "Nordugrid":
        vo_atlas_sw_dir = os.environ["VO_ATLAS_SW_DIR"]

        # on cvmfs the following dirs are symbolic links, so all tests are needed
        paths = [vo_atlas_sw_dir, os.path.join(vo_atlas_sw_dir, "software")]
        for path in paths:
            if os.path.exists(path):
                tolog("%s confirmed" % (path))
            else:
                tolog("!!FAILED!!1777!! %s does not exist" % (path))
                ec = error.ERR_NOSUCHFILE
                break

        # require that the "local" directory exists on cvmfs
        path = os.path.join(vo_atlas_sw_dir, "local")
        if "cvmfs" in path:
            if os.path.exists(path):
                tolog("%s confirmed" % (path))
            else:
                tolog("!!FAILED!!1777!! %s does not exist" % (path))
                ec = error.ERR_NOSUCHFILE
        else:
            tolog("Skipping verification of %s on non-cvmfs" % (path))

    # a missing RUCIO env var is harmless for now
    if os.environ.has_key("RUCIO_ACCOUNT"):
        tolog("Using RUCIO account: %s" % (os.environ["RUCIO_ACCOUNT"]))
    else:
        tolog("!!WARNING!!1114!! Pilot wrapper need to set RUCIO_ACCOUNT")

    return ec

def getTimeFloor(timefloor_default):
    """ Return a proper timefloor """
    # timefloor is the time limit within which the pilot is allowed to run multiple jobs
    # if at the end of a job, there is enough time (i.e. at least [timefloor] s left), the pilot
    # will ask for another job

    try:
        if timefloor_default != None:
            timefloor = timefloor_default
            tolog("(Overriding any schedconfig.timefloor with timefloor set by pilot option -C %d)" % (timefloor_default))
        else:
            timefloor = int(readpar('timefloor'))*60 # assumed to be in minutes, convert into seconds
    except:
        tolog("Timefloor not set in queuedata (multi-jobs disabled)")
        timefloor = 0
    else:
        _lower = 0
        _upper = 60*60*24 # one day
        if timefloor == 0:
            tolog("Timefloor set to zero in queuedata (multi-jobs disabled)")
        elif timefloor > _lower and timefloor <= _upper:
            tolog("Timefloor set to %d s" % (timefloor))
        else:
            tolog("Timefloor (%d s) out of limits (%d s, %d s) - multi-jobs disabled" % (timefloor, _lower, _upper))
            timefloor = 0

    return timefloor

def getCopysetup(mode="get"):
    """ extract a verified copysetup[in] script from queuedata """

    copysetup_tmp = readpar('copysetup')

    if mode == "get":
        _copysetup = readpar('copysetupin')
        if _copysetup == "":
            # not set, use same copysetup for stage-in as for stage-out
            _copysetup = copysetup_tmp
    else:
        _copysetup = copysetup_tmp

    # copysetup can contain ^-signs for remote i/o sites
    if _copysetup.find('^') > -1:
        copysetup = _copysetup.split('^')[0]
    else:
        copysetup = copysetup_tmp

    # make sure that the script exists
    if copysetup == "":
        tolog("No copysetup found")
    elif not os.path.exists(copysetup) and '^' not in copysetup:
        tolog("!!WARNING!!2998!! copysetup does not exist: %s (reset to empty string)" % (copysetup))
        copysetup = ""
    elif '^' in copysetup:
        tolog("No path in copysetup (%s, reset to empty string)" % (copysetup))
        copysetup = ""
    else:
        tolog("Extracted copysetup: %s" % (copysetup))

    return copysetup
    
def verifyLFNLength(outputFiles):
    """ Make sure that the LFNs are all within the allowed length """

    ec = 0
    pilotErrorDiag = ""
    error = PilotErrors()
    MAXFILENAMELENGTH = 150

    # loop over all output files
    for fileName in outputFiles:
        if len(fileName) > MAXFILENAMELENGTH:
            pilotErrorDiag = "LFN too long (length: %d, must be less than %d characters): %s" % (len(fileName), MAXFILENAMELENGTH, fileName)
            tolog("!!WARNING!!2994!! %s" % (pilotErrorDiag))
            ec = error.ERR_LFNTOOLONG
        else:
            tolog("LFN length verified for file %s" % (fileName))

    return ec, pilotErrorDiag

def getUlimitVMEM():
    """ return the VMEM limit (which should have been set by the wrapper) """

    mem = 0

    cmd = "ulimit -v"
    tolog("Executing command: %s" % (cmd))

    ec, res = commands.getstatusoutput(cmd)
    if ec != 0:
        tolog("WARNING: Failed to execute ulimit (using limit 0): %d, %s" % (ec, res))
    else:
        if res == "unlimited":
            tolog("Using limit 0 for unlimited")
        else:
            try:
                mem = int(res)
            except Exception, e:
                tolog("Using limit 0 since ulimit output could not be interpreted: %s" % (e))
            else:
                tolog("Using limit %d" % (mem))

    return mem

def getFileAccessInfo():
    """ return a tuple with all info about how the input files should be accessed """

    # default values
    oldPrefix = None
    newPrefix = None
    useFileStager = None
    directIn = None

    # move input files from local DDM area to workdir if needed using a copy tool (can be turned off below in case of remote I/O)
    useCT = True

    # remove all input root files for analysis job for xrootd sites
    # (they will be read by pAthena directly from xrootd)
    # create the direct access dictionary
    dInfo = getDirectAccessDic(readpar('copysetupin'))
    # if copysetupin did not contain direct access info, try the copysetup instead
    if not dInfo:
        dInfo = getDirectAccessDic(readpar('copysetup'))

    # check if we should use the copytool
    if dInfo:
        if not dInfo['useCopyTool']:
            useCT = False
        oldPrefix = dInfo['oldPrefix']
        newPrefix = dInfo['newPrefix']
        useFileStager = dInfo['useFileStager']
        directIn = dInfo['directIn']
    if useCT:
        tolog("Copy tool will be used for stage-in")
    else:
        if useFileStager:
            tolog("File stager mode: Copy tool will not be used for stage-in of root files")
        else:
            tolog("Direct access mode: Copy tool will not be used for stage-in of root files")
            if oldPrefix == "" and newPrefix == "":
                tolog("Will attempt to create a TURL based PFC")

    return useCT, oldPrefix, newPrefix, useFileStager, directIn

def verifySwbase(appdir):
    """ confirm existence of appdir/swbase if possible """

    ec = 0

    # get error handler
    error = PilotErrors()

    if not "|" in appdir and not "^" in appdir: # as can be the case at CERN
        swbase = getSwbase(appdir, "", "", "", "")
        if os.path.exists(swbase):
            tolog("Application dir confirmed: %s" % (swbase))
        else:
            tolog("!!FAILED!!1999!! Software directory does not exist: %s" % (swbase))
            ec = error.ERR_NOSOFTWAREDIR
    else:
        from SiteInformation import SiteInformation
        si = SiteInformation()
        tolog("Encountered complex appdir. Will verify each path individually (primary path must exist, other paths are optional)")

        appdirs = si.getAppdirs(appdir)
        tolog("appdirs = %s" % str(appdirs))
        primary = True
        for appdir in appdirs:
            if os.path.exists(appdir):
                if primary:
                    tolog("Primary application directory confirmed: %s" % (appdir))
                    primary = False
                else:
                    tolog("Additional application directory confirmed: %s" % (appdir))
            else:
                if primary: # must exist
                    tolog("!!FAILED!!1999!! Primary application directory does not exist: %s" % (appdir))
                    ec = error.ERR_NOSOFTWAREDIR
                    break
                else:
                    tolog("Additional application directory does not exist: %s (ignore)" % (appdir))

    return ec

def getExtension(alternative='pickle'):
    """ get the file extension (json or whatever 'alternative' is set to, pickle by default) """

    try:
        from json import load
    except:
        extension = alternative
    else:
        extension = "json"

    return extension

def isLogfileCopied(workdir):
    """ check whether the log file has been copied or not """

    if os.path.exists(workdir + '/LOGFILECOPIED'):
        return True
    else:
        return False

def isLogfileRegistered(workdir):
    """ check whether the log file has been registered or not """

    if os.path.exists(workdir + '/LOGFILEREGISTERED'):
        return True
    else:
        return False


def updateJobState(job, site, workNode, recoveryAttempt=0):
    """ update the job state file """

    status = True

    # create a job state object and give it the current job state information
    from JobState import JobState
    JS = JobState()
    if JS.put(job, site, workNode, recoveryAttempt):
        if recoveryAttempt > 0:
            tolog("Successfully updated job state file (recovery attempt number: %d)" % (recoveryAttempt))
        else:
            tolog("Successfully updated job state file at: %s" % (JS.getCurrentFilename()))
    else:
        tolog("!!WARNING!!1000!! Failed to update job state file")
        status = False

    return status

def chdir(dir):
    """ keep track of where we are... """

    tolog("chdir to: %s" % (dir))
    os.chdir(dir)
    tolog("current dir: %s" % (os.getcwd()))

def createJobSetupScript(workdir):
    """ create the job setup script (used to recreate the job locally if needed) """

    filename = os.path.basename(getJobSetupScriptName(workdir))
    tolog("Creating job setup script with stage-in and payload execution commands: %s" % (filename))
    to_script = "#!/bin/bash\n# %s %s\n\n" % (filename, time.strftime("%d %b %Y %H:%M:%S", time.gmtime(time.time())))

    # setup for EGI sites
    if os.environ.has_key('VO_ATLAS_SW_DIR'):
        to_script += "export VO_ATLAS_SW_DIR=%s\n" % (os.path.expandvars('$VO_ATLAS_SW_DIR'))
        to_script += "if [ -f $VO_ATLAS_SW_DIR/local/setup.sh ]; then\n  source $VO_ATLAS_SW_DIR/local/setup.sh\nfi"

    # add the string to the setup script
    addToJobSetupScript(to_script, workdir)

def getJobSetupScriptName(workdir):
    """ return the name of the job setup file """

    return os.path.join(workdir, "job_setup.sh")

def addToJobSetupScript(cmd, workdir):
    """ add/append command to job setup file """

    filename = getJobSetupScriptName(workdir)
    if not os.path.exists(filename):
        try:
            fp = open(filename, "w")
        except OSError, e:
            tolog("!!WARNING!!1880!! Could not open job setup file for writing: %s" % str(e))
    else:
        try:
            fp = open(filename, "a")
        except OSError, e:
            tolog("!!WARNING!!1880!! Could not open job setup file for appending: %s" % str(e))

    if fp:
        fp.write(cmd)
        fp.write("\n\n")
        fp.close()
        tolog("Updated %s: %s" % (filename, cmd))

def processDBRelease(inputFiles, inFilesGuids, realDatasetsIn, dispatchDblock, dispatchDBlockToken, prodDBlockToken, workdir, jobPars):
    """ remove any DBRelease files from the input file list and send back instruction to move the created DBRelease file to job dir """

    _inputFiles = inputFiles
    _inFilesGuids = inFilesGuids
    _realDatasetsIn = realDatasetsIn
    _dispatchDblock = dispatchDblock
    _dispatchDBlockToken = dispatchDBlockToken
    _prodDBlockToken = prodDBlockToken

    # are there any DBRelease files in the input file list?
    has_DBRelease_files = False
    from DBReleaseHandler import DBReleaseHandler
    dbh = DBReleaseHandler(workdir=workdir)

    # abort if no local DBRelease dir
    if dbh.getDBReleaseDir() == "":
        return _inputFiles, _inFilesGuids, _realDatasetsIn, _dispatchDblock, _dispatchDBlockToken, _prodDBlockToken

    for f in inputFiles:
        # if the DBRelease version can be extracted from the file name, then the file is a DBRelease file..
        if dbh.extractVersion(f):
            tolog("Found a DBRelease file: %s" % (f))
            has_DBRelease_files = True
            break

    if not has_DBRelease_files:
        tolog("No DBRelease files found in input file list")
    else:
        tolog("Found a DBRelease file in the input file list (will check local availability)")

        # get the DBRelease version
        # for testing: version = dbh.getDBReleaseVersion(jobPars=jobPars+" DBRelease-9.0.1.tar.gz")
        version = dbh.getDBReleaseVersion(jobPars=jobPars)

        # create the skeleton DBRelease tarball
        if dbh.createDBRelease(version, workdir):
            # update the input file list
            _inputFiles, _inFilesGuids, _realDatasetsIn, _dispatchDblock, _dispatchDBlockToken, _prodDBlockToken = \
                         dbh.removeDBRelease(list(inputFiles), list(inFilesGuids), list(realDatasetsIn), list(dispatchDblock), list(dispatchDBlockToken), list(prodDBlockToken))

    return _inputFiles, _inFilesGuids, _realDatasetsIn, _dispatchDblock, _dispatchDBlockToken, _prodDBlockToken

def updateXMLWithSURLs(experiment, node_xml, workDir, jobId, jobrec, format=''):
    """ update the XML with the SURLs """

    xml = ""

    # read back the SURL dictionary
    from SiteMover import SiteMover
    sitemover = SiteMover()
    surlDictionary = sitemover.getSURLDictionary(workDir, jobId)

    # get the experiment object
    thisExperiment = getExperiment(experiment)

    node_xml_list = node_xml.split("\n")
    # loop over the xml and update where it is needed
    if surlDictionary != {}:
        if format == 'NG':
            re_tobeset = re.compile('\<surl\>([a-zA-Z0-9-]+)\-surltobeset')
        else:
            if thisExperiment:
                metadata_attr_name = thisExperiment.getAttrForRegistration()
            else:
                metadata_attr_name = "surl"
            re_tobeset = re.compile('\<metadata att\_name\=\"%s\" att\_value\=\"([a-zA-Z0-9-]+)\-surltobeset\"\/\>' % (metadata_attr_name))
        for line in node_xml_list:
            tobeset = re_tobeset.search(line)
            if tobeset:
                # extract the guid and surl
                guid = tobeset.group(1)
                # note: in case of an earlier transfer error, the corresponding guid will not be in the surl dictionary
                # since it is only written to the surl dictionary for successful transfers
                try:
                    surl = surlDictionary[guid]
                except Exception, e:
                    tolog("!!WARNING!!2996!! Encountered a missing guid in the surl dictionary - did the corresponding transfer fail? guid = %s, %s" % (guid, e))
                    # add the 'surltobeset' line when job recovery is used
                    if jobrec:
                        xml += line + "\n"
                else:
                    # replace the guid and the "surltobeset"-string with the surl
                    if surl and surl != "":
                        xml += line.replace(guid + "-surltobeset", surl) + "\n"
                    else:
                        tolog("!!WARNING!!2996!! Could not extract guid %s from xml line: %s" % (guid, line))
            # fail safe in case something went wrong above, remove the guid+surltobeset line
            elif "surltobeset" in line:
                tolog("Failed to remove surltobeset from line: %s" % (line))
            else:
                xml += line + "\n"

    else:
        tolog("!!WARNING!!2997!! Encountered an empty SURL dictionary")

        # remove the metadata for the SURL since it cannot be updated
        for line in node_xml_list:
            if not jobrec:
                if not "surltobeset" in line:
                    xml += line + "\n"
            else:
                xml += line + "\n"

    if xml == "\n":
        xml = ""
        tolog("Reset XML")

    return xml

def putMetadata(workdir, jobId, strXML):
    """ """

    status = False

    filename = os.path.join(workdir, "metadata-%s.xml" % (jobId))
    try:
        f = open(filename, "w")
    except OSError, e:
        tolog("!!WARNING!!1200!! Failed to open metadata file for writing: %s" % (e))
    else:
        f.write(strXML)
        f.close()
        status = True

    return status

def getMetadata(workdir, id=None, athena=False):
    """ read metadata from file """

    BAK = ""
    if athena:
        BAK = ".ATHENA"

    # are we in recovery mode? then id is set
    if id:
        filename = "metadata-%s.xml%s" % (repr(id), BAK)
    else:
        filename = "metadata.xml%s" % (BAK)

    # try to open and read the meta data from file
    strXML = None
    tolog("Trying to read %s from dir: %s" % (filename, workdir))
    fname = "%s/%s" % (workdir, filename)
    if os.path.exists(fname):
        try:
            f = open(fname)
        except Exception, e:
            tolog("!!WARNING!!1000!! Can not open the file %s, %s" % (fname, str(e)))
        else:
            strXML = ""
            for line in f:
                strXML += line    
            f.close()
            if len(strXML) > 0:
                tolog("Found metadata")
            else:
                tolog("!!WARNING!!1000!! Empty metadata")
    else:
        tolog("getMetadata: metadata does not seem to have been created (file %s does not exist)" % (fname))

    return strXML

def makeJobReport(job, logExtracts, foundCoreDump, version, jobIds):
    """ Make the job summary error report. Use info from jobReport.pickle if available """

    error = PilotErrors()
    perr = job.result[2]
    terr = job.result[1]

    # was this a multi-trf job?
    nJobs = job.jobPars.count("\n")
    if nJobs > 0:
        multi_trf = True
    else:
        multi_trf = False

    tolog("..Job report..................................................................................................")
    tolog(". Pilot version             : %s" % (version))
    tolog(". Job id                    : %d" % (job.jobId))
    tolog(". Current job status        : %s" % (job.result[0]))

    if multi_trf:
        tolog(". Trf job type              : Multi-trf (%d jobs)" % (nJobs + 1))
    else:
        tolog(". Trf job type              : Single trf job")

    try: # protect against sites that run older pilots that don't have the finalstate defined
        fs = job.finalstate
        if fs != "":
            tolog(". Final job state           : %s" % (fs))
        else:
            tolog(". Final job state           : (not set, job should have failed)")
    except:
        tolog("(not set - site should update pilot distribution)")
        fs = None

    if verifyTransfer(job.workdir, verbose=False):
        tolog(". All out files transferred : Yes")
    else:
        tolog(". All out files transferred : No")
    if perr != 0:

        tolog(". Pilot error code          : %d, %s" % (perr, error.getPilotErrorDiag(perr)))
        if error.isRecoverableErrorCode(perr) and job.result[0] != "failed":
            tolog(". Job is recoverable        : Yes")
        else:
            tolog(". Job is recoverable        : No")
    else: # perr == 0
        tolog(". Pilot error code          : %d, (no pilot error)" % (perr))
        if fs == "failed" or job.result[0] == "failed":
            tolog(". Job is recoverable        : No")

    if job.pilotErrorDiag != None:
        lenPilotErrorDiag = len(job.pilotErrorDiag)
        if lenPilotErrorDiag > 250:
            tolog(". Length pilot error diag   : %d (will be truncated to 250)" % (lenPilotErrorDiag))
        else:
            tolog(". Length pilot error diag   : %d" % (lenPilotErrorDiag))
        if job.pilotErrorDiag != "":
            if lenPilotErrorDiag > 80:
                tolog(". Pilot error diag [:80]    : %s" % (job.pilotErrorDiag[:80]))
            else:
                tolog(". Pilot error diag          : %s" % (job.pilotErrorDiag))
        else:
            tolog(". Pilot error diag          : Empty")
    else:
        tolog(". Pilot error diag          : None")

    fname = getPilotstderrFilename()
    if os.path.exists(fname):
        _size = os.path.getsize(fname)
        if _size > 0:
            tolog(". Pilot produced stderr     : Yes (size: %d) see dump below" % (_size))
        else:
            tolog(". Pilot produced stderr     : No")

    fname = "%s/runjob.stderr" % (job.workdir)
    if os.path.exists(fname):
        _size = os.path.getsize(fname)
        if _size > 0:
            tolog(". runJob produced stderr    : Yes (size: %d) see dump below" % (_size))
        else:
            tolog(". runJob produced stderr    : No")

    tolog(". Trf error code            : %d" % (terr))

    # trf error should have been read from the jobInfo.xml or jobReport* files
    if terr != job.exeErrorCode:
        tolog(". Trf error code (2)        : %d" % job.exeErrorCode)
    tolog(". Trf error diagnosis       : %s" % job.exeErrorDiag)

        
    if (job.exeErrorCode != 0) and (job.result[1] != job.exeErrorCode):
        mismatch = "exeErrorCode = %d, transExitCode = %d" %\
                   (job.exeErrorCode, job.result[1])
        tolog(". Trf error code mismatch   : %s" % mismatch)

    lenLogExtracts = len(logExtracts)
    if lenLogExtracts <= 2048:
        tolog(". Length log extracts       : %d (preliminary)" % (lenLogExtracts))
    else:
        tolog(". Length log extracts       : %d (will be truncated to 2048)" % (lenLogExtracts))

    # did the payload produce any stderr?
    if multi_trf:
        for _i in range(nJobs + 1):
            _stderr = job.stderr
            _stderr = _stderr.replace(".txt", "_%d.txt" % (_i + 1))
            filename = "%s/%s" % (job.workdir, _stderr)
            if os.path.exists(filename):
                if os.path.getsize(filename) > 0:
                    tolog(". Payload %d produced stderr : Yes (check %s)" % (_i + 1, _stderr))
                else:
                    tolog(". Payload %d produced stderr : No (empty %s)" % (_i + 1, _stderr))
            else:
                tolog(". Payload %d produced stderr: No (%s does not exist)" % (_i + 1, _stderr))
    else:
        filename = "%s/%s" % (job.workdir, job.stderr)    
        if os.path.exists(filename):
            if os.path.getsize(filename) > 0:
                tolog(". Payload produced stderr   : Yes (check %s)" % (job.stderr))
            else:
                tolog(". Payload produced stderr   : No (empty %s)" % (job.stderr))
        else:
            tolog(". Payload produced stderr   : No (%s does not exist)" % (job.stderr))

    if foundCoreDump:
        tolog(". Found core dump in workdir: Yes")
    else:
        tolog(". Found core dump in workdir: No")

    if len(jobIds) > 1:
        tolog(". Executed multi-jobs       : %s" % str(jobIds))

    tolog(". Job was executed in dir   : %s" % job.workdir)
    tolog(". Error report produced at  : %s" % timeStamp())
    tolog("..Time report.................................................................................................")
    tolog(". CPU consumption time      : %s %s" % (str(job.cpuConsumptionTime), job.cpuConsumptionUnit))
    tolog(". Payload execution time    : %s s" % (str(job.timeExe)))
    tolog(". GetJob consumption time   : %s s" % (str(job.timeGetJob)))
    tolog(". Stage-in consumption time : %s s" % (str(job.timeStageIn)))
    tolog(". Stage-out consumption time: %s s" % (str(job.timeStageOut)))
    tolog("..............................................................................................................")

    # dump the pilot stderr if it exists
    fname = getPilotstderrFilename()
    if os.path.exists(fname):
        if os.path.getsize(fname) > 0:
            tolog("\n//begin %s ///////////////////////////////////////////////////////////////////////////" % os.path.basename(fname))
            dumpFile(fname, topilotlog=True)
            tolog("\n//end %s /////////////////////////////////////////////////////////////////////////////" % os.path.basename(fname))

    # dump the wrapper (runJob) stderr if it exists
    fname = "%s/runjob.stderr" % (job.workdir)
    if os.path.exists(fname):
        if os.path.getsize(fname) > 0:
            tolog("\n//begin %s ///////////////////////////////////////////////////////////////////////////" % os.path.basename(fname))
            dumpFile(fname, topilotlog=True)
            tolog("\n//end %s /////////////////////////////////////////////////////////////////////////////" % os.path.basename(fname))

    if job.result[0] == 'finished' or job.result[0] == 'holding':
        if job.result[0] == 'holding':
            tolog("Note that the following line is a message to the Panda monitor only")
        tolog("!!FINISHED!!0!!Job successfully completed")

def safe_call(func, *args):
    """ Try-statement wrapper around function call with traceback info """

    status = False
    try:
        func(*args)
    except Exception, e:
        tolog("!!WARNING!!1111!! Exception in function %s: %s" % (e, func))
        tolog("Stack trace:")

        import traceback

        exc, msg, tb = sys.exc_info()
        traceback.print_tb(tb)
#        tb = traceback.format_tb(sys.last_traceback)
#        for line in tb:
#            tolog(line)
    else:
        status = True

    return status

def getDispatcherErrorDiag(ec):
    """ Get the corresponding error diag for the dispatcher """

    # dispatcher codes
    codes = {}
    codes[0] = 'Success'
    codes[10] = 'Connection timed out'
    codes[20] = 'Dispatcher has no jobs'
    codes[30] = 'Failed'
    codes[40] = 'Non secure'
    codes[50] = 'Invalid token'
    codes[60] = 'Invalid role'
    codes[255] = 'EC_Failed'

    pilotErrorDiag = codes.get(ec, 'GETJOB encountered error %d' % (ec))
    tolog("!!WARNING!!2999!! %s" % (pilotErrorDiag))

    return pilotErrorDiag

def getCopyprefixFromTo(copyprefix):
    """ extract from and to info from copyprefix """

    pfrom = ""
    pto = ""

    if copyprefix != "":
        if copyprefix.count("^") == 1:
            pfrom, pto = copyprefix.split("^")
        elif copyprefix.startswith("^") or copyprefix.count("^") > 1:
            tolog("!!WARNING!!2988!! copyprefix has wrong format (not pfrom^pto): %s" % (copyprefix))
        else:
            pfrom = copyprefix

    if pfrom == "":
        pfrom = "dummy"
    else:
        if pfrom.endswith('/'):
            pfrom = pfrom[:-1]
            tolog("Cut away trailing / from %s (see copyprefix[in])" % (pfrom))
    if pto == "":
        pto = "dummy"

    return pfrom, pto

def getCopyprefixLists(copyprefix):
    """ Get the copyprefix lists """

    pfrom, pto = getCopyprefixFromTo(copyprefix)
    if "," in pfrom:
        pfroms = pfrom.split(",")
    else:
        pfroms = [pfrom]
    if "," in pto:
        ptos = pto.split(",")
    else:
        ptos = [pto]

    return pfroms, ptos

def getCmtconfig(jobCmtconfig):
    """ Get the cmtconfig from the job def or schedconfig """

    # the job def should always contain the cmtconfig
    if jobCmtconfig != "" and jobCmtconfig != "None" and jobCmtconfig != "NULL":
        cmtconfig = jobCmtconfig
        tolog("Will try to use cmtconfig: %s (from job definition)" % (cmtconfig))
    else:
        cmtconfig = readpar('cmtconfig')
        tolog("Will try to use cmtconfig: %s (from schedconfig DB)" % (cmtconfig))

    return cmtconfig

def getCmtconfigAlternatives(cmtconfig, swbase):
    """ get a list of locally available cmtconfig's that can be used as alternatives to the primary cmtconfig in case it doesn't work """
    # prepend the default cmtconfig

    alternatives = [cmtconfig]

    from glob import glob

    # grab all files/directories in swbase dir
    dirList = glob(os.path.join(swbase, '*'))

    # use a hardcoded cmtconfigvalidation listfor now
    valid_names= ['i686-', 'x86_64-']

    # are there any dirs that have the cmtconfig pattern? (require at least three '-')
    pattern = re.compile('([A-Za-z0-9]+\-[A-Za-z0-9]+\-[A-Za-z0-9]+\-[A-Za-z0-9]+)')
    for directory in dirList:
        d = os.path.basename(directory)
        found = re.search(pattern, d)
        if found and d != cmtconfig:
            # make surethat weare notpickingup unvalid names (eg 'tags-BNL-Subcluster-4-BNL-ATLAS' would slip through otherwise)
            verified = False
            for valid_name in valid_names:
                # require that the found directory begins with i686- or x86_64-
                if d[:len(valid_name)] == valid_name:
                    verified = True
                    break
            if verified:
                alternatives.append(d)
                                                                                                 

    return alternatives

def extractFilePaths(s):
    """ Extract file paths from given setup string """

    # s = "source /path/setup.sh;export X509_USER_PROXY=/path/x509_up;source aa"
    # -> setup_path = ['/path/setup.sh', 'aa']

    setup_paths = None

    if s != "" and "source " in s:
        setup_paths = []
        s = s.replace(";;", ";")

        # extract all occurances of "source " (with or without a trailing ;)

        # first try a pattern ending with a ;
        pattern = re.compile(r"source (\S+);")

        found = re.findall(pattern, s)
        if len(found) > 0:
            for i in range(len(found)):
                setup_paths.append(found[i])

                # remove the found pattern so not to disturb the remaining search
                s = s.replace("source %s" % (found[i]), "")

        # assume additional patterns with no trailing ;
        pattern = re.compile(r"source (\S+)")

        found = re.findall(pattern, s)
        if len(found) > 0:
            for i in range(len(found)):
                setup_paths.append(found[i])

    return setup_paths

def verifySetupCommand(error, _setup_str):
    """ Make sure the setup command exists """

    ec = 0
    pilotErrorDiag = ""

    if _setup_str != "" and "source " in _setup_str:
        # first extract the file paths from the source command(s)
        setup_paths = extractFilePaths(_setup_str)

        # only run test if string begins with an "/"
        if setup_paths:
            # verify that the file paths actually exists
            for setup_path in setup_paths:
                if os.path.exists(setup_path):
                    tolog("File %s has been verified" % (setup_path))
                else:
                    pilotErrorDiag = "No such file or directory: %s" % (setup_path)
                    tolog('!!WARNING!!2991!! %s' % (pilotErrorDiag))
                    ec = error.ERR_NOSUCHFILE
                    break
        else:
            # nothing left to test
            pass
    else:
        tolog("Nothing to verify in setup: %s (either empty string or no source command)" % (_setup_str))

    return ec, pilotErrorDiag

def getProperTimeout(paths):
    """ Return a proper time-out depending on if CVMFS is used or not"""
    # paths can contain several paths and commands, just look for the presence of /cvmfs/

    if "/cvmfs/" in paths:
        _timeout = 1000
    else:
        _timeout = 500
    return _timeout

def getPilotVersion():
    """ Load the pilot version string from file VERSION """
 
    version = "SULU"
    try:
        f = open("PILOTVERSION", "r")
    except Exception, e:
        print "!!WARNING!!0000!! Could not read pilot version from file: %s" % (e )
    else:
        _version = f.read()
        # remove trailing \n if present
        if "\n" in version:
            _version = _version.replace("\n", "")
        # trivial check
        pattern = re.compile(r"[A-Z]+ [A-Za-z0-9.]+")
        v = re.findall(pattern, _version)
        if v == []:
            print "!!WARNING!!0000!! Not a valid version format: %s" % (version)
        else:
            version = _version

    return version

def getExperiment(experiment):
    """ Return a reference to an experiment class """

    # The ExperimentFactory ensures that the returned object is a Singleton
    # Usage:
    #    _exp = getExperiment(readpar('experiment')) # or from pilot option
    #    if _exp:
    #        _exp.somemethod("Hello")
    #    else:
    #        tolog("!!WARNING!!1111!! Failed to instantiate experiment class")

    from ExperimentFactory import ExperimentFactory
    factory = ExperimentFactory()
    _exp = None

    try:
        experimentClass = factory.newExperiment(experiment)
    except Exception, e:
        tolog("!!WARNING!!1114!! Experiment factory threw an exception: %s" % (e))
    else:
        _exp = experimentClass()

    return _exp

def getSiteInformation(experiment):
    """ Return a reference to an experiment class """

    # The SiteInformationFactory ensures that the returned object is a Singleton
    # Usage:
    #    _exp = getSiteInformation(readpar('experiment')) # or from pilot option
    #    if _exp:
    #        _exp.somemethod("Hello")
    #    else:
    #        tolog("!!WARNING!!1111!! Failed to instantiate experiment class")

    from SiteInformationFactory import SiteInformationFactory
    factory = SiteInformationFactory()
    _exp = None

    try:
        siteInformationClass = factory.newSiteInformation(experiment)
    except Exception, e:
        tolog("!!WARNING!!1114!! SiteInformation factory threw an exception: %s" % (e))
    else:
        _exp = siteInformationClass()
        tolog("getSiteInformation: got experiment=%s" % (_exp.getExperiment()))

    return _exp

def setPilotPythonVersion():
    """ Set an environmental variable to the python version used by the pilot """
    # Needed to disentangle which python version runAthena should fall back to in case of problems with LFC import

    which_python = commands.getoutput("which python")
    if which_python.startswith('/'):
        os.environ['ATLAS_PYTHON_PILOT'] = which_python
        tolog("ATLAS_PYTHON_PILOT set to %s" % (which_python))
    else:
        tolog("!!WARNING!!1111!! Could not set ATLAS_PYTHON_PILOT to %s" % (which_python))

def removePattern(_string, _pattern):
    """ Remove the regexp pattern from the given string """

    pattern = re.compile(r"%s" % (_pattern))

    found = re.findall(pattern, _string)
    if len(found) > 0:
        _substring = found[0]
        tolog("Found regexp string: %s" % (_substring))
        _string = _string.replace(_substring, "")
        
    return _string

def isPilotTCPServerAlive(server, port):
    """ Verify that the pilot TCP server is still alive """

    status = False

    import socket
    try:
        # open the socket
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    except Exception, e:
        tolog("!!WARNING!!2911!! Caught a socket/connect exception: %s" % (e))
    else:
        # try to commucate with the TCP server
        s.settimeout(10)
        try:
            s.connect((server, port))
        except Exception, e:
            tolog("!!WARNING!!1912!! Caught a socket/connect exception: %s" % (e))
        else:
            status = True
        s.settimeout(None)
        s.close()

    return status

def encode_string(_string):
    """ Encode a string using urlencode """

    from urllib import urlencode

    # put the string to be encoded in a dictionary
    encoded_dict = {"x":_string}

    # handle the =-sign (not possible to have since the decoder of the pilot TCP message does a splitting using =-signs)
    pre = urlencode(encoded_dict)
    encoded_string = pre.replace('x=', '^!^')

    return encoded_string

def decode_string(encoded_string):
    """ Decode a string using parse_qs """

    tolog("Decoding: %s" % (encoded_string))
    imported = False
    try:
        # on modern python, get the parse function from urlparse
        from urlparse import parse_qs
    except:
        pass
    else:
        imported = True

    if not imported:
        # on ancient python, get the parse function from cgi
        from cgi import parse_qs

    # handle the =-sign (put back)
    if '^!^' in encoded_string:
        encoded_string = encoded_string.replace('^!^', 'x=')

    decoded_string = ""
    try:
        decoded_dict = parse_qs(encoded_string)
    except Exception, e:
        tolog("!!WARNING!!1234!! Failed to parse URL encoded string: %s" % (encoded_string))
    else:
        if decoded_dict.has_key('x'):
            try:
                decoded_string = decoded_dict['x'][0]
            except Exception, e:
                tolog("!!WARNING!!1234!! Failed to decode URL encoded string: %s" % (encoded_string))
        else:
            tolog("Empty URL encoded string (Nothing to decode)")

        # get rid of useless info
        if decoded_string == "^!^":
            tolog("Resetting decoded string (TCP ping signal)")
            decoded_string = ""

    return decoded_string
