import os, re, sys
import commands
from time import time
import urllib2

import SiteMover
from futil import *
from PilotErrors import PilotErrors
from pUtil import tolog, readpar, addToJobSetupScript, getDirectAccessDic
from timed_command import timed_command
from FileStateClient import updateFileState


class replica:
    """ Replica """

    sfn = None
    setname = None
    fs = None
    filesize = None
    csumvalue = None


# placing the import lfc here breaks compilation on non-lfc sites
# import lfc


# Always called for a single file. If metalink file is found, then all files
# therein are downloaded ont he first single file call. On subsequent calls
# the relevant file should be there and is just checked.
# If no metalink file is found then a single http turl is built by regexp
# from surl

class aria2cSiteMover(SiteMover.SiteMover):
    """ SiteMover for aria2c """

    copyCommand = "aria2c"
    checksum_command = "adler32"
    has_mkdir = False
    has_df = False
    has_getsize = False
    has_md5sum = True
    has_chmod = False
    timeout = 3600
    """ get proxy """
    
    try:
        sslCert = os.environ['X509_USER_PROXY']
    except:
        sslCert = ""
    try: 
        sslKey = os.environ['X509_USER_PROXY']
    except:
        sslKey = ""
    try:
        sslCertDir = os.environ['X509_CERT_DIR']
    except:
        os.environ['X509_CERT_DIR'] = "/etc/grid-security/certificates"
        sslCertDir = os.environ['X509_CERT_DIR']
               
    def __init__(self, setup_path, *args, **kwrds):
        self._setup = setup_path
        self.copyCommand = 'aria2c'
        self.commandInPATH()
        self.getSurl2httpsMap()

    def commandInPATH(self):
        _cmd_str = 'which %s'%self.copyCommand 
        tolog("Executing command: %s" % (_cmd_str))
        s, o = commands.getstatusoutput(_cmd_str)
        if s != 0:
          tolog("aria2c not found in PATH")
        cvmfs_aria2c = '/cvmfs/atlas.cern.ch/repo/sw/local/x86_64-slc5-gcc43-opt/bin/aria2c'  
        if os.path.exists(cvmfs_aria2c):
          tolog("Using %s"%cvmfs_aria2c)
          self.copyCommand = cvmfs_aria2c
        else:
          tolog("aria2c not in PATH or %s"%cvmfs_aria2c)
           
    def get_timeout(self):
        return self.timeout

    def check_space(self, ub):
        """ For when space availability is not verifiable """
        return 999999        
    def getSurl2httpsMap(self):
        """This will come from AGIS but for now from a url, wih fallback to hard-coded value"""
        s2hurl = 'http://walkerr.web.cern.ch/walkerr/surl2https.py'
        try:
          u = urllib2.urlopen(s2hurl)
          exec u.read()
        except:
          tolog('Problem getting surl2https map from: %s'%s2hurl)  
          surl2https_map = {}  
        if len(surl2https_map) > 0:
          tolog('Using surl2https map from %s of length %d'%(s2hurl,len(surl2https_map)))  
          self.surl2https_map = surl2https_map
        else:
          tolog('surl2https_map not set or zero length. Using default.')
          self.surl2https_map = {
              'atlassrm-fzk.gridka.de':('srm://atlassrm-fzk.gridka.de(:8443/srm/managerv2\?SFN=)*/pnfs/gridka.de/atlas',
                                        'https://f01-060-110-e.gridka.de:2880'),
              'srm.ndgf.org':('srm://srm.ndgf.org(:8443/srm/managerv2\?SFN=)*','https://fozzie.ndgf.org:2881'),
              'srm.grid.sara.nl':('srm://srm.grid.sara.nl(:8443/srm/managerv2\?SFN=)*','https://bee34.grid.sara.nl'),
              'lcg-lrz-se.lrz-muenchen.de':('srm://lcg-lrz-se.lrz.de(:8443/srm/managerv2\?SFN=)*',
                                            'http://lcg-lrz-dc66.grid.lrz-muenchen.de'),
              'dcache-se-atlas.desy.de':('srm://dcache-se-atlas.desy.de(:8443/srm/managerv2\?SFN=)*/pnfs/desy.de/atlas',
                                         'https://dcache-door-atlas19.desy.de:2880'),
              'golias100.farm.particle.cz':('srm://golias100.farm.particle.cz','https://golias100.farm.particle.cz'),
              'grid-se.physik.uni-wuppertal.de':('srm://grid-se.physik.uni-wuppertal.de(:8443/srm/managerv2\?SFN=)*/pnfs/physik.uni-wuppertal.de/data','https://grid-se.physik.uni-wuppertal.de:2881')
           }
                    

    def surls2metalink(self,replicas,metalinkFile):
        """ Convert list of replicas (of multiple files) to metalink
        Input argument, replicas, is dict with guid as key, and a list of surls
        Mappings from surl to https turl will come from ddm eventually
        to cover surls from remote SEs.
        For now just add the mapping for the local SE from copysetup.
        """
       # self.surl2https_map has key is srm hostname, then tuple of (from,to) regexp replace
        
        dirAcc = getDirectAccessDic(readpar('copysetupin'))
        if not dirAcc:
          dirAcc = getDirectAccessDic(readpar('copysetup'))
       # extract srm host for key
        if dirAcc:
          srmhost = self.hostFromSurl(dirAcc['oldPrefix'])
        if srmhost:
          self.surl2https_map[srmhost] = (dirAcc['oldPrefix'],dirAcc['newPrefix'])

          
       # Start building metalink
        metalink='<?xml version="1.0" encoding="utf-8"?>\n'
        metalink+='<metalink version="3.0" generator="Pilot" xmlns="http://www.metalinker.org/">\n'
        metalink+='<files>\n'
        for guid in replicas.keys():
          reps = replicas[guid]
         # surl can have __DQ2blah at the end - strip it 
          name = reps[0].sfn.split('/')[-1]
          extindex = name.rfind('__DQ2-')
          if extindex > 0: name = name[:extindex]
          metalink+='<file name="%s">\n'%name
          metalink+='<size>%s</size>'%reps[0].filesize
          metalink+='<verification><hash type="adler32">%s</hash></verification>\n'%reps[0].csumvalue
          metalink+='<resources>\n'
         # if the surl matches a list of https sites, then add a url 
          for rep in reps:
            srmhost =  self.hostFromSurl(rep.sfn)
            if srmhost in self.surl2https_map.keys():
              pair = self.surl2https_map[srmhost]
              metalink+='<url type="https" >%s</url>\n'% \
                                       re.sub(pair[0],pair[1],rep.sfn)
            else:
              tolog("Not found: %s"%rep.sfn)
          metalink+='</resources></file>\n'

        metalink+='</files></metalink>\n'
        print metalink
        mlfile = open(metalinkFile,'w')
        mlfile.write(metalink)
        mlfile.close()


    def hostFromSurl(self,surl):
        re_srmhost = re.compile('^srm://([^/|:|\(]*)')
        srmhost=re_srmhost.search(surl)
        if srmhost:
            return srmhost.group(1)
        else:
            return None  


    def get_data(self, gpfn, lfn, path, fsize=0, fchecksum=0, guid=0, **pdict):
        """ copy input file from SE to local dir """
       # determine which timeout option to use
        timeout_option = "--connect-timeout 300 --timeout %d" % (self.timeout)

        sslCert = self.sslCert
        sslKey = self.sslKey
        sslCertDir = self.sslCertDir
        
        # used aria2c options:
        # --certificate Client certificate file and password (SSL)(proxy)
        # --private-key user proxy again 
        # --ca-certificate: concatenate *.0 in cert dir to make bundle
        # --out: <file> Write output to <file> instead of stdout
        # --dir: output directory, needed when multiple files(metalink)
        # --continue: if file is already there (from previous) then success
        # --auto-file-renaming=false : don't rename existing file
        
        error = PilotErrors()
        pilotErrorDiag = ""

        # Get input parameters from pdict
        token = pdict.get('token', None)
        jobId = pdict.get('jobId', '')
        workDir = pdict.get('workDir', '')
        proxycheck = pdict.get('proxycheck', False)

        # try to get the direct reading control variable (False for direct reading mode; file should not be copied)
        useCT = pdict.get('usect', True)
        prodDBlockToken = pdict.get('access', '')

        # get the DQ2 tracing report
        try:
            report = pdict['report']
        except:
            report = {}
        else:
            # set the proper protocol
            report['protocol'] = 'aria2c'
            # mark the relative start
            report['catStart'] = time()
            # the current file
            report['filename'] = lfn
            # guid
            report['guid'] = guid.replace('-','')

        # get a proper envsetup
        envsetup = self.getEnvsetup(get=True)

        if proxycheck:
            # do we have a valid proxy?
            s, pilotErrorDiag = self.verifyProxy(envsetup=envsetup)
            if s != 0:
                self.__sendReport('PROXYFAIL', report)
                return s, pilotErrorDiag
        else:
            tolog("Proxy verification turned off")

        getfile = gpfn

        if path == '': path = './'
        fullname = os.path.join(path, lfn)

        # should the root file be copied or read directly by athena?
        directIn, useFileStager = self.getTransferModes()
        if directIn:
            if useCT:
                directIn = False
                tolog("Direct access mode is switched off (file will be transferred with the copy tool)")
                updateFileState(lfn, workDir, jobId, mode="transfer_mode", state="copy_to_scratch", type="input")
            else:
                # determine if the file is a root file according to its name
                rootFile = self.isRootFileName(lfn)

                if prodDBlockToken == 'local' or not rootFile:
                    directIn = False
                    tolog("Direct access mode has been switched off for this file (will be transferred with the copy tool)")
                    updateFileState(lfn, workDir, jobId, mode="transfer_mode", state="copy_to_scratch", type="input")
                elif rootFile:
                    tolog("Found root file according to file name: %s (will not be transferred in direct reading mode)" % (lfn))
                    report['relativeStart'] = None
                    report['transferStart'] = None
                    self.__sendReport('FOUND_ROOT', report)
                    if useFileStager:
                        updateFileState(lfn, workDir, jobId, mode="transfer_mode", state="file_stager", type="input")
                    else:
                        updateFileState(lfn, workDir, jobId, mode="transfer_mode", state="remote_io", type="input")
                    return 0, pilotErrorDiag
                else:
                    tolog("Normal file transfer")
        # If metalink file not created(including all inputs)
        # then make one just for this input
        
        if os.path.exists('AllInput.xml.meta4'):
          metalink='AllInput.xml.meta4'
        else:    
          rep = replica()
          rep.sfn = gpfn
          rep.filesize = fsize
          rep.csumvalue = fchecksum
          replicas = {guid:[rep]}
        
          self.surls2metalink(replicas,'oneInput.xml.meta4')
          metalink='oneInput.xml.meta4'

        # Build ca bundle if not already there
        cabundleFile='cabundle.pem'
        if not os.path.exists(cabundleFile):
           _cmd_str = 'cat %s/*.0 > %s'%(sslCertDir,cabundleFile)  
           tolog("Executing command: %s" % (_cmd_str))                        
           s, o = commands.getstatusoutput(_cmd_str)
           
        # build the copy command
        #--check-certificate=false makes it easier(sles11)
        _cmd_str = '%s --check-certificate=false --ca-certificate=%s --certificate=%s --private-key=%s --auto-file-renaming=false --continue --server-stat-of=aria2cperf.txt %s'%(self.copyCommand,cabundleFile,sslCert,sslCert,metalink)

        
        # invoke the transfer commands
        report['relativeStart'] = time()
        report['transferStart'] = time()
        tolog("Executing command: %s" % (_cmd_str))                        
        s, o = commands.getstatusoutput(_cmd_str)
        tolog(o)
        if s != 0:
          tolog("!!WARNING!!2990!! Command failed: %s" % (_cmd_str))
          check_syserr(s, o)
          pilotErrorDiag = "aria2c failed: %s" % (o)
          tolog("!!WARNING!!2999!! %s" % (pilotErrorDiag))
          ec = error.ERR_STAGEINFAILED
          return ec, pilotErrorDiag
        
        report['validateStart'] = time()

        # get the checksum type (md5sum or adler32)
        if fchecksum != 0 and fchecksum != "":
            csumtype = self.getChecksumType(fchecksum)
        else:
            csumtype = "default"

        if (fsize != 0 or fchecksum != 0) and self.doFileVerifications():
            loc_filename = lfn
            dest_file = os.path.join(path, loc_filename)

            # get the checksum type (md5sum or adler32)
            if fchecksum != 0 and fchecksum != "":
                csumtype = self.getChecksumType(fchecksum)
            else:
                csumtype = "default"

            # get remote file size and checksum 
            ec, pilotErrorDiag, dstfsize, dstfchecksum = self.getLocalFileInfo(dest_file, csumtype=csumtype)
            if ec != 0:
                self.__sendReport('LOCAL_FILE_INFO_FAIL', report)
                return ec, pilotErrorDiag

            # compare remote and local file size
            if long(fsize) != 0 and long(dstfsize) != long(fsize):
                pilotErrorDiag = "Remote and local file sizes do not match for %s (%s != %s)" %\
                                 (os.path.basename(gpfn), str(dstfsize), str(fsize))
                tolog("!!WARNING!!2990!! %s" % (pilotErrorDiag))
                self.__sendReport('FS_MISMATCH', report)
                return error.ERR_GETWRONGSIZE, pilotErrorDiag

            # compare remote and local file checksum
            if fchecksum and dstfchecksum != fchecksum and not self.isDummyChecksum(fchecksum):
                pilotErrorDiag = "Remote and local checksums (of type %s) do not match for %s (%s != %s)" %\
                                 (csumtype, os.path.basename(gpfn), dstfchecksum, fchecksum)
                tolog("!!WARNING!!2990!! %s" % (pilotErrorDiag))

                # report corrupted file to consistency server
                self.reportFileCorruption(gpfn)

                if csumtype == "adler32":
                    self.__sendReport('AD_MISMATCH', report)
                    return error.ERR_GETADMISMATCH, pilotErrorDiag
                else:
                    self.__sendReport('MD5_MISMATCH', report)
                    return error.ERR_GETMD5MISMATCH, pilotErrorDiag

        updateFileState(lfn, workDir, jobId, mode="file_state", state="transferred", type="input")
        self.__sendReport('DONE', report)
        return 0, pilotErrorDiag

    def put_data(self, source, destination, fsize=0, fchecksum=0, **pdict):
        """ copy output file from disk to local SE """
        # function is based on dCacheSiteMover put function

        error = PilotErrors()
        pilotErrorDiag = ""

        # Get input parameters from pdict
        lfn = pdict.get('lfn', '')
        guid = pdict.get('guid', '')
        token = pdict.get('token', '')
        dsname = pdict.get('dsname', '')
        analysisJob = pdict.get('analJob', False)
        testLevel = pdict.get('testLevel', '0')
        extradirs = pdict.get('extradirs', '')
        proxycheck = pdict.get('proxycheck', False)
        prodSourceLabel = pdict.get('prodSourceLabel', '')

        tolog("put_data received prodSourceLabel=%s" % (prodSourceLabel))
        if prodSourceLabel == 'ddm' and analysisJob:
            tolog("Treating PanDA Mover job as a production job during stage-out")
            analysisJob = False

        # get the DQ2 tracing report
        try:
            report = pdict['report']
        except:
            report = {}
        else:
            # set the proper protocol
            report['protocol'] = 'curl'
            # mark the relative start
            report['catStart'] = time()
            # the current file
            report['filename'] = lfn
            # guid
            report['guid'] = guid.replace('-','')

        # preparing variables
        if fsize == 0 or fchecksum == 0:
            ec, pilotErrorDiag, fsize, fchecksum = self.getLocalFileInfo(source, csumtype="adler32")
            if ec != 0:
                self.__sendReport('LOCAL_FILE_INFO_FAIL', report)
                return self.put_data_retfail(ec, pilotErrorDiag)

        # now that the file size is known, add it to the tracing report
        report['filesize'] = fsize

        # get the checksum type
        if fchecksum != 0 and fchecksum != "":
            csumtype = self.getChecksumType(fchecksum)
        else:
            csumtype = "default"

        # get a proper envsetup
        envsetup = self.getEnvsetup()

        if proxycheck:
            s, pilotErrorDiag = self.verifyProxy(envsetup=envsetup, limit=2)
            if s != 0:
                self.__sendReport('NO_PROXY', report)
                return self.put_data_retfail(error.ERR_NOPROXY, pilotErrorDiag)
        else:
            tolog("Proxy verification turned off")

        filename = os.path.basename(source)
        
        # get all the proper paths
        ec, pilotErrorDiag, tracer_error, dst_gpfn, lfcdir, surl = self.getProperPaths(error, analysisJob, token, prodSourceLabel, dsname, filename)
        if ec != 0:
            self.__sendReport(tracer_error, report)
            return self.put_data_retfail(ec, pilotErrorDiag)

        putfile = surl
        full_surl = putfile
        if full_surl[:len('token:')] == 'token:':
            # remove the space token (e.g. at Taiwan-LCG2) from the SURL info
            full_surl = full_surl[full_surl.index('srm://'):]

        # srm://dcache01.tier2.hep.manchester.ac.uk/pnfs/tier2.hep.manchester.ac.uk/data/atlas/dq2/
        #testpanda.destDB/testpanda.destDB.604b4fbc-dbe9-4b05-96bb-6beee0b99dee_sub0974647/
        #86ecb30d-7baa-49a8-9128-107cbfe4dd90_0.job.log.tgz
        tolog("putfile: %s" % (putfile))
        tolog("full_surl: %s" % (full_surl))

        # get https surl
        full_http_surl = full_surl.replace("srm://", "https://")
        
        # get the DQ2 site name from ToA
        try:
            _dq2SiteName = self.getDQ2SiteName(surl=putfile)
        except Exception, e:
            tolog("Warning: Failed to get the DQ2 site name: %s (can not add this info to tracing report)" % str(e))
        else:
            report['localSite'], report['remoteSite'] = (_dq2SiteName, _dq2SiteName)
            tolog("DQ2 site name: %s" % (_dq2SiteName))

        if testLevel == "1":
            source = "thisisjustatest"

        # determine which timeout option to use
        timeout_option = "--connect-timeout 300 --max-time %d" % (self.timeout)

        sslCert = self.sslCert
        sslKey = self.sslKey
        sslCertDir = self.sslCertDir

        # check htcopy if it is existed or env is set properly
        _cmd_str = 'which htcopy'
        try:
            s, o = commands.getstatusoutput(_cmd_str)
        except Exception, e:
            tolog("!!WARNING!!2990!! Exception caught: %s (%d, %s)" % (str(e), s, o))
            o = str(e)
        
        if s != 0:
            tolog("!!WARNING!!2990!! Command failed: %s" % (_cmd_str))
            o = o.replace('\n', ' ')
            tolog("!!WARNING!!2990!! check PUT command failed. Status=%s Output=%s" % (str(s), str(o)))
            return 999999

        # cleanup the SURL if necessary (remove port and srm substring)
        if token:
            # used lcg-cp options:
            # --srcsetype: specify SRM version
            #   --verbose: verbosity on
            #        --vo: specifies the Virtual Organization the user belongs to
            #          -s: space token description
            #          -b: BDII disabling
            #          -t: time-out
            # (lcg-cr) -l: specifies the Logical File Name associated with the file. If this option is present, an entry is added to the LFC
            #          -g: specifies the Grid Unique IDentifier. If this option is not present, a GUID is generated internally
            #          -d: specifies the destination. It can be the Storage Element fully qualified hostname or an SURL. In the latter case,
            #              the scheme can be sfn: for a classical SE or srm:. If only the fully qualified hostname is given, a filename is
            #              generated in the same format as with the Replica Manager
            # _cmd_str = '%s lcg-cr --verbose --vo atlas -T srmv2 -s %s -b -t %d -l %s -g %s -d %s file:%s' %\
            #           (envsetup, token, self.timeout, lfclfn, guid, surl, fppfn)
            # usage: lcg-cp [-h,--help] [-i,--insecure] [-c,--config config_file]
            #               [-n nbstreams] [-s,--sst src_spacetokendesc] [-S,--dst dest_spacetokendesc]
            #               [-D,--defaultsetype se|srmv1|srmv2] [-T,--srcsetype se|srmv1|srmv2] [-U,--dstsetype se|srmv1|srmv2]
            #               [-b,--nobdii] [-t timeout] [-v,--verbose]  [-V,--vo vo] [--version] src_file  dest_file

            # surl = putfile[putfile.index('srm://'):]
            _cmd_str = '%s htcopy --ca-path %s --user-cert %s --user-key %s "%s?spacetoken=%s"' % (envsetup, sslCertDir, sslCert, sslKey, full_http_surl, token)
            #_cmd_str = '%s lcg-cp --verbose --vo atlas -b %s -U srmv2 -S %s file://%s %s' % (envsetup, timeout_option, token, source, full_surl)
        else:
            # surl is the same as putfile
            _cmd_str = '%s htcopy --ca-path %s --user-cert %s --user-key %s "%s"' % (envsetup, sslCertDir, sslCert, sslKey, full_http_surl)
            #_cmd_str = '%s lcg-cp --vo atlas --verbose -b %s -U srmv2 file://%s %s' % (envsetup, timeout_option, source, full_surl)

        tolog("Executing command: %s" % (_cmd_str))
        ec = -1
        t0 = os.times()
        o = '(not defined)'
        report['relativeStart'] = time()
        report['transferStart'] =  time()
        try:
            ec, o = commands.getstatusoutput(_cmd_str)
        except Exception, e:
            tolog("!!WARNING!!2999!! lcg-cp threw an exception: %s" % (o))
            o = str(e)
        report['validateStart'] = time()
        t1 = os.times()
        t = t1[4] - t0[4]
        tolog("Command finished after %f s" % (t))
        tolog("ec = %d, o = %s, len(o) = %d" % (ec, o, len(o)))

        if ec != 0:
            tolog("!!WARNING!!2990!! Command failed: %s" % (_cmd_str))
            check_syserr(ec, o)
            tolog('!!WARNING!!2990!! put_data failed: Status=%d Output=%s' % (ec, str(o)))

            # check if file was partially transferred, if so, remove it
            _ec = self.removeFile(envsetup, self.timeout, dst_gpfn)
            if _ec == -2:
                pilotErrorDiag += "(failed to remove file) " # i.e. do not retry stage-out

            if "Could not establish context" in o:
                pilotErrorDiag += "Could not establish context: Proxy / VO extension of proxy has probably expired"
                tolog("!!WARNING!!2990!! %s" % (pilotErrorDiag))
                self.__sendReport('CONTEXT_FAIL', report)
                return self.put_data_retfail(error.ERR_NOPROXY, pilotErrorDiag)
            elif "No such file or directory" in o:
                pilotErrorDiag += "No such file or directory: %s" % (o)
                tolog("!!WARNING!!2990!! %s" % (pilotErrorDiag))
                self.__sendReport('NO_FILE_DIR', report)
                return self.put_data_retfail(error.ERR_STAGEOUTFAILED, pilotErrorDiag)
            elif "globus_xio: System error" in o:
                pilotErrorDiag += "Globus system error: %s" % (o)
                tolog("!!WARNING!!2990!! %s" % (pilotErrorDiag))
                self.__sendReport('GLOBUS_FAIL', report)
                return self.put_data_retfail(error.ERR_PUTGLOBUSSYSERR, pilotErrorDiag)
            else:
                if len(o) == 0 and t >= self.timeout:
                    pilotErrorDiag += "Copy command self timed out after %d s" % (t)
                    tolog("!!WARNING!!2990!! %s" % (pilotErrorDiag))
                    self.__sendReport('CP_TIMEOUT', report)
                    return self.put_data_retfail(error.ERR_PUTTIMEOUT, pilotErrorDiag)
                else:
                    if len(o) == 0:
                        pilotErrorDiag += "Copy command returned error code %d but no output" % (ec)
                    else:
                        pilotErrorDiag += o
                    self.__sendReport('CP_ERROR', report)
                    return self.put_data_retfail(error.ERR_STAGEOUTFAILED, pilotErrorDiag)

        verified = False

        # try to get the remote checksum with lcg-get-checksum
        remote_checksum = self.lcgGetChecksum(envsetup, self.timeout, full_surl)
        if not remote_checksum:
            # try to grab the remote file info using lcg-ls command
            remote_checksum, remote_fsize = self.getRemoteFileInfo(envsetup, self.timeout, full_surl)
        else:
            tolog("Setting remote file size to None (not needed)")
            remote_fsize = None

        # compare the checksums if the remote checksum was extracted
        tolog("Remote checksum: %s" % str(remote_checksum))
        tolog("Local checksum: %s" % (fchecksum))

        if remote_checksum:
            if remote_checksum != fchecksum:
                pilotErrorDiag = "Remote and local checksums (of type %s) do not match for %s (%s != %s)" %\
                                 (csumtype, os.path.basename(dst_gpfn), remote_checksum, fchecksum)
                tolog("!!WARNING!!1800!! %s" % (pilotErrorDiag))
                if csumtype == "adler32":
                    self.__sendReport('AD_MISMATCH', report)
                    return self.put_data_retfail(error.ERR_PUTADMISMATCH, pilotErrorDiag, surl=full_surl)
                else:
                    self.__sendReport('MD5_MISMATCH', report)
                    return self.put_data_retfail(error.ERR_PUTMD5MISMATCH, pilotErrorDiag, surl=full_surl)
            else:
                tolog("Remote and local checksums verified")
                verified = True
        else:
            tolog("Skipped primary checksum verification (remote checksum not known)")

        # if lcg-ls could not be used
        if "/pnfs/" in surl and not remote_checksum:
            # for dCache systems we can test the checksum with the use method
            tolog("Detected dCache system: will verify local checksum with the local SE checksum")
            # gpfn = srm://head01.aglt2.org:8443/srm/managerv2?SFN=/pnfs/aglt2.org/atlasproddisk/mc08/EVNT/mc08.109270.J0....
            path = surl[surl.find('/pnfs/'):]
            # path = /pnfs/aglt2.org/atlasproddisk/mc08/EVNT/mc08.109270.J0....#
            tolog("File path: %s" % (path))

            _filename = os.path.basename(path)
            _dir = os.path.dirname(path)

            # get the remote checksum
            tolog("Local checksum: %s" % (fchecksum))
            try:
                remote_checksum = self.getdCacheChecksum(_dir, _filename)
            except Exception, e:
                pilotErrorDiag = "Could not get checksum from dCache: %s (test will be skipped)" % str(e)
                tolog('!!WARNING!!2999!! %s' % (pilotErrorDiag))
            else:
                if remote_checksum == "NOSUCHFILE":
                    pilotErrorDiag = "The pilot will fail the job since the remote file does not exist"
                    tolog('!!WARNING!!2999!! %s' % (pilotErrorDiag))
                    self.__sendReport('NOSUCHFILE', report)
                    return self.put_data_retfail(error.ERR_NOSUCHFILE, pilotErrorDiag)
                elif remote_checksum:
                    tolog("Remote checksum: %s" % (remote_checksum))
                else:
                    tolog("Could not get remote checksum")

            if remote_checksum:
                if remote_checksum != fchecksum:
                    pilotErrorDiag = "Remote and local checksums (of type %s) do not match for %s (%s != %s)" %\
                                     (csumtype, _filename, remote_checksum, fchecksum)
                    if csumtype == "adler32":
                        self.__sendReport('AD_MISMATCH', report)
                        return self.put_data_retfail(error.ERR_PUTADMISMATCH, pilotErrorDiag, surl=full_surl)
                    else:
                        self.__sendReport('MD5_MISMATCH', report)
                        return self.put_data_retfail(error.ERR_PUTMD5MISMATCH, pilotErrorDiag, surl=full_surl)
                else:
                    tolog("Remote and local checksums verified")
                    verified = True
        else:
            tolog("Skipped secondary checksum test")

        # if the checksum could not be verified (as is the case for non-dCache sites) test the file size instead
        if not remote_checksum and remote_fsize:
            tolog("Local file size: %s" % (fsize))

            if remote_fsize and remote_fsize != "" and fsize != "" and fsize:
                if str(fsize) != remote_fsize:
                    pilotErrorDiag = "Remote and local file sizes do not match for %s (%s != %s)" %\
                                     (_filename, remote_fsize, str(fsize))
                    tolog('!!WARNING!!2999!! %s' % (pilotErrorDiag))
                    self.__sendReport('FS_MISMATCH', report)
                    return self.put_data_retfail(error.ERR_PUTWRONGSIZE, pilotErrorDiag, surl=full_surl)
                else:
                    tolog("Remote and local file sizes verified")
                    verified = True
            else:
                tolog("Skipped file size test")

        # was anything verified?
        if not verified:
            # fail at this point
            pilotErrorDiag = "Neither checksum nor file size could be verified (failing job)"
            tolog('!!WARNING!!2999!! %s' % (pilotErrorDiag))
            self.__sendReport('NOFILEVERIFICATION', report)
            return self.put_data_retfail(error.ERR_NOFILEVERIFICATION, pilotErrorDiag)

        self.__sendReport('DONE', report)
        return 0, pilotErrorDiag, full_surl, fsize, fchecksum, self.arch_type

    def __sendReport(self, state, report):
        """
        Send DQ2 tracing report. Set the client exit state and finish
        """
        if report.has_key('timeStart'):
            # finish instrumentation
            report['timeEnd'] = time()
            report['clientState'] = state
            # send report
            tolog("Updated tracing report: %s" % str(report))
            self.sendTrace(report)



# export PilotHomeDir=pants

if __name__ == "__main__":
#  surl='https://fozzie.ndgf.org:2881/atlas/disk/atlashotdisk/ddo/DBRelease/v200202/ddo.000001.Atlas.Ideal.DBRelease.v200202/DBRelease-20.2.2.tar.gz'
  surl='srm://lcg-lrz-se.lrz-muenchen.de/pnfs/lrz-muenchen.de/data/atlas/dq2/atlashotdisk/ddo/DBRelease/v200201/ddo.000001.Atlas.Ideal.DBRelease.v200201/DBRelease-20.2.1.tar.gz'

  mover=aria2cSiteMover("")
  mover.get_data(surl,'somelfn','/tmp',616103906,'checky','guidguid')
