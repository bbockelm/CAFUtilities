import os
import re
import sys
import time
import stat
import types
import random
import urllib
import struct
import commands
import cPickle as pickle
import xml.dom.minidom
import socket
import tempfile
import logging
from hashlib import sha1

LOGGER = logging.getLogger(__name__)

baseURL = 'http://voatlas220.cern.ch:25080/server/panda'
baseURLSSL = 'https://pandaserver.cern.ch:25443/server/panda'

baseURLCSRV    = "http://voatlas294.cern.ch:25080/server/panda"
baseURLCSRVSSL = "https://voatlas294.cern.ch:25443/server/panda"

# exit code
EC_Failed = 255

globalTmpDir = ''

serverCert = None
serverKey = None
serverDN = None
uiSource = None
credServerPath = '/tmp'

def initProxyParameters(serverkey, servercert, serverdn, uisource, credpath):
    global serverCert, serverKey, serverDN, uiSource, credServerPath
    serverCert, serverKey, serverDN, uiSource, credServerPath = servercert, serverkey, serverdn, uisource, credpath

def retrieveUserProxy(user, vo, group, role):
    from WMCore.Credential.Proxy import Proxy
    myproxyserver = "myproxy.cern.ch"
    defaultDelegation = { 'vo': vo,
                          'logger': LOGGER,
                          'myProxySvr': 'myproxy.cern.ch',
                          'proxyValidity' : '192:00',
                          'min_time_left' : 36000,
                          'userDN' : user,
                          'group' : group if group else '',
                          'role' : role if role else '',
                          'server_key': serverKey,
                          'server_cert': serverCert,
                          'serverDN': serverDN,
                          'uisource': uiSource,
                          'credServerPath': credServerPath,}
    timeleftthreshold = 60 * 60 * 24
    proxy = Proxy(defaultDelegation)
    userproxy = proxy.getProxyFilename(serverRenewer=True)
    if proxy.getTimeLeft(userproxy) < timeleftthreshold:
        proxy.logonRenewMyProxy()
        timeleft = proxy.getTimeLeft(userproxy)
        if timeleft is None or timeleft <= 0:
            raise RuntimeError("Impossible to retrieve proxy from %s for %s." %(defaultDelegation['myProxySvr'], defaultDelegation['userDN']))

    LOGGER.debug("User proxy file path: %s" % userproxy)
    return userproxy


def userCertFile(userDN, vo, group, role):
    userProxy = retrieveUserProxy(userDN, vo, group, role)
    if os.access(userProxy,os.R_OK):
        return userProxy
    LOGGER.error("No valid grid proxy certificate found")
    LOGGER.debug("Looking for proxy certificate in: %s" % userProxy)

def _x509():
    try:
        return os.environ['X509_USER_PROXY']
    except:
        pass
    # no valid proxy certificate
    # FIXME raise exception or something?

    x509 = '/tmp/x509up_u%s' % os.getuid()
    if os.access(x509,os.R_OK):
        return x509
    # no valid proxy certificate
    # FIXME
    raise PanDaException("No valid grid proxy certificate found")

# look for a CA certificate directory
def _x509_CApath():
    # use X509_CERT_DIR
    try:
        return os.environ['X509_CERT_DIR']
    except:
        pass
    # get X509_CERT_DIR
    gridSrc = _getGridSrc()
    com = "%s echo $X509_CERT_DIR" % gridSrc
    tmpOut = commands.getoutput(com)
    return tmpOut.split('\n')[-1]



# curl class
class _Curl:
    # constructor
    def __init__(self):
        # path to curl
        self.path = 'curl --user-agent "dqcurl" '
        # verification of the host certificate
        self.verifyHost = False
        # request a compressed response
        self.compress = True
        # SSL cert/key
        self.sslCert = ''
        self.sslKey  = ''
        # verbose
        self.verbose = False

    # GET method
    def get(self,url,data,rucioAccount=False):
        # make command
        com = '%s --silent --get' % self.path
        if not self.verifyHost:
            com += ' --insecure'
        else:
            com += ' --capath %s' %  _x509_CApath()
        if self.compress:
            com += ' --compressed'
        if self.sslCert != '':
            com += ' --cert %s' % self.sslCert
        if self.sslKey != '':
            com += ' --key %s' % self.sslKey
        # add rucio account info
        if rucioAccount:
            if os.environ.has_key('RUCIO_ACCOUNT'):
                data['account'] = os.environ['RUCIO_ACCOUNT']
            if os.environ.has_key('RUCIO_APPID'):
                data['appid'] = os.environ['RUCIO_APPID']
        # data
        strData = ''
        for key in data.keys():
            strData += 'data="%s"\n' % urllib.urlencode({key:data[key]})
        # write data to temporary config file
        if globalTmpDir != '':
            tmpFD,tmpName = tempfile.mkstemp(dir=globalTmpDir)
        else:
            tmpFD,tmpName = tempfile.mkstemp()
        os.write(tmpFD,strData)
        os.close(tmpFD)
        com += ' --config %s' % tmpName
        com += ' %s' % url
        # execute
        if self.verbose:
            LOGGER.debug(com)
            LOGGER.debug(strData[:-1])
        s,o = commands.getstatusoutput(com)
        if o != '\x00':
            try:
                tmpout = urllib.unquote_plus(o)
                o = eval(tmpout)
            except:
                pass
        ret = (s,o)
        # remove temporary file
        os.remove(tmpName)
        ret = self.convRet(ret)
        if self.verbose:
            LOGGER.debug(ret)
        return ret

    def post(self,url,data,rucioAccount=False):
        # make command
        com = '%s --silent' % self.path
        if not self.verifyHost:
            com += ' --insecure'
        else:
            com += ' --capath %s' %  _x509_CApath()
        if self.compress:
            com += ' --compressed'
        if self.sslCert != '':
            com += ' --cert %s' % self.sslCert
            #com += ' --cert %s' % '/tmp/mycredentialtest'
        if self.sslKey != '':
            com += ' --key %s' % self.sslCert
            #com += ' --key %s' % '/tmp/mycredentialtest'
        # add rucio account info
        if rucioAccount:
            if os.environ.has_key('RUCIO_ACCOUNT'):
                data['account'] = os.environ['RUCIO_ACCOUNT']
            if os.environ.has_key('RUCIO_APPID'):
                data['appid'] = os.environ['RUCIO_APPID']
        # data
        strData = ''
        for key in data.keys():
            strData += 'data="%s"\n' % urllib.urlencode({key:data[key]})
        # write data to temporary config file
        if globalTmpDir != '':
            tmpFD,tmpName = tempfile.mkstemp(dir=globalTmpDir)
        else:
            tmpFD,tmpName = tempfile.mkstemp()
        os.write(tmpFD,strData)
        os.close(tmpFD)
        com += ' --config %s' % tmpName
        com += ' %s' % url
        # execute
        if self.verbose:
            LOGGER.debug(com)
            LOGGER.debug(strData[:-1])
        s,o = commands.getstatusoutput(com)
        #print s,o
        if o != '\x00':
            try:
                tmpout = urllib.unquote_plus(o)
                o = eval(tmpout)
            except:
                pass
        ret = (s,o)
        # remove temporary file
        os.remove(tmpName)
        ret = self.convRet(ret)
        if self.verbose:
            LOGGER.debug(ret)
        return ret

    # PUT method
    def put(self,url,data):
        # make command
        com = '%s --silent' % self.path
        if not self.verifyHost:
            com += ' --insecure'
        if self.compress:
            com += ' --compressed'
        if self.sslCert != '':
            com += ' --cert %s' % self.sslCert
            #com += ' --cert %s' % '/data/certs/prova'
        if self.sslKey != '':
            com += ' --key %s' % self.sslKey
        # emulate PUT
        for key in data.keys():
            com += ' -F "%s=@%s"' % (key,data[key])
        com += ' %s' % url
        if self.verbose:
            LOGGER.debug(com)
        # execute
        ret = commands.getstatusoutput(com)
        ret = self.convRet(ret)
        if self.verbose:
            LOGGER.debug(ret)
        return ret


    # convert return
    def convRet(self,ret):
        if ret[0] != 0:
            ret = (ret[0]%255,ret[1])
        # add messages to silent errors
        if ret[0] == 35:
            ret = (ret[0],'SSL connect error. The SSL handshaking failed. Check grid certificate/proxy.')
        elif ret[0] == 7:
            ret = (ret[0],'Failed to connect to host.')
        elif ret[0] == 55:
            ret = (ret[0],'Failed sending network data.')
        elif ret[0] == 56:
            ret = (ret[0],'Failure in receiving network data.')
        return ret


# get site specs
def getSiteSpecs(siteType=None):
    # instantiate curl
    curl = _Curl()
    # execute
    url = baseURL + '/getSiteSpecs'
    data = {}
    if siteType != None:
        data['siteType'] = siteType
    status,output = curl.get(url,data)
    try:
        return status,pickle.loads(output)
    except:
        type, value, traceBack = sys.exc_info()
        errStr = "ERROR getSiteSpecs : %s %s" % (type,value)
        LOGGER.error(errStr)
        return EC_Failed,output+'\n'+errStr


# get cloud specs
def getCloudSpecs():
    # instantiate curl
    curl = _Curl()
    # execute
    url = baseURL + '/getCloudSpecs'
    status,output = curl.get(url,{})
    try:
        return status,pickle.loads(output)
    except:
        type, value, traceBack = sys.exc_info()
        errStr = "ERROR getCloudSpecs : %s %s" % (type,value)
        LOGGER.error(errStr)
        return EC_Failed,output+'\n'+errStr

# refresh specs
def refreshSpecs():

    global PandaSites
    global PandaClouds
    # get Panda Sites
    tmpStat,PandaSites = getSiteSpecs()
    if tmpStat != 0:
        LOGGER.error("ERROR : cannot get Panda Sites")
        sys.exit(EC_Failed)
    # get cloud info
    tmpStat,PandaClouds = getCloudSpecs()
    if tmpStat != 0:
        LOGGER.error("ERROR : cannot get Panda Clouds")
        sys.exit(EC_Failed)


# submit jobs
def submitJobs(jobs, user, vo, group, role, workflow, verbose=False):
    # set hostname
    hostname = commands.getoutput('hostname')
    for job in jobs:
        job.creationHost = hostname
    # serialize
    strJobs = pickle.dumps(jobs)
    # instantiate curl
    curl = _Curl()
    #curl.sslCert = _x509()
    #curl.sslKey  = _x509()
    curl.sslCert = userCertFile(user, vo, group, role)
    curl.sslKey  = userCertFile(user, vo, group, role)
    curl.verbose = True #verbose
    # execute
    url = baseURLSSL + '/submitJobs'
    data = {'jobs':strJobs}
    status,output = curl.post(url,data)
    #print 'SUBMITJOBS CALL --> status: %s - output: %s' % (status, output)
    if status!=0:
        LOGGER.error('==============================')
        LOGGER.error('submitJobs output: %s' % output)
        LOGGER.error('submitJobs status: %s' % status)
        LOGGER.error('==============================')
        return status,None
    try:
        return status,pickle.loads(output)
    except:
        type, value, traceBack = sys.exc_info()
        LOGGER.error("ERROR submitJobs : %s %s" % (type,value))
        return EC_Failed,None


# run brokerage
def runBrokerage(user, vo, group, role, sites,
                 atlasRelease=None, cmtConfig=None, verbose=False, trustIS=False, cacheVer='',
                 processingType='', loggingFlag=False, memorySize=0, useDirectIO=False, siteGroup=None,
                 maxCpuCount=-1):
    # use only directIO sites
    nonDirectSites = []
    if useDirectIO:
        tmpNewSites = []
        for tmpSite in sites:
            if isDirectAccess(tmpSite):
                tmpNewSites.append(tmpSite)
            else:
                nonDirectSites.append(tmpSite)
        sites = tmpNewSites
    if sites == []:
        if not loggingFlag:
            return 0,'ERROR : no candidate.'
        else:
            return 0,{'site':'ERROR : no candidate.','logInfo':[]}
    # choose at most 50 sites randomly to avoid too many lookup
    random.shuffle(sites)
    sites = sites[:50]
    # serialize
    strSites = pickle.dumps(sites)
    # instantiate curl
    curl = _Curl()
    #curl.sslCert = _x509()
    #curl.sslKey  = _x509()
    curl.sslKey = curl.sslCert = userCertFile(user, vo, group, role)
    curl.verbose = verbose
    # execute
    url = baseURLSSL + '/runBrokerage'
    data = {'sites':strSites,
            'atlasRelease':atlasRelease}
    if cmtConfig != None:
        data['cmtConfig'] = cmtConfig
    if trustIS:
        data['trustIS'] = True
    if maxCpuCount > 0:
        data['maxCpuCount'] = maxCpuCount
    if cacheVer != '':
        # change format if needed
        cacheVer = re.sub('^-','',cacheVer)
        match = re.search('^([^_]+)_(\d+\.\d+\.\d+\.\d+\.*\d*)$',cacheVer)
        if match != None:
            cacheVer = '%s-%s' % (match.group(1),match.group(2))
        else:
            # nightlies
            match = re.search('_(rel_\d+)$',cacheVer)
            if match != None:
                # use base release as cache version
                cacheVer = '%s:%s' % (atlasRelease,match.group(1))
        # use cache for brokerage
        data['atlasRelease'] = cacheVer
    if processingType != '':
        # set processingType mainly for HC
        data['processingType'] = processingType
    # enable logging
    if loggingFlag:
        data['loggingFlag'] = True
    # memory size
    if not memorySize in [-1,0,None,'NULL']:
        data['memorySize'] = memorySize
    # site group
    if not siteGroup in [None,-1]:
        data['siteGroup'] = siteGroup
    status,output = curl.get(url,data)
    try:
        if not loggingFlag:
            return status,output
        else:
            outputPK = pickle.loads(output)
            # add directIO info
            if nonDirectSites != []:
                if not outputPK.has_key('logInfo'):
                    outputPK['logInfo'] = []
                for tmpSite in nonDirectSites:
                    msgBody = 'action=skip site=%s reason=nondirect - not directIO site' % tmpSite
                    outputPK['logInfo'].append(msgBody)
            return status,outputPK
    except:
        type, value, traceBack = sys.exc_info()
        LOGGER.error(output)
        LOGGER.error("ERROR runBrokerage : %s %s" % (type,value))
        return EC_Failed,None


####################################################################################
# Only the following function -getPandIDsWithJobID- is directly called by the REST #
####################################################################################
# get PandaIDs for a JobID
#TODO check if we can remove user, vo, group, role,
def getPandIDsWithJobID(jobID, user, vo, group, role, dn=None, nJobs=0, verbose=False, userproxy=None, credpath=None):
    # instantiate curl
    curl = _Curl()
    curl.verbose = verbose
    # execute
    url = baseURLSSL + '/getPandIDsWithJobID'
    data = {'jobID':jobID, 'nJobs':nJobs}
    if dn != None:
        data['dn'] = dn

    # Temporary solution we cache the proxy file
    filehandler, proxyfile = tempfile.mkstemp(dir=credpath)
    with open(proxyfile, 'w') as pf:
        pf.write(userproxy)
    curl.sslCert = proxyfile
    curl.sslKey  = proxyfile 

    status = None
    output = None
    try:
        # call him ...
        status, output = curl.post(url, data)
    except:
        type, value, traceBack = sys.exc_info()
        LOGGER.error("ERROR getPandIDsWithJobID : %s %s" % (type, value))
    finally:
        # Always delete it!
        os.remove(proxyfile)

    if status is not None and status!=0:
        LOGGER.debug(str(output))
        return status, None
    try:
        return status, pickle.loads(output)
    except:
        type, value, traceBack = sys.exc_info()
        LOGGER.error("ERROR getPandIDsWithJobID : %s %s" % (type, value))
        return EC_Failed, None


# kill jobs
#TODO check if we can remove user, vo, group, role,
def killJobs(user, vo, group, role, ids, code=None, verbose=True, useMailAsID=False):
    # serialize
    strIDs = pickle.dumps(ids)
    # instantiate curl
    curl = _Curl()
    curl.sslCert = _x509()
    curl.sslKey  = _x509()
    curl.verbose = verbose
    # execute
    url = baseURLSSL + '/killJobs'
    data = {'ids':strIDs,'code':code,'useMailAsID':useMailAsID}
    status,output = curl.post(url,data)
    try:
        return status,pickle.loads(output)
    except:
        type, value, traceBack = sys.exc_info()
        errStr = "ERROR killJobs : %s %s" % (type,value)
        print errStr
        return EC_Failed,output+'\n'+errStr

# get full job status
def getFullJobStatus(ids,user,vo,group,role,verbose=False):
    # serialize
    strIDs = pickle.dumps(ids)
    # instantiate curl
    curl = _Curl()
    curl.sslCert = userCertFile(user, vo, group, role)
    curl.sslKey  = userCertFile(user, vo, group, role)
    curl.verbose = verbose
    # execute
    url = baseURLSSL + '/getFullJobStatus'
    data = {'ids':strIDs}
    status,output = curl.post(url,data)
    try:
        return status,pickle.loads(output)
    except Exception, ex:
        type, value, traceBack = sys.exc_info()
        LOGGER.error("ERROR getFullJobStatus : %s %s" % (type,value))
        LOGGER.error(str(traceBack))


def putFile(file,verbose=False,useCacheSrv=False,reuseSandbox=False):
    # size check for noBuild
    sizeLimit = 100*1024*1024

    fileSize = os.stat(file)[stat.ST_SIZE]
    if not file.startswith('sources.'):
        if fileSize > sizeLimit:
            errStr  = 'Exceeded size limit (%sB >%sB). ' % (fileSize,sizeLimit)
            errStr += 'Your working directory contains too large files which cannot be put on cache area. '
            errStr += 'Please submit job without --noBuild/--libDS so that your files will be uploaded to SE'
            # get logger
            raise PanDaException(errStr)
    # instantiate curl
    curl = _Curl()
    curl.sslCert = _x509()
    curl.sslKey  = _x509()
    curl.verbose = verbose
    # check duplicationn
    if reuseSandbox:
        # get CRC
        fo = open(file)
        fileContent = fo.read()
        fo.close()
        footer = fileContent[-8:]
        checkSum,isize = struct.unpack("II",footer)
        # check duplication
        url = baseURLSSL + '/checkSandboxFile'
        data = {'fileSize':fileSize,'checkSum':checkSum}
        status,output = curl.post(url,data)
        if status != 0:
            raise PanDaException('ERROR: Could not check Sandbox duplication with %s' % status)
        elif output.startswith('FOUND:'):
            # found reusable sandbox
            hostName,reuseFileName = output.split(':')[1:]
            # set cache server hostname
            global baseURLCSRV
            baseURLCSRV    = "http://%s:25080/server/panda" % hostName
            global baseURLCSRVSSL
            baseURLCSRVSSL = "https://%s:25443/server/panda" % hostName
            # return reusable filename
            return 0,"NewFileName:%s:%s" % (hostName, reuseFileName)
    # execute
    if useCacheSrv:
        url = baseURLCSRVSSL + '/putFile'
    else:
        url = baseURLSSL + '/putFile'
    data = {'file':file}
    status, output = curl.put(url, data)
    filename = ""
    servername = ""
    if status !=0:
        raise PanDaException("Failure uploading input file into PanDa")
    else:
       matchURL = re.search("(http.*://[^/]+)/", baseURLCSRVSSL)
       return 0, "True:%s:%s" % (matchURL.group(1), file.split('/')[-1])
    #return curl.put(url,data)


def wrappedUuidGen():
    # check if uuidgen is available
    tmpSt,tmpOut = commands.getstatusoutput('which uuidgen')
    if tmpSt == 0:
        # use uuidgen
        return commands.getoutput('uuidgen 2>/dev/null')
    else:
        # use python uuidgen
        try:
            import uuid
        except:
            raise ImportError,'uuidgen and uuid.py are unavailable on your system. Please install one of them'
        return str(uuid.uuid4())

