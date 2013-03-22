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

# configuration
"""
try:
    baseURL = os.environ['PANDA_URL']
except:
    baseURL = 'http://pandaserver.cern.ch:25080/server/panda'
try:
    baseURLSSL = os.environ['PANDA_URL_SSL']
except:
    baseURLSSL = 'https://voatlas249.cern.ch:25443/server/panda'
"""
baseURL = 'http://voatlas220.cern.ch:25080/server/panda'
#baseURLSSL = 'https://voatlas220.cern.ch:25443/server/panda'
baseURLSSL = 'https://pandaserver.cern.ch:25443/server/panda'
baseURLCSRVSSL = "https://voatlas178.cern.ch:25443/server/panda"
baseURLSUB     = "http://pandaserver.cern.ch:25080/trf/user"


# exit code
EC_Failed = 255

globalTmpDir = ''

def userCertFile(userDN, vo, group, role):
    x509 = os.environ.get('X509_USER_PROXY', '')
    LOGGER.debug(x509)
    #x509 = "/tmp/%s" % sha1( userDN + vo + group + role ).hexdigest()
    if os.access(x509,os.R_OK):
        return x509
    LOGGER.debug("No valid grid proxy certificate found")
    LOGGER.debug("Looking for proxy certificate in: %s" % x509)

def _x509():
    # see X509_USER_PROXY
    try:
        return os.environ['X509_USER_PROXY']
    except:
        pass
    # see the default place
    x509 = '/tmp/x509up_u%s' % os.getuid()
    if os.access(x509,os.R_OK):
        return x509
    # no valid proxy certificate
    # FIXME
    LOGGER.debug("No valid grid proxy certificate found")
    return ''


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

# refresh spacs at runtime
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


# initialize spacs
refreshSpecs()


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

# get PandaIDs for a JobID
def getPandIDsWithJobID(jobID,user,vo,group,role,dn=None,nJobs=0,verbose=False):
    # instantiate curl
    curl = _Curl()
    curl.sslCert = userCertFile(user, vo, group, role)
    curl.sslKey  = userCertFile(user, vo, group, role)

    curl.verbose = verbose
    # execute
    url = baseURLSSL + '/getPandIDsWithJobID'
    data = {'jobID':jobID, 'nJobs':nJobs}
    if dn != None:
        data['dn'] = dn
    status,output = curl.post(url,data)
    if status!=0:
        LOGGER.debug(output)
        return status,None
    try:
        return status,pickle.loads(output)
    except:
        type, value, traceBack = sys.exc_info()
        LOGGER.error("ERROR getPandIDsWithJobID : %s %s" % (type,value))
        return EC_Failed,None
