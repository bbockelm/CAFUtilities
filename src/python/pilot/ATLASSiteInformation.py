# Class definition:
#   ATLASSiteInformation
#   This class is the ATLAS site information class inheriting from SiteInformation
#   Instances are generated with SiteInformationFactory via pUtil::getSiteInformation()
#   Implemented as a singleton class
#   http://stackoverflow.com/questions/42558/python-and-the-singleton-pattern

# import relevant python/pilot modules
import os
import commands
import SiteMover
from SiteInformation import SiteInformation  # Main site information class
from pUtil import tolog                      # Logging method that sends text to the pilot log
from pUtil import readpar                    # Used to read values from the schedconfig DB (queuedata)
from pUtil import getExtension               # Used to determine file type of Tier-1 info file
from PilotErrors import PilotErrors          # Error codes

class ATLASSiteInformation(SiteInformation):

    # private data members
    __experiment = "ATLAS"
    __instance = None

    # Required methods

    def __init__(self):
        """ Default initialization """

        pass

    def __new__(cls, *args, **kwargs):
        """ Override the __new__ method to make the class a singleton """

        if not cls.__instance:
            cls.__instance = super(ATLASSiteInformation, cls).__new__(cls, *args, **kwargs)

        return cls.__instance

    def getExperiment(self):
        """ Return a string with the experiment name """

        return self.__experiment

    def isTier1(self, sitename):
        """ Is the given site a Tier-1? """
        # E.g. on a Tier-1 site, the alternative stage-out algorithm should not be used
        # Note: sitename is PanDA sitename, not DQ2 sitename

        status = False

        for cloud in self.getCloudList():
            if sitename in self.getTier1List(cloud):
                status = True
                break
        return status

    def isTier2(self, sitename):
        """ Is the given site a Tier-2? """
        # Logic: it is a T2 if it is not a T1 or a T3

        return (not (self.isTier1(sitename) or self.isTier3()))

    def isTier3(self):
        """ Is the given site a Tier-3? """
        # Note: defined by DB

        if readpar('ddm') == "local":
            status = True
        else:
            status = False

        return status

    def getCloudList(self):
        """ Return a list of all clouds """

        tier1 = self.setTier1Info()
        return tier1.keys()

    def setTier1Info(self):
        """ Set the Tier-1 information """

        tier1 = {"CA": ["TRIUMF", ""],
                 "CERN": ["CERN-PROD", ""],
                 "DE": ["FZK-LCG2", ""],
                 "ES": ["pic", ""],
                 "FR": ["IN2P3-CC", ""],
                 "IT": ["INFN-T1", ""],
                 "ND": ["ARC", ""],
                 "NL": ["SARA-MATRIX", ""],
                 "OSG": ["BNL_CVMFS_1", ""],
                 "TW": ["Taiwan-LCG2", ""],
                 "UK": ["RAL-LCG2", ""],
                 "US": ["BNL_CVMFS_1", "BNL_CVMFS_1-condor"]
                 }
        return tier1

    def getTier1List(self, cloud):
        """ Return a Tier 1 site/queue list """
        # Cloud : PanDA site, queue

        tier1 = self.setTier1Info()
        return tier1[cloud]

    def getTier1InfoFilename(self):
        """ Get the Tier-1 info file name """

        filename = "Tier-1_info.%s" % (getExtension())
        path = "%s/%s" % (os.environ['PilotHomeDir'], filename)

        return path

    def downloadTier1Info(self):
        """ Download the Tier-1 info file """

        ec = 0

        path = self.getTier1InfoFilename()
        filename = os.path.basename(path)
        dummy, extension = os.path.splitext(filename)

        # url = "http://adc-ssb.cern.ch/SITE_EXCLUSION/%s" % (filename)
        if extension == ".json":
            _cmd = "?json&preset=ssbpilot"
        else:
            _cmd = "?preset=ssbpilot"
        url = "http://atlas-agis-api.cern.ch/request/site/query/list/%s" % (_cmd)
        cmd = 'curl --connect-timeout 20 --max-time 120 -sS "%s" > %s' % (url, path)

        if os.path.exists(path):
            tolog("File %s already available" % (path))
        else:
            tolog("Will download file: %s" % (filename))

            try:
                tolog("Executing command: %s" % (cmd))
                ret, output = commands.getstatusoutput(cmd)
            except Exception, e:
                tolog("!!WARNING!!1992!! Could not download file: %s" % (e))
                ec = -1
            else:
                tolog("Done")

        return ec

    def getTier1Queue(self, cloud):
        """ Download the queuedata for the Tier-1 in the corresponding cloud and get the queue name """

        queuename = ""

        path = self.getTier1InfoFilename()
        ec = self.downloadTier1Info()
        if ec == 0:
            # process the downloaded T-1 info
            f = open(path, 'r')
            if getExtension() == "json":
                from json import loads
                data = loads(f.read())
            else:
                from pickle import load
                data = load(f)
            f.close()

            # extract the relevant queue info for the given cloud
            T1_info = [x for x in data if x['cloud']==cloud]

            # finally get the queue name
            if T1_info != []:
                info = T1_info[0]
                if info.has_key('PanDAQueue'):
                    queuename = info['PanDAQueue']
                else:
                    tolog("!!WARNING!!1222!! Returned Tier-1 info object does not have key PanDAQueue: %s" % str(info))
            else:
                tolog("!!WARNING!!1223!! Found no Tier-1 info for cloud %s" % (cloud))

        return queuename

    def allowAlternativeStageOut(self):
        """ Is alternative stage-out allowed? """
        # E.g. if stage-out to primary SE (at Tier-2) fails repeatedly, is it allowed to attempt stage-out to secondary SE (at Tier-1)?

        enableT1stageout = "False" #"True" # readpar('enableT1stageout')

        if enableT1stageout.lower() == "true" or enableT1stageout.lower() == "retry":
            status = True
        else:
            status = False
        return status

    def getProperPaths(self, error, analyJob, token, prodSourceLabel, dsname, filename, alt=False):
        """ Get proper paths (SURL and LFC paths) """

        ec = 0
        pilotErrorDiag = ""
        tracer_error = ""
        dst_gpfn = ""
        lfcdir = ""
        surl = ""

        # get the proper endpoint
        sitemover = SiteMover.SiteMover()
        se = sitemover.getProperSE(token, alt=alt)

        # for production jobs, the SE path is stored in seprodpath
        # for analysis jobs, the SE path is stored in sepath

        destination = self.getPreDestination(sitemover, analyJob, token, prodSourceLabel, alt=alt)
        if destination == '':
            pilotErrorDiag = "put_data destination path in SE not defined"
            tolog('!!WARNING!!2990!! %s' % (pilotErrorDiag))
            tracer_error = 'PUT_DEST_PATH_UNDEF'
            ec = error.ERR_STAGEOUTFAILED
            return ec, pilotErrorDiag, tracer_error, dst_gpfn, lfcdir, surl
        else:
            tolog("Going to store job output at: %s" % (destination))

        # get the LFC path
        lfcpath, pilotErrorDiag = sitemover.getLFCPath(analyJob, alt=alt)
        if lfcpath == "":
            tracer_error = 'LFC_PATH_EMPTY'
            ec = error.ERR_STAGEOUTFAILED
            return ec, pilotErrorDiag, tracer_error, dst_gpfn, lfcdir, surl

        tolog("LFC path = %s" % (lfcpath))

        ec, pilotErrorDiag, dst_gpfn, lfcdir = sitemover.getFinalLCGPaths(analyJob, destination, dsname, filename, lfcpath)
        if ec != 0:
            tracer_error = 'UNKNOWN_DSN_FORMAT'
            return ec, pilotErrorDiag, tracer_error, dst_gpfn, lfcdir, surl

        # define the SURL
        surl = "%s%s" % (se, dst_gpfn)
        tolog("SURL = %s" % (surl))
        tolog("dst_gpfn = %s" % (dst_gpfn))
        tolog("lfcdir = %s" % (lfcdir))
        tolog("ATLAS EXPERIMENT")

        return ec, pilotErrorDiag, tracer_error, dst_gpfn, lfcdir, surl

    def getPreDestination(self, sitemover, analJob, token, prodSourceLabel, alt=False):
        """ get the pre destination """

        destination = ""
        if not analJob:
            # process the destination path with getDirList since it can have a complex structure
            # as well as be a list of destination paths matching a corresponding space token
            if prodSourceLabel == 'ddm' and readpar('seprodpath') == '':
                sepath = readpar('sepath', alt=alt)
            else:
                sepath = readpar('seprodpath', alt=alt)
            destinationList = sitemover.getDirList(sepath)

            # decide which destination path to use depending on the space token for the current file
            if token:
                # find the proper path
                destination = sitemover.getMatchingDestinationPath(token, destinationList, alt=alt)
                if destination == "":
                    tolog("!!WARNING!!2990!! seprodpath not properly defined: seprodpath = %s, destinationList = %s, using sepath instead" %\
                          (sepath, str(destinationList)))
                    sepath = readpar('sepath', alt=alt)
                    destinationList = sitemover.getDirList(sepath)
                    destination = sitemover.getMatchingDestinationPath(token, destinationList, alt=alt)
                    if destination == "":
                        tolog("!!WARNING!!2990!! sepath not properly defined: sepath = %s, destinationList = %s" %\
                              (sepath, str(destinationList)))
            else:
                # space tokens are not used
                destination = destinationList[0]
        else:
            sepath = readpar('sepath', alt=alt)
            destinationList = sitemover.getDirList(sepath)

            # decide which destination path to use depending on the space token for the current file
            if token:
                # find the proper path
                destination = sitemover.getMatchingDestinationPath(token, destinationList, alt=alt)
                if destination == "":
                    tolog("!!WARNING!!2990!! sepath not properly defined: sepath = %s, destinationList = %s" %\
                          (sepath, str(destinationList)))
            else:
                # space tokens are not used
                destination = destinationList[0]

        return destination

if __name__ == "__main__":

    os.environ['PilotHomeDir'] = os.getcwd()

    si = ATLASSiteInformation()
    tolog("Experiment: %s" % (si.getExperiment()))

    cloud = "CERN"
    queuename = si.getTier1Queue(cloud)
    if queuename != "":
        tolog("Cloud %s has Tier-1 queue %s" % (cloud, queuename))
    else:
        tolog("Failed to find a Tier-1 queue name for cloud %s" % (cloud))
    
