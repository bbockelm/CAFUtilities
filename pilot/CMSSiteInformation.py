# Class definition:
#   CMSSiteInformation
#   This class is the prototype of a site information class inheriting from SiteInformation
#   Instances are generated with SiteInformationFactory via pUtil::getSiteInformation()
#   Implemented as a singleton class
#   http://stackoverflow.com/questions/42558/python-and-the-singleton-pattern

# import relevant python/pilot modules
import os
import re

from SiteInformation import SiteInformation  # Main site information class
from pUtil import tolog                      # Logging method that sends text to the pilot log
from pUtil import readpar                    # Used to read values from the schedconfig DB (queuedata)
from PilotErrors import PilotErrors          # Error codes

class CMSSiteInformation(SiteInformation):

    # private data members
    __experiment = "CMS"
    __instance = None

    # Required methods

    def __init__(self):
        """ Default initialization """
# not needed?

        pass

    def __new__(cls, *args, **kwargs):
        """ Override the __new__ method to make the class a singleton """

        if not cls.__instance:
            cls.__instance = super(CMSSiteInformation, cls).__new__(cls, *args, **kwargs)

        return cls.__instance

    def getExperiment(self):
        """ Return a string with the experiment name """

        return self.__experiment

    def isTier1(self, sitename):
        """ Is the given site a Tier-1? """

        return False

    def isTier2(self, sitename):
        """ Is the given site a Tier-2? """

        return False

    def isTier3(self):
        """ Is the given site a Tier-3? """

        return False

    def allowAlternativeStageOut(self):
        """ Is alternative stage-out allowed? """
        # E.g. if stage-out to primary SE (at Tier-2) fails repeatedly, is it allowed to attempt stage-out to secondary SE (at Tier-1)?

        return False

    def belongsTo(self, value, rangeStart, rangeEnd):
        if value >= rangeStart and value <= rangeEnd:
                return True, rangeEnd
        rangeStart=(rangeStart+1000)
        return False, rangeStart

    def findRange(self, value, ftype = 'root'):
        rangeStart = 0
        while True:
           a,range = self.belongsTo(value,rangeStart,rangeStart+999)
           if a:
               break
           else:
               rangeStart = range
        range = int(range)+1
        if ftype == 'log': range = '%s/log' % str(range)
        return str(range)

    def getProperPaths(self, error, analyJob, token, prodSourceLabel, dsname, filename, JobData, alt=False):
        """ Called by LocalSiteMover, from put_data method, instead of using SiteMover.getProperPaths 
            needed only if LocalSiteMover is used instead of specific Mover
            serve solo se utilizziamo LocalSiteMover al posto dello specifico Mover 
        """
        """
        # define the full lfn.
        error = kwargs.get('error', None)
        analyJob = kwargs.get('analyJob', None)
        token = kwargs.get('token', None)
        prodSourceLabel = kwargs.get('prodSourceLabel', None)
        dsname = kwargs.get('dsname', None)
        filename = kwargs.get('filename', None)
        sitename = kwargs.get('sitename', None)
        JobData = kwargs.get('JobData', None)
        #appid = kwargs.get('appid', None)
        """

        remoteSE=''
        try:
            import newJobDef
            #todo with JSON
            locals()['newJobDef'] = __import__(os.path.splitext(string.strip(JobData))[0])
            remoteSE = newJobDef.job.get('fileDestinationSE')
            if remoteSE.find(','):
                remoteSE = remoteSE.split(',')[0]
            tolog("############# getProperPaths - remoteSE: %s - filename: %s" % (remoteSE, filename))
        except:
            tolog("############# getProperPaths except!! remoteSE: %s - filename: %s" % (remoteSE, filename)) 
            pass

        ec = 0
        pilotErrorDiag = ""
        tracer_error = ""
        dst_gpfn = ""
        lfcdir = ""
        full_lfn = ""

        if dsname and filename:
            to_strip= '%s%s' % ('-v',dsname.split('-v')[-1:][0])
            tomatch = re.compile('-v([Z0-9-]+)/USER')
            ext = os.path.splitext(filename)[1]
            filenameNew = filename
            if ext[1:].isdigit():
                filenameNew = os.path.splitext(filename)[0]
            ftype = 'root'
            if filename.find('job.log_')>0: ftype = 'log'
            value = int(os.path.splitext(filenameNew)[0].split('_')[-1:][0])
            if tomatch.match(to_strip):
                full_lfn_suffix = (('%s/%s/%s') % (dsname.split(to_strip)[0], self.findRange(value,ftype), filename ))
            else:
                tolog(dsname)
                tolog(to_strip)
                tolog(tomatch.match(to_strip))
                tolog(dsname.split(to_strip)[0])
                pilotErrorDiag = "problem building full_lfn_suffix"
                tolog('!!WARNING!!2990!! %s' % (pilotErrorDiag))
                tracer_error = 'FULL_LFN_PROBLEM'
                ec = error.ERR_STAGEOUTFAILED
                return ec, pilotErrorDiag, tracer_error, dst_gpfn, lfcdir, full_lfn
        else:
            pilotErrorDiag = "dataset name or filename not defined"
            tolog('!!WARNING!!2990!! %s' % (pilotErrorDiag))
            tracer_error = 'CMS_OUTDS_NOT_DEFINED'
            ec = error.ERR_STAGEOUTFAILED
            return ec, pilotErrorDiag, tracer_error, dst_gpfn, lfcdir, full_lfn

        #here we should have a check on the async destination and the local site.
        #   if they are the same we should use full_lfn_prefix = '/store/user' otherwise
        #   we should have full_lfn_prefix = '/store/temp/user/'

        full_lfn_prefix = '/store/temp/user/'

        full_lfn = '%s%s'%( full_lfn_prefix, full_lfn_suffix )

        tolog("full_lfn = %s" % full_lfn )
        tolog("dst_gpfn = %s" % (None))
        tolog("lfcdir = %s" % (None))

        return ec, pilotErrorDiag, tracer_error, dst_gpfn, lfcdir, full_lfn

    def extractAppdir(self, appdir, processingType, homePackage):
        """ Called by pilot.py, runMain method """
        tolog("CMSExperiment - extractAppdir - nothing to do")

        return 0, ""


if __name__ == "__main__":

    a = CMSSiteInformation()
    tolog("Experiment: %s" % (a.getExperiment()))

