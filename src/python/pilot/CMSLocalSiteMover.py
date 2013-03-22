#!/usr/bin/env python2.6
"""
_CMSLocalSiteMover_

Script to perform local stage out at a site for CMS

"""

import time
import os
import sys

from optparse import OptionParser

from WMCore.Storage.SiteLocalConfig import loadSiteLocalConfig

from WMCore.Storage.Registry import retrieveStageOutImpl

import WMCore.Storage.Backends
import WMCore.Storage.Plugins

class LocalStageOut:
    """
    _LocalStageOutDiagnostic_

    Object to perform the local stage out

    """

    def __init__(self,verbose=False):
        self.verbose=verbose
        self.summary = {}
        self.summary.setdefault('SiteConf' , "NotRun" )
        self.summary.setdefault('TFC' , "NotRun")
        self.summary.setdefault('LocalStageOut' , "NotRun")
        self.summary.setdefault('CleanUp' , "NotRun")
        self.status = -1
        self.siteConf = None
        self.tfc = None
        self.datestamp = time.asctime(time.localtime(time.time()))
        self.datestamp = self.datestamp.replace(" ", "-").replace(":", "_")

    def __call__(self, source, destination, token=None, filesize=None, checksum=None):
        """
        _operator()_
        
        Perform stageout step by step tests and create the summary
        
        """
        try:
            self.loadSiteConf()
        except Exception, ex:
            print str(ex)
            self.status = 1
            raise ex
        
        try:
            self.loadTFC()
        except Exception, ex:
            print str(ex)
            self.status = 2
            raise ex
        
        try:
            self.stageOut(source,destination,token,filesize,checksum)
        except Exception, ex:
            print str(ex)
            self.status = 3
            raise ex
        
        self.status = 0
        return
    

    def printSummary(self):
        """
        _printSummary_

        Print summary
        
        """
        msg = "==== StageOut Summary ====\n"
        if self.status != 0:
            msg += "Status: FAILED: %s\n" % self.status
        else:
            msg += "Status Successful\n"
            
        for key, val in self.summary.items():
            msg += "  Step: %s : %s\n" % (key, val)
        print msg
        return
        
        
    def loadSiteConf(self):
        """
        _loadSiteConf_

        Read the site conf file

        """
        if not os.environ.has_key("CMS_PATH"):
            msg = "CMS_PATH Not Set: Cannot find SiteConf"
            self.summary['SiteConf'] = "Failed: CMS_PATH not set"
            raise RuntimeError, msg

        try:
            self.siteConf = loadSiteLocalConfig()
        except Exception, ex:
            msg = "Error loading Site Conf File: %s" % str(ex)
            self.summary['SiteConf'] = "Failed: Cannot load SiteConf"
            raise RuntimeError, msg
        
        if self.siteConf.localStageOut['command'] == None:
            msg = "LocalStageOut Command is not set"
            self.summary['SiteConf'] = \
                    "Failed: local-stage-out command not set"
            raise RuntimeError, msg

        if self.siteConf.localStageOut['se-name'] == None:
            msg = "LocalStageOut SE Name is not set"
            self.summary['SiteConf'] = \
                    "Failed: local-stage-out se-name not set"
            raise RuntimeError, msg

        if self.siteConf.localStageOut['catalog'] == None:
            msg = "LocalStageOut Catalog is not set"
            self.summary['SiteConf'] = \
                    "Failed: local-stage-out catalog not set"
            raise RuntimeError, msg

        msg = "SiteConf loaded successfully:\n"
        for key, val in self.siteConf.localStageOut.items():
            msg += "   %s = %s\n" % (key, val)
        self.summary['SiteConf'] = "Successful"
        if self.verbose:
            print msg
        
        return
    
            
    def loadTFC(self):
        """
        _loadTFC_

        Load the Trivial File Catalog and test LFN matching

        """
        try:
            self.tfc = self.siteConf.trivialFileCatalog()
        except Exception, ex:
            msg = "Failed to load Trivial File Catalog: %s" % str(ex)
            self.summary['TFC'] = "Failed: Cannot load TFC"
            raise RuntimeError, msg

        sampleLFN = "/store/temp/user/sampleLFN"
        try:
            samplePFN = self.tfc.matchLFN(self.tfc.preferredProtocol,
                                          sampleLFN)
        except Exception, ex:
            msg = "Failed to translate LFN: %s" % str(ex)
            self.summary['TFC'] = "Failed: Cannot translate LFN to PFN"
            raise RuntimeError, msg

        msg = "TFC test successful:\n"
        msg += "Mapped LFN: %s\n To PFN: %s\n" % (sampleLFN, samplePFN)

        self.summary['TFC'] = "Successful"
        if self.verbose:
            print msg
        return
        
        
    def stageOut(self, source, destination, token=None, filesize=None, checksum=None):
        """
        _stageOut_

        Perform a local stage out

        """
        wasSuccessful = False
        msg = ""

        sourcePFN = os.path.join(os.getcwd(), source)
        
        
        seName   = self.siteConf.localStageOut['se-name']
        command  = self.siteConf.localStageOut['command']
        options  = self.siteConf.localStageOut.get('option', None)
        protocol = self.tfc.preferredProtocol

        targetPFN = self.tfc.matchLFN(self.tfc.preferredProtocol, destination)
        msg += "Target PFN is: %s\n" % targetPFN
        if self.verbose:
            print msg
        
        # first try the regular stageout
        try: # an exception around normal stageout
            try:
                impl = retrieveStageOutImpl(command)
            except Exception, ex:
                msg += "Unable to retrieve impl for local stage out:\n"
                msg += "Error retrieving StageOutImpl for command named: %s\n" % (
                    command,)
                self.summary['LocalStageOut'] = \
                          "Failure: Cant retrieve StageOut Impl"
                raise RuntimeError, msg
            
            try:
                impl.retryPause = 15
                impl(protocol, sourcePFN, targetPFN, options)
                wasSuccessful = True
            except Exception, ex:
                msg += "Failure for local stage out:\n"
                msg += str(ex)
                self.summary['LocalStageOut'] = \
                                          "Failure: Local Stage Out Failed"
                raise RuntimeError, msg
            if wasSuccessful:
                self.summary['LocalStageOut'] = "Successful"
                self.status=0
                return
                
        except RuntimeError, ex:
    
            ### FALLBACK ###
            ### there are N fallbacks in a list called fallbackStageOut ###
            for fallbackCount in range(len(self.siteConf.fallbackStageOut)):
                seName   = self.siteConf.fallbackStageOut[fallbackCount]['se-name']
                command  = self.siteConf.fallbackStageOut[fallbackCount]['command']
                options  = self.siteConf.fallbackStageOut[fallbackCount].get('option', None)
                try:
                    targetPFN = self.siteConf.fallbackStageOut[fallbackCount]['lfn-prefix'] + destination
                except KeyError:
                    targetPFN = destination
        
                try:
                    impl = retrieveStageOutImpl(command)
                except Exception, ex:
                    msg += "Unable to retrieve impl for local stage out:\n"
                    msg += "Error retrieving StageOutImpl for command named: %s\n" % (
                        command,)
                    self.summary['LocalStageOut'] += \
                              "\nFailure: Cant retrieve StageOut Impl for fallback %s" % fallbackCount
                    raise RuntimeError, msg
                
                try:
                    impl.retryPause = 15
                    impl(protocol, sourcePFN, targetPFN, options)
                    wasSuccessful = True
                except Exception, ex:
                    msg += "Failure for local stage out:\n"
                    msg += str(ex)
                    self.summary['LocalStageOut'] += \
                                              "\nFailure: Fallback %s Stage Out Failed" % fallbackCount
                    raise RuntimeError, msg
                
                if wasSuccessful:
                    self.summary['LocalStageOut'] = "Fallback successful"
                    self.status=0
                    return
        
        # If we got here, nothing worked
        raise RuntimeError, msg
	

    def cleanUp(self,destination):
        """
        _cleanUp_

        Clean up the file from SE

        """
        commandList = [ self.siteConf.localStageOut[ 'command' ] ]
        pfnList     = [ self.tfc.matchLFN(self.tfc.preferredProtocol, destination) ]
        
        for fallback in self.siteConf.fallbackStageOut:
           commandList.append( fallback[ 'command' ])
       	   try:
               pfnList.append( fallback[ 'lfn-prefix' ] + destination )
           except KeyError:
               pfnList.append( destination )
        		 	
        wasSuccessful = False
        msg = ""
        for (command, pfn) in zip( commandList, pfnList ):
            try: # outer try to catch the fallback as a whole
        	
                try: # inner try for getting the impl
                    implInstance = retrieveStageOutImpl(command)
                except Exception, ex:
                    msg += "Unable to retrieve impl for clean up:\n"
                    msg += "Error retrieving StageOutImpl for command named: %s\n" % (command,)
                    self.summary['CleanUp'] = "Failure: Cant retrieve StageOut Impl"
                    raise RuntimeError, msg
		        
                #  //
                # //  Invoke StageOut Impl removeFile method
                #//
                try: # inner try for calling removeFile
                    implInstance.removeFile(pfn)
                except Exception, ex:
                    msg += "Error performing Cleanup command for impl "
                    msg += "%s\n" % command
                    msg += "On PFN: %s\n" % pfn
                    msg += str(ex)
                    self.summary['CleanUp'] = "Failure: Cleanup operation Failed"
                    raise RuntimeError, msg
                self.summary['CleanUp'] = "Successful"
                wasSuccessful = True
		    
            except: # except for outer try
                wasSuccessful = False
	        
            # See if this fallback worked
            if wasSuccessful:
                self.summary['CleanUp'] = "Fallback successful"
                return
            else:
                msg += "Trying Fallback...\n"
       	
        # nothing worked, bomb out
        raise RuntimeError, msg
    
if __name__ == '__main__':

    usage = "usage: %prog [options] SOURCE DESTINATION"
    parser = OptionParser(usage=usage)
    parser.add_option("-v","--verbose",action="store_true",
                      default=False,dest="verbose")
    parser.add_option("-t", "--token", dest="token",
                      type="string",
                      help="target space token (currently dummy)")
    parser.add_option("-s", "--size",
                      dest="filesize",
                      type="int",
                      help="file size in bytes (currently dummy)")
    parser.add_option("-c","--checksum",
                      dest="checksum",
                      type="string",
                      help="file checksum in format type:value (currently dummy)")
    parser.add_option("-g","--guid",
                      dest="guid",
                      type="string",
                      help="LFC guid (dummy for CMS)")
		 	

    (options, args) = parser.parse_args()

    if len(args) != 2:
        parser.error("Incorrect number of arguments")

    stageout = LocalStageOut(verbose=options.verbose)

    try:
        stageout(args[0],args[1],token=options.token,filesize=options.filesize,checksum=options.checksum)
    except Exception, ex:
        print str(ex)

    stageout.printSummary()
    sys.exit(stageout.status)
