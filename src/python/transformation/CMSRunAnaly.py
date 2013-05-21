import os
import shutil
import getopt
import time
import commands
import sys
import re
import json
import traceback
import pickle
from ast import literal_eval

EC_MissingArg  =        50113 #10 for ATLAS trf
EC_CMSRunWrapper =      10040
EC_MoveOutErr =         99999 #TODO define an error code
EC_ReportHandlingErr =  50115
EC_WGET =               99998 #TODO define an error code

print "=== start ==="
print time.ctime()


def handleException(exitAcronymn, exitCode, exitMsg):
    report = {}
    report['exitAcronym'] = exitAcronymn
    report['exitCode'] = exitCode
    report['exitMsg'] = exitMsg
    #report['exitMsg'] += traceback.format_exc()
    with open('jobReport.json','w') as of:
        json.dump(report, of)
    with open('jobReportExtract.pickle','w') as of:
        pickle.dump(report, of)

#removed -p parameter of generic transformation
opts, args = getopt.getopt(sys.argv[1:], "a:o:r:", ["sourceURL=",
    #paramters coming from -p:
    "jobNumber=", "cmsswVersion=", "scramArch=", "inputFile=", "runAndLumis="
])

for o, a in opts:
    if o == "-a":
        archiveJob = a
    if o == "-o":
        outFiles = a
    if o == "-r":
        runDir = a
    if o == "--sourceURL":
        sourceURL = a
    if o == "--jobNumber":
        jobNumber = a
    if o == "--cmsswVersion":
        cmsswVersion = a
    if o == "--scramArch":
        scramArch = a
    if o == "--inputFile":
        inputFile = a
    if o == "--runAndLumis":
        runAndLumis = a

try:
    print "=== parameters ==="
    print archiveJob
    print runDir
    print sourceURL
    print jobNumber
    print cmsswVersion
    print scramArch
    print inputFile
    print runAndLumis
    print outFiles
    print "==================="
except:
    type, value, traceBack = sys.exc_info()
    print 'ERROR: missing parameters : %s - %s' % (type,value)
    handleException("FAILED", EC_MissingArg, 'CMSRunAnaly ERROR: missing parameters : %s - %s' % (type,value))
    sys.exit(EC_MissingArg)

#clean workdir ?

#wget sandnox
if archiveJob:
    os.environ['WMAGENTJOBDIR'] = os.getcwd()
    print "--- wget for jobO ---"
    output = commands.getoutput('wget -h')
    wgetCommand = 'wget'
    for line in output.split('\n'):
        if re.search('--no-check-certificate',line) != None:
            wgetCommand = 'wget --no-check-certificate'
            break
    com = '%s %s/cache/%s' % (wgetCommand,sourceURL,archiveJob)
    nTry = 3
    for iTry in range(nTry):
        print 'Try : %s' % iTry
        status,output = commands.getstatusoutput(com)
        print output
        if status == 0:
            break
        if iTry+1 == nTry:
            print "ERROR : cound not get jobO files from panda server"
            handleException("FAILED", EC_WGET, 'CMSRunAnaly ERROR: cound not get jobO files from panda server')
            sys.exit(EC_WGET)
        time.sleep(30)
    print commands.getoutput('tar xvfzm %s' % archiveJob)

#move the pset in the right place
destDir = 'WMTaskSpace/cmsRun'
if os.path.isdir(destDir):
    shutil.rmtree(destDir)
os.makedirs(destDir)
os.rename('PSet.py', destDir + '/PSet.py')
open('WMTaskSpace/__init__.py','w').close()
open(destDir + '/__init__.py','w').close()

#Tracer? line 555 of runGen

#WMCore import here
from WMCore.WMSpec.Steps.Executors.CMSSW import executeCMSSWStack
from WMCore.WMRuntime.Bootstrap import setupLogging
from WMCore.FwkJobReport.Report import Report
from WMCore.FwkJobReport.Report import FwkJobReportException
from WMCore.WMSpec.Steps.WMExecutionFailure import WMExecutionFailure


try:
    setupLogging('.')
    jobExitCode, _, _, _ = executeCMSSWStack(taskName = 'Analysis', stepName = 'cmsRun', scramSetup = '', scramCommand = 'scramv1', scramProject = 'CMSSW', scramArch = scramArch, cmsswVersion = cmsswVersion, jobReportXML = 'FrameworkJobReport.xml', cmsswCommand = 'cmsRun', cmsswConfig = 'PSet.py', cmsswArguments = '', workDir = os.getcwd(), userTarball = archiveJob, userFiles ='', preScripts = [], scramPreScripts = ['%s/TweakPSet.py %s \'%s\' \'%s\'' % (os.getcwd(), os.getcwd(), inputFile, runAndLumis)], stdOutFile = 'cmsRun-stdout.log', stdInFile = 'cmsRun-stderr.log', jobId = 223, jobRetryCount = 0, invokeCmd = 'python')
except WMExecutionFailure, WMex:
    print "caught WMExecutionFailure - code = %s - name = %s - detail = %s" % (WMex.code, WMex.name, WMex.detail)
    exmsg = WMex.name
    #exmsg += WMex.detail
    #print "jobExitCode = %s" % jobExitCode
    handleException("FAILED", WMex.code, exmsg)
    #sys.exit(WMex.code)
    sys.exit(0)
except Exception, ex:
    #print "jobExitCode = %s" % jobExitCode
    handleException("FAILED", EC_CMSRunWrapper, "failed to generate cmsRun cfg file at runtime")
    #sys.exit(EC_CMSRunWrapper)
    sys.exit(0)

#PoolFileCatalog.xml? Ce ne importa?

# rename output files
if jobExitCode == 0:
    try:
        for oldName,newName in literal_eval(outFiles).iteritems():
            os.rename(oldName, newName)
    except Exception, ex:
        handleException("FAILED", EC_MoveOutErr, "Exception while moving the files.")
        sys.exit(EC_MoveOutErr)

#Create the report file
try:
    report = Report("cmsRun")
    report.parse('FrameworkJobReport.xml', "cmsRun")
    jobExitCode = report.getExitCode()
    report = report.__to_json__(None)
    if jobExitCode: #TODO check exitcode from fwjr
        report['exitAcronym'] = "FAILED"
        report['exitCode'] = jobExitCode #TODO take exitcode from fwjr
        report['exitMsg'] = "Error while running CMSSW:\n"
        for error in report['steps']['cmsRun']['errors']:
            report['exitMsg'] += error['type'] + '\n'
            report['exitMsg'] += error['details'] + '\n'
    else:
        report['exitAcronym'] = "OK"
        report['exitCode'] = 0
        report['exitMsg'] = "OK"
    with open('jobReport.json','w') as of:
        json.dump(report, of)
    with open('jobReportExtract.pickle','w') as of:
        pickle.dump(report, of)
except FwkJobReportException, FJRex:
    msg = "BadFWJRXML"
    handleException("FAILED", EC_ReportHandlingErr, msg)
    sys.exit(EC_ReportHandlingErr)
except Exception, ex:
    msg = "Exception while handling the job report."
    handleException("FAILED", EC_ReportHandlingErr, msg)
    sys.exit(EC_ReportHandlingErr)


#create the Pool File Catalog (?)
pfcName = 'PoolFileCatalog.xml'
pfcFile = open(pfcName,'w')
pfcFile.write("""<?xml version="1.0" encoding="UTF-8" standalone="no" ?>
<!-- Edited By POOL -->
<!DOCTYPE POOLFILECATALOG SYSTEM "InMemory">
<POOLFILECATALOG>

</POOLFILECATALOG>
""")
pfcFile.close()


