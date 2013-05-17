
"""
Create a set of files for a DAG submission.

Generates the condor submit files and the master DAG.
"""

import os
import json
import shutil
import logging
import commands
import tempfile

import TaskWorker.Actions.TaskAction as TaskAction
import TaskWorker.DataObjects.Result
import TaskWorker.WorkerExceptions

import WMCore.WMSpec.WMTask

dag_fragment = """
JOB Job%(count)d Job.submit
#SCRIPT PRE  Job%(count)d dag_bootstrap.sh PREJOB $RETRY $JOB
#SCRIPT POST Job%(count)d dag_bootstrap.sh POSTJOB $RETRY $JOB
#PRE_SKIP Job%(count)d 3
#TODO: Disable retries for now - fail fast to help debug
#RETRY Job%(count)d 3
VARS Job%(count)d count="%(count)d" runAndLumiMask="%(runAndLumiMask)s" inputFiles="%(inputFiles)s" +desiredSites="\\"%(desiredSites)s\\"" +CRAB_localOutputFiles="\\"%(localOutputFiles)s\\""

JOB ASO%(count)d ASO.submit
VARS ASO%(count)d count="%(count)d" outputFiles="%(remoteOutputFiles)s"
RETRY ASO%(count)d 3

PARENT Job%(count)d CHILD ASO%(count)d
"""

dag_fragment_workaround = """
JOB Job%(count)d Job.submit.%(count)d
#SCRIPT PRE  Job%(count)d dag_bootstrap.sh PREJOB $RETRY $JOB
#SCRIPT POST Job%(count)d dag_bootstrap.sh POSTJOB $RETRY $JOB
#PRE_SKIP Job%(count)d 3
#TODO: Disable retries for now - fail fast to help debug
#RETRY Job%(count)d 3
VARS Job%(count)d count="%(count)d" runAndLumiMask="%(runAndLumiMask)s" inputFiles="%(inputFiles)s"

JOB ASO%(count)d ASO.submit
VARS ASO%(count)d count="%(count)d" outputFiles="%(remoteOutputFiles)s"
RETRY ASO%(count)d 3

PARENT Job%(count)d CHILD ASO%(count)d
"""

CRAB_HEADERS = \
"""
+CRAB_ReqName = %(requestname)s
+CRAB_Workflow = %(workflow)s
+CRAB_JobType = %(jobtype)s
+CRAB_JobSW = %(jobsw)s
+CRAB_JobArch = %(jobarch)s
+CRAB_InputData = %(inputdata)s
+CRAB_ISB = %(userisburl)s
+CRAB_SiteBlacklist = %(siteblacklist)s
+CRAB_SiteWhitelist = %(sitewhitelist)s
+CRAB_AdditionalUserFiles = %(adduserfiles)s
+CRAB_AdditionalOutputFiles = %(addoutputfiles)s
+CRAB_EDMOutputFiles = %(edmoutfiles)s
+CRAB_TFileOutputFiles = %(tfileoutfiles)s
+CRAB_SaveLogsFlag = %(savelogsflag)s
+CRAB_UserDN = %(userdn)s
+CRAB_UserHN = %(userhn)s
+CRAB_AsyncDest = %(asyncdest)s
+CRAB_Campaign = %(campaign)s
+CRAB_BlacklistT1 = %(blacklistT1)s
"""

JOB_SUBMIT = CRAB_HEADERS + \
"""
CRAB_Attempt = %(attempt)d
CRAB_ISB = %(userisburl_flatten)s
CRAB_AdditionalUserFiles = %(adduserfiles_flatten)s
CRAB_AdditionalOutputFiles = %(addoutputfiles_flatten)s
CRAB_JobSW = %(jobsw_flatten)s
CRAB_JobArch = %(jobarch_flatten)s
CRAB_Archive = %(cachefilename_flatten)s
CRAB_Id = $(count)
+CRAB_Id = $(count)
+CRAB_Dest = "cms://%(temp_dest)s"
+TaskType = "Job"

+JOBGLIDEIN_CMSSite = "$$([ifThenElse(GLIDEIN_CMSSite is undefined, \\"Unknown\\", GLIDEIN_CMSSite)])"
job_ad_information_attrs = MATCH_EXP_JOBGLIDEIN_CMSSite, JOBGLIDEIN_CMSSite

universe = vanilla
Executable = gWMS-CMSRunAnaly.sh
Output = job_out.$(CRAB_Id)
Error = job_err.$(CRAB_Id)
Log = job_log.$(CRAB_Id)
Arguments = "-o $(CRAB_AdditionalOutputFiles) -a $(CRAB_Archive) --sourceURL=$(CRAB_ISB) '--inputFile=$(inputFiles)' '--lumiMask=$(runAndLumiMask)' --cmsswVersion=$(CRAB_JobSW) --scramArch=$(CRAB_JobArch) --jobNumber=$(CRAB_Id)"
transfer_input_files = CMSRunAnaly.sh, cmscp.py
transfer_output_files = jobReport.json.$(count)
Environment = SCRAM_ARCH=$(CRAB_JobArch)
should_transfer_files = YES
x509userproxy = %(x509up_file)s
# TODO: Uncomment this when we get out of testing mode
Requirements = ((target.IS_GLIDEIN =!= TRUE) || ((target.GLIDEIN_CMSSite =!= UNDEFINED) && (stringListIMember(target.GLIDEIN_CMSSite, desiredSites) )))
leave_in_queue = (JobStatus == 4) && ((StageOutFinish =?= UNDEFINED) || (StageOutFinish == 0)) && (time() - EnteredCurrentStatus < 14*24*60*60)
queue
"""

ASYNC_SUBMIT = CRAB_HEADERS + \
"""
+TaskType = "ASO"
+CRAB_Id = $(count)
CRAB_AsyncDest = %(asyncdest_flatten)s

universe = local
Executable = dag_bootstrap.sh
Arguments = "ASO $(CRAB_AsyncDest) %(temp_dest)s %(output_dest)s $(count) $(Cluster).$(Process) cmsRun_$(count).log.tar.gz $(outputFiles)"
Output = aso.$(count).out
transfer_input_files = job_log.$(count), jobReport.json.$(count)
+TransferOutput = ""
Error = aso.$(count).err
Environment = PATH=/usr/bin:/bin
x509userproxy = %(x509up_file)s
leave_in_queue = (JobStatus == 4) && ((StageOutFinish =?= UNDEFINED) || (StageOutFinish == 0)) && (time() - EnteredCurrentStatus < 14*24*60*60)
queue
"""

SPLIT_ARG_MAP = { "LumiBased" : "lumis_per_job",
                  "FileBased" : "files_per_job",}

htcondor_78_workaround = True

logger = None

def escape_strings_to_classads(input):
    """
    Poor-man's string escaping for ClassAds.  We do this as classad module isn't guaranteed to be present.

    Converts the arguments in the input dictionary to the arguments necessary
    for the job submit file string.
    """
    info = {}
    for var in 'workflow', 'jobtype', 'jobsw', 'jobarch', 'inputdata', 'splitalgo', 'algoargs', 'configdoc', 'userisburl', \
           'cachefilename', 'cacheurl', 'userhn', 'publishname', 'asyncdest', 'campaign', 'dbsurl', 'publishdbsurl', \
           'userdn', 'requestname':
        val = input[var]
        if val == None:
            info[var] = 'undefined'
        else:
            info[var] = json.dumps(val)

    for var in 'savelogsflag', 'blacklistT1':
        info[var] = int(input[var])

    for var in 'siteblacklist', 'sitewhitelist', 'blockwhitelist', 'blockblacklist', 'adduserfiles', 'addoutputfiles', \
           'tfileoutfiles', 'edmoutfiles':
        val = input[var]
        if val == None:
            info[var] = "{}"
        else:
            info[var] = "{" + json.dumps(val)[1:-1] + "}"

    #TODO: We don't handle user-specified lumi masks correctly.
    info['lumimask'] = '"' + json.dumps(WMCore.WMSpec.WMTask.buildLumiMask(input['runs'], input['lumis'])).replace(r'"', r'\"') + '"'
    splitArgName = SPLIT_ARG_MAP[input['splitalgo']]
    info['algoargs'] = '"' + json.dumps({'halt_job_on_file_boundaries': False, 'splitOnRun': False, splitArgName : input['algoargs']}).replace('"', r'\"') + '"'
    info['attempt'] = 0

    for var in ["userisburl", "jobsw", "jobarch", "cachefilename", "asyncdest"]:
        info[var+"_flatten"] = input[var]
    info["adduserfiles_flatten"] = json.dumps(input['adduserfiles'])

    # TODO: PanDA wrapper wants some sort of dictionary.
    info["addoutputfiles_flatten"] = '{}'

    info["output_dest"] = os.path.join("/store/user", input['userhn'], input['workflow'], input['publishname'])
    info["temp_dest"] = os.path.join("/store/temp/user", input['userhn'], input['workflow'], input['publishname'])
    info['x509up_file'] = os.path.split(input['userproxy'])[-1]
    info['userproxy'] = input['userproxy']
    info['scratch'] = input['scratch']

    return info

# TODO: DagmanCreator started life as a flat module, then the DagmanCreator class
# was later added.  We need to come back and make the below methods class methods

def makeJobSubmit(task):
    if os.path.exists("Job.submit"):
        return
    # From here on out, we convert from tm_* names to the DataWorkflow names
    info = dict(task)
    info['workflow'] = task['tm_taskname'].split("_")[-1]
    info['jobtype'] = 'analysis'
    info['jobsw'] = info['tm_job_sw']
    info['jobarch'] = info['tm_job_arch']
    info['inputdata'] = info['tm_input_dataset']
    info['splitalgo'] = info['tm_split_algo']
    info['algoargs'] = info['tm_split_args']
    info['configdoc'] = ''
    info['userisburl'] = info['tm_cache_url']
    info['cachefilename'] = info['tm_user_sandbox']
    info['cacheurl'] = info['userisburl']
    info['userhn'] = info['tm_username']
    info['publishname'] = info['tm_publish_name']
    info['asyncdest'] = info['tm_asyncdest']
    info['campaign'] = ''
    info['dbsurl'] = info['tm_dbs_url']
    info['publishdbsurl'] = info['tm_publish_dbs_url']
    info['userdn'] = info['tm_user_dn']
    info['requestname'] = task['tm_taskname']
    info['savelogsflag'] = 0
    info['blacklistT1'] = 0
    info['siteblacklist'] = task['tm_site_blacklist']
    info['sitewhitelist'] = task['tm_site_whitelist']
    info['blockwhitelist'] = ''
    info['blockblacklist'] = ''
    info['adduserfiles'] = ''
    info['addoutputfiles'] = task['tm_outfiles']
    info['tfileoutfiles'] = task['tm_tfile_outfiles']
    info['edmoutfiles'] = task['tm_edm_outfiles']
    # TODO: pass through these correctly.
    info['runs'] = []
    info['lumis'] = []
    info = escape_strings_to_classads(info)
    with open("Job.submit", "w") as fd:
        fd.write(JOB_SUBMIT % info)

    return info

def make_specs(task, jobgroup, availablesites, outfiles, startjobid):
    specs = []
    i = startjobid
    for job in jobgroup.getJobs():
        inputFiles = json.dumps([inputfile['lfn'] for inputfile in job['input_files']]).replace('"', r'\"\"')
        runAndLumiMask = json.dumps(job['mask']['runAndLumis']).replace('"', r'\"\"')
        desiredSites = ", ".join(availablesites)
        i += 1
        remoteOutputFiles = []
        localOutputFiles = []
        for file in outfiles:
            info = file.rsplit(".", 1)
            if len(info) == 2:
                fileName = "%s_%d.%s" % (info[0], i, info[1])
            else:
                fileName = "%s_%d" % (file, i)
            remoteOutputFiles.append("%s" % fileName)
            localOutputFiles.append("%s?remoteName=%s" % (file, fileName))
        remoteOutputFiles = " ".join(remoteOutputFiles)
        localOutputFiles = ", ".join(localOutputFiles)
        specs.append({'count': i, 'runAndLumiMask': runAndLumiMask, 'inputFiles': inputFiles,
                      'desiredSites': desiredSites, 'remoteOutputFiles': remoteOutputFiles,
                      'localOutputFiles': localOutputFiles})
        logger.debug(specs[-1])
    return specs, i

def create_subdag(splitter_result, **kwargs):

    global logger
    if not logger:
        logger = logging.getLogger("DagmanCreator")

    startjobid = 0
    specs = []

    info = makeJobSubmit(kwargs['task'])

    outfiles = kwargs['task']['tm_outfiles'] + kwargs['task']['tm_tfile_outfiles'] + kwargs['task']['tm_edm_outfiles']

    os.chmod("CMSRunAnaly.sh", 0755)

    #fixedsites = set(self.config.Sites.available)
    for jobgroup in splitter_result:
        jobs = jobgroup.getJobs()
        if not jobs:
            possiblesites = []
        else:
            possiblesites = jobs[0]['input_files'][0]['locations']
        logger.debug("Possible sites: %s" % possiblesites)
        logger.debug('Blacklist: %s; whitelist %s' % (kwargs['task']['tm_site_blacklist'], kwargs['task']['tm_site_whitelist']))
        if kwargs['task']['tm_site_whitelist']:
            availablesites = set(kwargs['task']['tm_site_whitelist'])
        else:
            availablesites = set(possiblesites) - set(kwargs['task']['tm_site_blacklist'])
        #availablesites = set(availablesites) & fixedsites
        availablesites = [str(i) for i in availablesites]
        logger.info("Resulting available sites: %s" % ", ".join(availablesites))

        if not availablesites:
            msg = "No site available for submission of task %s" % (kwargs['task'])
            raise TaskWorker.WorkerExceptions.NoAvailableSite(msg)

        jobgroupspecs, startjobid = make_specs(kwargs['task'], jobgroup, availablesites, outfiles, startjobid)
        specs += jobgroupspecs

    dag = ""
    for spec in specs:
        if htcondor_78_workaround:
            with open("Job.submit", "r") as fd:
                with open("Job.submit.%(count)d" % spec, "w") as out_fd:
                    out_fd.write("+desiredSites=\"%(desiredSites)s\"\n" % spec)
                    out_fd.write("+CRAB_localOutputFiles=\"%(localOutputFiles)s\"\n" % spec)
                    out_fd.write(fd.read())
            dag += dag_fragment_workaround % spec
        else:
            dag += dag_fragment % spec

    with open("RunJobs.dag", "w") as fd:
        fd.write(dag)

    task_name = kwargs['task'].get('CRAB_ReqName', kwargs['task']['tm_taskname'])
    userdn = kwargs['task'].get('CRAB_UserDN', kwargs['task']['tm_user_dn'])

    # When running in standalone mode, we want to record the number of jobs in the task
    if ('CRAB_ReqName' in kwargs['task']) and ('CRAB_UserDN' in kwargs['task']):
        const = 'TaskType =?= \"ROOT\" && CRAB_ReqName =?= "%s" && CRAB_UserDN =?= "%s"' % (task_name, userdn)
        cmd = "condor_qedit -const '%s' CRAB_JobCount %d" % (const, len(jobgroup.getJobs()))
        logger.debug("+ %s" % cmd)
        status, output = commands.getstatusoutput(cmd)
        if status:
            logger.error(output)
            logger.error("Failed to record the number of jobs.")
            return 1

    return info


# Stubs for later.
def async_stageout():
    raise NotImplementedError()
    return

def postjob(retryStr, _):
    retry = int(retryStr)
    raise NotImplementedError()
    return 0

def prejob(retryStr, _):
    retry = int(retryStr)
    raise NotImplementedError()
    return 0

def getLocation(default_name, checkout_location):
    loc = default_name
    if not os.path.exists(loc):
        if 'CRAB3_CHECKOUT' not in os.environ:
            raise Exception("Unable to locate %s" % loc)
        loc = os.path.join(os.environ['CRAB3_CHECKOUT'], checkout_location, loc)
    loc = os.path.abspath(loc)
    return loc

class DagmanCreator(TaskAction.TaskAction):
    """
    Given a task definition, create the corresponding DAG files for submission
    into HTCondor
    """

    def execute(self, *args, **kw):
        global logger
        logger = self.logger

        cwd = None
        if hasattr(self.config, 'TaskWorker') and hasattr(self.config.TaskWorker, 'scratchDir'):
            temp_dir = tempfile.mkdtemp(prefix='_' + kw['task']['tm_taskname'], dir=self.config.TaskWorker.scratchDir)

            transform_location = getLocation(kw['task']['tm_transformation'], 'CAFUtilities/src/python/transformation/')
            cmscp_location = getLocation('cmscp.py', 'CRABServer/bin/')
            gwms_location = getLocation('gWMS-CMSRunAnaly.sh', 'CAFTaskWorker/bin/')
            bootstrap_location = getLocation('dag_bootstrap_startup.sh', 'CRABServer/bin/')

            cwd = os.getcwd()
            os.chdir(temp_dir)
            shutil.copy(transform_location, '.')
            shutil.copy(cmscp_location, '.')
            shutil.copy(gwms_location, '.')
            shutil.copy(bootstrap_location, '.')

            if 'X509_USER_PROXY' in os.environ:
                kw['task']['userproxy'] = os.environ['X509_USER_PROXY']
            kw['task']['scratch'] = temp_dir

        try:
            info = create_subdag(*args, **kw)
        finally:
            if cwd:
                os.chdir(cwd)
        return TaskWorker.DataObjects.Result.Result(task=kw['task'], result=(temp_dir, info))

