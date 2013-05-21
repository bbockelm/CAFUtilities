
"""
Submit a DAG directory created by the DagmanCreator component.
"""

import os
import random
import traceback

import TaskWorker.Actions.TaskAction as TaskAction

from TaskWorker.Actions.DagmanCreator import CRAB_HEADERS

# Bootstrap either the native module or the BossAir variant.
try:
    import classad
    import htcondor
except ImportError:
    #pylint: disable=C0103
    classad = None
    htcondor = None
try:
    import WMCore.BossAir.Plugins.RemoteCondorPlugin as RemoteCondorPlugin
except ImportError:
    if not htcondor:
        raise

CRAB_META_HEADERS = \
"""
+CRAB_SplitAlgo = %(splitalgo)s
+CRAB_AlgoArgs = %(algoargs)s
+CRAB_ConfigDoc = %(configdoc)s
+CRAB_PublishName = %(publishname)s
+CRAB_DBSUrl = %(dbsurl)s
+CRAB_PublishDBSUrl = %(publishdbsurl)s
+CRAB_LumiMask = %(lumimask)s
"""

# NOTE: Changes here must be synchronized with the submitDirect function below
MASTER_DAG_SUBMIT_FILE = CRAB_HEADERS + CRAB_META_HEADERS + \
"""
+CRAB_Attempt = 0
+CRAB_Workflow = %(workflow)s
+CRAB_UserDN = %(userdn)s
universe = vanilla
+CRAB_ReqName = %(requestname)s
scratch = %(scratch)s
bindir = %(bindir)s
output = $(scratch)/request.out
error = $(scratch)/request.err
executable = $(bindir)/dag_bootstrap_startup.sh
transfer_input_files = %(inputFilesString)s
transfer_output_files = %(outputFilesString)s
leave_in_queue = (JobStatus == 4) && ((StageOutFinish =?= UNDEFINED) || (StageOutFinish == 0)) && (time() - EnteredCurrentStatus < 14*24*60*60)
on_exit_remove = ( ExitSignal =?= 11 || (ExitCode =!= UNDEFINED && ExitCode >=0 && ExitCode <= 2))
+OtherJobRemoveRequirements = DAGManJobId =?= ClusterId
remove_kill_sig = SIGUSR1
+HoldKillSig = "SIGUSR1"
on_exit_hold = (ExitCode =!= UNDEFINED && ExitCode != 0)
+Environment= strcat("PATH=/usr/bin:/bin CONDOR_ID=", ClusterId, ".", ProcId)
+RemoteCondorSetup = %(remote_condor_setup)s
+TaskType = "ROOT"
X509UserProxy = %(userproxy)s
queue 1
"""

SUBMIT_INFO = [ \
            ('CRAB_Workflow', 'workflow'),
            ('CRAB_ReqName', 'requestname'),
            ('CRAB_JobType', 'jobtype'),
            ('CRAB_JobSW', 'jobsw'),
            ('CRAB_JobArch', 'jobarch'),
            ('CRAB_InputData', 'inputdata'),
            ('CRAB_ISB', 'cacheurl'),
            ('CRAB_SiteBlacklist', 'siteblacklist'),
            ('CRAB_SiteWhitelist', 'sitewhitelist'),
            ('CRAB_AdditionalOutputFiles', 'addoutputfiles'),
            ('CRAB_EDMOutputFiles', 'edmoutfiles'),
            ('CRAB_TFileOutputFiles', 'tfileoutfiles'),
            ('CRAB_SaveLogsFlag', 'savelogsflag'),
            ('CRAB_UserDN', 'userdn'),
            ('CRAB_UserHN', 'userhn'),
            ('CRAB_AsyncDest', 'asyncdest'),
            ('CRAB_BlacklistT1', 'blacklistT1'),
            ('CRAB_SplitAlgo', 'splitalgo'),
            ('CRAB_AlgoArgs', 'algoargs'),
            ('CRAB_PublishName', 'publishname'),
            ('CRAB_DBSUrl', 'dbsurl'),
            ('CRAB_PublishDBSUrl', 'publishdbsurl'),
            ('CRAB_LumiMask', 'lumimask')]

def addCRABInfoToClassAd(ad, info):
    """
    Given a submit ClassAd, add in the appropriate CRAB_* attributes
    from the info directory
    """
    for adName, dictName in SUBMIT_INFO:
        ad[adName] = classad.ExprTree(str(info[dictName]))

class DagmanSubmitter(TaskAction.TaskAction):

    """
    Submit a DAG to a remote HTCondor schedd
    """

    def getSchedd(self):
        """
        Determine a schedd to use for this task.
        """
        if not htcondor:
            return self.config.BossAir.remoteUserHost
        collector = None
        if self.config and hasattr(self.config, 'General') and hasattr(self.config.General, 'condorPool'):
            collector = self.config.General.condorPool
        elif self.config and hasattr(self.config, 'TaskWorker') and hasattr(self.config.TaskWorker, 'htcondorPool'):
            collector = self.config.TaskWorker.htcondorPool
        schedd = "localhost"
        if self.config and hasattr(self.config, 'General') and hasattr(self.config.General, 'condorScheddList'):
            random.shuffle(self.config.General.condorScheddList)
            schedd = self.config.General.condorScheddList[0]
        elif self.config and hasattr(self.config, 'TaskWorker') and hasattr(self.config.TaskWorker, 'htcondorSchedds'):
            random.shuffle(self.config.TaskWorker.htcondorSchedds)
            schedd = self.config.TaskWorker.htcondorSchedds[0]
        if collector:
            return "%s:%s" % (schedd, collector)
        return schedd

    def getScheddObj(self, name):
        """
        Return a tuple (schedd, address) containing an object representing the
        remote schedd and its corresponding address.

        If address is None, then we are using the BossAir plugin.  Otherwise,
        the schedd object is of type htcondor.Schedd.
        """
        if htcondor:
            if name == "localhost":
                schedd = htcondor.Schedd()
                with open(htcondor.param['SCHEDD_ADDRESS_FILE']) as fd:
                    address = fd.read().split("\n")[0]
            else:
                info = name.split(":")
                pool = "localhost"
                if len(info) == 2:
                    pool = info[1]
                coll = htcondor.Collector(self.getCollector(pool))
                scheddAd = coll.locate(htcondor.DaemonTypes.Schedd, info[0])
                address = scheddAd['MyAddress']
                schedd = htcondor.Schedd(scheddAd)
            return schedd, address
        else:
            return RemoteCondorPlugin.RemoteCondorPlugin(self.config, logger=self.logger), None

    def getCollector(self, name="localhost"):
        """
        Return an object representing the collector given the pool name.

        If the BossAir plugin is used, this simply returns the name
        """
        # Direct submission style
        if self.config and hasattr(self.config, 'General') and hasattr(self.config.General, 'condorPool'):
            return self.config.General.condorPool
        # TW style
        elif self.config and hasattr(self.config, 'TaskWorker') and hasattr(self.config.TaskWorker, 'htcondorPool'):
            return self.config.TaskWorker.htcondorPool
        return name

    def execute(self, *args, **kw):
        task = kw['task']
        tempDir = args[0][0]
        info = args[0][1]

        cwd = os.getcwd()
        os.chdir(tempDir)

        inputFiles = ['gWMS-CMSRunAnaly.sh', task['tm_transformation'], 'cmscp.py', 'RunJobs.dag']
        inputFiles += [i for i in os.listdir('.') if i.startswith('Job.submit')]
        info['inputFilesString'] = ", ".join(inputFiles)
        outputFiles = ["RunJobs.dag.dagman.out", "RunJobs.dag.rescue.001"]
        info['outputFilesString'] = ", ".join(outputFiles)
        arg = "RunJobs.dag"

        try:
            info['remote_condor_setup'] = ''
            scheddName = self.getSchedd()
            schedd, address = self.getScheddObj(scheddName)
            if address:
                self.submitDirect(schedd, 'dag_bootstrap_startup.sh', arg, info)
            else:
                jdl = MASTER_DAG_SUBMIT_FILE % info
                schedd.submitRaw(task['tm_taskname'], jdl, task['userproxy'], inputFiles)
        finally:
            os.chdir(cwd)

    def submitDirect(self, schedd, cmd, arg, info): #pylint: disable=R0201
        """
        Submit directly to the schedd using the HTCondor module
        """
        dagAd = classad.ClassAd()
        addCRABInfoToClassAd(dagAd, info)

        # NOTE: Changes here must be synchronized with the job_submit in DagmanCreator.py in CAFTaskWorker
        dagAd["CRAB_Attempt"] = 0
        dagAd["JobUniverse"] = 12
        dagAd["HoldKillSig"] = "SIGUSR1"
        dagAd["Out"] = os.path.join(info['scratch'], "request.out")
        dagAd["Err"] = os.path.join(info['scratch'], "request.err")
        dagAd["Cmd"] = cmd
        dagAd['Args'] = arg
        dagAd["TransferInput"] = info['inputFilesString']
        dagAd["LeaveJobInQueue"] = classad.ExprTree("(JobStatus == 4) && ((StageOutFinish =?= UNDEFINED) || (StageOutFinish == 0))")
        dagAd["TransferOutput"] = info['outputFilesString']
        dagAd["OnExitRemove"] = classad.ExprTree("( ExitSignal =?= 11 || (ExitCode =!= UNDEFINED && ExitCode >=0 && ExitCode <= 2))")
        dagAd["OtherJobRemoveRequirements"] = classad.ExprTree("DAGManJobId =?= ClusterId")
        dagAd["RemoveKillSig"] = "SIGUSR1"
        dagAd["Environment"] = classad.ExprTree('strcat("PATH=/usr/bin:/bin CONDOR_ID=", ClusterId, ".", ProcId)')
        dagAd["RemoteCondorSetup"] = info['remote_condor_setup']
        dagAd["Requirements"] = classad.ExprTree('true || false')
        dagAd["TaskType"] = "ROOT"
        dagAd["X509UserProxy"] = info['userproxy']

        r, w = os.pipe()
        rpipe = os.fdopen(r, 'r')
        wpipe = os.fdopen(w, 'w')
        if os.fork() == 0:
            #pylint: disable=W0212
            try:
                rpipe.close()
                try:
                    resultAds = []
                    htcondor.SecMan().invalidateAllSessions()
                    os.environ['X509_USER_PROXY'] = info['userproxy']
                    schedd.submit(dagAd, 1, True, resultAds)
                    schedd.spool(resultAds)
                    wpipe.write("OK")
                    wpipe.close()
                    os._exit(0)
                except Exception: #pylint: disable=W0703
                    wpipe.write(str(traceback.format_exc()))
            finally:
                os._exit(1)
        wpipe.close()
        results = rpipe.read()
        if results != "OK":
            raise Exception("Failure when submitting HTCondor task: %s" % results)

        schedd.reschedule()

