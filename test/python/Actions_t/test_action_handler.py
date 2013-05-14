
import os
import json
import types
import pickle
import unittest
import logging

import WMCore.Configuration

import TaskWorker.Actions.Handler as Handler
import TaskWorker.Actions.DBSDataDiscovery
import TaskWorker.Actions.Splitter
import TaskWorker.Actions.DagmanCreator
import TaskWorker.Actions.DagmanSubmitter

test_base = os.environ.get("CRAB3_TEST_BASE", ".")

class TestActionHandler(unittest.TestCase):

    def setUp(self):
        logging.basicConfig(level=logging.DEBUG)

        self.job_group_sample_file = os.path.join(test_base, "test/data/Actions/sample_job_group.pkl")
        self.task = json.load(open(os.path.join(test_base, "test/data/Actions/task1"), "r"))
        # DBS API does not accept unicode:
        for key, val in self.task.items():
            if isinstance(val, types.UnicodeType):
                self.task[key] = str(val)
        self.panda_config = WMCore.Configuration.loadConfigurationFile(os.path.abspath(os.path.join(test_base, "test/etc/panda_test.py")))
        self.gwms_config = WMCore.Configuration.loadConfigurationFile(os.path.abspath(os.path.join(test_base, "test/etc/gwms_test.py")))
        # PanDA submission fails without this environment variable.
        if 'X509_USER_PROXY' not in os.environ:
            os.environ['X509_USER_PROXY'] = '/tmp/x509up_u%d' % os.geteuid()

    def testPreSubmitSteps(self):
        handler = Handler.TaskHandler(self.task)
        handler.addWork( TaskWorker.Actions.DBSDataDiscovery.DBSDataDiscovery(config=self.panda_config) )
        handler.addWork( TaskWorker.Actions.Splitter.Splitter(config=self.panda_config) )
        job_group = handler.actionWork()[0]
        # The sample dataset should result in 120 jobs.
        self.assertEquals(len(job_group.jobs), 120)
        # Record the job group for future runs.
        pickle.dump(job_group, open(self.job_group_sample_file, "w"))

    # TODO: no way to emulate panda
    # Currently, PanDA submission requires us to query the database.
    # Current failure traceback:
    #   File "/home/cse496/bbockelm/projects/CAFUtilities/src/python/TaskDB/Connection.py", line 26, in getConnection
    #     dbinterface = myThread.dbi)
    #   AttributeError: '_MainThread' object has no attribute 'dbi'
    #
    #def testCreateNewPandaTask(self):
    #    Handler.handleNewTask(self.panda_config, self.task)
    #    Handler.handleKill(self.panda_config, self.task)

    def testCreateNewHTCondorTask(self):
        self.assertTrue(hasattr(self.gwms_config, "TaskWorker"))
        self.assertTrue(hasattr(self.gwms_config.TaskWorker, "backend"))
        self.assertEquals(self.gwms_config.TaskWorker.backend, "htcondor")
        self.assertTrue(hasattr(self.gwms_config.TaskWorker, "htcondorPool"))
        self.assertEquals(self.gwms_config.TaskWorker.htcondorPool, "glidein.unl.edu")
        self.assertTrue(hasattr(self.gwms_config.TaskWorker, "htcondorSchedds"))
        self.assertTrue("glidein.unl.edu" in self.gwms_config.TaskWorker.htcondorSchedds)
        self.assertTrue(hasattr(self.gwms_config.TaskWorker, "scratchDir"))
        handler = Handler.TaskHandler(self.task)
        action_args = []
        if os.path.exists(self.job_group_sample_file):
            action_args = [pickle.load(open(self.job_group_sample_file))]
        else:
            handler.addWork( TaskWorker.Actions.DBSDataDiscovery.DBSDataDiscovery(config=self.gwms_config) )
            handler.addWork( TaskWorker.Actions.Splitter.Splitter(config=self.gwms_config) )
        handler.addWork( TaskWorker.Actions.DagmanCreator.DagmanCreator(config=self.gwms_config) )
        result = handler.actionWork(*action_args)

        result = result[0]
        self.assertTrue(os.path.exists(result))
        self.assertTrue(os.path.exists(os.path.join(result, 'CMSRunAnaly.sh')))
        self.assertTrue(os.path.exists(os.path.join(result, 'cmscp.py')))
        self.assertTrue(os.path.exists(os.path.join(result, 'gWMS-CMSRunAnaly.sh')))
        self.assertTrue(os.path.exists(os.path.join(result, 'dag_bootstrap_startup.sh')))
        self.assertTrue(os.path.exists(os.path.join(result, 'RunJobs.dag')))

    def testSubmitNewHTCondorTask(self):
        handler = Handler.TaskHandler(self.task)
        action_args = []
        if os.path.exists(self.job_group_sample_file):
            action_args = [pickle.load(open(self.job_group_sample_file))]
        else:
            handler.addWork( TaskWorker.Actions.DBSDataDiscovery.DBSDataDiscovery(config=self.gwms_config) )
            handler.addWork( TaskWorker.Actions.Splitter.Splitter(config=self.gwms_config) )
        handler.addWork( TaskWorker.Actions.DagmanCreator.DagmanCreator(config=self.gwms_config) )
        handler.addWork( TaskWorker.Actions.DagmanSubmitter.DagmanSubmitter(config=self.gwms_config) )
        result = handler.actionWork(*action_args)


if __name__ == '__main__':
    unittest.main()

