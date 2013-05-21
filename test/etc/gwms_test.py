
import WMCore.Configuration as Configuration

config = Configuration.Configuration()

config.section_("Services")
config.Services.DBSUrl = 'http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet'

config.section_("Sites")
config.Sites.available = ["T2_US_Nebraska"]

config.section_("TaskWorker")
config.TaskWorker.backend = "htcondor"
config.TaskWorker.htcondorPool = 'glidein.unl.edu'
config.TaskWorker.htcondorSchedds = ['glidein.unl.edu']
config.TaskWorker.scratchDir = 'tmp'

