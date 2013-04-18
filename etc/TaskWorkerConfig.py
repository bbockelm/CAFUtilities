from multiprocessing import cpu_count

from WMCore.Configuration import Configuration

config = Configuration()

## External services url's
config.section_("Services")
config.Services.PanDAurl = 'https://pandaserver.cern.ch:8888'
config.Services.PhEDExurl = 'https://phedex.cern.ch'
#config.Services.DBSUrl = 'https://cmsweb.cern.ch/dbs/dev/global/DBSReader'
config.Services.DBSUrl = 'http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet'

config.section_("TaskWorker")
config.TaskWorker.polling = 60 #seconds
 # we can add one worker per core, plus some spare ones since most of actions wait for I/O
config.TaskWorker.nslaves = cpu_count() + cpu_count()/2

#The following parameters assumes the installation in "one box" together with the REST
config.section_("MyProxy")
config.MyProxy.serverhostcert = '/path/to/hostcert.pem'
config.MyProxy.serverhostkey = '/path/to/hostkey.pem'
config.MyProxy.uisource = '/afs/cern.ch/cms/LCG/LCG-2/UI/cms_ui_env.sh'
config.MyProxy.credpath = '/tmp'
config.MyProxy.serverdn = '' #PLEASE SET ME!!!

config.section_("Sites")
config.Sites.available = ["T2_CH_CERN", "T2_IT_Pisa", "T1_US_FNAL", "T2_DE_DESY"]
