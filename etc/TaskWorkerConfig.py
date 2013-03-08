from multiprocessing import cpu_count

from WMCore.Configuration import Configuration

config = Configuration()

## External services url's
config.section_("Services")
config.Services.PanDAurl = 'https://pandaserver.cern.ch:8888'
config.Services.PhEDExurl = 'https://phedex.cern.ch'
config.Services.DBSUrl = 'https://cmsweb.cern.ch/dbs/dev/global/DBSReader'

config.section_("TaskWorker")
config.TaskWorker.polling = 60 #seconds
config.TaskWorker.nslaves = cpu_count()

config.section_("Sites")
config.Sites.available = ["T2_CH_CERN", "T2_IT_Pisa", "T1_US_FNAL", "T2_DE_DESY"]
