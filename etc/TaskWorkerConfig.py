from multiprocessing import cpu_count

from WMCore.Configuration import Configuration

config = Configuration()

## External services url's
config.section_("Services")
config.Services.PanDAurl = 'https://pandaserver.cern.ch:8888'
config.Services.PhEDExurl = 'https://phedex.cern.ch'

config.section_("TaskWorker")
config.TaskWorker.polling = 60 #seconds
config.TaskWorker.nslaves = cpu_count()
