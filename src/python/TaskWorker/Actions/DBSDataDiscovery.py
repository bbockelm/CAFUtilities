from WMCore.WorkQueue.WorkQueueUtils import get_dbs

from Databases.TaskDB.Interface.Task.SetTasks import setFailedTasks

from TaskWorker.Actions.DataDiscovery import DataDiscovery
from TaskWorker.WorkerExceptions import StopHandler


class DBSDataDiscovery(DataDiscovery):
    """Performing the data discovery through CMS DBS service."""

    def execute(self, *args, **kwargs):
        self.logger.info("Data discovery with DBS") ## to be changed into debug
        dbs = get_dbs(self.config.Services.DBSUrl)
        if kwargs['task']['tm_dbs_url']:
            dbs = get_dbs(kwargs['task']['tm_dbs_url'])
        self.logger.debug("Data discovery through %s for %s" %(dbs, kwargs['task']))
        # Get the list of blocks for the locations and then call dls.
        # The WMCore DBS3 implementation makes one call to dls for each block
        # with locations = True
        blocks = [ x['Name'] for x in dbs.getFileBlocksInfo(kwargs['task']['tm_input_dataset'], locations=False)]
        #Create a map for block's locations: for each block get the list of locations
        ll = dbs.dls.getLocations(list(blocks),  showProd = True)
        if len(ll) == 0:
            msg = "No location was found for %s in %s." %(kwargs['task']['tm_input_dataset'],kwargs['task']['tm_dbs_url'])
            setFailedTasks(kwargs['task']['tm_taskname'], "Failed", msg)
            raise StopHandler(msg)
        locations = map(lambda x: map(lambda y: y.host, x.locations), ll)
        locationsmap = dict(zip(blocks, locations))
        filedetails = dbs.listDatasetFileDetails(kwargs['task']['tm_input_dataset'], True)

        return self.formatOutput(task=kwargs['task'], requestname=kwargs['task']['tm_taskname'], datasetfiles=filedetails, locations=locationsmap)


if __name__ == '__main__':
    datasets = ['/GenericTTbar/HC-CMSSW_5_3_1_START53_V5-v1/GEN-SIM-RECO',
                '/GenericTTbar/HC-CMSSW_5_3_1_START53_V5-v1/GEN-SIM-RECO',
                '/SingleMu/Run2012C-PromptReco-v2/AOD',
                '/SingleMu/Run2012D-PromptReco-v1/AOD',
                '/DYJetsToLL_M-50_TuneZ2Star_8TeV-madgraph-tarball/Summer12_DR53X-PU_S10_START53_V7A-v1/AODSIM',
                '/WJetsToLNu_TuneZ2Star_8TeV-madgraph-tarball/Summer12_DR53X-PU_S10_START53_V7A-v2/AODSIM',
                '/TauPlusX/Run2012D-PromptReco-v1/AOD']

    from WMCore.Configuration import Configuration
    config = Configuration()
    config.section_("Services")
    #config.Services.DBSUrl = 'https://cmsweb.cern.ch/dbs/dev/global/DBSReader'
    config.Services.DBSUrl = 'http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet'
    for dataset in datasets:
        fileset = DBSDataDiscovery(config)
        print fileset.execute(task={'tm_input_dataset':dataset, 'tm_taskname':'pippo1', 'tm_dbs_url': config.Services.DBSUrl})

