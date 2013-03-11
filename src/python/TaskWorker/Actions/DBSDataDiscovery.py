from WMCore.WorkQueue.WorkQueueUtils import get_dbs

from TaskWorker.Actions.DataDiscovery import DataDiscovery


class DBSDataDiscovery(DataDiscovery):
    """Performing the data discovery through CMS DBS service."""

    def execute(self, *args, **kwargs):
        self.logger.info("Data discovery with DBS") ## to be changed into debug
        dbs = get_dbs(self.config.Services.DBSUrl)
        # Get the list of blocks for the locations and then call dls.
        # The WMCore DBS3 implementation makes one call to dls for each block
        # with locations = True
        blocks = [ x['Name'] for x in dbs.getFileBlocksInfo(kwargs['task']['tm_input_dataset'], locations=False)]
        #Create a map for block's locations: for each block get the list of locations
        locations = map(lambda x: map(lambda y: y.host, x.locations), dbs.dls.getLocations(list(blocks),  showProd = True))
        locationsmap = dict(zip(blocks, locations))
        filedetails = dbs.listDatasetFileDetails(kwargs['task']['tm_input_dataset'], True)

        return self.formatOutput(requestname=kwargs['task']['tm_taskname'], datasetfiles=filedetails, locations=locationsmap)


if __name__ == '__main__':
    datasets = ['/GenericTTbar/HC-CMSSW_5_3_1_START53_V5-v1/GEN-SIM-RECO',
                '/GenericTTbar/HC-CMSSW_5_3_1_START53_V5-v1/GEN-SIM-RECO',
                '/SingleMu/Run2012C-PromptReco-v2/AOD',
                '/SingleMu/Run2012D-PromptReco-v1/AOD',
                '/DYJetsToLL_M-50_TuneZ2Star_8TeV-madgraph-tarball/Summer12_DR53X-PU_S10_START53_V7A-v1/AODSIM',
                '/WJetsToLNu_TuneZ2Star_8TeV-madgraph-tarball/Summer12_DR53X-PU_S10_START53_V7A-v2/AODSIM',
                '/TauPlusX/Run2012D-PromptReco-v1/AOD']

    for dataset in datasets:
        fileset = DBSDataDiscovery(dataset=dataset)
