from TaskWorker.Actions.DataDiscovery import DataDiscovery

class DBSDataDiscovery(DataDiscovery):
    """Performing the data discovery through CMS DBS service."""

    def execute(self, *args, **kwargs):
        self.logger.info(" Data discovery with DBS3 ")
        import time
        time.sleep(5)
        return self.formatOutput()
