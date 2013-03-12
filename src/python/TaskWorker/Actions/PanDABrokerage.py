import PandaServerInterface 

from TaskWorker.Actions.PanDAAction import PanDAAction
from TaskWorker.DataObjects.Result import Result


class PanDABrokerage(PanDAAction):
    """Given a list of possible sites, ask PanDA which one is the
       best one at the current time for the job submission."""

    def execute(self, *args, **kwargs):
        results = []
        for jgroup in args[0]:
            possiblesites = jgroup.jobs[0]['input_files'][0]['locations']
            self.logger.debug("white list == " + str(set(kwargs['task']['tm_site_whitelist'])))
            self.logger.debug("black list == " + str(set(kwargs['task']['tm_site_blacklist'])))
            availablesites = list( set(kwargs['task']['tm_site_whitelist']) if kwargs['task']['tm_site_whitelist'] else set(possiblesites) &
                                   set(possiblesites) -
                                   set(kwargs['task']['tm_site_blacklist']))
            self.logger.info( str(availablesites))
            fixedsites = set(self.config.Sites.available)
            availablesites = list( set(availablesites) & fixedsites )
            self.logger.info("Asking best site to PanDA between %s" % str(availablesites))
            selectedsite = PandaServerInterface.runBrokerage(kwargs['task']['tm_user_dn'],
                                                              kwargs['task']['tm_user_vo'],
                                                              kwargs['task']['tm_user_group'],
                                                              kwargs['task']['tm_user_role'],
                                                              ['ANALY_'+el for el in availablesites])[-1]
            self.logger.info("Choosed site after brokering " +str(selectedsite))
            if not selectedsite:
                msg = "No site available after brokering, skipping injection"
                self.logger.error(msg)
                ##TODO: handle this issue
                results.append(Result(err=msg))
            else:
                results.append(Result(result=(jgroup, selectedsite)))
        return results
