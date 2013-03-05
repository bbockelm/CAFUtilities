import logging

class TaskAction(object):
    """The abstract father of all actions"""

    def __init__(self, config):
        self.config = config
        self.logger = logging.getLogger(type(self).__name__)

    def execute(self):
        raise NotImplementedError

