import logging

class TaskAction(object):
    """The abstract father of all actions"""

    def __init__(self, config):
        self.logger = logging.getLogger(type(self).__name__)
        self.config = config

    def execute(self):
        raise NotImplementedError

