class Result(object):
    """Result of an action. This can potentially be subclassed."""

    def __init__(self, result=None, err=None, warn=None):
        """Inintializer

        :arg * result: the result cna actually be any needed type
        :arg * result: the error can actually be any needed type
                       (exception, traceback, int, ...)
        :arg str warn: a warning message."""
        self._result = result
        self._error = err
        self._warning = warn

    @property 
    def result(self):
        """Get the result value"""
        return self._result if self._result else None

    @property
    def error(self):
        """Get the error if any"""
        return self._error if self._error else None

    @property
    def warning(self):
        """Get the wanring if any"""
        return self._warning if self._warning else None

    def __str__(self):
        """Use me just to print out in case it is needed to debug"""
        msg = ''
        if self.result:
            msg += "Result = " + str(self.result)
        if self.error:
            msg += "Error = " + str(self.error)
        if self.warning:
            msg += "Warning = " + str(self.warning)
        return msg
