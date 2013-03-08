#!/usr/bin/env python
"""
_MakeJobGroup_
"""
import logging
import CAFUtilities.TaskDB.Connection as DBConnect

def addJobGroup(taskName, jobdefid, status, blocks, jobgroup_failure):
    """
    _addJobGroup_
    """
    factory = DBConnect.getConnection()
    newJobGroup = factory(classname = "JobGroup.AddJobGroup")
    try:
        jobgroupId = newJobGroup.execute(
	    taskName, jobdefid, status, blocks, jobgroup_failure)
    except Exception, ex:
        msg = "Unable to create task named %s\n" % taskName
        msg += str(ex)
        raise RuntimeError, msg
    return jobgroupId
