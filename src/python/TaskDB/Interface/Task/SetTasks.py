#!/usr/bin/env python
"""
_SetTasks_
"""
import logging
import TaskDB.Connection as DBConnect

def setStatusTask(taskName, status):
    """
    _setQueuedTask_
    """
    factory = DBConnect.getConnection()
    tasks = factory(classname = "Task.SetStatusTask")
    try:
        tasks.execute(taskName, status)
    except Exception, ex:
        msg = "Unable to get new resubmit tasks \n"
        msg += str(ex)
        raise RuntimeError, msg
    return

def setJobSetId(taskName, jobSetId):
    """
    _setJobSetId_
    """
    factory = DBConnect.getConnection()
    tasks = factory(classname = "Task.SetJobSetId")
    try:
        tasks.execute(taskName, jobSetId)
    except Exception, ex:
        msg = "Unable to set jobSetId %s for taskname %s\n" %(jobSetId, taskName)
        msg += str(ex)
        raise RuntimeError, msg
    return

def setStartInjection(taskName):
    """
    _setStartInjection_
    """
    factory = DBConnect.getConnection()
    tasks = factory(classname = "Task.SetStartInjection")
    try:
        tasks.execute(taskName)
    except Exception, ex:
        msg = "Unable to set taskname %s\n" %taskName
        msg += str(ex)
        raise RuntimeError, msg
    return

def setEndInjection(taskName):
    """
    _setStartInjection_
    """
    factory = DBConnect.getConnection()
    tasks = factory(classname = "Task.SetEndInjection")
    try:
        tasks.execute(taskName)
    except Exception, ex:
        msg = "Unable to set taskname %s\n" %taskName
        msg += str(ex)
        raise RuntimeError, msg
    return

def setReadyTasks(taskName, status):
    """
    _setStartInjection_
    """
    factory = DBConnect.getConnection()
    tasks = factory(classname = "Task.SetReadyTasks")
    try:
        tasks.execute(taskName, status)
    except Exception, ex:
        msg = "Unable to set status %s for taskname %s\n" %(status, taskName)
        msg += str(ex)
        raise RuntimeError, msg
    return

def setInjectedTasks(taskName, status, jobSetId):
    """
    _setStartInjection_
    """
    factory = DBConnect.getConnection()
    tasks = factory(classname = "Task.SetInjectedTasks")
    try:
        tasks.execute(taskName, status, jobSetId)
    except Exception, ex:
        msg = "Unable to set status %s and jobSetId %s for taskname %s\n" %(status, jobSetId, taskName)
        msg += str(ex)
        raise RuntimeError, msg
    return

def setFailedTasks(taskName, status, failure_reason):
    """
    _setStartInjection_
    """
    factory = DBConnect.getConnection()
    tasks = factory(classname = "Task.SetFailedTasks")
    try:
        tasks.execute(taskName, status, failure_reason)
    except Exception, ex:
        msg = "Unable to set status %s and failure_reason %s for taskname %s\n" %(status, failure_reason, taskName)
        msg += str(ex)
        raise RuntimeError, msg
    return

