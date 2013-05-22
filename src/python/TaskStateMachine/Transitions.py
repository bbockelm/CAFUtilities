#!/usr/bin/env python
"""
_Transitions_

Controls what state transitions are allowed.
"""

class TaskStateException(Exception):
    """General exception to be returned in case of failures
       for status transitions"""
    exitcode = 9000

def changeState(curr, new):
    """Verifies is the transition can be made or not.
       If yes, returns the new status name,
       otherwise raises an exception.

       :str curr: current status name
       :str new: new status name."""
    trans = Transitions()
    if new not in trans.states():
        raise TaskStateException("New '%s' status is not valid" %new)
    if curr not in trans:
        raise TaskStateException("Current '%s' status is not valid" %curr)
    if new not in trans[curr]:
        raise TaskStateException("Transition from '%s' to '%s' is forbidden." %(curr, new))
    ## transition is valid
    return new

class Transitions(dict):
    """
    All allowed state transitions in the JSM.
    """
    def __init__(self):
        # A workflow doesn't have any state at the beginning
        self.setdefault('none', ['new'])

        # A 'new' workflow can:
        #  - be acquired by the task-worker, 'queued'
        #  - be killed before the worker acquires it, 'killed'
        self.setdefault('new', ['queued', 'killed'])

        # A 'queued' workflow can:
        #  - succeed when processed by the task-worker, 'submitted'
        #  - fail when processed by the task-worker, 'failed'
        self.setdefault('queued', ['submitted','failed'])

        # An 'submitted' workflow can:
        #  - be closed when considered finished/(to-)archive, 'closed'
        #  - be 'killed'
        #  - asked to be resubmitted, 'resubmit'
        self.setdefault('submitted', ['killed', 'resubmit', 'failed', 'completed'])

        # A 'failed' workflow can:
        #  - only be 'closed'
        self.setdefault('failed', ['closed'])

        #self.setdefault('closed', [])

        # A 'resubmit' workflow can:
        #  - be acquired by the task-worker, 'queued'
        #  - be killed before the worker acquires it, 'killed'
        self.setdefault('resubmit', ['queued', 'killed'])

        # A 'killed' workflow can:
        #  - be resubmitted, 'resubmit'
        #  - be closed when considered finished/(to-)archive, 'closed'
        self.setdefault('killed', ['resubmit', 'closed'])

    def states(self):
        """
        _states_

        Return a list of all known states, derive it in case we add new final
        states.
        """
        knownstates = set(self.keys())
        for possiblestates in self.values():
            for i in possiblestates:
                knownstates.add(i)
        return list(knownstates)



if __name__ == '__main__':
    import Transitions
    a = Transitions.Transitions()
    print "Available states", a.states()

    try:
        Transitions.changeState('submitted','failed')
    except:
        pass
    Transitions.changeState('submitted','killed')
    Transitions.changeState('submitted','resubmit')
    Transitions.changeState('submitted','closed')

    try:
        Transitions.changeState('queued','queued')
    except:
        pass
    Transitions.changeState('queued','submitted')
    Transitions.changeState('queued','failed')

    try:
        Transitions.changeState('killed','submitted')
    except:
        pass
    Transitions.changeState('killed','resubmit')
    Transitions.changeState('killed','closed')

    try:
        Transitions.changeState('resubmit','failed')
    except:
        pass
    Transitions.changeState('resubmit','queued')
    Transitions.changeState('resubmit','killed')

    try:
        Transitions.changeState('failed','killed')
    except:
        pass
    Transitions.changeState('failed','closed')

    try:
        Transitions.changeState('new', 'closed')
    except:
        pass
    Transitions.changeState('new','queued')
    Transitions.changeState('new','killed')
