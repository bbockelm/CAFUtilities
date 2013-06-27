from WMCore.WMRuntime.Scripts.SetupCMSSWPset import SetupCMSSWPsetCore
import os
import sys
import json
from ast import literal_eval

print "Beginning TweakPSet"
print " arguments: %s" % sys.argv
agentNumber = 0
#lfnBase = '/store/temp/user/mmascher/RelValProdTTbar/mc/v6' #TODO how is this built?
lfnBase = None
outputMods = [] #TODO should not be hardcoded but taken from the config (how?)
inputFiles = literal_eval(sys.argv[2])
runAndLumis = literal_eval(sys.argv[3])
oneEventMode = 0
if len(sys.argv) == 5:
    print " Setting oneEventMode to %s" % sys.argv[4]
    oneEventMode = sys.argv[4]

pset = SetupCMSSWPsetCore( sys.argv[1], map(str, inputFiles), runAndLumis, agentNumber, lfnBase, outputMods, oneEventMode=oneEventMode)
pset()
