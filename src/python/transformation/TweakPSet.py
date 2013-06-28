from WMCore.WMRuntime.Scripts.SetupCMSSWPset import SetupCMSSWPsetCore
from optparse import OptionParser
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

parser = OptionParser()
parser.add_option('--oneEventMode', dest='oneEventMode', default=False)
opts, args = parser.parse_args()
oneEventMode = opts.oneEventMode
if opts.oneEventMode:
    print "TweakPSet.py is going to force one event mode"
else:
    print "NO DICE %s %s" % (opts, args)
location = sys.argv[2]
inputFiles = literal_eval(sys.argv[3])
runAndLumis = literal_eval(sys.argv[4])

if sys.argv[1]=='MC':
    firstEvent=sys.argv[5]
    lastEvent=sys.argv[6]
    firstLumi=sys.argv[7]
    firstRun=sys.argv[8]
    seeding=sys.argv[9]
    lheInputFiles=bool(literal_eval(sys.argv[10]))
    pset = SetupCMSSWPsetCore( location, map(str, inputFiles), runAndLumis, agentNumber, lfnBase, outputMods, int(firstEvent), int(lastEvent), int(firstLumi),\
                    int(firstRun), seeding, lheInputFiles, oneEventMode=oneEventMode)
else:
    pset = SetupCMSSWPsetCore( location, map(str, inputFiles), runAndLumis, agentNumber, lfnBase, outputMods, oneEventMode=oneEventMode)

pset()
