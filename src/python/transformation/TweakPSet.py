from WMCore.WMRuntime.Scripts.SetupCMSSWPset import SetupCMSSWPsetCore
import os
import sys
import json

agentNumber = 0
#lfnBase = '/store/temp/user/mmascher/RelValProdTTbar/mc/v6' #TODO how is this built?
lfnBase = None
outputMods = [] #TODO should not be hardcoded but taken from the config (how?)
inputFiles = json.loads(sys.argv[2])
lumiMask = json.loads(sys.argv[3])

pset = SetupCMSSWPsetCore( sys.argv[1], map(str, inputFiles), lumiMask, agentNumber, lfnBase, outputMods)
pset()
