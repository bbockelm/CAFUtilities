# Class definition:
#   SiteInformation
#   This class is responsible for downloading, verifying and manipulating queuedata
#   Note: not compatible with Singleton Design Pattern due to the subclassing

import os
import re
import commands
from pUtil import tolog, getExtension, replace, readpar
from PilotErrors import PilotErrors

class SiteInformation(object):
    """

    Should this class ask the Experiment class which the current experiment is?
    Not efficient if every readpar() calls some Experiment method unless Experiment is a singleton class as well

    """

    # private data members
    __experiment = "generic"
    __instance = None                      # Boolean used by subclasses to become a Singleton
    __error = PilotErrors()                # PilotErrors object

    def __init__(self):
        """ Default initialization """

        # e.g. self.__errorLabel = errorLabel
        pass

    def readpar(self, par, alt=False):
        """ Read par variable from queuedata """

        value = ""
        fileName = self.getQueuedataFileName(alt=alt)
        try:
            fh = open(fileName)
        except:
            try:
                # try without the path
                fh = open(os.path.basename(fileName))
            except Exception, e:
                tolog("!!WARNING!!2999!! Could not read queuedata file: %s" % str(e))
                fh = None
        if fh:
            queuedata = fh.read()
            fh.close()
            if queuedata != "":
                value = self.getpar(par, queuedata, containsJson=fileName.endswith("json"))

        # repair JSON issue
        if value == None:
            value = ""

        return value

    def getpar(self, par, s, containsJson=False):
        """ Extract par from s """

        parameter_value = ""
        if containsJson:
            # queuedata is a json string
            from json import loads
            pars = loads(s)
            if pars.has_key(par):
                parameter_value = pars[par]
                if type(parameter_value) == unicode: # avoid problem with unicode for strings
                    parameter_value = parameter_value.encode('ascii')
            else:
                tolog("WARNING: Could not find parameter %s in queuedata" % (par))
                parameter_value = ""
        else:
            # queuedata is a string on the form par1=value1|par2=value2|...
            matches = re.findall("(^|\|)([^\|=]+)=",s)
            for tmp,tmpPar in matches:
                if tmpPar == par:
                    patt = "%s=(.*)" % par
                    idx = matches.index((tmp,tmpPar))
                    if idx+1 == len(matches):
                        patt += '$'
                    else:
                        patt += "\|%s=" % matches[idx+1][1]
                    mat = re.search(patt,s)
                    if mat:
                        parameter_value = mat.group(1)
                    else:
                        parameter_value = ""

        return parameter_value

    def getQueuedataFileName(self, useExtension=None, check=True, alt=False):
        """ Define the queuedata filename """

        # use a forced extension if necessary
        if useExtension:
            extension = useExtension
        else:
            extension = getExtension(alternative='dat')

        # prepend alt. for alternative stage-out site queuedata
        if alt:
            extension = "alt." + extension

        path = "%s/queuedata.%s" % (os.environ['PilotHomeDir'], extension)

        # remove the json extension if the file cannot be found (complication due to wrapper)
        if not os.path.exists(path) and check:
            if extension == 'json':
                _path = path.replace('.json', '.dat')
                if os.path.exists(_path):
                    tolog("Updating queuedata file name to: %s" % (_path))
                    path = _path
                else:
                    tolog("!!WARNING!! Queuedata paths do not exist: %s, %s" % (path, _path))
            if extension == 'dat':
                _path = path.replace('.dat', '.json')
                if os.path.exists(_path):
                    tolog("Updating queuedata file name to: %s" % (_path))
                    path = _path
                else:
                    tolog("!!WARNING!! Queuedata paths do not exist: %s, %s" % (path, _path))
        return path

    def replaceQueuedataField(self, field, value, verbose=True):
        """ replace a given queuedata field with a new value """
        # copytool = <whatever> -> lcgcp
        # replaceQueuedataField("copytool", "lcgcp")

        status = False

        verbose = True
        queuedata_filename = self.getQueuedataFileName()
        if "json" in queuedata_filename.lower():
            if self.replaceJSON(queuedata_filename, field, value):
                if verbose:
                    tolog("Successfully changed %s to: %s" % (field, value))
                    status = True
        else:
            stext = field + "=" + self.readpar(field)
            rtext = field + "=" + value
            if replace(queuedata_filename, stext, rtext):
                if verbose:
                    tolog("Successfully changed %s to: %s" % (field, value))
                    status = True
            else:
                tolog("!!WARNING!!1999!! Failed to change %s to: %s" % (field, value))

        return status

    def replaceJSON(self, queuedata_filename, field, value):
        """ Replace/update queuedata field in JSON file """

        status = False
        from json import load, dump
        try:
            fp = open(queuedata_filename, "r")
        except Exception, e:
            tolog("!!WARNING!!4003!! Failed to open file: %s, %s" % (queuedata_filename, e))
        else:
            try:
                dic = load(fp)
            except Exception, e:
                tolog("!!WARNING!!4004!! Failed to load dictionary: %s" % (e))
            else:
                fp.close()
                if dic.has_key(field):
                    dic[field] = value
                    try:
                        fp = open(queuedata_filename, "w")
                    except Exception, e:
                        tolog("!!WARNING!!4005!! Failed to open file: %s, %s" % (queuedata_filename, e))
                    else:
                        try:
                            dump(dic, fp)
                        except Exception, e:
                            tolog("!!WARNING!!4005!! Failed to dump dictionary: %s" % (e))
                        else:
                            fp.close()
                            status = True
                else:
                    tolog("!!WARNING!!4005!! No such field in queuedata dictionary: %s" % (field))

        return status

    def evaluateQueuedata(self):
        """ Evaluate environmental variables if used and replace the value in the queuedata """

        tolog("Evaluating queuedata")

        # the following fields are allowed to contain environmental variables
        fields = ["copysetup", "copysetupin", "recoverdir", "wntmpdir", "sepath", "seprodpath", "lfcpath", "lfcprodpath"]

        # process each field and evaluate the environment variables if present
        for field in fields:
            # grab the field value and split it since some fields can contain ^-separators
            old_values = self.readpar(field)
            new_values = []
            try:
                for value in old_values.split("^"):
                    if value.startswith("$"):
                        # evaluate the environmental variable
                        new_value = os.path.expandvars(value)
                        if new_value == "":
                            tolog("!!WARNING!!2999!! Environmental variable not set: %s" % (value))
                        value = new_value
                    new_values.append(value)

                # rebuild the string (^-separated if necessary)
                new_values_joined = '^'.join(new_values)

                # replace the field value in the queuedata with the new value
                if new_values_joined != old_values:
                    if self.replaceQueuedataField(field, new_values_joined, verbose=False):
                        tolog("Updated field %s in queuedata (replaced \'%s\' with \'%s\')" % (field, old_values, new_values_joined))
            except:
                # ignore None values
                continue

    def verifyQueuedata(self, queuename, filename, _i, _N, url):
        """ Check if the downloaded queuedata has the proper format """

        hasQueuedata = False
        try:
            f = open(filename, "r")
        except Exception, e:
            tolog("!!WARNING!!1999!! Open failed with %s" % (e))
        else:
            output = f.read()
            f.close()
            if not ('appdir' in output and 'copytool' in output):
                if len(output) == 0:
                    tolog("!!WARNING!!1999!! curl command returned empty queuedata (wrong queuename %s?)" % (queuename))
                else:
                    tolog("!!WARNING!!1999!! Attempt %d/%d: curl command did not return valid queuedata from config DB server %s" %\
                          (_i, _N, url))
                    output = output.replace('\n', '')
                    output = output.replace(' ', '')
                    tolog("!!WARNING!!1999!! Output begins with: %s" % (output[:64]))
                try:
                    os.remove(filename)
                except Exception, e:
                    tolog("!!WARNING!!1999!! Failed to remove file %s: %s" % (filename, e))
            else:
                # found valid queuedata info, break the for-loop
                tolog("schedconfigDB returned: %s" % (output))
                hasQueuedata = True

        return hasQueuedata

    def getQueuedata(self, queuename, forceDownload=False, alt=False):
        """ Download the queuedata if not already downloaded """

        # read the queue parameters and create the queuedata.dat file (if the queuename varible is set)
        # pilot home dir is not the workdir. It's the sandbox directory for the pilot.
        # queuedata structure: (with example values)
        # appdir=/ifs0/osg/app/atlas_app/atlas_rel
        # dq2url=http://gk03.swt2.uta.edu:8000/dq2/
        # copytool=gridftp
        # copysetup=
        # ddm=UTA_SWT2
        # se=http://gk03.swt2.uta.edu:8000/dq2/
        # sepath=
        # envsetup=
        # region=US
        # copyprefix=
        # lfcpath=
        # lfchost=
        # sein=
        # wntmpdir=/scratch

        # curl --connect-timeout 20 --max-time 120 -sS "http://pandaserver.cern.ch:25085/cache/schedconfig/BNL_CVMFS_1-condor.pilot.pilot"

        if not os.environ.has_key('PilotHomeDir'):
            os.environ['PilotHomeDir'] = commands.getoutput('pwd')
        hasQueuedata = False

        # try the config servers one by one in case one of them is not responding

        # in case the wrapper has already downloaded the queuedata, it might have a .dat extension
        # otherwise, give it a .json extension if possible
        filename_dat = self.getQueuedataFileName(useExtension='dat', check=False, alt=alt)
        if os.path.exists(filename_dat):
            filename = filename_dat
        else:
            filename = self.getQueuedataFileName(check=False, alt=alt)

        if os.path.exists(filename) and not forceDownload:
            tolog("Queuedata has already been downloaded by pilot wrapper script (will confirm validity)")
            hasQueuedata = self.verifyQueuedata(queuename, filename, 1, 1, "(see batch log for url)")
            if hasQueuedata:
                tolog("Queuedata was successfully downloaded by pilot wrapper script")
            else:
                tolog("Queuedata was not downloaded successfully by pilot wrapper script, will try again")

        if not hasQueuedata:
            # loop over pandaserver round robin _N times until queuedata has been verified, or fail
            ret = -1
            url = 'http://pandaserver.cern.ch'
            if os.environ.has_key('X509_USER_PROXY'):
                sslCert = os.environ['X509_USER_PROXY']
            else:
                sslCert  = '/tmp/x509up_u%s' % str(os.getuid())
            cmd = 'curl --connect-timeout 20 --max-time 120 --cacert %s -sS "%s:25085/cache/schedconfig/%s.pilot.%s" > %s' % \
                  (sslCert, url, queuename, getExtension(alternative='pilot'), filename)
            _N = 3
            for _i in range(_N):
                tolog("Executing command: %s" % (cmd))
                try:
                    # output will be empty since we pipe into a file
                    ret, output = commands.getstatusoutput(cmd)
                except Exception, e:
                    tolog("!!WARNING!!1999!! Failed with curl command: %s" % str(e))
                    return -1, False
                else:
                    if ret == 0:
                        # read back the queuedata to verify its validity
                        hasQueuedata = self.verifyQueuedata(queuename, filename, _i, _N, url)
                        if hasQueuedata:
                            break
                    else:
                        tolog("!!WARNING!!1999!! curl command exited with code %d" % (ret))

        return 0, hasQueuedata

    def processQueuedata(self, queuename, forceDownload=False, alt=False):
        """ Process the queuedata """

        ec = 0
        hasQueuedata = False

        if queuename != "":
            ec, hasQueuedata = self.getQueuedata(queuename, forceDownload=forceDownload, alt=alt)
            if ec != 0:
                tolog("!!FAILED!!1999!! getQueuedata failed: %d" % (ec))
                ec = self.__error.ERR_QUEUEDATA
            if not hasQueuedata:
                tolog("!!FAILED!!1999!! Found no valid queuedata - aborting pilot")
                ec = self._error.ERR_QUEUEDATANOTOK
            else:
                tolog("curl command returned valid queuedata")
        else:
            tolog("WARNING: queuename not set (queuedata will not be downloaded and symbols not evaluated)")

        return ec, hasQueuedata

    def postProcessQueuedata(self, queuename, pshttpurl, thisSite, _jobrec, force_devpilot):
        """ Update queuedata fields if necessary """

        if 'pandadev' in pshttpurl or force_devpilot or thisSite.sitename == "CERNVM":
            ec = self.replaceQueuedataField("status", "online")

        if thisSite.sitename == "UTA_PAUL_TEST" or thisSite.sitename == "ANALY_UTA_PAUL_TEST":
            ec = self.replaceQueuedataField("status", "online")
            ec = self.replaceQueuedataField("allowfax", "True")
            ec = self.replaceQueuedataField("faxredirector", "root://glrd.usatlas.org/")

#        if thisSite.sitename == "ANALY_OX" or thisSite.sitename == "ANALY_RAL_XROOTD" or thisSite.sitename == "ANALY_QMUL":
#            ec = self.replaceQueuedataField("status", "online")
#            ec = self.replaceQueuedataField("allowfax", "True")
#            ec = self.replaceQueuedataField("faxredirector", "root://atlas-xrd-uk.cern.ch/")
#        if "CERN" in thisSite.sitename:
#            ec = self.replaceQueuedataField("copysetup", "/cvmfs/atlas.cern.ch/repo/sw/local/xrootdsetup.sh")
#            ec = self.replaceQueuedataField("copysetup", "/afs/cern.ch/project/xrootd/software/setup_stable_for_atlas2.sh")
#            ec = self.replaceQueuedataField("copysetup", "/afs/cern.ch/project/xrootd/software/setup_stable_for_atlas_cvmfs.sh")
#            if thisSite.sitename == "ANALY_CERN_XROOTD":
#                ec = self.replaceQueuedataField("copysetupin", "/afs/cern.ch/project/xrootd/software/setup_stable_for_atlas2.sh^False^True")
#                ec = self.replaceQueuedataField("copysetupin", "/cvmfs/atlas.cern.ch/repo/sw/local/xrootdsetup.sh^srm://srm-eosatlas.cern.ch^root://eosatlas.cern.ch/^False^True")
#                ec = self.replaceQueuedataField("copysetupin", "/afs/cern.ch/project/xrootd/software/setup_stable_for_atlas2.sh^srm://srm-eosatlas.cern.ch^root://eosatlas.cern.ch/^False^True")
#                ec = self.replaceQueuedataField("copysetupin", "/afs/cern.ch/project/xrootd/software/setup_stable_for_atlas_cvmfs.sh^srm://srm-eosatlas.cern.ch^root://eosatlas.cern.ch/^False^True")
#            else:
#                ec = self.replaceQueuedataField("copysetupin", "/afs/cern.ch/project/xrootd/software/setup_stable_for_atlas2.sh^srm://srm-eosatlas.cern.ch^root://eosatlas.cern.ch/^False^False")
#                ec = self.replaceQueuedataField("copysetupin", "/afs/cern.ch/project/xrootd/software/setup_stable_for_atlas_cvmfs.sh^srm://srm-eosatlas.cern.ch^root://eosatlas.cern.ch/^False^False")
#            ec = self.replaceQueuedataField("copytoolin", "fax")
#            ec = self.replaceQueuedataField("allowdirectaccess", "True")
#            ec = self.replaceQueuedataField("allowfax", "True")
#            ec = self.replaceQueuedataField("faxredirector", "root://xrdfed01.cern.ch/")

#        if thisSite.sitename == "ANALY_MWT2":
#            ec = self.replaceQueuedataField("timefloor", "140")

#        if thisSite.sitename == "ANALY_SWT2_CPB":
#            ec = self.replaceQueuedataField("copysetup", "/cluster/atlas/xrootd/atlasutils/setup_xcp.sh^srm://gk03.atlas-swt2.org(:[0-9]+)*/srm/v2/server?SFN=/xrd^root://xrdb.local:1094//xrd^False^False")
#            ec = self.replaceQueuedataField("allowfax", "True")
#            ec = self.replaceQueuedataField("faxredirector", "root://glrd.usatlas.org/")

#        if thisSite.sitename == "ANALY_GLASGOW_XROOTD":
#            ec = self.replaceQueuedataField("copysetup", "^srm://svr018.gla.scotgrid.ac.uk^root://svr018.gla.scotgrid.ac.uk/^False^False")
#            ec = self.replaceQueuedataField("allowfax", "True")
#            ec = self.replaceQueuedataField("faxredirector", "root://svr018.gla.scotgrid.ac.uk/")

#        if thisSite.sitename == "ANALY_BNL_ATLAS_1" or thisSite.sitename == "ANALY_OU_OCHEP_SWT2" or thisSite.sitename == "ANALY_NET2":
#            ec = self.replaceQueuedataField("timefloor", "")
#            ec = self.replaceQueuedataField("allowfax", "True")
#wrong?            ec = self.replaceQueuedataField("faxredirector", "xrd-central.usatlasfacility.org")
#            ec = self.replaceQueuedataField("faxredirector", "root://glrd.usatlas.org/")

#        if thisSite.sitename == "ANALY_AGLT2":
#            ec = self.replaceQueuedataField("copysetup", "^srm://head01.aglt2.org.*/pnfs/^dcache:/pnfs/^False^False")
#            ec = self.replaceQueuedataField("allowfax", "True")
#            ec = self.replaceQueuedataField("faxredirector", "root://glrd.usatlas.org/")

#        if thisSite.sitename == "ANALY_LANCS":
#            ec = self.replaceQueuedataField("copytool", "lcgcp")
#            ec = self.replaceQueuedataField("lfcregister", "")

#        if thisSite.sitename == "ANALY_MWT2":
#            ec = self.replaceQueuedataField("lfchost", "ust2lfc.usatlas.bnl.gov")
#            ec = self.replaceQueuedataField("copytoolin", "fax")
#            ec = self.replaceQueuedataField("envsetup", "source /share/wn-client/setup.sh")
#            ec = self.replaceQueuedataField("copysetup", "^srm://uct2-dc1.uchicago.edu.*/pnfs/^root://dcap.mwt2.org:1096/pnfs/^False^False")
#            ec = self.replaceQueuedataField("timefloor", "")

#        if thisSite.sitename == "ANALY_RAL_XROOTD":
#            ec = self.replaceQueuedataField("copytool", "xrdcp")
#            ec = self.replaceQueuedataField("copyprefix", "srm://srm-atlas.gridpp.rl.ac.uk^root://catlasdlf.ads.rl.ac.uk/")

#        if "wuppertalprod" in thisSite.sitename:
#            ec = self.replaceQueuedataField("appdir", "/cvmfs/atlas.cern.ch/repo/sw|nightlies^/cvmfs/atlas-nightlies.cern.ch/repo/sw/nightlies")
#            ec = self.replaceQueuedataField("allowdirectaccess", "True")
#            ec = self.replaceQueuedataField("copysetupin", "^True^True")
#        if "ANALY_BNL_" in thisSite.sitename:
#            ec = self.replaceQueuedataField("allowdirectaccess", "True")
#            ec = self.replaceQueuedataField("copysetupin", "/afs/usatlas.bnl.gov/i386_redhat72/opt/dcache/dcache_client_config.sh^True^True")

        _status = self.readpar('status')
        if _status != None and _status != "":
            if _status.upper() == "OFFLINE":
                tolog("Site %s is currently in %s mode - aborting pilot" % (thisSite.sitename, _status.lower()))
                return -1, None, None
            else:
                tolog("Site %s is currently in %s mode" % (thisSite.sitename, _status.lower()))

        # override pilot run options
        temp_jobrec = self.readpar('retry')
        if temp_jobrec.upper() == "TRUE":
            tolog("Job recovery turned on")
            _jobrec = True
        elif temp_jobrec.upper() == "FALSE":
            tolog("Job recovery turned off")
            _jobrec = False
        else:
            tolog("Job recovery variable (retry) not set")

        # evaluate the queuedata if needed
        self.evaluateQueuedata()

        # set pilot variables in case they have not been set by the pilot launcher
        thisSite = self.setUnsetVars(thisSite)

        return 0, thisSite, _jobrec

    def extractQueuedataOverwrite(self, jobParameters):
        """ Extract the queuedata overwrite key=value pairs from the job parameters """
        # The dictionary will be used to overwrite existing queuedata values
        # --overwriteQueuedata={key1=value1,key2=value2}

        queuedataUpdateDictionary = {}

        # define regexp pattern for the full overwrite command
        pattern = re.compile(r'\ \-\-overwriteQueuedata\=\{.+}')
        fullCommand = re.findall(pattern, jobParameters)

        if fullCommand[0] != "":
            # tolog("Extracted the full command from the job parameters: %s" % (fullCommand[0]))
            # e.g. fullCommand[0] = '--overwriteQueuedata={key1=value1 key2=value2}'

            # remove the overwriteQueuedata command from the job parameters
            jobParameters = jobParameters.replace(fullCommand[0], "")
            tolog("Removed the queuedata overwrite command from job parameters: %s" % (jobParameters))

            # define regexp pattern for the full overwrite command
            pattern = re.compile(r'\-\-overwriteQueuedata\=\{(.+)\}')

            # extract the key value pairs string from the already extracted full command
            keyValuePairs = re.findall(pattern, fullCommand[0])
            # e.g. keyValuePairs[0] = 'key1=value1,key2=value2'

            if keyValuePairs[0] != "":
                # tolog("Extracted the key value pairs from the full command: %s" % (keyValuePairs[0]))

                # remove any extra spaces if present
                keyValuePairs[0] = keyValuePairs[0].replace(" ", "")

                # define the regexp pattern for the actual key=value pairs
                # full backslash escape, see (adjusted for python):
                # http://stackoverflow.com/questions/168171/regular-expression-for-parsing-name-value-pairs
                pattern = re.compile( r'((?:\\.|[^=,]+)*)=("(?:\\.|[^"\\]+)*"|(?:\\.|[^,"\\]+)*)' )

                # finally extract the key=value parameters
                keyValueList = re.findall(pattern, keyValuePairs[0])
                # e.g. keyValueList = [('key1', 'value1'), ('key2', 'value_2')]

                # put the extracted pairs in a proper dictionary
                if keyValueList != []:
                    tolog("Extracted the following key value pairs from job parameters: %s" % str(keyValueList))

                    for keyValueTuple in keyValueList:
                        key = keyValueTuple[0]
                        value = keyValueTuple[1]

                        if key != "":
                            queuedataUpdateDictionary[key] = value
                        else:
                            tolog("!!WARNING!!1223!! Bad key detected in key value tuple: %s" % str(keyValueTuple))
                else:
                    tolog("!!WARNING!!1223!! Failed to extract the key value pair list from: %s" % (keyValuePairs[0]))

            else:
                tolog("!!WARNING!!1223!! Failed to extract the key value pairs from: %s" % (keyValuePairs[0]))

        else:
            tolog("!!WARNING!!1223!! Failed to extract the full queuedata overwrite command from jobParameters=%s" % (jobParameters))

        return jobParameters, queuedataUpdateDictionary

    def updateQueuedataFromJobParameters(self, jobParameters):
        """ Extract queuedata overwrite command from job parameters and update queuedata """

        tolog("called updateQueuedataFromJobParameters with: %s" % (jobParameters))

        # extract and remove queuedata overwrite command from job parameters
        if "--overwriteQueuedata" in jobParameters:
            tolog("Encountered an --overwriteQueuedata command in the job parameters")

            # (jobParameters might be updated [queuedata overwrite command should be removed if present], so they needs to be returned)
            jobParameters, queuedataUpdateDictionary = self.extractQueuedataOverwrite(jobParameters)

            # update queuedata
            if queuedataUpdateDictionary != {}:
                tolog("Queuedata will be updated from job parameters")
                for field in queuedataUpdateDictionary.keys():
                    ec = self.replaceQueuedataField(field, queuedataUpdateDictionary[field])
                    tolog("Updated %s in queuedata: %s (read back from file)" % (field, self.readpar(field)))

        # disable FAX if set in schedconfig
        if "--disableFAX" in jobParameters:
            tolog("Encountered a --disableFAX command in the job parameters")

            # remove string from jobParameters
            jobParameters = jobParameters.replace(" --disableFAX", "")

            # update queuedata if necessary
            if readpar("allowfax").lower() == "true":
                field = "allowfax"
                ec = self.replaceQueuedataField(field, "False")
                tolog("Updated %s in queuedata: %s (read back from file)" % (field, self.readpar(field)))

            else:
                tolog("No need to update queuedata for --disableFAX (allowfax is not set to True)")

        return jobParameters

    def setUnsetVars(self, thisSite):
        """ Set pilot variables in case they have not been set by the pilot launcher """
        # thisSite will be updated and returned

        tolog('Setting unset pilot variables using queuedata')
        if thisSite.appdir == "":
            scappdir = self.readpar('appdir')
            if os.environ.has_key("OSG_APP") and not os.environ.has_key("VO_ATLAS_SW_DIR"):
                if scappdir == "":
                    scappdir = os.environ["OSG_APP"]
                    if scappdir == "":
                        scappdir = "/usatlas/projects/OSG"
                        tolog('!!WARNING!!4000!! appdir not set in queuedata or $OSG_APP: using default %s' % (scappdir))
                    else:
                        tolog('!!WARNING!!4000!! appdir not set in queuedata - using $OSG_APP: %s' % (scappdir))
            tolog('appdir: %s' % (scappdir))
            thisSite.appdir = scappdir

        if thisSite.dq2url == "":
            _dq2url = self.readpar('dq2url')
            if _dq2url == "":
                tolog('Note: dq2url not set')
            else:
                tolog('dq2url: %s' % (_dq2url))
            thisSite.dq2url = _dq2url

        if thisSite.wntmpdir == "":
            _wntmpdir = self.readpar('wntmpdir')
            if _wntmpdir == "":
                _wntmpdir = thisSite.workdir
                tolog('!!WARNING!!4000!! wntmpdir not set - using site workdir: %s' % (_wntmpdir))
            tolog('wntmpdir: %s' % (_wntmpdir))
            thisSite.wntmpdir = _wntmpdir

        return thisSite

    def isTier1(self, sitename):
        """ Is the given site a Tier-1? """

        return False

    def isTier2(self, sitename):
        """ Is the given site a Tier-2? """

        return False

    def isTier3(self):
        """ Is the given site a Tier-3? """
        # Note: defined by DB

        return False

    def updateCopysetup(self, jobParameters, field, _copysetup, transferType=None, useCT=None, directIn=None, useFileStager=None):
        """
        Update copysetup in the presence of directIn and/or useFileStager in jobParameters
        Possible copysetup's are:
        "setup^oldPrefix^newPrefix^useFileStager^directIn"
        "setup^oldPrefix1,oldPrefix2^newPrefix1,newPrefix2^useFileStager^directIn"
        "setup^useFileStager^directIn"
        "setup"
        "None"
        """

        # get copysetup from queuedata
        copysetup = _copysetup

        tolog("updateCopysetup: copysetup=%s" % (copysetup))

        if "^" in copysetup:
            fields = copysetup.split("^")
            n = len(fields)
            # fields[0] = setup
            # fields[1] = useFileStager
            # fields[2] = directIn
            # or
            # fields[0] = setup
            # fields[1] = oldPrefix or oldPrefix1, oldPrefix2
            # fields[2] = newPrefix or newPrefix1, newPrefix2
            # fields[3] = useFileStager
            # fields[4] = directIn
            if n == 3 or n == 5:
                # update the relevant fields if necessary
                if useCT:
                    tolog("Copy tool is enforced, turning off any set remote I/O or file stager options")
                    fields[n-1] = "False"
                    fields[n-2] = "False"
                else:
                    # in case directIn or useFileStager were set by accessmode via jobParameters
                    if directIn or useFileStager:
                        if useFileStager and directIn:
                            fields[n-1] = "True" # directIn
                            fields[n-2] = "True" # useFileStager
                        elif directIn and not useFileStager:
                            fields[n-1] = "True"  # directIn
                            fields[n-2] = "False" # useFileStager
                        if transferType == "direct":
                            fields[n-1] = "True"  # directIn 
                            fields[n-2] = "False" # make sure file stager is turned off
                    # in case directIn or useFileStager were set in jobParameters or with transferType
                    else:
                        if fields[n-1].lower() == "false" and ("--directIn" in jobParameters or transferType == "direct"):
                            fields[n-1] = "True"  # directIn
                            fields[n-2] = "False" # useFileStager
                        if fields[n-2].lower() == "false" and "--useFileStager" in jobParameters:
                            fields[n-1] = "True" # directIn
                            fields[n-2] = "True" # useFileStager

                if "," in copysetup:
                    tolog("Multiple old/new prefices, turning off any set remote I/O or file stager options")
                    fields[n-1] = "False"
                    fields[n-2] = "False"

                copysetup = "^".join(fields)
            else:
                tolog("!!WARNING!!2990!! This site is not setup properly for using direct access/file stager: copysetup=%s" % (copysetup))
        else:
            if transferType == "direct" or directIn and not useFileStager:
                copysetup += "^False^True"
            elif useFileStager:
                copysetup += "^True^True"

        # undo remote I/O copysetup modification if requested
        if transferType == "undodirect" and "^" in copysetup:
            tolog("Requested re-modification of copysetup due to previous error")
            fields = copysetup.split("^")
            copysetup = fields[0]
            copysetup += "^False^False"

        # update copysetup if updated
        if copysetup != _copysetup:
            ec = self.replaceQueuedataField(field, copysetup)
            tolog("Updated %s in queuedata: %s (read back from file)" % (field, self.readpar(field)))
        else:
            tolog("copysetup does not need to be updated")

    def getAppdirs(self, appdir):
        """ Create a list of all appdirs in appdir """

        # appdir = '/cvmfs/atlas.cern.ch/repo/sw|nightlies^/cvmfs/atlas-nightlies.cern.ch/repo/sw/nightlies'
        # -> ['/cvmfs/atlas.cern.ch/repo/sw', '/cvmfs/atlas-nightlies.cern.ch/repo/sw/nightlies']

        appdirs = []
        if "|" in appdir:
            for a in appdir.split("|"):
                # remove any processingType
                if "^" in a:
                    a = a.split("^")[1]
                appdirs.append(a)
        else:
            appdirs.append(appdir)

        return appdirs

    def extractAppdir(self, appdir, processingType, homePackage):
        """ extract and (re-)confirm appdir from possibly encoded schedconfig.appdir """
        # e.g. for CERN:
        # processingType = unvalid
        # schedconfig.appdir = /afs/cern.ch/atlas/software/releases|release^/afs/cern.ch/atlas/software/releases|unvalid^/afs/cern.ch/atlas/software/unvalidated/caches
        # -> appdir = /afs/cern.ch/atlas/software/unvalidated/caches
        # if processingType does not match anything, use the default first entry (/afs/cern.ch/atlas/software/releases)
        # NOTE: this function can only be called after a job has been downloaded since processType is unknown until then

        ec = 0

        # override processingType for analysis jobs that use nightlies
        if "rel_" in homePackage:
            tolog("Temporarily modifying processingType from %s to nightlies" % (processingType))
            processingType = "nightlies"

        _appdir = appdir
        if "|" in _appdir and "^" in _appdir:
            # extract appdir by matching with processingType
            appdir_split = _appdir.split("|")
            appdir_default = appdir_split[0]
            # loop over all possible appdirs
            sub_appdir = ""
            for i in range(1, len(appdir_split)):
                # extract the processingType and sub appdir
                sub_appdir_split = appdir_split[i].split("^")
                if processingType == sub_appdir_split[0]:
                    # found match
                    sub_appdir = sub_appdir_split[1]
                    break
            if sub_appdir == "":
                _appdir = appdir_default
                tolog("Using default appdir: %s (processingType = \'%s\')" % (_appdir, processingType))
            else:
                _appdir = sub_appdir
                tolog("Matched processingType %s to appdir %s" % (processingType, _appdir))
        else:
            # check for empty appdir's on LCG
            if _appdir == "":
                if os.environ.has_key("VO_ATLAS_SW_DIR"):
                    _appdir = os.environ["VO_ATLAS_SW_DIR"]
                    tolog("Set site.appdir to %s" % (_appdir))
            else:
                tolog("Got plain appdir: %s" % (_appdir))

        # verify the existence of appdir
        if os.path.exists(_appdir):
            tolog("Software directory %s exists" % (_appdir))

            # force queuedata update
            _ec = self.replaceQueuedataField("appdir", _appdir)
            del _ec
        else:
            if _appdir != "":
                tolog("!!FAILED!!1999!! Software directory does not exist: %s" % (_appdir))
            else:
                tolog("!!FAILED!!1999!! Software directory (appdir) is not set")
            ec = self.__error.ERR_NOSOFTWAREDIR

        return ec, _appdir

    def getExperiment(self):
        """ Return a string with the experiment name """

        return self.__experiment

    def allowAlternativeStageOut(self):
        """ Is alternative stage-out allowed? """
        # E.g. if stage-out to primary SE (at Tier-2) fails repeatedly, is it allowed to attempt stage-out to secondary SE (at Tier-1)?

        return False

    def getSSLCertificate(self):
        """ Return the path to the SSL certificate """

        if os.environ.has_key('X509_USER_PROXY'):
            sslCertificate = os.environ['X509_USER_PROXY']
        else:
            sslCertificate  = '/tmp/x509up_u%s' % str(os.getuid())

        return sslCertificate

    def getSSLCertificatesDirectory(self):
        """ Return the path to the SSL certificates directory """

        sslCertificatesDirectory = ''
        if os.environ.has_key('X509_CERT_DIR'):
            sslCertificatesDirectory = os.environ['X509_CERT_DIR']
        else:
            _dir = '/etc/grid-security/certificates'
            if os.path.exists(_dir):
                sslCertificatesDirectory = _dir
            else:
                tolog("!!WARNING!!2999!! $X509_CERT_DIR is not set and default location %s does not exist" % (_dir))

        return sslCertificatesDirectory

    def getProperPaths(self):
        """ Return proper paths for the storage element """

        # Implement in sub-class

        return ""

    def getTier1Queue(self, cloud):
        """ Download the queuedata for the Tier-1 in the corresponding cloud and get the queue name """

        # Implement in sub-class
        # This method is used during stage-out to alternative [Tier-1] site when primary stage-out on a Tier-2 fails
        # See methods in ATLASSiteInformation

        return None

if __name__ == "__main__":
    from SiteInformation import SiteInformation
    import os
    os.environ['PilotHomeDir'] = os.getcwd()
    s1 = SiteInformation()
    print "copytool=",s1.readpar('copytool')
