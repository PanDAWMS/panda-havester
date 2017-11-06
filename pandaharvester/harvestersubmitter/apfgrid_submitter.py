
import logging
import os
import sys

from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.plugin_base import PluginBase
from pandaharvester.harvestercore.work_spec import WorkSpec

from autopyfactory.plugins.factory.config.queues.Agis import Agis
from autopyfactory.configloader import Config
from autopyfactory.queueslib import SubmitAPFQueue 
from autopyfactory.authmanager import AuthManager
# setup base logger
baseLogger = core_utils.setup_logger()

class APFGridSubmitter(PluginBase):
    
    factorymock = None
    authman = None
    agis = None
    logserver = None
    cleanlogs = None
  
    def __init__(self, **kwarg):
        PluginBase.__init__(self, **kwarg)
        self.log = core_utils.make_logger(baseLogger)
        
        cp = Config()
        factoryconffile = os.path.expanduser('~/harvester/etc/autopyfactory/autopyfactory.conf')
        self.log.debug('Reading config: %s' % factoryconffile)
        okread = cp.read(factoryconffile)
        self.log.debug('Successfully read %s' % okread)       

        # Setup Authmanager
        authconfigfile = os.path.expanduser(cp.get('Factory','authConf'))
        ac = Config()
        self.log.debug('Reading config: %s' % authconfigfile)
        okread = ac.read(authconfigfile)
        self.log.debug('Successfully read %s' % okread) 
        if APFGridSubmitter.authman is None :
            APFGridSubmitter.authman = AuthManager(ac)
        self.authman = APFGridSubmitter.authman           
        APFGridSubmitter.authman.startHandlers() 


        # Setup factory mock object. 
        from autopyfactory.factory import Factory
        if APFGridSubmitter.factorymock is None:
            APFGridSubmitter.factorymock = Factory.getFactoryMock(fcl=cp, am=self.authman)
        
        # Setup AGIS
        if APFGridSubmitter.agis is None:
            APFGridSubmitter.agisobj = Agis(None, cp, None)
        self.agisobj = APFGridSubmitter.agisobj
        self.log.debug("AGIS object: %s" % self.agisobj)
        
      
        
        # Setup logserver
        
        
        
        # Setup cleanlogs
        
        
                
        self.log.debug('APFGridSubmitter initialized.')       


    def _print_config(self, config):
        s=""
        for section in config.sections():
            s+= "[%s]\n" % section
            for opt in config.options(section):
                s+="%s = %s\n" % (opt, config.get(section, opt))
        return s

    # submit workers
    def submit_workers(self, workspec_list):
        '''
        Submit workers to a scheduling system like batch systems and computing elements.
        This method takes a list of WorkSpecs as input argument, and returns a list of tuples.
        Each tuple is composed of a return code and a dialog message.
        Nth tuple in the returned list corresponds to submission status and dialog message for Nth worker
        in the given WorkSpec list.
        A unique identifier is set to WorkSpec.batchID when submission is successful,
        so that they can be identified in the scheduling system.


        :param workspec_list: a list of work specs instances
        :return: A list of tuples. Each tuple is composed of submission status (True for success, False otherwise)
        and dialog message
        :rtype: [(bool, string),]
        
        '''
        self.log.debug('start nWorkers={0}'.format(len(workspec_list)))
        self.log.debug("Update AGIS info...")
        qc = self.agisobj.getConfig()
        #self.log.debug('Agis config output %s' % self._print_config(qc))
        #self.log.debug('Agis config defaults= %s' % qc.defaults() )
        retlist = []
        
        # wsmap is indexed by computing site:
        # { <computingsite>  : [ws1, ws2, ws3 ],
        #   <computingsite2> : [ws4, ws5]
        # } 
        wsmap = {}
        jobmap = {}
        for workSpec in workspec_list:
            self.log.debug("Worker(workerId=%s queueName=%s computingSite=%s nCore=%s status=%s " % (workSpec.workerID, 
                                                                               workSpec.queueName,
                                                                               workSpec.computingSite,
                                                                               workSpec.nCore, 
                                                                               workSpec.status) )
            try:
                wslist = wsmap[workSpec.computingSite]
                wslist.append(workSpec)
            except KeyError:
                wsmap[workSpec.computingSite] = [workSpec] 
            self.log.debug("wsmap = %s" % wsmap)
        
        for pq in wsmap.keys():
            found = False
            section = None        
            for s in qc.sections():
                qcq = qc.get(s, 'wmsqueue').strip()
                #self.log.debug('Checking %s' % qcq)
                if qcq == pq:
                    found = True
                    section = s
                    self.log.debug("Found queues config for %s" % pq)
            if found:
                # make apfq and submit
                self.log.debug("Agis config found for PQ")
                pqc = qc.getSection(section)
                ac = os.path.expanduser('~/harvester/etc/autopyfactory/autopyfactory.conf')
                pqc.set(section, 'factoryconf', ac) 
                self.log.debug("Section config= %s" % pqc)
                self.log.debug("Making APF queue for PQ %s with label %s"% (pq, section))
                apfq = SubmitAPFQueue( pqc, self.authman )
                self.log.debug("Successfully made APFQueue")
                joblist = []
                for ws in wsmap[pq]:
                    jobentry = { "+workerid" : ws.workerID }
                    joblist.append(jobentry)
                self.log.debug("joblist made= %s. Submitting..." % joblist)
                jobinfo = apfq.submitlist(joblist)
                self.log.debug("Got jobinfo %s" % jobinfo)
                
                wslist = wsmap[pq]
                self.log.debug("wslist for pq %s is length %s" % (pq, len(wslist)))
                for i in range(0, len(wslist)):
                    self.log.debug("Setting ws.batchID to %s" % jobinfo[i].jobid )
                    wslist[i].batchID = jobinfo[i].jobid
                    wslist[i].set_status(WorkSpec.ST_submitted)
                    retlist.append((True, ''))
                
                
            else:
                self.log.info('No AGIS config found for PQ %s skipping.' % pq)        

        self.log.debug("return list=%s " % retlist)
        return retlist
