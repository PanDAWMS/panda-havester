
import logging
import sys
logging.debug("%s" % sys.path)

import os
from sets import Set
import uuid

from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.plugin_base import PluginBase
from pandaharvester.harvestercore.work_spec import WorkSpec

# setup base logger
baseLogger = core_utils.setup_logger()

from autopyfactory.plugins.factory.config.Agis import Agis
from autopyfactory.configloader import Config
from autopyfactory.queueslib import StaticAPFQueueJC 


class APFGridSubmitter(PluginBase):
    instance = None
    workers = []

    def __new__(cls, *args, **kwargs):
        if cls.instance is None:
            cls.instance = super(APFGridSubmitter, cls).__new__(cls, *args, **kwargs)
        return cls.instance
  
    def __init__(self, **kwarg):
        PluginBase.__init__(self, **kwarg)
        self.log = core_utils.make_logger(baseLogger)
        
        cp = Config()
        cp.add_section('Factory')
        cp.set('Factory','config.agis.baseurl','http://atlas-agis-api.cern.ch/request/pandaqueue/query/list/?json&preset=schedconf.all')
        df = os.path.expanduser('~/harvester/etc/autopyfactory/agisdefaults.conf')
        cp.set('Factory','config.agis.defaultsfiles', df)
        cp.set('Factory','config.agis.sleep', '3600')
        cp.set('Factory','config.agis.vos','atlas')
        cp.set('Factory','config.agis.clouds','US')
        cp.set('Factory','config.agis.activities','production')
        cp.set('Factory','config.agis.jobsperpilot','1.5')
        cp.set('Factory','config.agis.numfactories','1')
        
        self.agisobj = Agis(None, cp, None)
        self.log.debug("AGIS object: %s" % self.agisobj)
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
        """Submit workers to a scheduling system like batch systems and computing elements.
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
        
        """
        self.log.debug('start nWorkers={0}'.format(len(workspec_list)))
        self.log.debug("Update AGIS info...")
        qc = self.agisobj.getConfig()
        self.log.debug('Agis config output %s' % self._print_config(qc))
        self.log.debug('Agis config defaults= %s' % qc.defaults() )
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
                apfq = StaticAPFQueueJC( pqc )
                self.log.debug("Successfully made APFQueue")

                for ws in wsmap[pq]:
                    jobentry = { "+workerid" : ws.workerID }
                    joblist.append(jobentry)
                self.log.debug("joblist made= %s. Submitting..." % joblist)
                jobinfo = apfq.submitlist(joblist)
                wslist = wsmap[pq]
                for i in range(0, len(wslist)):
                    wslist[i].batchID = jobinfo[i].jobid
                    wslist[i].set_status(WorkSpec.ST_submitted)
                    retlist.append((True, ''))
                self.log.debug("Got jobinfo %s" % jobinfo)
            else:
                self.log.info('No AGIS config found for PQ %s skipping.' % pq)        

        self.log.debug("return list=%s " % retlist)
        return retlist
