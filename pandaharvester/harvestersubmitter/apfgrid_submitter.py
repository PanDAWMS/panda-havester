
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

class APFGridSubmitter(PluginBase):

    
    workers = []
    
    
    # constructor
    def __init__(self, **kwarg):
        PluginBase.__init__(self, **kwarg)
        self.log = core_utils.make_logger(baseLogger)
            #configplugin = Agis
        #config.agis.baseurl = http://atlas-agis-api.cern.ch/request/pandaqueue/query/list/?json&preset=schedconf.all
        #config.agis.defaultsfile= /etc/autopyfactory/agisdefaults.conf
        #config.agis.sleep = 3600
        #config.agis.vos = atlas
        #config.agis.clouds = US
        #config.agis.activities = analysis,production
        #config.agis.jobsperpilot = 1.5
        #config.agis.numfactories = 4
        
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
        #self.log.debug('Calling AGIS getConfig()...')
        #qc = self.agisobj.getConfig()
        #self.log.debug('%s' % self._print_config(qc))
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
        #self.log.debug('%s' % self._print_config(qc))
        
        retlist = []
        pqset = Set()
        
        for workSpec in workspec_list:
            self.log.debug("Worker(workerId=%s queueName=%s computingSite=%s nCore=%s status=%s " % (workSpec.workerID, 
                                                                               workSpec.queueName,
                                                                               workSpec.computingSite,
                                                                               workSpec.nCore, 
                                                                               workSpec.status) )
            
            #
            nclist = [] 
            for s in qc.sections():
                qcq = qc.get(s, 'wmsqueue').strip()
                self.log.debug('Finding %s' % workSpec.computingSite)
                if qcq == workSpec.computingSite:
                    pqname = workSpec.computingSite
                    self.log.debug("Found worker for PQ %s" % workSpec.computingSite)
                    pqset.add(pqname)
                    #nc = gc.getSection(s)
                    #nclist.append(nc)
                    #self.log.debug("%s" % nc.getContent())
        
        self.log.debug("This submit_workers() call concerns Panda Queues: %s" % pqset)
        
        for workSpec in workspec_list:               
            workSpec.batchID = uuid.uuid4().hex
            workSpec.set_status(WorkSpec.ST_submitted)
            APFGridSubmitter.workers.append(workSpec)
            retlist.append((True, ''))
            self.log.debug("return list=%s " % retlist)
        return retlist
