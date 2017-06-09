import logging
import sys

from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.work_spec import WorkSpec
from pandaharvester.harvestercore.plugin_base import PluginBase
from pandaharvester.harvestersubmitter.apfgrid_submitter import APFGridSubmitter

try:
    from autopyfactory import condorlib
except ImportError:
    logging.error("Unable to import htcondor/condorlib. sys.path=%s" % sys.path)

# setup base logger
baseLogger = core_utils.setup_logger()


class APFGridMonitor(PluginBase):
    instance = None

    def __new__(cls, **kwargs ):
        if not APFGridMonitor.instance:
            APFGridMonitor.instance = _APFGridMonitor(**kwargs)
        return APFGridMonitor.instance


class _APFGridMonitor(PluginBase):
    '''
    1  WorkSpec.ST_submitted = 'submitted'   
    2  WorkSpec.ST_running = 'running'       
    4  WorkSpec.ST_finished = 'finished'     
    5  WorkSpec.ST_failed = 'failed'        
    6  WorkSpec.ST_ready = 'ready'           
    3  WorkSpec.ST_cancelled = 'cancelled '  
    
    CONDOR_JOBSTATUS 
    1    Idle       I              
    2    Running    R
    3    Removed    X
    4    Completed  C
    5    Held       H
    6    Submission_err  E
    '''
    instance = None 
    
    STATUS_MAP = {
        1 : WorkSpec.ST_submitted,
        2 : WorkSpec.ST_running,
        3 : WorkSpec.ST_cancelled,
        4 : WorkSpec.ST_finished,
        5 : WorkSpec.ST_failed,
        6 : WorkSpec.ST_ready,
        }   

    # constructor
    def __init__(self, **kwarg):      
        PluginBase.__init__(self, **kwarg)
        self.log = core_utils.make_logger(baseLogger)
        self.jobinfo = None
        self.historyinfo = None      
        self.log.debug('APFGridMonitor initialized.')
        
    def _updateJobInfo(self):
        self.log.debug("Getting job info from Condor...")
        #out = condorlib._querycondorlib(['match_apf_queue', 'jobstatus', 'workerid'])
        out = condorlib.queryjobs(['match_apf_queue', 'jobstatus', 'workerid'])
        self.log.debug("Got jobinfo %s" % out)
        self.jobinfo = out
        out = condorlib.condorhistorylib(attributes = ['workerid'], constraints=[])
        self.log.debug("Got history info %s" % out)
        self.historyinfo = out

    # check workers
    def check_workers(self, workspec_list):
        '''Check status of workers. This method takes a list of WorkSpecs as input argument
        and returns a list of worker's statuses.
        Nth element if the return list corresponds to the status of Nth WorkSpec in the given list. Worker's
        status is one of WorkSpec.ST_finished, WorkSpec.ST_failed, WorkSpec.ST_cancelled, WorkSpec.ST_running,
        WorkSpec.ST_submitted.

        :param workspec_list: a list of work specs instances
        :return: A tuple of return code (True for success, False otherwise) and a list of worker's statuses.
        :rtype: (bool, [string,])
        '''
        self.jobinfo = []
        self.historyinfo = []
        self._updateJobInfo()
                
        retlist = []
        for workSpec in workspec_list:
            self.log.debug("Worker(workerId=%s queueName=%s computingSite=%s status=%s )" % (workSpec.workerID, 
                                                                               workSpec.queueName,
                                                                               workSpec.computingSite, 
                                                                               workSpec.status) )
            #newStatus = WorkSpec.ST_submitted
            found = False
            
            alljobs = self.jobinfo + self.historyinfo
            
            for jobad in alljobs:
                if jobad['workerid'] == workSpec.workerID:
                    self.log.debug("Found matching job: ID %s" % jobad['workerid'])
                    found = True
                    jobstatus = int(jobad['jobstatus'])
                    retlist.append((APFGridMonitor.STATUS_MAP[jobstatus], ''))
            if not found:
                retlist.append((WorkSpec.ST_cancelled, ''))
        self.log.debug('retlist=%s' % retlist)
        return True, retlist

    