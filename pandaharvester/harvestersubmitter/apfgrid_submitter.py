import uuid

from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.plugin_base import PluginBase
from pandaharvester.harvestercore.work_spec import WorkSpec

# setup base logger
baseLogger = core_utils.setup_logger()


class APFGridSubmitter(PluginBase):
    
    workers = []
    
    
    # constructor
    def __init__(self, **kwarg):
        PluginBase.__init__(self, **kwarg)
        self.log = core_utils.make_logger(baseLogger)
        self.log.debug('APFGridSubmitter initialized.')       

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
        retList = []
        for workSpec in workspec_list:
            self.log.debug("Worker(workerId=%s queueName=%s status=%s " % (workSpec.workerID, 
                                                                               workSpec.queueName, 
                                                                               workSpec.status) )
            workSpec.batchID = uuid.uuid4().hex
            workSpec.set_status(WorkSpec.ST_submitted)
            APFGridSubmitter.workers.append(workSpec)
            retList.append((True, ''))
            self.log.debug("return list=%s " % retList)
        return retList
