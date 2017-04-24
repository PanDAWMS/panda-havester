import os.path
from pandaharvester.harvestercore.work_spec import WorkSpec
from pandaharvester.harvestercore.plugin_base import PluginBase


# dummy monitor
class APFGridMonitor(PluginBase):
    # constructor
    def __init__(self, **kwarg):
        PluginBase.__init__(self, **kwarg)

    # check workers
    def check_workers(self, workspec_list):
        """Check status of workers. This method takes a list of WorkSpecs as input argument
        and returns a list of worker's statuses.
        Nth element if the return list corresponds to the status of Nth WorkSpec in the given list. Worker's
        status is one of WorkSpec.ST_finished, WorkSpec.ST_failed, WorkSpec.ST_cancelled, WorkSpec.ST_running,
        WorkSpec.ST_submitted.

        :param workspec_list: a list of work specs instances
        :return: A tuple of return code (True for success, False otherwise) and a list of worker's statuses.
        :rtype: (bool, [string,])
        """
        retList = []
        for workSpec in workspec_list:
            dummyFilePath = os.path.join(workSpec.get_access_point(), 'status.txt')
            newStatus = WorkSpec.ST_finished
            with open(dummyFilePath) as dummyFile:
                newStatus = dummyFile.readline()
                newStatus = newStatus.strip()
            retList.append((newStatus, ''))
        return True, retList
