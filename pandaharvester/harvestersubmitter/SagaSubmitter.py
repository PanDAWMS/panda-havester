import uuid
import saga
import subprocess
import threading

from pandaharvester.harvestercore import CoreUtils
from pandaharvester.harvestercore.PluginBase import PluginBase

# setup base logger
baseLogger = CoreUtils.setupLogger()


# SAGA submitter
class SagaSubmitter (PluginBase):

    # constructor
    # constructor define job service with particular adaptor (can be extended to support remote
    def __init__(self,**kwarg):
        PluginBase.__init__(self,**kwarg)
        adaptor = ""
        if kwarg.has_key('adaptor'):
            adaptor = kwarg['adaptor']
        self.adaptor = adaptor
        self.job_service = saga.saga.job.Service(adaptor)


    def jobs_list(self):

        jobs = []
        for j in self.job_service.jobs():
            job = self.job_service.get_job(j)
            jobs.append((j. job.state))

        return jobs

    # temporary workaround to get common executor. Eventualy executor should be defined in workload description
    def get_executable(self, workSpec):

        #  Description of executable may include set of calls:
        #  - setup procedure(s);
        #  - environment variables;
        #  - executor [srun, aprun, mpirun etc.]; executor my depend as from facility so from payload
        #  - executor options (set of parameters for distributing payload across nodes/cores);
        #  - payload
        #  - payload parameters

        executor_str = ""
        if ("pbspro" in self.adaptor):
            executable_str = "aprun -n %s" % self.application
        elif ("slurm" in self.adaptor):
            executable_str = "srun -n " % self.application

        return executable_str

    def _execute(self, workSpec):

        tmpLog = CoreUtils.makeLogger(baseLogger)

        try:
            jd = saga.job.Description()
            if workSpec.project:
                jd.project = workSpec.project  # an association with HPC allocation needful for bookkeeping system for Titan jd.project = "CSC108"
            # launching job at HPC
            jd.wall_time_limit = workSpec.walltime  # minutes
            jd.executable = self.get_executable(workSpec)

            jd.total_cpu_count = 16  # for lonestar this has to be a multiple of 12
            # jd.spmd_variation    = '12way' # translates to the qsub -pe flag

            jd.queue = workSpec.queueName

            jd.working_directory = workSpec.working_dir  # working directory of all task
            jd.output = workSpec.stdout
            jd.error = workSpec.stderr

            # Create a new job from the job description. The initial state of
            # the job is 'New'.
            task = self.job_service.create_job(jd)
            task.run()
            workSpec.batchID = task.id
            workSpec.submitTime = task.created
            task.wait()
            workSpec.startTime = task.started
            workSpec.endTime = task.finished
            if task.state == saga.job.DONE:
                workSpec.status = workSpec.ST_finished
            else:
                workSpec.status = workSpec.ST_failed

            return 0

        except saga.SagaException, ex:
            # Catch all saga exceptions
            tmpLog.error("An exception occured: (%s) %s " % (ex.type, (str(ex))))
            # Trace back the exception. That can be helpful for debugging.
            tmpLog.error("\n*** Backtrace:\n %s" % ex.traceback)
            workSpec.status = workSpec.ST_failed
            return -1

    # submit workers
    def submitWorkers(self,workSpecs):
        tmpLog = CoreUtils.makeLogger(baseLogger)
        tmpLog.debug('start nWorkers={0}'.format(len(workSpecs)))
        retList = []
        procs = []
        for workSpec in workSpecs:
            t = threading.Thread(target=submit, args=(i, js, tasks[i]))
            t.start()
            procs.append(t)

        for p in procs():
            p.join()

        retList.append((True,''))

        tmpLog.debug('done')

        return retList
