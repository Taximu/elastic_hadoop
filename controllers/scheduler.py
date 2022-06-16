import time
import pprint
import threading
import jobwatcher
import jobresourceallocator
from jobwatcher import JobWatcher
from subprocess import call
from yarn_api_client import ResourceManager
from jobresourceallocator import JobResourceAllocator

class Scheduler():

    std_configs = {'minResources' : '1024 mb,1vcores', 
                   'maxResources' : '1024 mb,1vcores', 
                   'maxRunningApps' : '1', 
                   'weight' : '1.0',
                   'schedulingPolicy' : 'fair'}

    def __init__(self, params):
        self.params = params
        self.jobs_profile = {} #[app_name] : {Mavg, Mmax, Ravg, Rmax}
        self.jobs_scheduled = {} #[app_id] : {app_name, queue_name, state, start_time, maps, reduces, estim_maps, estim_reducers}
        thread = JobWatcher(self.schedule, self.params, self.jobs_profile, self.jobs_scheduled)
        thread.daemon = False
        thread.start()
        thread.join()    
    
    def updateJobProfile(job_name, job_data):
        pass #by name update Map Reduce
    
    def calculateMapRed():
	pass

    def schedule(self, job, job_data, STATE):
        if STATE == 'FINISHED':
            jobs_profile[job] = job_data
            self.updateJobProfile(job, job_data)
        elif STATE == 'RUNNING':
            jobs_scheduled[job] = job_data
            if job not in jobs_profile:
                std_configs['name'] = job_data['name']
                queue = JobResourceAllocator(std_configs)
            else:
                self.calculateMapRed()
        elif STATE == 'FMC':
            updateJobProfile(job, job_data)
        elif STATE == 'FRC':
            updateJobProfile(job, job_data)
        elif MESSAGE == 'MAP_COMPLETE':
            updateJobProfile(job, job_data)
            jobAlloc = JobResourceAllocator(std_configs)
            if jobAlloc == None:
                #configs = getConfigsFromFunction()
                JobResourceAllocator.editQueue(job_data['name'], configs)
                call(["yarn rmadmin", "-refreshQueues"])

    def estimateResources(app_id):
        pass #formulas to count minResources, maxResources

    def reviseMapRedEquity(app_id):
        if job_scheduled[app_id]['estim_maps'] != job_scheduled[app_id]['estim_reducers']:
            pass
            #JobResourceAllocator.editQueue(jobs_scheduled[app_id]['queue_name'], std_configs)
            #call(["yarn rmadmin", "-refreshQueues"])

    def checkClusterStatus(self):
        rm = ResourceManager(self.params['address'], self.params['port'], self.params['timeout'])
        print '****************'
        print 'CLUSTER_METRICS'
        print '****************'
        json = rm.cluster_metrics()
        pprint.pprint(json.data)
        json = rm.cluster_nodes(state=None, healthy=None)
        print '**************'
        print 'CLUSTER_NODES'
        print '**************'
        pprint.pprint(json.data)
