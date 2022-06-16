import time
import pprint
import threading
import urllib2
import json
import jobhistorymonitor
from jobhistorymonitor import JobHistoryMonitor
from yarn_api_client import ApplicationMaster, ResourceManager, HistoryServer

STATE = ['FINISHED', 'RUNNING', 'FMC', 'FRC', 'MAP_COMPLETE']

class JobWatcher(threading.Thread):
    '''Class to track jobs'''

    def __init__(self, callback, params, jobs_profile, jobs_scheduled):
        threading.Thread.__init__(self)
        self.callback = callback
        self.address = params['address']
        self.port = int(params['port'])
        self.timeout = int(params['timeout'])
        self.jobs_profile = jobs_profile
        self.jobs_scheduled = jobs_scheduled
        self.app_master = ApplicationMaster(self.address, self.port, self.timeout)
        self.resource_manager = ResourceManager(self.address, self.port, self.timeout)

    def displayParams(self):
        '''Displays jobwatcher parameters'''
        attrs = vars(self)
        print 'JobWatcher parameters:'
        print '\n'.join("%s: %s" %item for item in attrs.items())

    def watchJobs(self):
        '''Watching for job submission'''
        res = self.resource_manager.cluster_applications(
            state=None,
            final_status=None,
            user=None,
            queue=None,
            limit=None,
            started_time_begin=None,
            started_time_end=None,
            finished_time_begin=None,
            finished_time_end=None
        )
        if res.data[u'apps'] is not None:
            jobs_list = res.data[u'apps'][u'app']
            for d in jobs_list:
                app_id = str(d[u'id'])
                job_id = 'job_' + str(app_id.split('_')[1]) + '_' + str(app_id.split('_')[2])
                name = str(d[u'name'])
                state = str(d[u'state'])
                if state == 'FINISHED':
                    if name not in self.jobs_profile:
                        hs = HistoryServer(self.address, 19888, self.timeout)
                        res2 = hs.job(job_id)
                        job_name = str(res2.data[u'job'][u'name'])
                        job_history = JobHistoryMonitor.getJobHistory(job_id)
                        self.jobs_profile[job_name] = job_history
                        #self.callback(job_name, job_history, STATE[0])
                if state == 'RUNNING':
                    url = 'http://' + self.address + ':' + '8088/proxy/' + app_id + '/ws/v1/mapreduce/jobs/' + job_id + '/tasks'
                    f = urllib2.urlopen(url)
                    #st = f.read()
                    data = json.load(f)
                    for dict in data['tasks']['task'][0]:
                        if dict['id'][-8] == 'm_000000':
                            print dict['elapsedTime']
                        if dict['id'][-8] == 'r_000000':
                            print dict['elapsedTime']
                    if app_id not in self.jobs_scheduled:
                        start_time = d[u'startedTime']
                        queue_name = str(d[u'queue'])
                        mapred = self.app_master.job(app_id, job_id)
                        job = {'app_name' : name,
                               'queue_name' : queue_name,
                               'state' : state,
                               'start_time' : start_time, 
                               'maps' : mapred.data[u'job'][u'mapsTotal'], 
                               'reduces' : mapred.data[u'job'][u'reducesTotal']}
                        self.jobs_scheduled[app_id] = job
                        #self.callback(app_id, job, STATE[1])
                    self.watchMapRed(app_id, job_id)
            else:
                print 'Waiting for jobs...'

    def run(self):
        '''Method that runs forever'''
        while True:
            self.watchJobs()
            time.sleep(1)

    def watchMapRed(self, app_id, job_id):
        json = self.app_master.job(app_id, job_id)
        if json.data[u'job'][u'mapsCompleted'] == '1':
            pass
            #self.callback(job_id, app_id, STATE[2])
        elif json.data[u'job'][u'reducesCompleted'] == '1':
            #self.callback(job_id, app_id, STATE[3])
            pass
        elif json.data[u'job'][u'mapProgress'] == '100':
            #self.callback(job_id, app_id, STATE[4])
            pass
