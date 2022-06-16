from yarn_api_client import HistoryServer

class JobHistoryMonitor:
        '''Class to collect job history'''

        historyserverhost = 'stream3'
        
        @staticmethod
        def getJobHistory(job_id):
                hs = HistoryServer(JobHistoryMonitor.historyserverhost)
                jobs = hs.jobs()
                jobsdata = jobs.data['jobs']['job']
                MAP_time = 0
                map_counter = 0
                maxMAP_time = 0
                REDUCE_time = 0
                reduce_counter = 0
                maxREDUCE_time = 0
                jobIsFound = False
                for job in jobsdata:
                        jobid = job['id']
                        if jobid == job_id:
                                jobIsFound = True
                                jobstate = job['state']
                                tasks = hs.job_tasks(jobid)
                                tasksdata = tasks.data['tasks']['task']
                                for task in tasksdata:
                                        taskid = task['id']   
                                        taskstate = task['state'] 
                                        tasktype = task['type']
                                        attempts = hs.task_attempts(jobid, taskid)
                                        attemptsdata = attempts.data['taskAttempts']['taskAttempt']
                                        for attempt in attemptsdata:
                                                attemptstate = attempt['state']
                                                if taskstate == 'SUCCEEDED' and attemptstate == 'SUCCEEDED':
                                                        if tasktype == 'MAP':
                                                                calc_map_time = int(attempt['finishTime']) - int(attempt['startTime'])
                                                                if calc_map_time > maxMAP_time:
                                                                        maxMAP_time = calc_map_time
                                                                MAP_time += calc_map_time
                                                                map_counter += 1
                                                        if tasktype == 'REDUCE':
                                                                calc_red_time = int(attempt['finishTime']) - int(attempt['startTime'])
                                                                if calc_red_time > maxREDUCE_time:
                                                                        maxREDUCE_time = calc_red_time
                                                                REDUCE_time += calc_red_time
                                                                reduce_counter += 1
                                break
                if jobIsFound == True:
                        avg_map_time = MAP_time / map_counter
                        avg_reduce_time = REDUCE_time / reduce_counter
                        job_history = {'avg_map_time' : avg_map_time, 
                                       'max_map_time': maxMAP_time, 
                                       'avg_reduce_time' : avg_reduce_time, 
                                       'max_reduce_time' : maxREDUCE_time}
                        return job_history
                else:
                        return 'No job history is found!'
