import json
from yarn_api_client import ApplicationMaster, HistoryServer, NodeManager, ResourceManager
import argparse

historyserverhost = 'stream3'
resourcemanagerhost = 'stream3'

def addToStr(stats, value, delim = ','):
    if stats == '':
        stats = value
    else:
        stats = delim.join([str(stats), str(value)])
    return stats   

def dumpStatsPerNM(dumpfile):
    with open(dumpfile, 'w') as fout:
        headerStr = 'jobid jobstate taskid taskstate tasktype attemptid attemptstate attemptstartTime attemptfinishTime shuffleFinishTime nodeHttpAddress'
        print >> fout, headerStr
        hs = HistoryServer(historyserverhost)
        jobs = hs.jobs()
        jobsdata = jobs.data['jobs']['job']
        for job in jobsdata:
            strData = ''
            jobid = job['id']
            jobstate = job['state']
            strData = addToStr(strData, jobid, ' ')
            strData = addToStr(strData, jobstate, ' ')
            tasks = hs.job_tasks(jobid)
            tasksdata = tasks.data['tasks']['task']
            for task in tasksdata:
                taskid = task['id']   
                taskstate = task['state'] 
                tasktype = task['type'] 
                taskData = ''
                taskData = addToStr(taskData, taskid, ' ')
                taskData = addToStr(taskData, taskstate, ' ')
                taskData = addToStr(taskData, tasktype, ' ')
                attempts = hs.task_attempts(jobid, taskid)
                attemptsdata = attempts.data['taskAttempts']['taskAttempt']
                for attempt in attemptsdata:
                    attemptid = attempt['id']   
                    attemptstate = attempt['state']  
                    attemptstartTime = attempt['startTime'] 
                    attemptfinishTime = attempt['finishTime'] 
                    shuffleFinishTime = '0'
                    if tasktype == 'REDUCE':
                        shuffleFinishTime = attempt['shuffleFinishTime'] 
                    nodeHttpAddress = attempt['nodeHttpAddress']  
                    attemptData = ''
                    attemptData = addToStr(attemptData, attemptid, ' ')
                    attemptData = addToStr(attemptData, attemptstate, ' ')
                    attemptData = addToStr(attemptData, attemptstartTime, ' ')
                    attemptData = addToStr(attemptData, attemptfinishTime, ' ')
                    attemptData = addToStr(attemptData, shuffleFinishTime, ' ')
                    attemptData = addToStr(attemptData, nodeHttpAddress, ' ')   
                    outData = ''
                    outData = addToStr(outData, strData, ' ')
                    outData = addToStr(outData, taskData, ' ')
                    outData = addToStr(outData, attemptData, ' ')
                    #print jobid, jobstate, taskid, taskstate, tasktype, attemptid, attemptstate, attemptstartTime, attemptfinishTime, shuffleFinishTime, nodeHttpAddress
                    print >> fout, outData


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Enter dump file location')
    parser.add_argument('--mode', metavar='D', type=str, choices=['dump'],
                       help='functional mode')
    parser.add_argument('--dumpfile', metavar='f', type=str, default='/tmp/stats', 
                       help='file to dump db')
    
    args = parser.parse_args()
    if args.mode == 'dump':
        dumpStatsPerNM(args.dumpfile)   
