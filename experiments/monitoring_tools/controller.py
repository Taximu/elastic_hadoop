import redis
import os
import sys
import time
import signal
import logging
import argparse
from math import exp
from math import ceil

terminate  = False



###MESSAGE STRUCTURE####
#timestamp:cloud:vm:metric:value


#########HELPER FUNCTION###############

def addToStr(stats, value, delim = ','):
    if stats == '':
        stats = value
    else:
        stats = delim.join([str(stats), str(value)]) 
    return stats


def getAllMetrics():
    metrics = []
    keypattern = '*'
    keys = rServer.keys(keypattern)
    for key in keys:
        metrics.append(key.split(':')[2])
    return metrics


def getKeys():
    keypattern = "*"
    response = rServer.keys(keypattern)
    return response


def parseKey():
    data = {'clouds' : {},
            'vms' : {},
            'sources': {}}
    keys = getKeys()
    for line in keys:
        elements = line.split(":")
        data['clouds'][elements[0]] = None
        data['vms'][elements[1]] = None
        data['sources'][elements[2]] = None
    return data

##weighted mean average
def WMA(valList) :
    wma = 0
    el_sum = 0
    for i in range(0, len(valList)) :
        el_sum = el_sum + (i+1)
    if el_sum > 0 :
        el_wg = 0        
        for i in range(0, len(valList)) :
            el_wg = el_wg + (i + 1)*valList[i]
        wma = float(el_wg)/float(el_sum)
    return wma 

###########################################            

def metricHistory(cloud, vm, source, startTime, endTime):
    key = ":".join([str(cloud), str(vm), str(source)])
    response = rServer.zrangebyscore(key, startTime, endTime)
    return response


def metricHistoryParsed(cloud, vm, sources, startTime, endTime, roundTimestamp = True):
    history = {}
    for source in sources:
        result = metricHistory(cloud, vm, source, startTime, endTime) 
        #print 'parse ', vm, source, len(result), startTime, endTime
        for line in result:
            data  = {}
            elements = line.split(',')
            for elem in elements:
                parameterData = elem.split(':')
                if len(parameterData) > 1:
                    parameterName = parameterData[0]
                    parameterValue = parameterData[1]
                    if 'timestamp' == parameterName:
                        timestamp = float(parameterValue)
                        if roundTimestamp:
                            timestamp = int(timestamp)##round time to seconds
                        data[parameterName] = timestamp
                    else:
                        data[parameterName] = parameterValue
            if 'cpuquota' in data: ###default value cpuquota == -1
                if int(data['cpuquota']) == -1:
                    data['cpuquota'] = float(data['cpuperiod'])
    
            if timestamp in history:
                oldData = history[timestamp]
                oldData.update(data)
                history[timestamp] = oldData
            else:
                history[timestamp] = data
    return history


def getHeaders(history):
    headers = {}
    for timestamp in history:
        currHeaders = history[timestamp]
        if len(currHeaders) > len(headers):
            headers = currHeaders.copy()
        ###take the line with max elements
    return headers

###################DUMP ALL DATA ####################

def dumpDB(logger, dumpfile):
    #try: 
    #stats = {}
    parsedKey = parseKey()
    clouds = parsedKey['clouds'].keys()
    vms = parsedKey['vms'].keys()
    sources = parsedKey['sources'].keys()
    #print clouds, vms, sources
    with open(dumpfile, 'w') as fout:
        ##find longest header
        headersInFile = {}
        for cloud in clouds:
            for vm in vms:
                ###get data
                history = metricHistoryParsed(cloud, vm, sources, '-inf', 'inf') 
                #stats[cloud + ':'+ vm] = history
                ###get headers
                headers = getHeaders(history)
                if len(headers) > len(headersInFile):
                    headersInFile = headers.copy()
        history = []
        ##write headers to file    
        headerStr = 'cloud vm'
        for parameterName in headersInFile:
            headerStr = addToStr(headerStr, parameterName, delim = ' ')
        print >> fout, headerStr
        for cloud in clouds:
            for vm in vms:
                #history = stats[cloud + ':' + vm]
                ###get data
                history = metricHistoryParsed(cloud, vm, sources, '-inf', 'inf') 
                ###get headers
                headers = getHeaders(history)
                ###write data
                #test = []
                for timestamp in sorted(history): ##data may be unordered
                    data = history[timestamp]    
                    strData = ''
                    strData = addToStr(strData, cloud, delim = ' ') 
                    strData = addToStr(strData, vm, delim = ' ') 
                    for parameterName in headersInFile:
                        if parameterName in headers:
                            if parameterName in data:
                                #test.append(0)
                                parameterValue = data[parameterName]
                            else:
                                if 'timestamp' == parameterName:
                                    parameterValue = timestamp
                                else:
                                    parameterValue = 'NA'
                            strData = addToStr(strData, parameterValue, delim = ' ')   
                        else:
                            ###if the parameter is not available 
                            parameterValue = 'NULL'
                            strData = addToStr(strData, parameterValue, delim = ' ')
                    print >> fout, strData
                    #print strData
            break
    rServer.flushall()
    #except Exception, e:
    #    logger.error("Error in dumpDB method : " + str(e))            
        
###################DUMP ALL DATA ####################    


#################MONITORING#####################

#def getCPUStats(cloud, vm, sources, startTime, endTime, metrics):
#    stats
#    history = metricHistoryParsed(cloud, vm, sources, startTime, endTime, False)
#    for timestamp in sorted(history):
#        data = history[timestamp]
#        if 'cpu_used' in data:
#            print now, timestamp, now - lookupWindow, vm, data['cpu_used']
    
def monitorInit(clouds, vms):
    knob = 'cpu'
    for cloud in clouds:
        for vm in vms:
            scaleCPU(cloud, vm, knob, -1)
            

def monitor(logger, conf) :
#    try:
    parsedKey = parseKey()
    stats  = {}    
    clouds = parsedKey['clouds'].keys()
    vms = parsedKey['vms'].keys()
    sources = parsedKey['sources'].keys()
    print 'clouds ', clouds
    print 'vms ', vms
    print 'sources', sources
    ##get memory usage over last 5 seconds
    lookupWindow = 5
    duration_sec = 1.0
    monitorInit(clouds, vms)
    time.sleep(duration_sec)
    lastActionTime = {}
    for vm in vms:
        lastActionTime[vm] = 0   
    while not terminate :
        now = time.time()
        print now
        knob = 'cpu'
        for cloud in clouds:
            for vm in vms: #['141.76.50.201']:          
                control(cloud, vm, sources, knob, lastActionTime, conf)
        time.sleep(duration_sec)
#    except Exception, e:
#        logger.error("Error in control method : " + str(e))


def deadline(logger,conf) :
    fin = open('conf', 'w')
    elements = conf.split(':')
    slo = int(elements[0])
    ratio = float(elements[1])
    #try:
    parsedKey = parseKey()
    clouds = parsedKey['clouds'].keys()
    vms = parsedKey['vms'].keys()
    sources = parsedKey['sources'].keys()
    print 'clouds ', clouds
    print 'vms ', vms
    print 'sources', sources
    duration_sec = 1.0
    monitorInit(clouds, vms)
    time.sleep(duration_sec)
    lastActionTime = 0
    prevProgress = {}
    jobStarted = None
    maptime = None
    knob = 'cpu' 
    while not terminate :
        now = time.time()
        print now    
        lastActionTime, prevProgress, jobStarted, maptime = controltime(logger, clouds, vms, sources, knob, lastActionTime, prevProgress, now, jobStarted, maptime, slo, ratio)
        time.sleep(duration_sec)
    #except Exception, e:
    #    logger.error("Error in control method : " + str(e))

################################################

##############CONTROL TIME###########################
def controltime(logger, clouds, vms, sources, knob, lastActionTime, prevProgress, now, jobStarted, maptime, slo, ratio):
    elapsed = 0
    controlInterval = 5
    timeLeft = 0
    deltaP = -1
    phase = ''
    timeout = 10 # sec
    #slo = 700 #deadline time
    currProgress, currentQuotaValue = getStats(sources, now)
    newQuotaValue = currentQuotaValue
    if currProgress['map'] != -1:
        if currProgress['map'] <100:
            phase = 'map'
        else:
            phase = 'reduce'    
        progresScoreUpdated = False
        newJob = False
#        if len(prevProgress)==0:
#            prevProgress.update(currProgress)  
#            jobStarted  = now
#            lastActionTime = now  
#            maptime = None
#        else:  
        for key in prevProgress:
            if key == phase:
                if currProgress[key]!= prevProgress[key]:
                    progresScoreUpdated = True   
            if currProgress['jobid']!= prevProgress['jobid']:
                newJob = True   
        if newJob or len(prevProgress)==0:
            jobStarted  = now
            lastActionTime = now  
            maptime = None
            prevProgress.update(currProgress)  
#        elapsed = now -  lastActionTime
        elapsed = currProgress['timestamp'] - prevProgress['timestamp']
        if maptime == None and currProgress['map'] == 100:
            maptime = now - jobStarted  
        if progresScoreUpdated==True and elapsed >= controlInterval:
#            newQuotaValue = currentQuotaValue
            if currProgress['map'] <100:
                timeLeft = ratio*slo - (now -jobStarted)
                deltaP = currProgress['map'] - prevProgress['map']
                currentP = currProgress['map']
            else:
                timeLeft = slo - (now - jobStarted)
                deltaP = currProgress['reduce'] - prevProgress['reduce']
                currentP = currProgress['reduce']
            

            if deltaP > 0:
                value = currentQuotaValue*elapsed*float(100-currentP)/(float(deltaP)*timeLeft)
                newQuotaValue = ceil(float(value)/100)*100
            #else:  
                
            #    deltaP = -1 ## we do not control reduce phase
            #    newQuotaValue = -1
                #deltaP = currProgress['reduce'] - prevProgress['reduce']
                #currentP = currProgress['reduce']
            print deltaP
            prevProgress.update(currProgress)
            
    else:
        maxCPUquota = 100000
        newQuotaValue = maxCPUquota
    minCPUquota = 5000
    if now - lastActionTime > timeout and currentQuotaValue==minCPUquota:
        newQuotaValue = currentQuotaValue + minCPUquota
        prevProgress.update(currProgress)
    if newQuotaValue!=currentQuotaValue:
        for cloud in clouds:
            for vm in vms: #['141.76.50.201']:   
                if vm not in ['141.76.50.212']:  ##don't scale master vm
                    scaleCPU(cloud, vm, knob, newQuotaValue)
        lastActionTime = now
#    if newQuotaValue!=currentQuotaValue:
            

    print currProgress, prevProgress, newQuotaValue, lastActionTime, elapsed, currentQuotaValue, timeLeft, maptime, deltaP, jobStarted
    info = ''
    info = addToStr(info, currProgress, ' ')
    info = addToStr(info, prevProgress, ' ')
    info = addToStr(info, newQuotaValue, ' ')
    info = addToStr(info, lastActionTime, ' ')
    info = addToStr(info, elapsed, ' ')
    info = addToStr(info, currentQuotaValue, ' ')
    info = addToStr(info, timeLeft, ' ')
    info = addToStr(info, maptime, ' ')
    info = addToStr(info, deltaP, ' ')
    info = addToStr(info, jobStarted, ' ')
    logger.info(info)

    return lastActionTime, prevProgress, jobStarted, maptime

    
   
def getStats(sources, now):
    maxCPUquota = 100000
    currentQuotaValue = maxCPUquota
    lookupwindow = 5 # sec
    #masterVM = '141.76.50.212'
    slaveVM = '141.76.50.213'
    cloud = '0'
    currProgress = {'map':-1, 'reduce':-1, 'jobid':-1, 'timestamp': -1}
    maxTimestamp = {'cpuquota':-1, 'map':-1}

    history = metricHistoryParsed(cloud, slaveVM, sources, now - lookupwindow, now, False)
    for timestamp in sorted(history):
        data = history[timestamp]
        for key in maxTimestamp:
            if key in data:
                if maxTimestamp[key] < timestamp:##we have different sources
                    maxTimestamp[key] = timestamp
    if maxTimestamp['cpuquota'] != -1 and maxTimestamp['map'] != -1  :
        currentQuotaValue = int(history[maxTimestamp['cpuquota']]['cpuquota'])
        currProgress['map'] = float(history[maxTimestamp['map']]['map'])
        currProgress['reduce'] = float(history[maxTimestamp['map']]['reduce'])
        currProgress['jobid'] = float(history[maxTimestamp['map']]['jobid'])
        currProgress['timestamp'] = float(maxTimestamp['map'])


    return currProgress, currentQuotaValue


##############CONTROL###########################
def control(cloud, vm, sources, knob, lastActionTime, conf):

    period = 1 ## monitoring period 1 second

    thUp = 60 ## scale up threshold
    upCalmPeriod = 5 #sec, during the calm period, the capacity cannot be changed  
    upEvaluationPeriod = 5 # don't trigger the policy on the first scary number. Check this many before acting. 
    upAdjustment = 20000 ##CPU quota by which to scale virtual CPU

    thDown = 30 ## scale down threshold
    downCalmPeriod = 10 ##sec, during the calm period, the capacity cannot be changed  
    downEvaluationPeriod = 5 # don't trigger the policy on the first scary number. Check this many before acting. 
    downAdjustment = -5000 ##CPU quota by which to scale virtual CPU

    actionType = -1 ## -1 - do nothing, 0 - scale down, 1 - scale up

    #lastActionTime = 0 ##upCalmPeriod or downCalmPeriod depends on what was the last action
    

    now = time.time()  
    elapsed = now -  lastActionTime[vm] 
    print vm 
    if conf == '8':
        maxCPUquota = 100000
        actionType, currentValue, alfa = checkCPUAlarms(cloud, vm, sources, upEvaluationPeriod, now, thUp, 1, conf)
        newValue = int(maxCPUquota)
        if actionType == -1:
            actionType, currentValue, alfa = checkCPUAlarms(cloud, vm, sources, downEvaluationPeriod, now, thDown, 0, conf)
            newValue = currentValue + downAdjustment
    if conf == '7':
        actionType, currentValue, alfa = checkCPUAlarms(cloud, vm, sources, upEvaluationPeriod, now, thUp, 1, conf)
        newValue = int(currentValue*alfa)
        if actionType == -1:
            actionType, currentValue, alfa = checkCPUAlarms(cloud, vm, sources, downEvaluationPeriod, now, thDown, 0, conf)
            newValue = currentValue + downAdjustment
    if conf == '6':
        actionType, currentValue, alfa = checkCPUAlarms(cloud, vm, sources, upEvaluationPeriod, now, thUp, 1, conf)
        newValue = currentValue + upAdjustment/2
        if actionType == -1:
            actionType, currentValue, alfa = checkCPUAlarms(cloud, vm, sources, downEvaluationPeriod, now, thDown, 0, conf)
            newValue = currentValue + downAdjustment
    if conf == '5':
        actionType, currentValue, alfa = checkCPUAlarms(cloud, vm, sources, upEvaluationPeriod, now, thUp, 1, conf)
        newValue = int(currentValue*alfa)
        if actionType == -1:
            actionType, currentValue, alfa = checkCPUAlarms(cloud, vm, sources, downEvaluationPeriod, now, thDown, 0, conf)
            newValue = currentValue + downAdjustment
    if conf == '4':
        actionType, currentValue, alfa = checkCPUAlarms(cloud, vm, sources, upEvaluationPeriod, now, thUp, 1, conf)
        newValue = int(currentValue*alfa)
        if actionType == -1:
            actionType, currentValue, alfa = checkCPUAlarms(cloud, vm, sources, downEvaluationPeriod, now, thDown, 0, conf)
            newValue = currentValue + downAdjustment
    if conf == '3':
        actionType, currentValue, alfa = checkCPUAlarms(cloud, vm, sources, upEvaluationPeriod, now, thUp, 1, conf)
        newValue = currentValue + upAdjustment
        if actionType == -1:
            actionType, currentValue, alfa = checkCPUAlarms(cloud, vm, sources, downEvaluationPeriod, now, thDown, 0, conf)
            newValue = currentValue + downAdjustment
    if conf == '2':
        actionType, currentValue, alfa = checkCPUAlarms(cloud, vm, sources, upEvaluationPeriod, now, thUp, 1, conf)
        newValue = currentValue + upAdjustment
        if actionType == -1:
            actionType, currentValue, alfa = checkCPUAlarms(cloud, vm, sources, downEvaluationPeriod, now, thDown, 0, conf)
            newValue = currentValue + downAdjustment
    if conf == '1':
        if elapsed >= upCalmPeriod:
            actionType, currentValue, alfa = checkCPUAlarms(cloud, vm, sources, upEvaluationPeriod, now, thUp, 1, conf) 
            newValue = currentValue + upAdjustment
        if actionType == -1:
            if elapsed >= downCalmPeriod:
                actionType, currentValue, alfa = checkCPUAlarms(cloud, vm, sources, downEvaluationPeriod, now, thDown, 0, conf)
                newValue = currentValue + downAdjustment
    if actionType != -1:
        print actionType, currentValue
        scaleCPU(cloud, vm, knob, newValue)
        lastActionTime[vm] = now
        elapsed = 0
        

def scaleCPU(cloud, vm, knob, value):
    maxCPUquota = 100000
    minCPUquota = 5000  
    if value != -1:
        if value > maxCPUquota:
            value = maxCPUquota    
        if value < minCPUquota:
            value = minCPUquota  
    print value
    key = ":".join([str(cloud), str(vm), str(knob)])
    rServer.publish(key, 'v' + str(value))

 
##thresholdType ## 1 - upper threshold, 0 - lower threshold
def checkCPUAlarms(cloud, vm, sources, lookupWindow, now, threshold, thresholdType, conf):
    min_a = 1.2
    max_a = 2
    alfa = 1
    actionType = -1
    maxTimestamp = -1
    cpuUtil = []
    history = metricHistoryParsed(cloud, vm, sources, now - lookupWindow, now, False)
    for timestamp in sorted(history):
        data = history[timestamp]
        if 'cpu_used' in data:
            PCPULimit = float(data['cpuquota'])/float(data['cpuperiod'])
            cpuUtil.append(float(data['cpu_used'])/float(int(data['vcpus'])*PCPULimit)) ##in %
        if maxTimestamp < timestamp and 'cpuquota' in history[timestamp]:
            maxTimestamp = timestamp
    currentValue = int(history[maxTimestamp]['cpuquota'])
    if conf in ['3', '4', '5']:
        avgCPUUtil = WMA(cpuUtil)
    else:
        avgCPUUtil = float(sum(cpuUtil))/float(len(cpuUtil))
    if avgCPUUtil > threshold and thresholdType==1:
        if conf in ['5', '7']:
            alfa = exp(2*float(avgCPUUtil - threshold)/float(100-threshold))
        else:
            alfa = round(float(max_a-min_a)*float(avgCPUUtil - threshold)/float(100-threshold) + min_a, 1)
        actionType = 1
    if avgCPUUtil < threshold and thresholdType==0: 
        if conf in ['7']:
            avgCPUUtil = float(sum(cpuUtil))/float(len(cpuUtil))
            if avgCPUUtil < threshold:
                actionType = 0
            else:
                actionType = -1
        else:     
            actionType = 0
    print 'avgCPUUtil = ',  avgCPUUtil, alfa
    return actionType, currentValue, alfa  
        
    
#df$cpuutil<-df$cpu_used/(maxcpu*100*(df$cpu/100))

################################################

def sigterm_handler(signum, frame) :
    print 'Terminate was called'
    print 'Signal handler called with signal', signum
    print "At ", frame.f_code.co_name, " in ", frame.f_code.co_filename, " line " , frame.f_lineno
    global terminate
    terminate = True

def createLogger(vmname):
    loggername = 'controller_'+ str(vmname)
    logger = logging.getLogger(loggername)
    hdlr = logging.FileHandler(loggername + '_.log')
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    hdlr.setFormatter(formatter)
    logger.addHandler(hdlr)
    logger.setLevel(logging.INFO)
    return logger

if __name__ == '__main__':
    signal.signal(signal.SIGINT, sigterm_handler)
    signal.signal(signal.SIGTERM, sigterm_handler)
    parser = argparse.ArgumentParser(description='Enter VM location data')
    parser.add_argument('--redisserver', metavar='R', type=str, default='141.76.50.84', 
                       help='redis server ip')
    parser.add_argument('--redisport', metavar='P', type=int, default=6379,
                       help='redis server port')
    parser.add_argument('--mode', metavar='D', type=str, choices=['dump', 'monitor', 'deadline'],
                       help='data source(vm or host)')
    parser.add_argument('--conf', metavar='C', type=str,
                       help='configuration')
    parser.add_argument('--dumpfile', metavar='f', type=str, default='/tmp/stats', 
                       help='file to dump db')

    args = parser.parse_args()
    infoStr = 'redisServer:' + str(args.redisserver) + ' redisPort:' + str(args.redisport)  + ' configuration:' + str(args.conf)  + ' dumpfile:' + str(args.dumpfile)
    logger = createLogger('localhost')
    #try:
    rServer = redis.Redis(host=args.redisserver, port=args.redisport, db=0)
    if args.mode == 'dump':
        dumpDB(logger, args.dumpfile)    
    if args.mode == 'monitor':
        monitor(logger, args.conf)
    if args.mode == 'deadline':
        deadline(logger, args.conf)
    #except Exception, e:
     #   logger.error("Error in Main method : " + str(e))

