import redis
import os
import sys
sys.path.insert(0, '/usr/share/dstat/')
#import dstat
import time
import threading
import signal
import socket
import logging
import argparse
import subprocess


#####CONSTANTS######
TERMINATE  = False
REPORT_PERIOD = 1.0 ##sec
####################


############NETWORK STRUCTURE#######
vms = {'address': 'name'}

hostvms = { 'stream26' : ['vmD1', 'vmD2', 'vmD3', 'vmD4', 'vmD5',  'hmaster'],
        'stream27' :  ['vmD6', 'vmD7', 'vmD8', 'vmD9', 'vmD10', 'hslave2'],
        'stream28' :  ['vmD11', 'vmD12', 'vmD13', 'vmD14', 'vmD15', 'hslave1'],
}

####################################



#########REDIS COMMUNICATION###############
###MESSAGE STRUCTURE####
#key - cloud:vm:vd 
#score - timestamp 
#value - metric1:value1, metric2:value2, metric3:value3, ... 
def setVariable(timestamp, cloud, vm, source, values):
    key = ":".join([str(cloud), str(vm), str(source)])
    score = timestamp
    value = addToStr(addToStr('timestamp', timestamp, ':'), values)
    rServer.zadd(key, value, score)

def getKeys():
    keypattern = "*"
    response = rServer.keys(keypattern)
    return response
###########################################


#########HELPER FUNCTION###############
def addToStr(stats, value, delim = ','):
    if stats == '':
        stats = value
    else:
        stats = delim.join([str(stats), str(value)]) 
    return stats

def print_xml(ctx, path):
    res = ctx.xpathEval(path)
    if res is None or len(res) == 0:
        value = None
    else:
        value = res[0].content
    return value

def getvms():
    vms = []
    keys = getKeys()
    for line in keys:
        elements = line.split(":")
        vms.append(elements[1])
    return vms
###########################################


#########VM DAEMON#############
#########METRICS###############



class vmStats :

    def __init__(self, logger) :
        self.logger = logger
        self.stats = []
        dstat.loop = 0
        dstat.inittime = time.time()
        dstat.tick = dstat.ticks()
        dstat.op = dstat.Options(['-c'])
        #dstat.op.netlist=['eth1']
        #dstat.op.disklist=['sda2']
        for o in (dstat.dstat_cpu(), dstat.dstat_mem(), dstat.dstat_net(), dstat.dstat_disk(), dstat.dstat_swap()):
            try: 
                o.check()
                o.prepare()
            except Exception, e:
                self.logger.error("Error in vmStats init method : " + str(e))
                print e
            else: self.stats.append(o)


    def probe(self, elapsed_sec, vmip) :
        dstat.update = 1
        if dstat.loop == 0:
            dstat.elapsed = dstat.tick
        else:
            dstat.elapsed = elapsed_sec

        statsstr = ''
        for o in self.stats:
            o.extract()
            if 'total' in o.val:
                values = o.val['total']
                i = 0
                for elnick in o.nick:
                    value = values[i]
                    statsstr = addToStr(statsstr, addToStr(elnick, values[i], ':'))
                    i = i + 1 
            else:
                for key in o.val:
                    statsstr = addToStr(statsstr, addToStr(key, o.val[key], ':'))
        return statsstr



class Memory :

    def __init__(self, logger):
        self.metrics = ['MemTotal', 'MemFree', 'Buffers', 'Cached', 'SwapTotal', 'SwapFree']
        self.logger = logger

    def probe(self, elapsed_sec, vmip) :
        try:
            stats = ''
            fin = open('/proc/meminfo', 'r')
            for line in fin:
                elements = line.split()   
                metric = elements[0].split(':')[0]
                if metric in self.metrics:
                    stats = addToStr(stats, addToStr(metric, elements[1], ':'))
            fin.close()
        except Exception, e:
            self.logger.error("Error in Memory probe method : " + str(e))
        return stats


class Network :

    rx_prev_byte = {}
    tx_prev_byte = {}

    def __init__(self, logger):
        self.metrics = metrics()
        self.ifaces()
        self.logger = logger

    def ifaces(self):
        f = open('/proc/net/dev', 'r')
        for line in f.readlines()[2:] :
            iface = line.split(':')[0].strip()
            self.rx_prev_byte[iface] = 0
            self.tx_prev_byte[iface] = 0
        f.close()

    def metrics(self) :
        metrics = {}
        f = open('/proc/net/dev', 'r')
        for line in f.readlines()[2:] :
            result = []
            iface = line.split(':')[0].strip()
            result.append(iface+'_rx_byte')
            result.append(iface+'_tx_byte')
            result.append(iface+'_rx_MBs')
            result.append(iface+'_tx_MBs')
            metrics[iface] = result
        f.close()
        return metrics

    def probe(self, elapsed_sec, vmip) :
        try:
            fin = open('/proc/net/dev', 'r')
            result = ''
            for line in fin.readlines()[2:] :
                line = line.strip()
                
                (iface, vals) = line.split(':')
                vals = vals.split()

                rx_byte = int(vals[0])
                tx_byte = int(vals[8])

                if elapsed_sec == 0 :
                    rx_rate_mbps = 0
                    tx_rate_mbps = 0
                else :
                    rx_rate_mbps = ((rx_byte-self.rx_prev_byte[iface])/1024/1024)/elapsed_sec
                    tx_rate_mbps = ((tx_byte-self.tx_prev_byte[iface])/1024/1024)/elapsed_sec

                stats = addToStr(stats, addToStr(self.metrics[iface][0], rx_byte, ':'))
                stats = addToStr(stats, addToStr(self.metrics[iface][1], tx_byte, ':'))
                stats = addToStr(stats, addToStr(self.metrics[iface][2], rx_rate_mbps, ':'))
                stats = addToStr(stats, addToStr(self.metrics[iface][3], tx_rate_mbps, ':'))

                self.rx_prev_byte[iface] = rx_byte
                self.tx_prev_byte[iface] = tx_byte
        except Exception, e:
            self.logger.error("Error in Network probe method : " + str(e))
        return stats
        


class Metrics :

    def __init__(self, logger):
        self.logger = logger
        #self.metrics = ['user', 'nice', 'system', 'idle', 'iowait']
        cpu = '-u'
        ram = '-r'
        disk = '-d -p'
        net = '-n DEV'
        swap = '-S'
        interval  = 1
        self.sarProcess = subprocess.Popen(["sar", cpu, ram, disk.split()[0], disk.split()[1], swap, net.split()[0], net.split()[1],  str(interval)], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        line = self.sarProcess.stdout.readline()

    def probe(self, elapsed_sec, vmip) :
        parse = True
        elements = ''
        statsstr = ''
        while parse:
            line = self.sarProcess.stdout.readline()
            if 'CPU' in line:
                metrics = line.split()
                while True:
                    line = self.sarProcess.stdout.readline()
                    if not line.strip():
                        break
                    elements = line.split()
                    startindex = 2
                    ###in some cases AM/PM values for time  are present
                    if len(elements) == 9:
                        startindex = 3
                    for i in range(startindex, len(elements)):  
                        statsstr = addToStr(statsstr, addToStr(metrics[i].split('%')[1], elements[i], ':'))
                    #print elements
            if 'mem' in line:
                metrics = line.split()
                while True:
                    line = self.sarProcess.stdout.readline()
                    if not line.strip():
                        break
                    elements = line.split()
                    for i in range(2, 7):  
                        statsstr = addToStr(statsstr, addToStr(metrics[i], elements[i], ':'))
                    #print elements
            if 'swp' in line:
                metrics = line.split()
                while True:
                    line = self.sarProcess.stdout.readline()
                    if not line.strip():
                        break
                    elements = line.split()
                    for i in range(2, 4):  
                        statsstr = addToStr(statsstr, addToStr(metrics[i], elements[i], ':'))
                    #print elements
            if 'DEV' in line:
                metrics = line.split()
                while True:
                    line = self.sarProcess.stdout.readline()
                    if not line.strip():
                        break
                    elements = line.split()
                    for i in [4, 5]:  
                        metric = addToStr(elements[2], metrics[i], '_')
                        statsstr = addToStr(statsstr, addToStr(metric, elements[i], ':'))
                    #print elements
            if 'IFACE' in line:
                parse = False
                metrics = line.split()
                while True:
                    line = self.sarProcess.stdout.readline()
                    if not line.strip():
                        break
                    elements = line.split()
                    for i in [5, 6]:  
                        metric = addToStr(elements[2], metrics[i], '_')
                        statsstr = addToStr(statsstr, addToStr(metric, elements[i], ':'))
                    #print elements

        return statsstr  
                    


###############################



#########HOST DAEMON#############
#########METRICS###############
class Network :

    def __init__(self, cloud,  vmIPs, logger, conn):
        self.cloud = cloud        
        self.doms = dict((key, conn.lookupByName(vms[key])) for key  in vmIPs)
        self.ifaces = dict((key, self.getIface(key)) for key  in vmIPs)
        self.logger = logger
        self.metrics = ['rx_byte', 'tx_byte', 'rx_MBs', 'tx_MBs']
        self.rx_prev_byte = dict((key, 0) for key  in vmIPs)
        self.tx_prev_byte = dict((key, 0) for key  in vmIPs)

    def getIface(self, vmip):
        iface = ''
        dom = self.doms[vmip]
        xmldesc = dom.XMLDesc(0)
        doc = libxml2.parseDoc(xmldesc)
        ctx = doc.xpathNewContext()
        devs = ctx.xpathEval("/domain/devices/interface/target")
        for d in devs:
            ctx.setContextNode(d)
            iface = print_xml(ctx, '@dev')
            break
        #interfaceFound = False
        #proc = subprocess.Popen(["virsh", "dumpxml", vms[vmip]], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        #(out, err) = proc.communicate()
        #for line in out.splitlines():   
        #    if 'interface' in line:
        #        interfaceFound = True
        #    if 'target dev' in line and interfaceFound:
        #        iface = line.split("'")[1]
        #        break
        return iface
                   
    def probe(self, elapsed_sec, vmip) :
        try:
            stats = ''
            #proc = subprocess.Popen(["virsh", "domifstat", vms[vmip], self.vmIPs[vmip]], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            #(out, err) = proc.communicate()
            dom = self.doms[vmip]
            ifstats = dom.interfaceStats(self.ifaces[vmip])
            rx_byte = int(ifstats[0])
            tx_byte = int(ifstats[4])
            #for line in out.splitlines():
            #    if self.metrics[0] in line:
            #        rx_byte = int(line.split()[2])
            #    if self.metrics[1] in line:
            #        tx_byte = int(line.split()[2])
                    
            if elapsed_sec == 0:
                rx_rate_mbps = 0
                tx_rate_mbps = 0
            else :
                rx_rate_mbps = (float(rx_byte-self.rx_prev_byte[vmip])/1024.0/1024.0)/elapsed_sec
                tx_rate_mbps = (float(tx_byte-self.tx_prev_byte[vmip])/1024.0/1024.0)/elapsed_sec

            stats = addToStr(stats, addToStr(self.metrics[0], rx_byte, ':'))
            stats = addToStr(stats, addToStr(self.metrics[1], tx_byte, ':'))
            stats = addToStr(stats, addToStr(self.metrics[2], rx_rate_mbps, ':'))
            stats = addToStr(stats, addToStr(self.metrics[3], tx_rate_mbps, ':'))

            self.rx_prev_byte[vmip] = rx_byte
            self.tx_prev_byte[vmip] = tx_byte
        except Exception, e:
            self.logger.error("Error in Network probe method : " + str(e))
        return stats


class Disk :

    def __init__(self, cloud,  vmIPs, logger, conn):
        self.cloud = cloud
        self.doms = dict((key, conn.lookupByName(vms[key])) for key  in vmIPs)
        self.disks = dict((key, self.getDev(key)) for key  in vmIPs)
        self.logger = logger
        self.metrics = ['rd_req', 'rd_bytes', 'wr_req', 'wr_bytes', 'rd_reqs', 'rd_MBs', 'wr_reqs', 'wr_MBs']
        self.rd_prev_req = dict((key, 0) for key  in vmIPs)
        self.rd_prev_byte = dict((key, 0) for key  in vmIPs)
        self.wr_prev_req = dict((key, 0) for key  in vmIPs)
        self.wr_prev_byte = dict((key, 0) for key  in vmIPs)

    def getDev(self, vmip):
        dev = ''
        dom = self.doms[vmip]
        xmldesc = dom.XMLDesc(0)
        doc = libxml2.parseDoc(xmldesc)
        ctx = doc.xpathNewContext()
        devs = ctx.xpathEval("/domain/devices/disk/target")
        for d in devs:
            ctx.setContextNode(d)
            dev = print_xml(ctx, '@dev')
            break
        #interfaceFound = False
        #proc = subprocess.Popen(["virsh", "dumpxml", vms[vmip]], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        #(out, err) = proc.communicate()
        #for line in out.splitlines():   
        #    if "device='disk'" in line:
        #        interfaceFound = True
        #    if 'target dev' in line and interfaceFound:
        #        dev = line.split("'")[1]
        #        break
        return dev


    def probe(self, elapsed_sec, vmip) :
        try:
            stats = ''
            #proc = subprocess.Popen(["virsh", "domblkstat", vms[vmip], self.vmIPs[vmip]], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            #(out, err) = proc.communicate()
            dom = self.doms[vmip]
            blkstats = dom.blockStats(self.disks[vmip])
            rd_req = int(blkstats[0])
            rd_bytes = int(blkstats[1])
            wr_req = int(blkstats[2])
            wr_bytes = int(blkstats[3])
            #for line in out.splitlines():
            #    if self.metrics[0] in line:
            #        rd_req = int(line.split()[2])
            #    if self.metrics[1] in line:
            #        rd_bytes = int(line.split()[2])
            #    if self.metrics[2] in line:
            #        wr_req = int(line.split()[2])
            #    if self.metrics[3] in line:
            #        wr_bytes = int(line.split()[2])
            if elapsed_sec == 0 :
                rd_rate_reqs = 0
                rd_rate_mbps = 0
                wr_rate_reqs = 0
                wr_rate_mbps = 0
            else :
                rd_rate_reqs = (float(rd_req-self.rd_prev_req[vmip])/1024.0/1024.0)/elapsed_sec
                rd_rate_mbps = (float(rd_bytes-self.rd_prev_byte[vmip])/1024.0/1024.0)/elapsed_sec
                wr_rate_reqs = (float(wr_req-self.wr_prev_req[vmip])/1024.0/1024.0)/elapsed_sec
                wr_rate_mbps = (float(wr_bytes-self.wr_prev_byte[vmip])/1024.0/1024.0)/elapsed_sec


            stats = addToStr(stats, addToStr(self.metrics[0], rd_req, ':'))
            stats = addToStr(stats, addToStr(self.metrics[1], rd_bytes, ':'))
            stats = addToStr(stats, addToStr(self.metrics[2], wr_req, ':'))
            stats = addToStr(stats, addToStr(self.metrics[3], wr_bytes, ':'))
            stats = addToStr(stats, addToStr(self.metrics[4], rd_rate_reqs, ':'))
            stats = addToStr(stats, addToStr(self.metrics[5], rd_rate_mbps, ':'))
            stats = addToStr(stats, addToStr(self.metrics[6], wr_rate_reqs, ':'))
            stats = addToStr(stats, addToStr(self.metrics[7], wr_rate_mbps, ':'))

            self.rd_prev_req[vmip] = rd_req
            self.rd_prev_byte[vmip] = rd_bytes
            self.wr_prev_req[vmip] = wr_req
            self.wr_prev_byte[vmip] = wr_bytes
        except Exception, e:
            self.logger.error("Error in Disk probe method : " + str(e))
        return stats


class Processor :

    def __init__(self, cloud,  vmIPs, logger, conn):
        self.lastCall = dict((key, 0) for key  in vmIPs)
        self.cloud = cloud
        self.hostcpus = conn.getInfo()[2]
        self.doms = dict((key, conn.lookupByName(vms[key])) for key  in vmIPs)
        self.logger = logger
        self.metrics = ['cpu_used', 'cpuperiod', 'cpuquota', 'hostcpus', 'vcpus']
        self.prevcputime = dict((key, 0) for key  in vmIPs)

    def getHostCPUS(self):
        hostcpus = 0
        #proc = subprocess.Popen(["virsh", "nodeinfo"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        (out, err) = proc.communicate()
        #for line in out.splitlines():   
        #    if "CPU(s)" in line:
        #        hostcpus = int(line.split()[1])
        #        break
        #return hostcpus

    def probe(self, elapsed_sec, vmip) :
        try:
            now = time.time()
            stats = ''
            dom = self.doms[vmip]
            infos = dom.info()
            #proc = subprocess.Popen(["virsh", "dominfo", vms[vmip]], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            #(out, err) = proc.communicate()
            #cputime = float(infos[4]) ##nanoseconds
            #cputime = cputime/1000000000 ##get seconds
            cpus = int(infos[3])
            sched = dom.schedulerParameters()
            cpuperiod = int(sched['vcpu_period'])
            cpuquota = int(sched['vcpu_quota'])
            if cpuquota > cpuperiod:
                cpuquota = -1
            #for line in out.splitlines():
            #    if 'CPU time' in line:
            #        cputime = float(line.split()[2].split('s')[0])
            #    if 'CPU(s)' in line:
            #        cpus = int(line.split()[1])
            #proc = subprocess.Popen(["virsh", "schedinfo", vms[vmip]], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            #(out, err) = proc.communicate()
            #for line in out.splitlines():
            #    if 'vcpu_period' in line:
            #        cpuperiod = int(line.split(':')[1])
            #    if 'vcpu_quota' in line:
            #        cpuquota = int(line.split(':')[1])
            #if elapsed_sec == 0 :
            #    cpuused = 0
            #else :
                ##we need high precision lets recalculate
            #    elapsed_sec = now - self.lastCall[vmip]
                #cpuused = round((100*float(cputime-self.prevcputime[vmip]))/float(elapsed_sec), 2)
                ##value of cpu_used can be higher than the cpu limit due to low precision of the time measurement
                #if cpuquota == -1:
                #    cpu_limit =  100*cpus   
                #    
                #else:
                #    cpu_limit = round(100*cpus*float(cpuquota)/float(cpuperiod), 2)
                #if cpuused > cpu_limit:
                #    cpuused=cpu_limit    

            #stats = addToStr(stats, addToStr(self.metrics[0], cpuused, ':'))
            stats = addToStr(stats, addToStr(self.metrics[1], cpuperiod, ':'))
            stats = addToStr(stats, addToStr(self.metrics[2], cpuquota, ':'))
            stats = addToStr(stats, addToStr(self.metrics[3], self.hostcpus, ':'))
            stats = addToStr(stats, addToStr(self.metrics[4], cpus, ':'))
            #self.prevcputime[vmip] = cputime
            #self.lastCall[vmip] = now
        except Exception, e:
            self.logger.error("Error in Processor probe method : " + str(e))
        return stats



#########APP DAEMON#############
#########METRICS###############
class Progress :

    def __init__(self, logger, vmIPs) :
        self.metrics = ['map', 'reduce', 'jobid']
        self.jobid = 0
        self.jobName = ''
        self.IP = vmIPs[0]
        self.port = 50030
        self.initStats()
        self.logger = logger

    def initStats(self):
        self.stats = { 'Map % Complete' : -1,
                        'Reduce % Complete' : -1,
                        'Jobid' : -1}

    def probe(self, elapsed_sec, vmip) :
        try:
            response = urllib2.urlopen('http://'+ str(self.IP) +':'+ str(self.port) +'/jobtracker.jsp',timeout=0.05)
            html = response.read()
            soup = bs(html)
            tables = soup.findAll("table")
            i = 0 
            self.initStats()
            for table in tables:
                if table.findParent("table") is None:
                    if i == 2: #third table is running job stats
                        tree = ElementTree.fromstring(str(table))
                        rows = tree.findall("tr")
                        headrow = rows[0]
                        datarows = rows[1:]
                        for num, h in enumerate(headrow):
                            tdElem = [row[num] for row in datarows]
                            #print 'mark', h[0].text, tdElem[0].text
                            if h[0].text in self.stats:
                                if h[0].text== 'Jobid':
                                    jobName = tdElem[0].find('a').text
                                    if self.jobName != jobName:
                                        self.jobName = jobName
                                        self.jobid = self.jobid + 1
                                    self.stats[h[0].text] = self.jobid
                                else:
                                    self.stats[h[0].text] = tdElem[0].text.split('%')[0]
                                   
                i = i + 1
        except urllib2.URLError, e:
            self.logger.error("Oops, progress score request timed out? : " + str(e))
        except socket.timeout:
            self.logger.error("Progress score request timed out.")
        stats = ''
        i = 0
        for key in self.stats:
            stats = addToStr(stats, addToStr(self.metrics[i], self.stats[key], ':'))
            i = i + 1
        return stats


class Power :

    pdu_snmp_user="readonly"
    pdu_snmp_password="readonly"
    pdus = ['141.76.50.20', '141.76.50.21', '141.76.50.22', '141.76.50.23', '141.76.50.24']

    def __init__(self, logger, hostiplookup = True) :
        self.hostiplookup = hostiplookup
        self.logger = logger
        self.e = pdu_snmp.EnergyConsumptionTracker(self.pdus, self.pdu_snmp_user, self.pdu_snmp_password)


    def probe(self, elapsed_sec, vmip) :
        outlet_labels = []
        if self.hostiplookup:
            for host in hostvms:
                if vms[vmip] in hostvms[host]: 
                    outlet_labels.append(host)
                    break
        else:
            outlet_labels.append(vmip)
        watts = self.e.get_outlet_active_power(outlet_labels)
        stats = ''
        for machine in watts:
            stats = addToStr(stats, addToStr('watts', watts[machine], ':'))
        return stats

###############################



#########HOST DAEMON#############
#########ACTUATOR###############

class Actuator(threading.Thread):

    def __init__(self, cloud,  vmIPs, logger, conn):
        threading.Thread.__init__(self)
        self.setDaemon(True)
        self.cloud = cloud 
        self.logger = logger
        self.pubsub = rServer.pubsub()
        self.subscribe(cloud, vmIPs, conn)
        self.doms = dict((key, conn.lookupByName(vms[key])) for key  in vmIPs)
        self.subscribed = 0

    def subscribe(self, cloud, vmIPs, conn):
        channels = []
        knobs = ['cpu', 'mem']
        for vm in vmIPs:
            for knob in knobs:         
                channels.append(":".join([str(cloud), str(vm), str(knob)]))
        self.logger.info("Subscribed to channels :" + str(channels))
        self.pubsub.subscribe(channels)

    def run(self):
        for item in self.pubsub.listen():
            channel = item['channel'].split(':')
            vmip = channel[1]
            dom = self.doms[vmip]
            resource = channel[2]
            self.logger.info("item =  :" + str(item))
#            if self.subscribed >= len(self.doms)*2:
            valueStr = str(item['data'])
            print item
            if  'v' in valueStr:
                print valueStr
                value = int(float(valueStr.split('v')[1]))
                if resource == 'mem':  
                    self.logger.info("memVal =  :" + str(value))
                    info = dom.info()  
                    maxMem = int(info[1]) ##in kbytes
                    currMem = int(info[2]) ##in kbytes
                    dom.setMemory(value)
                if  resource == 'cpu':              
                    self.logger.info("cpuVal =  :" + str(value))
                    sched = dom.schedulerParameters()
                    sched['vcpu_quota'] = int(value)
                    if sched['vcpu_quota'] > sched['vcpu_period']:
                        sched['vcpu_quota'] = sched['vcpu_period']
                    if sched['emulator_quota'] > sched['emulator_period']:
                        sched['emulator_quota'] = sched['emulator_period']
                    print sched
                    #dom.setSchedulerParameters(sched)
                    quota =  'vcpu_quota='+str(sched['vcpu_quota'])
                    subprocess.call(['virsh', 'schedinfo', vms[vmip], quota])
#            else:
#                self.subscribed = self.subscribed + 1
            self.logger.info("subscribed =  :" + str(self.subscribed))



#########LOGGING###############
def createLogger(source , vmname):
    loggername = 'DoLen_'+ str(source) + '_' + str(vmname)
    logger = logging.getLogger(loggername)
    hdlr = logging.FileHandler(loggername + '_.log')
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    hdlr.setFormatter(formatter)
    logger.addHandler(hdlr)
    logger.setLevel(logging.INFO)
    return logger
###############################




############REPORT LOOP########
def record(probes, cloud, vmsList, source) :
    prev = 0
    vms = []
    prevms = vms
    while not TERMINATE :
#        #print vms
        if source == 'app' and (len(vms) == 0 or len(vms)!=len(prevms)):
            prevms = vms
            vms = getvms()
#            #prev = dict((key, 0) for key  in vms)
        if source == 'app':
            mapstats = ''
            for vm in vms: 
                stats = ''
                now = time.time()
                elapsed_sec = 0 if prev == 0 else (now - prev)
                for p in probes :   
                    if p.__class__.__name__ == 'Progress':
                        if mapstats=='':
                            mapstats = addToStr(mapstats, p.probe(elapsed_sec, vm))
                    else:
                        stats = addToStr(stats, p.probe(elapsed_sec, vm))
                stats = addToStr(stats, mapstats) 
                setVariable(now, cloud, vm, source, stats) 
                prev = now
        else:
            for vm in vmsList:
                stats = ''
                now = time.time()
                elapsed_sec = 0 if prev == 0 else (now - prev)
                for p in probes :   
                    stats = addToStr(stats, p.probe(elapsed_sec, vm))
    #            if source == 'app':
    #                break
                setVariable(now, cloud, vm, source, stats)
                prev = now
#        if source == 'app':
#            for vm in vmsList:
#                setVariable(now, cloud, vm, source, stats)  
                   
        time.sleep(REPORT_PERIOD)
###############################


############TERMINATE HANDLER########
def sigterm_handler(signum, frame) :
    print 'Terminate was called'
    print 'Signal handler called with signal', signum
    print "At ", frame.f_code.co_name, " in ", frame.f_code.co_filename, " line " , frame.f_lineno
    global TERMINATE
    TERMINATE = True
######################################


############MAIN###########
if __name__ == '__main__':
    signal.signal(signal.SIGINT, sigterm_handler)
    signal.signal(signal.SIGTERM, sigterm_handler)
    parser = argparse.ArgumentParser(description='Enter VM location data')
    parser.add_argument('--source', metavar='D', type=str, choices=['vm', 'host', 'app'],
                       help='data source(vm or host)')
    parser.add_argument('--cloud', metavar='C', type=str, default='None',
                       help='cloud identificator')
    parser.add_argument('--vms', metavar='I', type=str, nargs='+', default='None', 
                       help='VMs IP addresses')
    parser.add_argument('--redisserver', metavar='R', type=str, default='localhost', 
                       help='redis server ip')
    parser.add_argument('--redisport', metavar='P', type=int, default=6379,
                       help='redis server port')
    parser.add_argument('--actuator', action='store_true', default=False,
                       help='Work as host capacity manager')

    args = parser.parse_args()
    logger = createLogger(args.source, args.vms)
#    try:
    infoStr = 'cloud:' + str(args.cloud) + ' vms:' + str(args.vms) + ' dataSource:' + str(args.source) + ' redisServer:' + str(args.redisserver) + ' redisPort:' + str(args.redisport) + ' actuator:' + str(args.actuator)  
    logger.info(infoStr)
    rServer = redis.Redis(host=args.redisserver, port=args.redisport, db=0)
    vmsList = list(args.vms)
    if args.source == 'vm':
        #record([vmStats(logger)], args.cloud, vmsList, args.source)
        record([Metrics(logger)], args.cloud, vmsList, args.source)
        #record([Memory(logger)], args.cloud, vmsList, args.source)
    if args.source == 'app':
        import pdu_snmp
        record([Power(logger, False)], args.cloud, vmsList, args.source)
        #from xml.etree import ElementTree
        #import urllib2
        #record([Progress(logger, vmsList)], args.cloud, vmsList, args.source)
    if args.source == 'host':
        import libxml2
        import libvirt
        conn = libvirt.open("qemu:///system")
        if args.actuator:
            ###start actuator###
            vmActuator = Actuator(args.cloud, vmsList, logger, conn)
            vmActuator.start()
        record([Processor(args.cloud, vmsList, logger, conn)], args.cloud, vmsList, args.source)
        #record([Network(args.cloud, vmsList, logger, conn), Disk(args.cloud, vmsList, logger, conn), Processor(args.cloud, vmsList, logger, conn)], args.cloud, vmsList, args.source)
        if args.actuator:
            if vmActuator.isAlive():
                vmActuator.join(0.1)
#    except Exception, e:
#        logger.error("Error in Main method : " + str(e))
###########################
