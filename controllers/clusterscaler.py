from fabric.api import *
from fabric.contrib.files import append

env.keepalive = True

hypervisor =   ['gorbunovmaksim@gorbunovmaksim']
redisserver =  ['maxim@stream1']
master =       ['maxim@stream3']
corenodes =    ['maxim@stream2', 'maxim@stream4']
computenodes = ['maxim@stream9', 'maxim@stream10']

env.roledefs = {
    'hypervisor':   hypervisor,
    'master':       master,
    'corenodes':    corenodes,
    'computenodes': computenodes,
    'redisserver':  redisserver
}

def addnodes(qty=0):
    numcompnodes = len(computenodes)
    if int(qty) > numcompnodes:
        answer = raw_input("Only " + str(numcompnodes) + " computenodes can be added. Agree (y/n)? ")
        if answer.lower() == 'y':
            execute(runcomputenodes, numcompnodes)
        else:
            print 'No compute nodes were added to the cluster.'
    else:
        execute(runcomputenodes, qty)

@roles('computenodes')
def runcomputenodes(qty):
    with settings(warn_only = True):
        if env.host_string in computenodes[0:int(qty)]:
            run('/mnt/maxim/hadoop/sbin/yarn-daemon.sh start nodemanager')
            execute(notifymaster, env.host_string)

@roles('master')
def notifymaster(host):
    with cd('/mnt/maxim/hadoop/etc/hadoop/'):
        run('echo \'' + host + '\' >> yarn.include')
        run('/mnt/maxim/hadoop/bin/yarn rmadmin -refreshNodes')

@roles('computenodes')
def removenodes(qty=0):
    if int(qty) > len(computenodes):
        qty = len(computenodes)
        print 'Only ' + str(qty) + ' available compute nodes to stop.'
    with settings(warn_only = True):
        if env.host_string in computenodes[0:int(qty)]:
            execute(decomission, env.host_string)
            run('/mnt/maxim/hadoop/sbin/yarn-daemon.sh stop nodemanager')            

@roles('master')
def decomission(hostname):
    with cd('/mnt/maxim/hadoop/etc/hadoop/'):
        run('sed --in-place \'/' + hostname + '/d\' yarn.include')
        append('yarn.exclude', hostname, use_sudo=False)
        run('/mnt/maxim/hadoop/bin/yarn rmadmin -refreshNodes')
