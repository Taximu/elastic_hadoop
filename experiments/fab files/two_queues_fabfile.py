import time
import logging
from fabric.api import *
from fabric.context_managers import cd
from fabric.operations import run, put, env
from fabric.decorators import hosts, parallel, serial
from fabric.contrib.files import append, sed, comment

configpath = '/home/gorbunovmaksim/Documents/ElasticHadoop/experiments/monitoring_tools'
yarnapipath = configpath + '/yarnapiclient.py'

env.keepalive = True

env.roledefs = {
    'hypervisor':  ['gorbunovmaksim@gorbunovmaksim'],
    'master':      ['maxim@stream3'],
    'datanodes':   ['maxim@stream2', 'maxim@stream4'],
    'workers':     ['maxim@stream7', 'maxim@stream8', 'maxim@stream9', 'maxim@stream10'],
    'redisserver': ['maxim@stream1'],
    'logmachine':  ['maxim@stream1'],
    'jobhistory':  ['maxim@stream2']
}

experiments = {
     'expm1':     ['maxim@stream2', 'maxim@stream4', 'maxim@stream7'],
     'expm2':     ['maxim@stream2', 'maxim@stream4', 'maxim@stream7', 'maxim@stream8'],
     'expm3':     ['maxim@stream2', 'maxim@stream4', 'maxim@stream7', 'maxim@stream8', 'maxim@stream9'],
     'expm4':     ['maxim@stream2', 'maxim@stream4', 'maxim@stream7', 'maxim@stream8', 'maxim@stream9', 'maxim@stream10']
}

benchmarks = ['wordcount', 'sort', 'pagerank']

path = '/home/gorbunovmaksim/Documents/Elastic Hadoop/experiments/hadoop_configs/QUEUES/Two queues/'


@roles('redisserver')
def clearredis():
    run('redis-cli flushall')

@roles('redisserver')
def startredis():
    sudo('sudo service redis-server start')

def putconfigpath():
    put(configpath + '/*', '/tmp/')

@roles('master')
def putyarnapiclient():
    put(yarnapipath, '/tmp/')

@roles('master', 'datanodes', 'workers', 'redisserver', 'logmachine')
def uploadmonitor(expm):
    if isSlaveInExpm(expm):
        putconfigpath() 
    elif env.host_string in env.roledefs['master']:
        putconfigpath()
        putyarnapiclient()
    elif env.host_string in env.roledefs['redisserver']:
        putconfigpath()
    elif env.host_string in env.roledefs['logmachine']:
        putconfigpath()

def startscript():
    run('nohup python /tmp/DoLen.py --source vm  --cloud 0 --vms ' + env.host + ' --redisserver ' + env.roledefs['redisserver'][0].split('@')[1] +' >& /dev/null < /dev/null &', pty = False)
      
@roles('master', 'workers', 'datanodes', 'redisserver')
def startmonitor(expm):
    if env.host_string in env.roledefs['master']:
        startscript()
    elif isSlaveInExpm(expm):
        startscript()
    elif env.host_string in env.roledefs['redisserver']:
        startscript()

def killscript():
    with settings(warn_only = True):
        run("pkill -9 -f DoLen.py")
        run("pkill -9 -f 'sar -u -r -d -p -S -n DEV 1'")

@roles('master', 'workers', 'datanodes', 'redisserver')
def stopmonitor(expm):
    if env.host_string in env.roledefs['master']:
        killscript()
    elif isSlaveInExpm(expm):
        killscript()
    elif env.host_string in env.roledefs['redisserver']:
        killscript()

@roles('redisserver')
def dumpStats(dumpfile = '/tmp/stats'):
    run('python '+ '/tmp/controller.py --mode dump --dumpfile ' + dumpfile + ' --redisserver localhost')


@roles('master', 'logmachine')
def copydumpfiles(expm, bench):
    if env.host_string in env.roledefs['logmachine']:
        run('cp /mnt/maxim/stats ' + '/home/maxim/expms/' + expm + '.' + bench + '.' + 'stats')
    elif env.host_string in env.roledefs['master']: 
        run('python '+ '/tmp/' +'yarnapiclient.py --mode dump --dumpfile /mnt/maxim/exps/' + expm + '.' + bench + '.stat.perNM')

####################EXPERIMENT####################
def runexpm():
    with settings(warn_only = True):
        path = '/mnt/maxim/'
        file = 'stats'
        dumpfile = path + file
        for bench in benchmarks:
            for expm in sorted(experiments.keys()):
                print '\n@@@@@@@@[' + expm + ']@@@@@@@@'
                print '=========================================================\n'
		execute(putconfigs, expm)
                execute(uploadmonitor, expm)
                execute(startnodes, expm)
                execute(checkjps)
                execute(stopsafemode)
                execute(startmonitor, expm)
                execute(runbench, bench)
                execute(stopmonitor, expm)
                execute(dumpStats, dumpfile)
                execute(copydumpfiles, expm, bench)
                execute(clearredis)
                execute(clearlogs)
                execute(stopnodes, expm)
                execute(destroycluster)
                print '=========================================================\n'
                time.sleep(10)

@roles('workers', 'datanodes')
def isSlaveInExpm(expm):
    if env.host_string in experiments[expm]:
        return True

def startnodes(expm):
    execute(startmaster)
    execute(startworkers, expm)

@roles('master')
def startmaster():
    run('$HADOOP_HOME/sbin/hadoop-daemon.sh start namenode')
    run('$HADOOP_HOME/sbin/yarn-daemon.sh start resourcemanager')
    run('$HADOOP_HOME/sbin/hadoop-daemons.sh start datanode')
    run('$HADOOP_HOME/sbin/mr-jobhistory-daemon.sh start historyserver')

@roles('workers')
def startworkers(expm):
    if isSlaveInExpm(expm):
            run('$HADOOP_HOME/sbin/yarn-daemon.sh start nodemanager')

@roles('master')
def stopsafemode():
    run('$HADOOP_HOME/bin/hadoop dfsadmin -safemode leave')

@roles('master')
def runbench(bench):
    with cd('/mnt/maxim/HiBench/'):
        run('./' + bench + '/bin/run.sh', pty = False)

def stopnodes(expm):
    execute(stopworkers, expm)
    execute(stopmaster)

@roles('workers')
def stopworkers(expm):
    if isSlaveInExpm(expm):
        run('$HADOOP_HOME/sbin/yarn-daemon.sh stop nodemanager')

@roles('master')
def stopmaster():
    run('$HADOOP_HOME/sbin/mr-jobhistory-daemon.sh stop historyserver')
    run('$HADOOP_HOME/sbin/yarn-daemon.sh stop resourcemanager')
    run('$HADOOP_HOME/sbin/hadoop-daemons.sh stop datanode')
    run('$HADOOP_HOME/sbin/hadoop-daemon.sh stop namenode')

###########DOLEN MANAGEMENT#####################################
@roles('master', 'datanodes', 'workers', 'redisserver')
def installpackages():
    with settings(warn_only = True):
        sudo("apt-get install -y dstat")
        sudo("apt-get install -y sysstat")   
        #sudo("apt-get install -y python-pysnmp4")
        sudo("apt-get install -y ntp")   
        sudo('apt-get install -y python-pip')
        sudo('pip install redis')

@roles('redisserver')
def installredis():
    sudo('apt-get install -y redis-server')
    comment('/etc/redis/redis.conf', 'bind 127.0.0.1', use_sudo = True)
    append('/etc/redis/redis.conf', 'bind 0.0.0.0', use_sudo = True)
    sudo('sudo service redis-server restart')

@roles('master')
def installyarnclientapi():
    run('pip install yarn-api-client')
    
def preparemonitorenv():
    execute(installpackages)
    execute(installredis)

###HIBENCH###
@roles('master')
def setyarnvars():
    shell_file = ['/home/maxim/.profile', '/home/maxim/.bashrc']
    append(shell_file[0], 'export JAVA_HOME=/usr/lib/jvm/java-7-openjdk-amd64', use_sudo = False)
    append(shell_file[0], 'export HADOOP_HOME=/mnt/maxim/hadoop', use_sudo = False)
    append(shell_file[0], 'export HADOOP_EXECUTABLE=$HADOOP_HOME/bin/hadoop', use_sudo = False)
    append(shell_file[0], 'export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop', use_sudo = False)
    append(shell_file[0], 'export HADOOP_EXAMPLES_JAR=$HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples*.jar', use_sudo = False)
    append(shell_file[0], 'export MAPRED_EXECUTABLE=$HADOOP_HOME/bin/mapred', use_sudo = False)
    append(shell_file[0], 'export HADOOP_JOBCLIENT_TESTS_JAR=$HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-2.5.1-tests.jar', use_sudo = False)
    append(shell_file[0], 'export HADOOP_MAPRED_HOME=$HADOOP_HOME', use_sudo = False)
    append(shell_file[0], 'export HADOOP_VERSION=hadoop2', use_sudo = False)
    append(shell_file[0], 'export PATH=$PATH:$HADOOP_HOME', use_sudo = False)
    append(shell_file[0], 'export PATH=$PATH:$HADOOP_HOME/bin', use_sudo = False)

@roles('master', 'workers', 'datanodes')
def putconfigs(expm):
    hadoop_config_path = '/mnt/maxim/hadoop/etc/hadoop/'
    if isSlaveInExpm(expm):
            put(path + '/*', hadoop_config_path)
    elif env.host_string in env.roledefs['master']:
        put(path +'/*', hadoop_config_path)

###REBORN HADOOP###
@roles('master', 'workers', 'datanodes')
def destroycluster():
    with settings(warn_only = True):
        run("pkill -f 'java'")

@roles('master', 'workers', 'datanodes')
def clearlogs():
    with settings(warn_only = True):
        with cd('$HADOOP_HOME'):
            run('rm -rf logs/*')
            run('rm -rf /tmp/hadoop*')
            if env.host_string in env.roledefs['jobhistory']:
                run('bin/hdfs dfs -rm -r /tmp/hadoop-yarn/staging/history/*')

@roles('master')
def formatnode():
    namenode_path = '/mnt/maxim/hadoop/hdfs/namenode'
    with settings(warn_only = True):
        with settings(prompts = {'Re-format filesystem in Storage Directory ' + namenode_path + ' ? (Y or N) ': 'Y'}):
            run('hdfs namenode -format')

@roles('master', 'workers', 'datanodes')
def checkjps():
    with settings(warn_only = True):
        run('jps')

@roles('master')
def preparebench():
    for bench in benchmarks:
        with cd('/mnt/maxim/HiBench/'):
            run('./' + bench + '/bin/prepare.sh')
            print '=============================================================================\n'
