#!/usr/bin/python2
# Yes, python2
'''
A problem arises when a metalogger is used to recover data after multiple
failovers, this daemon is intended to monitor failovers on the master daemon
and clean up the metaloggers so that residual metalogger data does not cause
problems for the failover sequence.
'''

import os
import sys
import time
import subprocess

def daemonize():
    '''
    Daemonize a process
    '''
    try: 
        pid = os.fork() 
        if pid > 0:
            # exit first parent
            sys.exit(0) 
    except OSError as e:
        sys.exit(1)

    # decouple from parent environment
    os.chdir("/") 
    os.setsid() 
    os.umask(0o22)

    # do second fork
    try: 
        pid = os.fork() 
        if pid > 0:
            # print "Daemon PID %d" % pid 
            sys.exit(0) 
    except OSError as e:
        sys.exit(1) 

    dev_null = open('/dev/null','w') 
    os.dup2(dev_null.fileno(), sys.stdin.fileno()) 
    os.dup2(dev_null.fileno(), sys.stdout.fileno()) 
    os.dup2(dev_null.fileno(), sys.stderr.fileno()) 

def parse_mfsmeta(conf='/etc/mfsmetalogger.cfg'):
    '''
    Parses a key value config file, aka the config files used by MooseFS
    Arguments:
        conf => String - the location of a config file
    Returns:
        dict => The values of the config file
    '''
    ret = {'WORKING_USER': 'nobody',
           'WORKING_GROUP': '',
           'SYSLOG_IDENT': 'mfsmetalogger',
           'LOCK_MEMORY': 0,
           'NICE_LEVEL': -19,
           'DATA_PATH': '/var/lib/mfs',
           'BACK_LOGS': 50,
           'META_DOWNLOAD_FREQ': 24,
           'MASTER_RECONNECTION_DELAY': 5,
           'MASTER_HOST': 'mfsmaster',
           'MASTER_PORT': 9419,
           'MASTER_TIMEOUT': 60}
    if os.path.isfile(conf):
        for line in open(conf, 'r').readlines():
            if line.startswith('#'):
                continue
            if not line.strip():
                continue
            comps = line.split('=')
            if len(comps) < 2:
                continue
            ret[comps[0]] = comps[1]
    return ret


class MetaMan(object):
    '''
    The metaman object is used to manage the mfsmetalogger in failover
    scenarios.
    '''
    def __init__(self):
        self.opts = parse_mfsmeta()
        self.local_macs = self.__local_macs()

    def __local_macs(self):
        '''
        Returns a set of all of the local mac addrs
        '''
        lm_cmd = 'ip a | grep link | grep ether | awk "{print $2}" | sort -u'
        return set(subprocess.Popen(lm_cmd,
            shell=True,
            stdout=subprocess.PIPE).communicate()[0].split())

    def master_macs(self):
        '''
        Returns a set of the mac addrs used on the mfsmaster interface
        '''
        mm_cmd = "arping -c 2 " + self.opts['MASTER_HOST']\
               + " | grep reply | cut -d'[' -f2 | cut -d']' -f1 | sort -u"
        return set(subprocess.Popen(mm_cmd,
            shell=True,
            stdout=subprocess.PIPE).communicate()[0].split())

    def comp_master(self, last_mac):
        '''
        Determines what the present situation is and where the master server
        is. Returns 'restart' if the metalogger needs to be restarted, 'stop' 
        if the metalogger should be stopped, and 'start' it the metalogger
        should be on and unchanged.
        '''
        mmacs = self.master_macs()
        if not last_mac:
            # This is the first run of the loop
            last_mac = mmacs
            if self.i_master(mmacs):
                return ('stop', mmacs)
            return ('restart', mmacs)
        if mmacs == last_mac:
            return ('start', mmacs)
        if self.i_master(mmacs):
            # This is the master, make sure we are stopped
            return ('stop', mmacs)
        if mmacs != last_mac:
            # Woah, the master has changed, clean house!
            return ('restart', mmacs)
        
    def i_master(self, mmacs):
        '''
        Takes the master macs as the argument, returns true if the master is
        the local system.
        '''
        if len(mmacs.union(self.local_macs)) == len(self.local_macs):
            # We is the master!
            return True
        else:
            return False

    def check_logger(self):
        '''
        Querries to see if the mfsmetalogger is running, returns true or false.
        '''
        c_cmd = 'ps aux | grep -v grep | grep mfsmetalogger'
        if not subprocess.call(c_cmd, shell=True):
            return True
        else:
            return False

    def stop(self):
        '''
        Stop the metalogger
        '''
        if self.check_logger():
            l_cmd = 'mfsmetalogger stop'
            subprocess.call(l_cmd, shell=True)

    def start(self):
        '''
        Ensure that the metalogger is running, if it is not running then call
        restart
        '''
        if not self.check_logger():
            self.restart()

    def restart(self):
        '''
        This is the method that this whole dumb script is for, if restart is
        called then we need to clean out the old metalogs and bring up the
        mfsmetalogger fresh so that it has only clean logs!
        '''
        mfs = self.opts['DATA_PATH']
        metalogs = os.path.join(mfs, 'metalogs')
        destdir = os.path.join(metalogs, time.strftime('%Y_%m_%d_%H:%M.%S'))
        if not os.path.isdir(destdir):
            os.makedirs(destdir)
        mv_cmd = 'mv ' + mfs + '/*_ml* ' + destdir
        # Double check that an mfsmetarestore is not running
        while True:
            p_cmd = 'ps aux | grep -v grep | grep mfsmetarestore'
            if not subprocess.call(p_cmd, shell=True):
                time.sleep(5)
            else:
                break
        self.stop()
        subprocess.call(mv_cmd, shell=True)
        subprocess.call('mfsmetalogger start', shell=True)

    def loop(self):
        '''
        This is the main server loop, enjoy!
        '''
        last_mac = None
        while True:
            execute = ''
            ret = None
            while not execute:
                action, mmacs = self.comp_master(last_mac)
                if action == ret:
                    execute = action
                time.sleep(10)
                ret = action
            getattr(self, execute)()
            last_mac = mmacs
            time.sleep(10) # Once this is stable change these sleeps to 60+


if __name__ == '__main__':
    metaman = MetaMan()
    daemonize()
    metaman.loop()




