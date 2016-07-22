# -*- coding: utf-8 -*-

# postgresql library
import psycopg2 as psy
import sys
import subprocess
from time import sleep
from time import time
"""

    Script for checking failures in postgresql instances
"""
# database name and database user, common to both databases
db = "tpcw"
user = "tpcw_user"

# checking argv size
if len(sys.argv) != 2:
    print "Usage: python %s <sleeptime>" % sys.argv[0]
    print "<sleeptime>: time to check db connections (in secs)"
    sys.exit()

sleeptime = float(sys.argv[1])
print "Sleeptime set to %ss" % sleeptime


""" function to check db status 
    if database is online, return recovery status
    return -1 if there's no connection
    return 0 if db is slave
    return 1 if db is master
"""
def check_db_status(addr, port, db, user):
    conn = None
    try:
        conn = psy.connect(database=db, user=user, host=addr, port=port)

    except psy.DatabaseError, e:
        print "No database connection in %s" % addr
        return -1

    finally:
        if conn:
            cursor = conn.cursor()
            cursor.execute("SELECT pg_is_in_recovery()")
            is_in_recovery = cursor.fetchone()
            # fetchone returns a tuple, and we need the first element
            is_in_recovery = is_in_recovery[0]
            is_master = None
            if is_in_recovery == True:
                is_master = 0
            else:
                is_master = 1 
            conn.close()
            return is_master

""" Promote slave to master and put master in slave mode after a failure """
def promote(master, master_port, slave, slave_port):
        print "Master (%s) is down!" % master
        sshOne = "ssh " + master + " "
        sshTwo = "ssh " + slave  + " "
        etc_dir = '/etc/postgresql/9.5/grupo04/'
        var_dir = '/var/lib/postgresql/9.5/grupo04/'

        promote_time_start = time()
	print "Promoting %s to Master" % slave
        subprocess.check_call(sshTwo+"pg_ctlcluster 9.5 grupo04 promote", shell=True)
        
        # wait until new db is promoted..
 		
	print "Waiting until %s is promoted.." % slave
        while check_db_status(slave, slave_port, db, user) != 1:
            pass
	promote_time_end = time()
	total_time = promote_time_end - promote_time_start
	print "Time to promote: %ds" % total_time	 
        reload_haproxy_cfg(slave)
	
	subprocess.call(['ssh', master,'sh', 'start.sh'])

        """
        subprocess.check_call(sshOne+"cp "+ etc_dir + 'recovery.conf ' + var_dir, shell=True)

	print "wait until %s is really off" % master
	while check_db_status(master, master_port, db, user) != -1:
	    print "Still on.."
	    pass
        
        print "Rewind %s" % master
	pg_rewind = '/usr/lib/postgresql/9.5/bin/pg_rewind'
        source_server = '--source-server="host='+slave+' port='+slave_port+' user=tpcw_user dbname=tpcw"'
        subprocess.check_call(['ssh', master, pg_rewind, '-D', var_dir, source_server])

        print "Turning on database in %s" % master
        subprocess.check_call(sshOne+"pg_ctlcluster 9.5 grupo04 start", shell=True)
	"""

""" Reload haproxy config with master turning into slave and vice versa """
def reload_haproxy_cfg(newMaster):
    haproxy_cfg_path = '/etc/haproxy/haproxy.cfg'
    haproxy_pid_dir = '/var/run/haproxy.pid'
    haproxy_pid = subprocess.check_output(["pidof", "haproxy"]).rstrip()
    replaceStr = None
    replaceStrBackup = None
    if newMaster == '10.1.1.10':
        replaceStr = r's/replicaTwo  10.1.1.20:5435/replicaOne  10.1.1.10:5436/'
        replaceStrBackup = r's/replicaOne  10.1.1.10:5436 backup/replicaTwo  10.1.1.20:5435 backup/'
    elif newMaster == '10.1.1.20':
        replaceStr = r's/replicaOne  10.1.1.10:5436/replicaTwo  10.1.1.20:5435/'
        replaceStrBackup = r's/replicaTwo  10.1.1.20:5435 backup/replicaOne  10.1.1.10:5436 backup/'
    #sed -i 's/replicaTwo  10.1.1.20:5435 backup/replicaOne  10.1.1.10:5436 backup/' /etc/haproxy/haproxy.cfg
    subprocess.check_call(['sudo','sed','-i',replaceStr,haproxy_cfg_path])
    subprocess.check_call(['sudo','sed','-i',replaceStrBackup,haproxy_cfg_path])
    print "Reloading HAproxy cfg.."
    subprocess.check_call(['sudo','haproxy','-f',haproxy_cfg_path,'-p',haproxy_pid_dir,'-sf',haproxy_pid])

# database connections
replicaOne_conn = None
replicaTwo_conn = None

# database addresses and ports
replicaOne_addr = "10.1.1.10"
replicaOne_port = "5436"

replicaTwo_addr = "10.1.1.20"
replicaTwo_port = "5435"

while True:
    sleep(sleeptime)
    replicaOne_status = check_db_status(replicaOne_addr, replicaOne_port, db, user)
    replicaTwo_status = check_db_status(replicaTwo_addr, replicaTwo_port, db, user)

# check if replicaOne is online and replicaTwo is off
# if replicaOne is slave, promote it to master
# rewind Two to One state and demote Two to slave

    if replicaOne_status == -1 and replicaTwo_status == 0:
        promote(replicaOne_addr, replicaOne_port, replicaTwo_addr,
                replicaTwo_port)

    elif replicaOne_status  == 0 and replicaTwo_status == -1:
        promote(replicaTwo_addr, replicaTwo_port, replicaOne_addr,
                replicaOne_port)
