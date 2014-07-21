import os, subprocess, json

# Run a command on a host through ssh, retrying up to two times
# and then throwing an exception if ssh continues to fail.
def ssh(host, OPTS, command, stdin=open(os.devnull, 'w')):
  command = command.replace('\n', ' ')
  cmd = "ssh -t -t -o StrictHostKeyChecking=no -i %s %s@%s '%s'" % ('warehouse-benchmark.pem', 'root', host, command)
  print cmd
  tries = 0
  while True:
    try:
      return subprocess.check_call(
        cmd, shell=True, stdin=stdin)
    except subprocess.CalledProcessError as e:
      if (tries > 2):
        raise e
      print "Couldn't connect to host {0}, waiting 30 seconds".format(e)
      time.sleep(30)
      tries = tries + 1

# Setup the Ambari Slave and start the ambari server
def setup_ambari_slave(slave, ambari, OPTS):
  cmd = """
        wget http://public-repo-1.hortonworks.com/ambari/centos6/1.x/updates/1.4.1.25/ambari.repo;
        cp ambari.repo /etc/yum.repos.d;
        yum -y install epel-release;
        yum -y repolist;
        yum -y install ambari-agent;
        sed -i "/^hostname/ s/.*/hostname=%s/" /etc/ambari-agent/conf/ambari-agent.ini;
        ambari-agent start;
        """ % (ambari['public_dns_name'])
  ssh(slave['public_dns_name'], OPTS, cmd, stdin=None)

def setup_ambari_master(ambari):
  ssh(ambari, {}, 'echo n > resp; echo 1 >> resp; echo y >> resp; echo n>> resp; ambari-setup << resp')

#ambari = { 'public_dns_name' : 'ec2-54-237-165-90.compute-1.amazonaws.com'}
#slaves = [ {'public_dns_name':'ec2-54-89-79-133.compute-1.amazonaws.com', 'public_dns_name':'ec2-54-87-84-95.compute-1.amazonaws.com'} ]
#for s in slaves:
#  setup_ambari_slave(s, ambari, {})
slaves = [ 'ip-10-100-242-203.ec2.internal' ]
master = 'ip-10-40-222-174.ec2.internal'

print json.dumps ({"blueprint":"multi-node-hdfs-yarn", "host_groups":[{"name":"master", "hosts":[{"fqdn":master}]}, {"name":"slaves", "hosts":[{'fqdn':f} for f in slaves]}]})
