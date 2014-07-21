import requests
import json, os, subprocess, time

ambari = 'ec2-54-82-176-190.compute-1.amazonaws.com' 
master = 'ip-10-118-193-172.ec2.internal'
slaves = ['ip-10-77-1-179.ec2.internal']

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


# Setup the Ambari Master and start the ambari server
def setup_ambari_master(ambari, OPTS):
  cmd = """
        wget http://public-repo-1.hortonworks.com/ambari/centos6/1.x/updates/1.6.0/ambari.repo;
        cp ambari.repo /etc/yum.repos.d;
        yum -y install epel-release;
        yum -y repolist;
        yum -y install ambari-server;
        echo n > heredoc;
        echo 1 >> heredoc;
        echo y >> heredoc;
        echo n >> heredoc;
        ambari-server setup <heredoc;
        rm heredoc;
        ambari-server start;
        ambari-server status;
        """
  ssh(ambari, OPTS, cmd, stdin=None)

def ambari_get(host, path):
  return requests.get("http://%s:8080%s" % (host, path), auth=('admin', 'admin'), headers={"X-Requested-By":'Pythian'})

def ambari_post(host, path, body):
  return requests.post("http://%s:8080%s" % (host, path), auth=('admin','admin'), data=body, headers={"X-Requested-By":'Pythian'})

def wait_for_ambari_master(ambari):
  print "Waiting for Ambari master to start"
  while True:
    try:
      r = ambari_get(ambari, '/api/v1/clusters')
      return
    except Exception, e: 
      print "Server unavailable, waiting 10 seconds"
      print e
      time.sleep(10) 

def upload_ambari_blueprint(ambari, master, slaves):

  with open('blueprint-5.json') as f:
    doc = json.loads(f.read())
    for g in doc['host_groups']:
      if g['name'] == 'slaves':
        g['cardinality'] = len(slaves)
  
  print ambari_post(ambari, '/api/v1/blueprints/hadoop-benchmark', json.dumps(doc)) 
  blueprint_hosts = json.dumps ({"blueprint":"hadoop-benchmark", "host_groups":[{"name":"master", "hosts":[{"fqdn":master}]}, {"name":"slaves", "hosts":[{'fqdn':f} for f in slaves]}]})
  print blueprint_hosts
  r=ambari_post(ambari, '/api/v1/clusters/hive-bench-test-2', blueprint_hosts)
  print r.text

wait_for_ambari_master(ambari)
upload_ambari_blueprint(ambari, master, slaves)
