import os, subprocess

class Engine(object):
  """
  Provides some generic functions for working with the master (where queries are sent)
  and the set of all slave nodes. Any specific engine should subclass this.
  """
  name = "GenericEngine"
  LOCAL_TMP_DIR = "/tmp"

  def ssh_master(self, command, flags='-t'):
    subprocess.check_call(
     "ssh %s -o StrictHostKeyChecking=no -i %s %s@%s '%s'" %
     (flags, self.identity_file, self.username, self.host, command), shell=True) 

  def ssh_slaves(self, command, flags='-t'):
    for s in self.slaves:
      subprocess.check_call(
       "ssh %s -o StrictHostKeyChecking=no -i %s %s@%s '%s'" %
       (flags, self.identity_file, self.username, s, command), shell=True) 

  def scp_to(self, host, local_file, remote_file):
    subprocess.check_call(
      "scp -q -o StrictHostKeyChecking=no -i %s '%s' '%s@%s:%s'" %
      (self.identity_file, local_file, self.username, host, remote_file), shell=True)

  def scp_to_master(self, local_file, remote_file):
     self.scp_to(self.host, local_file, remote_file)

  def scp_to_slaves(self, local_file, remote_file):
     for s in self.slaves:
       self.scp_to(s, local_file, remote_file)

  def scp_from(self, remote_file, local_file):
    subprocess.check_call(
      "scp -q -o StrictHostKeyChecking=no -i %s '%s@%s:%s' '%s'" %
      (self.identity_file, self.username, self.host, remote_file, local_file), shell=True)

  def add_aws_credentials_host(self, host, remote_xml_file):
    local_xml = os.path.join(self.LOCAL_TMP_DIR, "temp.xml")
    self.scp_from(remote_xml_file, local_xml)
    lines = open(local_xml).readlines()
    # Manual XML munging... this makes me cry a little bit
    lines = filter(lambda x: "configuration" not in x and "xml" not in x 
                             and "fs.s3n" not in x, lines)
    lines = map(lambda x: x.strip(), lines)
    key_conf = "<property><name>fs.s3n.awsAccessKeyId</name>" \
      + ("<value>%s</value>" % self.aws_key_id) + "</property><property>" \
      + "<name>fs.s3n.awsSecretAccessKey</name>" \
      + ("<value>%s</value>" % self.aws_key) + "</property>" 
    lines.insert(0, "<configuration>")
    lines.append(key_conf)
    lines.append("</configuration>")
    out = open(local_xml, 'w')
    for l in lines:
      print >> out, l
    out.close()
    self.scp_to(host, local_xml, remote_xml_file) 

  def add_aws_credentials(self, remote_xml_file):
    self.add_aws_credentials_host(self.host, remote_xml_file)
    for s in self.slaves:
      self.add_aws_credentials_host(self.host, remote_xml_file)

  def mkdir(self, path):
    try:
      self.ssh_master("sudo -u hdfs hadoop fs -mkdir %s" % path)
    except Exception:
      pass

  def run_hql(self, query):
    query = query.replace("\n", "")
    self.ssh_master("cd /tmp; sudo -u hdfs hive -e \"%s\"" % query)

  def copy_from_s3(self, source, target):
    self.ssh_master("sudo -u hdfs hadoop distcp s3n://%s %s" %(source, target))

  def copy_from_disk(self, source, target):
    self.ssh_master("sudo -u hdfs hadoop fs -put %s %s" %(source, target))

