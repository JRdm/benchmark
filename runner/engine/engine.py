import os, subprocess

class Engine(object):
  name = "GenericEngine"
  LOCAL_TMP_DIR = "/tmp"

  def ssh(self, command):
    subprocess.check_call(
     "ssh -t -o StrictHostKeyChecking=no -i %s %s@%s '%s'" %
     (self.identity_file, self.username, self.host, command), shell=True) 

  def scp_to(self, local_file, remote_file):
    subprocess.check_call(
      "scp -q -o StrictHostKeyChecking=no -i %s '%s' '%s@%s:%s'" %
      (self.identity_file, local_file, self.username, self.host, remote_file), shell=True)

  def scp_from(self, remote_file, local_file):
    subprocess.check_call(
      "scp -q -o StrictHostKeyChecking=no -i %s '%s@%s:%s' '%s'" %
      (self.identity_file, self.username, self.host, remote_file, local_file), shell=True)

  def add_aws_credentials(self, remote_xml_file):
    local_xml = os.path.join(self.LOCAL_TMP_DIR, "temp.xml")
    self.scp_from(remote_xml_file, local_xml)
    lines = open(local_xml).readlines()
    # Manual XML munging... this makes me cry a little bit
    lines = filter(lambda x: "configuration" not in x and "xml" not in x 
                             and "fs.s3" not in x, lines)
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
    self.scp_to(local_xml, remote_xml_file) 

  def mkdir(self, path):
    try:
      self.ssh("sudo -u hdfs hadoop fs -mkdir %s" % path)
    except Exception:
      pass

  def run_hql(self, query):
    query = query.replace("\n", "")
    self.ssh("cd /tmp; sudo -u hdfs hive -e \"%s\"" % query)

  def copy_from_s3(self, source, target):
    self.ssh("sudo -u hdfs hadoop distcp s3n://%s %s" %(source, target))

  def copy_from_disk(self, source, target):
    self.ssh("sudo -u hdfs hadoop fs -put %s %s" %(source, target))

