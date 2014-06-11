import engine

class ImpalaEngine(engine.Engine):
  name = "Impala"

  @classmethod  
  def parser_options(cls, parser):
    parser.add_option("--impala", action="store_true", default=False,
      help="Whether to include Impala")
    parser.add_option("--impala-host",
      help="Hostname of Impala master node")
    parser.add_option("--impala-identity-file",
      help="SSH private key file to use for logging into Shark node")

  @classmethod  
  def is_enabled(cls, opts):
    return opts.impala
 
  def __init__(self, opts):
    if (opts.impala_identity_file is None or 
        opts.impala_host is None or
        opts.aws_key_id is None or
        opts.aws_key is None):
      raise Exception("Impala requires identity file, hostname, and AWS creds")
    self.identity_file = opts.impala_identity_file
    self.host = opts.impala_host
    self.aws_key_id = opts.aws_key_id
    self.aws_key = opts.aws_key
    self.username = "ubuntu"

  def setup_env(self):
    self.ssh("sudo chmod 777 /etc/hadoop/conf/hdfs-site.xml") 
    self.ssh("sudo chmod 777 /etc/hadoop/conf/core-site.xml")
    self._add_aws_credentials("/etc/hadoop/conf/hdfs-site.xml")
    self._add_aws_credentials("/etc/hadoop/conf/core-site.xml")

  def is_format_supported(self, tbl_fmt):
    return tbl_fmt in [ 'parquet', 'rc']

  def run_hql(self, query):
    self.ssh("sudo -u hdfs hive -e \"%s\"")  

  def copy_from_s3(self, source, target):
    self.ssh("sudo -u hdfs hadoop distcp s3n://%s %s" % (source, target))

  def copy_from_disk(self, source, target):
    self.ssh("sudo -u hdfs hadoop fs -put %s %s" % (source, target))
