import engine
from optparse import OptionGroup

class ImpalaEngine(engine.Engine):
  name = "Impala"

  @classmethod  
  def parser_options(cls, parser):
    opt_group = OptionGroup(parser, "Impala")
    opt_group.add_option("--impala", action="store_true", default=False,
      help="Benchmark Impala")
    opt_group.add_option("--impala-host",
      help="Hostname of Impala master node")
    opt_group.add_option("--impala-slaves",
      help="Comma separated list of Impala slave nodes")
    opt_group.add_option("--impala-identity-file",
      help="SSH private key file to use for logging into Impala node")
    parser.add_option_group(opt_group) 

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
    self.slaves = opts.impala_slaves.split(',')
    self.username = "root"

  def setup_env(self):
    self.add_aws_credentials("/etc/hadoop/conf/hdfs-site.xml")
    self.add_aws_credentials("/etc/hadoop/conf/core-site.xml")
   

  def is_format_supported(self, tbl_fmt):
    return tbl_fmt in [ 'parquet', 'rcfile']

  def run_hql(self, query):
    self.ssh_master("sudo -u hdfs hive -e \"%s\"")  

  def copy_from_s3(self, source, target):
    self.ssh_master("sudo -u hdfs hadoop distcp s3n://%s %s" % (source, target))

  def copy_from_disk(self, source, target):
    self.ssh_master("sudo -u hdfs hadoop fs -put %s %s" % (source, target))
