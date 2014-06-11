import engine

class SharkEngine(engine.Engine):
  name = "Shark"

  @classmethod
  def parser_options(cls, parser):
    parser.add_option("--shark", action="store_true", default=False,
      help="Whether to include Shark")
    parser.add_option("--shark-host",
      help="Hostname of Shark master node")
    parser.add_option("--shark-identity-file",
      help="SSH private key file to use for logging into Shark node")

  @classmethod  
  def is_enabled(cls, opts):
    return opts.shark

  def is_format_supported(self, tbl_fmt):
    return tbl_fmt in [ 'parquet', 'orc', 'rc']

  def __init__ (self, opts):
    if (opts.shark_identity_file is None or 
        opts.shark_host is None or
        opts.aws_key_id is None or
        opts.aws_key is None): 
      raise Exception("Shark requires identity file, shark hostname, and AWS credentials")
    self.identity_file = opts.shark_identity_file
    self.host = opts.shark_host
    self.aws_key_id = opts.aws_key_id
    self.aws_key = opts.aws_key
    self.username = "root"

  def setup_env(self):
    self.add_aws_credentials("/root/mapreduce/conf/core-site.xml")
    self.ssh("/root/mapreduce/bin/start-mapred.sh")   

  def run_hql(self, query):
    self.ssh("/root/shark/bin/shark -e \"%s\"")

  def copy_from_s3(self, source, target):
    self.ssh("/root/mapreduce/bin/hadoop distcp s3n://%s %s" % (source, target))

  def copy_from_disk(self, source, target):
    self.ssh("/root/mapreduce/bin/hadoop fs -put %s %s" % (source, target))
 
  def mkdir(self, directory):
    try:
      self.ssh("/root/ephemeral-hdfs/bin/hdfs dfs -mkdir %s" % directory)
    except Exception:
      pass
