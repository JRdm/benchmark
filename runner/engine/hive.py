import engine
from optparse import OptionGroup

class HiveEngine(engine.Engine):
  name = "Hive"
  
  @classmethod
  def parser_options(cls, parser):
    opt_group = OptionGroup(parser, "Hive")
    opt_group.add_option("--hive", action="store_true", default=False,
      help="Test Hive (without Tez)")
    opt_group.add_option("--hive-identity-file",
      help="SSH private key file to use for logging into Hive node")
    opt_group.add_option("--hive-host",
      help="Hostname of Hive master node")
    opt_group.add_option("--hive-slaves",
      help="Comma separated list of Hive slaves")
    parser.add_option_group(opt_group)

  @classmethod  
  def is_enabled(cls, opts):
    return opts.hive

  def is_format_supported(self, tbl_fmt):
    return tbl_fmt in [ 'parquet', 'orcfile', 'rcfile']

  def __init__(self, opts):
    if (opts.hive_identity_file is None or 
        opts.hive_host is None or
        opts.hive_slaves is None or
        opts.aws_key_id is None or
        opts.aws_key is None):
      raise Exception("Hive requires identity file, hostname, slaves and AWS creds")
    self.identity_file = opts.hive_identity_file
    self.host = opts.hive_host
    self.aws_key_id = opts.aws_key_id
    self.aws_key = opts.aws_key
    self.username = "root"

  def setup_env(self):
    self.add_aws_credentials("/etc/hadoop/conf/core-site.xml")
