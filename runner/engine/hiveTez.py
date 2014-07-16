import engine
from optparse import OptionGroup

class HiveTezEngine(engine.Engine):
  name = "Hive-Tez"
  
  @classmethod
  def parser_options(cls, parser):
    opt_group = OptionGroup(parser, "Hive-Tez")
    opt_group.add_option("--hive-tez", action="store_true", default=False,
      help="Benchmark Hive-on-Tez")
    opt_group.add_option("--hive-tez-identity-file",
      help="SSH private key file to use for logging into Hive Tez node")
    opt_group.add_option("--hive-tez-host",
      help="Hostname of Hive Tez master node")
    opt_group.add_option("--hive-tez-slaves",
      help="Comma separated list of Hive Tez slaves")
    parser.add_option_group(opt_group)

  @classmethod  
  def is_enabled(cls, opts):
    return opts.hive_tez

  def is_format_supported(self, tbl_fmt):
    return tbl_fmt in [ 'parquet', 'orcfile', 'rcfile']

  def __init__(self, opts):
    if (opts.hive_tez_identity_file is None or 
        opts.hive_tez_host is None or
        opts.aws_key_id is None or
        opts.aws_key is None):
      raise Exception("Hive-Tez requires identity file, hostname, slaves and AWS creds")
    self.identity_file = opts.hive_tez_identity_file
    self.host = opts.hive_tez_host
    self.aws_key_id = opts.aws_key_id
    self.aws_key = opts.aws_key
    self.slaves = opts.hive_tez_slaves.split(',')
    self.username = "root"

  def setup_env(self):
    self.add_aws_credentials('/etc/hadoop/conf/core-site.xml')

  def run_bench_query(self, script_file, executions):
    results = []
    self.scp_to(script_file, "/tmp/bench_query.hql") 
    for f in xrange(executions):
      self.ssh_master('HIVE_HOME=/opt/apache-hive-0.13.0.2.1.0.0-92-bin HIVE_CONF_DIR=$HIVE_HOME/conf PATH=$HIVE_HOME/bin:$PATH HADOOP_CLASSPATH=/opt/tez-0.2.0.2.1.0.0-92/*:/opt/tez-0.2.0.2.1.0.0-92/lib/* HADOOP_USER_CLASSPATH_FIRST=true HADOOP_USER_NAME=hdfs /opt/apache-hive-0.13.0.2.1.0.0-92-bin/bin/hive -i /root/benchmark/runner/tez/Stinger-Preview-Quickstart/configs/stinger.settings -hiveconf hive.optimize.tez=true -f /tmp/bench_query.hql 2>&1 | grep "Time taken" | sed "s/Time taken:\([0-9.]*\) seconds/\\\\1/" >> /tmp/result.csv')
      self.ssh_slaves('sudo sync && sudo echo 3 > /proc/sys/vm/drop_caches')
    self.scp_from("result.csv", "/tmp/result.csv")
    self.ssh_master("rm /tmp/result.csv")
    with open("result.csv") as f:
      for line in f:
         print line 
