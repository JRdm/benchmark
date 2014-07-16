import engine
from optparse import OptionGroup

class SharkEngine(engine.Engine):
  name = "Shark"
  
  @classmethod
  def parser_options(cls, parser):
    opt_group = OptionGroup(parser, "Shark")
    opt_group.add_option("--shark", action="store_true", default=False,
      help="Benchmark Shark")
    opt_group.add_option("--shark-identity-file",
      help="SSH private key file to use for logging into Shark node")
    opt_group.add_option("--shark-host",
      help="Hostname of Shark master node")
    opt_group.add_option("--shark-slaves",
      help="Comma separated list of Shark slaves")
    parser.add_option_group(opt_group)

  @classmethod  
  def is_enabled(cls, opts):
    return opts.shark

  def is_format_supported(self, tbl_fmt):
    return tbl_fmt in [ 'parquet', 'orcfile', 'rcfile']

  def __init__(self, opts):
    if (opts.shark_identity_file is None or 
        opts.shark_host is None or
        opts.shark_slaves is None or
        opts.aws_key_id is None or
        opts.aws_key is None):
      raise Exception("Shark requires identity file, hostname, slaves and AWS creds")
    self.identity_file = opts.shark_identity_file
    self.host = opts.shark_host
    self.aws_key_id = opts.aws_key_id
    self.aws_key = opts.aws_key
    slef.slaves = opts.shark_slaves.split(',')
    self.username = "root"

  def setup_env(self):
    cmd = """
    wget http://public-repo-1.hortonworks.com/spark/centos6/rpms/spark-core-0.9.1.2.1.1.0-22.el6.noarch.rpm;
    rpm -i spark-core-0.9.1.2.1.1.0-22.el6.noarch.rpm;
 
    cd /tmp;
    wget https://s3.amazonaws.com/spark-related-packages/shark-0.9.1-bin-hadoop2.tgz;
    tar xvf shark-0.9.1-bin-hadoop2.tgz;
    cd shark-0.9.1-bin-hadoop2.tgz;
    sed -i SharkBuild.scala 's/DEFAULT_HADOOP_VERSION = \\"1.0.4\\"/DEFAULT_HADOOP_VERSION = \\"2.4.0\\"/';
    sbt/sbt assembly;
    cp /etc/hadoop/conf/* conf/;
    cp /etc/hive/conf/* conf/;
    cp /usr/lib/spark/lib/spark-yarn_2.10-0.9.1.2.1.1.0-22.jar lib_managed/jars/org.apache.spark/spark-core_2.10/;
    mv conf/shark-env.sh.template conf/shark-env.sh
    echo 'export SHARK_EXEC_MODE=yarn' >> conf/shark-env.sh;
    echo 'export SPARK_ASSEMBLY_JAR=\\"/usr/lib/spark/lib/spark-assembly_2.10-0.9.1.2.1.1.0-22-hadoop2.4.0.2.1.1.0-385.jar\\"' >> conf/shark-env.sh;
    echo 'export SHARK_ASSEMBLY_JAR=\\"/tmp/shark-0.9.1-bin-hadoop2/target/scala-2.10/shark-assembly-0.9.1-hadoop2.2.0.jar\\"' >> conf/shark-env.sh;
    """

    self.ssh(cmd)

  def run_bench_query(self, script_file, executions):
    results = []
    self.scp_to(script_file, "/tmp/bench_query.hql") 
    for f in xrange(executions):
      self.ssh_master('HIVE_HOME=/opt/apache-hive-0.13.0.2.1.0.0-92-bin HIVE_CONF_DIR=$HIVE_HOME/conf PATH=$HIVE_HOME/bin:$PATH HADOOP_CLASSPATH=/opt/tez-0.2.0.2.1.0.0-92/*:/opt/tez-0.2.0.2.1.0.0-92/lib/* HADOOP_USER_CLASSPATH_FIRST=true HADOOP_USER_NAME=hdfs /opt/apache-hive-0.13.0.2.1.0.0-92-bin/bin/hive -i /root/benchmark/runner/tez/Stinger-Preview-Quickstart/configs/stinger.settings -hiveconf hive.optimize.tez=true -f /tmp/bench_query.hql 2>&1 | grep "Time taken" | sed "s/Time taken:\([0-9.]*\) seconds/\\\\1/" >> /tmp/result.csv')
      self.ssh_slaves('sudo sync && sudo echo 3 > /proc/sys/vm/drop_caches')
    self.scp_from("result.csv", "/tmp/result.csv")
    self.ssh("rm /tmp/result.csv")
    with open("result.csv") as f:
      for line in f:
         print line 
