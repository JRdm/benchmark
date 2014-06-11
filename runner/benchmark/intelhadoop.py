import benchmark

QUERIES = ['1a', '1b', '1c', '2a', '2b', '2c', '3a', '3b', '3c']
QUERY_FILE_ROOT = 'benchmark/queries/intelhadoop/'

class IntelHadoopBenchmark(benchmark.Benchmark):
  name = "Intel Hadoop Benchmark"
  
  @classmethod  
  def parser_options(cls, parser):
    parser.add_option("--intel-hadoop", action="store_true", default=False,
      help="Whether to run the Intel Hadoop benchmark")

  @classmethod  
  def is_enabled(cls, opts):
    return opts.intel_hadoop

  def __init__(self, opts):
    self.scale_factor = "5-nodes" 
    self.prefix = "sequence-snappy"
    
  def load_data(self, engine):
    engine.mkdir("/tmp/benchmark") 
    engine.copy_from_s3 ( "big-data-benchmark/pavlo/%s/%s/rankings/" % (self.prefix, self.scale_factor), 
                         "/tmp/benchmark/rankings/")
    engine.copy_from_s3 ( "big-data-benchmark/pavlo/%s/%s/uservisits/" % (self.prefix, self.scale_factor),
                         "/tmp/benchmark/uservisits/")
    engine.copy_from_s3 ( "big-data-benchmark/pavlo/%s/%s/crawl/" % (self.prefix, self.scale_factor),
                         "/tmp/benchmark/crawl/")
    engine.copy_from_s3 ( "big-data-benchmark/pavlo/%s/%s/rankings/" % (self.prefix, self.scale_factor), 
                         "/tmp/benchmark/scratch/")

  def run_test(self, engine, table_suffix, iterations=3):
    if table_suffix != '':
      table_suffix = '-%s' % table_suffix

    for q in QUERIES:
      script = "%s/query-%s%s.hql" % (QUERY_FILE_ROOT, q, table_suffix)
      print "Running script %s" % script
      engine.run_bench_query(script, iterations) 

  def create_tables(self, engine):
    engine.run_hql(
      """
      DROP TABLE IF EXISTS rankings;
      CREATE EXTERNAL TABLE rankings (pageURL STRING, pageRank INT,
      avgDuration INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY \\",\\" 
      STORED AS TEXTFILE LOCATION \\"/tmp/benchmark/rankings\\";
      """ 
    )

    engine.run_hql(
      """
      DROP TABLE IF EXISTS scratch;
      CREATE EXTERNAL TABLE scratch (pageURL STRING, pageRank INT,
      avgDuration INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY \\",\\" 
      STORED AS TEXTFILE LOCATION \\"/tmp/benchmark/scratch\\";
      """
    )

    engine.run_hql(
      """
      DROP TABLE IF EXISTS uservisits;
      CREATE EXTERNAL TABLE uservisits (sourceIP STRING,destURL STRING,
      visitDate STRING,adRevenue DOUBLE,userAgent STRING,countryCode STRING,
      languageCode STRING,searchWord STRING,duration INT )
      ROW FORMAT DELIMITED FIELDS TERMINATED BY \\",\\"
      STORED AS TEXTFILE LOCATION \\"/tmp/benchmark/uservisits\\";
      """
    )

    engine.run_hql(
      """
      DROP TABLE IF EXISTS documents;
      CREATE EXTERNAL TABLE documents (line STRING) STORED AS TEXTFILE
      LOCATION \\"/tmp/benchmark/crawl\\";  
      """
    )

  def create_rcfiles(self, engine):
    engine.run_hql(
      """
      DROP TABLE IF EXISTS rankings_rc;
      SET hive.exec.compress.output=true;
      SET mapred.output.compression.type=BLOCK;
      SET mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;
      CREATE TABLE rankings_rc (pageURL STRING, pageRank INT, avgDuration INT) 
      ROW FORMAT SERDE \\"org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe\\"
      STORED AS RCFILE;
      INSERT OVERWRITE TABLE rankings_rc SELECT * FROM rankings;
      """ 
    )

    engine.run_hql(
      """
      DROP TABLE IF EXISTS uservisits_rc;
      SET hive.exec.compress.output=true;
      SET mapred.output.compression.type=BLOCK;
      SET mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;
      CREATE TABLE uservisits_rc (sourceIP STRING,destURL STRING,
      visitDate STRING,adRevenue DOUBLE,userAgent STRING,countryCode STRING,
      languageCode STRING,searchWord STRING,duration INT )
      ROW FORMAT SERDE \\"org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe\\"
      STORED AS RCFILE;
      INSERT OVERWRITE TABLE uservisits_rc SELECT * FROM uservisits;
      """
    )

    engine.run_hql(
      """
      DROP TABLE IF EXISTS documents_rc;
      SET hive.exec.compress.output=true;
      SET mapred.output.compression.type=BLOCK;
      SET mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;
      CREATE EXTERNAL TABLE documents_rc (line STRING)
      ROW FORMAT SERDE \\"org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe\\"
      STORED AS RCFILE;
      INSERT OVERWRITE TABLE documents_rc SELECT * FROM documents;
      """
    )

  def create_orcfiles(self, engine):
    engine.run_hql(
      """
      DROP TABLE IF EXISTS rankings_orc;
      CREATE TABLE rankings_orc (pageURL STRING, pageRank INT, avgDuration INT) 
      STORED AS ORC TBLPROPERTIES (\\"orc.compress\\"=\\"SNAPPY\\");
      INSERT OVERWRITE TABLE rankings_orc SELECT * FROM rankings;
      """ 
    )

    engine.run_hql(
      """
      DROP TABLE IF EXISTS uservisits_orc;
      CREATE TABLE uservisits_orc (sourceIP STRING,destURL STRING,
      visitDate STRING,adRevenue DOUBLE,userAgent STRING,countryCode STRING,
      languageCode STRING,searchWord STRING,duration INT )
      STORED AS ORC TBLPROPERTIES (\\"orc.compress\\"=\\"SNAPPY\\");
      INSERT OVERWRITE TABLE uservisits_orc SELECT * FROM uservisits;
      """
    )

    engine.run_hql(
      """
      DROP TABLE IF EXISTS documents_orc;
      CREATE EXTERNAL TABLE documents_orc (line STRING)
      STORED AS ORC TBLPROPERTIES (\\"orc.compress\\"=\\"SNAPPY\\");
      INSERT OVERWRITE TABLE documents_orc SELECT * FROM documents;
      """
    )

  def create_parquet(self, engine):
    engine.run_hql(
      """
      set parquet.compression=SNAPPY;
      DROP TABLE IF EXISTS rankings_parquet;
      CREATE TABLE rankings_parquet (pageURL STRING, pageRank INT, avgDuration INT)
      STORED AS PARQUET; 
      INSERT OVERWRITE TABLE rankings_parquet SELECT * FROM rankings;
      """ 
    )

    engine.run_hql(
      """
      set parquet.compression=SNAPPY;
      DROP TABLE IF EXISTS uservisits_parquet;
      CREATE TABLE uservisits_parquet (sourceIP STRING,destURL STRING,
      visitDate STRING,adRevenue DOUBLE,userAgent STRING,countryCode STRING,
      languageCode STRING,searchWord STRING,duration INT )
      STORED AS PARQUET;
      INSERT OVERWRITE TABLE uservisits_parquet SELECT * FROM uservisits;
      """
    )

    engine.run_hql(
      """
      set parquet.compression=SNAPPY;
      DROP TABLE IF EXISTS documents_parquet;
      CREATE EXTERNAL TABLE documents_parquet (line STRING)
      STORED AS PARQUET;
      INSERT OVERWRITE TABLE documents_parquet SELECT * FROM documents;
      """
    ) 
