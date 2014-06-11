import benchmark

class TPCH(benchmark.Benchmark):
  name = "TPC-H"
  
  @classmethod  
  def parser_options(cls, parser):
    parser.add_option("--tpc-h", action="store_true", default=False,
      help="Whether to run the TPC-H benchmark at 100GB")

  @classmethod  
  def is_enabled(cls, opts):
    return opts.tpc_h

  def __init__(self, opts):
    self.scale_factor = 100
    self.prefix = "text"
    
  def load_data(self, engine):
    engine.mkdir("/benchmark") 
    engine.copy_from_s3 ( "big-data-tpch/%s/%s/lineitem/" % (self.prefix, self.scale_factor), 
                         "/tmp/benchmark/lineitem/")
    engine.copy_from_s3 ( "big-data-tpch/%s/%s/part/" % (self.prefix, self.scale_factor), 
                         "/tmp/benchmark/part/")
    engine.copy_from_s3 ( "big-data-tpch/%s/%s/supplier/" % (self.prefix, self.scale_factor), 
                         "/tmp/benchmark/supplier/")
    engine.copy_from_s3 ( "big-data-tpch/%s/%s/partsupp/" % (self.prefix, self.scale_factor), 
                         "/tmp/benchmark/partsupp/")
    engine.copy_from_s3 ( "big-data-tpch/%s/%s/nation/" % (self.prefix, self.scale_factor), 
                         "/tmp/benchmark/nation/")
    engine.copy_from_s3 ( "big-data-tpch/%s/%s/region/" % (self.prefix, self.scale_factor), 
                         "/tmp/benchmark/region/")
    engine.copy_from_s3 ( "big-data-tpch/%s/%s/orders/" % (self.prefix, self.scale_factor), 
                         "/tmp/benchmark/orders/")  

  def create_tables(self, engine):
    engine.run_hql(
      """
      DROP TABLE IF EXISTS lineitem;
      Create external table lineitem (L_ORDERKEY INT, L_PARTKEY INT, L_SUPPKEY INT, L_LINENUMBER INT, 
      L_QUANTITY DOUBLE, L_EXTENDEDPRICE DOUBLE, L_DISCOUNT DOUBLE, L_TAX DOUBLE, L_RETURNFLAG STRING, 
      L_LINESTATUS STRING, L_SHIPDATE STRING, L_COMMITDATE STRING, L_RECEIPTDATE STRING, L_SHIPINSTRUCT 
      STRING, L_SHIPMODE STRING, L_COMMENT STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY \\"|\\" STORED 
      AS TEXTFILE LOCATION \\"/tmp/benchmark/lineitem\\"; 
      """ 
    )

    engine.run_hql(
      """
      DROP TABLE IF EXISTS part;
      create external table part (P_PARTKEY INT, P_NAME STRING, P_MFGR STRING, P_BRAND STRING, P_TYPE 
      STRING, P_SIZE INT, P_CONTAINER STRING, P_RETAILPRICE DOUBLE, P_COMMENT STRING) ROW FORMAT 
      DELIMITED FIELDS TERMINATED BY \\"|\\" STORED AS TEXTFILE LOCATION \\"/tmp/benchmark/part\\";
      """
    ) 
    
    engine.run_hql(
      """
      DROP TABLE IF EXISTS supplier;
      create external table supplier (S_SUPPKEY INT, S_NAME STRING, S_ADDRESS STRING, S_NATIONKEY INT, 
      S_PHONE STRING, S_ACCTBAL DOUBLE, S_COMMENT STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY \\"|\\" 
      STORED AS TEXTFILE LOCATION \\"/tmp/benchmark/supplier\\";
      """
    )

    engine.run_hql(
      """
      DROP TABLE IF EXISTS partsupp;
      create external table partsupp (PS_PARTKEY INT, PS_SUPPKEY INT, PS_AVAILQTY INT, PS_SUPPLYCOST 
      DOUBLE, PS_COMMENT STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY \\"|\\" STORED AS TEXTFILE 
      LOCATION \\"/tmp/benchmark/partsupp\\";
      """
    )

    engine.run_hql(
      """
      DROP TABLE IF EXISTS nation;
      create external table nation (N_NATIONKEY INT, N_NAME STRING, N_REGIONKEY INT, N_COMMENT STRING) 
      ROW FORMAT DELIMITED FIELDS TERMINATED BY \\"|\\" STORED AS TEXTFILE LOCATION \\"/tmp/benchmark/nation\\";  
      """
    )
  
    engine.run_hql(
      """
      DROP TABLE IF EXISTS region;
      create external table region (R_REGIONKEY INT, R_NAME STRING, R_COMMENT STRING) ROW FORMAT 
      DELIMITED FIELDS TERMINATED BY \\"|\\" STORED AS TEXTFILE LOCATION \\"/tmp/benchmark/region\\";
      """
    )


  def create_rcfiles(self, engine):
    engine.run_hql(
      """
      DROP TABLE IF EXISTS lineitem_rc;
      SET hive.exec.compress.output=true;
      SET mapred.output.compression.type=BLOCK;
      SET mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;
      Create table lineitem_rc (L_ORDERKEY INT, L_PARTKEY INT, L_SUPPKEY INT, L_LINENUMBER INT, 
      L_QUANTITY DOUBLE, L_EXTENDEDPRICE DOUBLE, L_DISCOUNT DOUBLE, L_TAX DOUBLE, L_RETURNFLAG STRING, 
      L_LINESTATUS STRING, L_SHIPDATE STRING, L_COMMITDATE STRING, L_RECEIPTDATE STRING, L_SHIPINSTRUCT 
      STRING, L_SHIPMODE STRING, L_COMMENT STRING) 
      ROW FORMAT SERDE \\"org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe\\"
      STORED AS RCFILE;
      INSERT OVERWRITE TABLE lineitem_rc SELECT * FROM lineitem;
      """ 
    )

    engine.run_hql(
      """
      DROP TABLE IF EXISTS part_rc;
      SET hive.exec.compress.output=true;
      SET mapred.output.compression.type=BLOCK;
      SET mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;
      create table part_rc (P_PARTKEY INT, P_NAME STRING, P_MFGR STRING, P_BRAND STRING, P_TYPE 
      STRING, P_SIZE INT, P_CONTAINER STRING, P_RETAILPRICE DOUBLE, P_COMMENT STRING)
      ROW FORMAT SERDE \\"org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe\\"
      STORED AS RCFILE;
      INSERT OVERWRITE TABLE part_rc SELECT * FROM part;
      """
    ) 
    
    engine.run_hql(
      """
      DROP TABLE IF EXISTS supplier_rc;
      SET hive.exec.compress.output=true;
      SET mapred.output.compression.type=BLOCK;
      SET mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;
      create table supplier_rc (S_SUPPKEY INT, S_NAME STRING, S_ADDRESS STRING, S_NATIONKEY INT, 
      S_PHONE STRING, S_ACCTBAL DOUBLE, S_COMMENT STRING) 
      ROW FORMAT SERDE \\"org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe\\"
      STORED AS RCFILE;
      INSERT OVERWRITE TABLE supplier_rc SELECT * FROM supplier;
      """
    )

    engine.run_hql(
      """
      DROP TABLE IF EXISTS partsupp_rc;
      SET hive.exec.compress.output=true;
      SET mapred.output.compression.type=BLOCK;
      SET mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;
      create table partsupp_rc (PS_PARTKEY INT, PS_SUPPKEY INT, PS_AVAILQTY INT, PS_SUPPLYCOST 
      DOUBLE, PS_COMMENT STRING) 
      ROW FORMAT SERDE \\"org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe\\"
      STORED AS RCFILE;
      INSERT OVERWRITE TABLE partsupp_rc SELECT * FROM partsupp;
      """
    )

    engine.run_hql(
      """
      DROP TABLE IF EXISTS nation_rc;
      SET hive.exec.compress.output=true;
      SET mapred.output.compression.type=BLOCK;
      SET mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;
      create table nation_rc (N_NATIONKEY INT, N_NAME STRING, N_REGIONKEY INT, N_COMMENT STRING)
      ROW FORMAT SERDE \\"org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe\\"
      STORED AS RCFILE;
      INSERT OVERWRITE TABLE nation_rc SELECT * FROM nation;       
      """
    )
  
    engine.run_hql(
      """
      DROP TABLE IF EXISTS region_rc;
      SET hive.exec.compress.output=true;
      SET mapred.output.compression.type=BLOCK;
      SET mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;
      create table region_rc (R_REGIONKEY INT, R_NAME STRING, R_COMMENT STRING)
      ROW FORMAT SERDE \\"org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe\\"
      STORED AS RCFILE;
      INSERT OVERWRITE TABLE region_rc SELECT * FROM region;             
      """
    )

def create_orcfiles(self, engine):
    engine.run_hql(
      """
      DROP TABLE IF EXISTS lineitem_orc;
      Create table lineitem_orc (L_ORDERKEY INT, L_PARTKEY INT, L_SUPPKEY INT, L_LINENUMBER INT, 
      L_QUANTITY DOUBLE, L_EXTENDEDPRICE DOUBLE, L_DISCOUNT DOUBLE, L_TAX DOUBLE, L_RETURNFLAG STRING, 
      L_LINESTATUS STRING, L_SHIPDATE STRING, L_COMMITDATE STRING, L_RECEIPTDATE STRING, L_SHIPINSTRUCT 
      STRING, L_SHIPMODE STRING, L_COMMENT STRING) 
      STORED AS ORC TBLPROPERTIES (\\"orc.compress\\"=\\"SNAPPY\\");
      INSERT OVERWRITE TABLE lineitem_orc SELECT * FROM lineitem;
      """ 
    )

    engine.run_hql(
      """
      DROP TABLE IF EXISTS part_orc;
      create table part_orc (P_PARTKEY INT, P_NAME STRING, P_MFGR STRING, P_BRAND STRING, P_TYPE 
      STRING, P_SIZE INT, P_CONTAINER STRING, P_RETAILPRICE DOUBLE, P_COMMENT STRING)
      STORED AS ORC TBLPROPERTIES (\\"orc.compress\\"=\\"SNAPPY\\");
      INSERT OVERWRITE TABLE part_orc SELECT * FROM part;
      """
    ) 
    
    engine.run_hql(
      """
      DROP TABLE IF EXISTS supplier_orc;
      create table supplier_orc (S_SUPPKEY INT, S_NAME STRING, S_ADDRESS STRING, S_NATIONKEY INT, 
      S_PHONE STRING, S_ACCTBAL DOUBLE, S_COMMENT STRING) 
      STORED AS ORC TBLPROPERTIES (\\"orc.compress\\"=\\"SNAPPY\\");
      INSERT OVERWRITE TABLE supplier_orc SELECT * FROM supplier;
      """
    )

    engine.run_hql(
      """
      DROP TABLE IF EXISTS partsupp_orc;
      create table partsupp_orc (PS_PARTKEY INT, PS_SUPPKEY INT, PS_AVAILQTY INT, PS_SUPPLYCOST 
      DOUBLE, PS_COMMENT STRING) 
      STORED AS ORC TBLPROPERTIES (\\"orc.compress\\"=\\"SNAPPY\\");
      INSERT OVERWRITE TABLE partsupp_orc SELECT * FROM partsupp;
      """
    )

    engine.run_hql(
      """
      DROP TABLE IF EXISTS nation_orc;
      create table nation_orc (N_NATIONKEY INT, N_NAME STRING, N_REGIONKEY INT, N_COMMENT STRING)
      STORED AS ORC TBLPROPERTIES (\\"orc.compress\\"=\\"SNAPPY\\");
      INSERT OVERWRITE TABLE nation_orc SELECT * FROM nation;       
      """
    )
  
    engine.run_hql(
      """
      DROP TABLE IF EXISTS region_orc;
      create table region_orc (R_REGIONKEY INT, R_NAME STRING, R_COMMENT STRING)
      STORED AS ORC TBLPROPERTIES (\\"orc.compress\\"=\\"SNAPPY\\");
      INSERT OVERWRITE TABLE region_orc SELECT * FROM region;             
      """
    )


def create_parquet(self, engine):
    engine.run_hql(
      """
      DROP TABLE IF EXISTS lineitem_parquet;
      set parquet.compression=SNAPPY;
      Create table lineitem_parquet (L_ORDERKEY INT, L_PARTKEY INT, L_SUPPKEY INT, L_LINENUMBER INT, 
      L_QUANTITY DOUBLE, L_EXTENDEDPRICE DOUBLE, L_DISCOUNT DOUBLE, L_TAX DOUBLE, L_RETURNFLAG STRING, 
      L_LINESTATUS STRING, L_SHIPDATE STRING, L_COMMITDATE STRING, L_RECEIPTDATE STRING, L_SHIPINSTRUCT 
      STRING, L_SHIPMODE STRING, L_COMMENT STRING) 
      STORED AS PARQUET;
      INSERT OVERWRITE TABLE lineitem_parquet SELECT * FROM lineitem;
      """ 
    )

    engine.run_hql(
      """
      DROP TABLE IF EXISTS part_parquet;
      set parquet.compression=SNAPPY;
      create table part_parquet (P_PARTKEY INT, P_NAME STRING, P_MFGR STRING, P_BRAND STRING, P_TYPE 
      STRING, P_SIZE INT, P_CONTAINER STRING, P_RETAILPRICE DOUBLE, P_COMMENT STRING)
      STORED AS PARQUET;
      INSERT OVERWRITE TABLE part_parquet SELECT * FROM part;
      """
    ) 
    
    engine.run_hql(
      """
      DROP TABLE IF EXISTS supplier_parquet;
      set parquet.compression=SNAPPY;
      create table supplier_parquet (S_SUPPKEY INT, S_NAME STRING, S_ADDRESS STRING, S_NATIONKEY INT, 
      S_PHONE STRING, S_ACCTBAL DOUBLE, S_COMMENT STRING) 
      STORED AS PARQUET;
      INSERT OVERWRITE TABLE supplier_parquet SELECT * FROM supplier;
      """
    )

    engine.run_hql(
      """
      DROP TABLE IF EXISTS partsupp_parquet;
      set parquet.compression=SNAPPY;
      create table partsupp_parquet (PS_PARTKEY INT, PS_SUPPKEY INT, PS_AVAILQTY INT, PS_SUPPLYCOST 
      DOUBLE, PS_COMMENT STRING) 
      STORED AS PARQUET;
      INSERT OVERWRITE TABLE partsupp_parquet SELECT * FROM partsupp;
      """
    )

    engine.run_hql(
      """
      DROP TABLE IF EXISTS nation_parquet;
      set parquet.compression=SNAPPY;
      create table nation_parquet (N_NATIONKEY INT, N_NAME STRING, N_REGIONKEY INT, N_COMMENT STRING)
      STORED AS PARQUET;
      INSERT OVERWRITE TABLE nation_parquet SELECT * FROM nation;       
      """
    )
  
    engine.run_hql(
      """
      DROP TABLE IF EXISTS region_parquet;
      set parquet.compression=SNAPPY;
      create table region_parquet (R_REGIONKEY INT, R_NAME STRING, R_COMMENT STRING)
      STORED AS PARQUET;
      INSERT OVERWRITE TABLE region_parquet SELECT * FROM region;             
      """
    )
