Big Data Benchmark Scripts
======

Collection of scripts to create HDP and CDH clusters on EC2 and run benchmarks.

All of the scripts require some Python dependencies. These are in requirements.txt, so you can `pip install -r requirements.txt`

Creating Clusters
--------

To create a CDH cluster, use `prepare_cm.py`, for HDP use `prepare_hdp.py`. They both take the same arguments. To create a typical 5-node cluster, first create a new EC2 keypair in the AWS console, and download the associated PEM file. Generate an AWS access key and the corresponding secret. Then run:

```
export AWS_ACCESS_KEY_ID=<AWS ACCESS KEY ID>
export AWS_SECRET_ACCESS_KEY=<AWS SECRET ACCESS KEY> 
python prepare_cm.py launch -m r3.xlarge --slaves 5 --key-pair <EC2 keypair name> --identity-file <PEM for the EC2 keypair> --instance-type i2.xlarge test_cluster 

```

With the name of the EC2 key pair in `key-pair`, and the path to the PEM file in `identity-file`. 

This will create five i2.xlarge instances as DataNodes, each with one 800GB SSD which will be used for HDFS. r3.xlarge instances will be used for the NameNode/ResourceManager, and the Cloudera Manager server.

Internally the cluster will be named `test_cluster`. To get the hostnames for all the instances you can use `python prepare_cdh.py info <cluster_name>`.

Cluster startup can be restarted at various stages (like deploying the blueprints, installing RPMs, starting Ambari). See the actions provided in the help page.

Connecting to the cluster
--------

In a CDH cluster, Cloudera Manager will be accessible by pointing your browser to port 7180 on the master. On HDP, Ambari will be accessible on 8080. Both use admin/admin as the default login.

To copy files and issue Hive queries, SSH to the NameNode/JobTracker using your EC2 PEM from setup. To do your own benchmark, copy files from S3 to HDFS using ```distcp``` (see https://wiki.apache.org/hadoop/AmazonS3 for details), then use the Hive or Impala client on the NameNode.


Staging Benchmark Data
--------

`prepare_benchmark.py` automates the process of copying sample data from S3, converting it and running benchmarks. The following copies TPC-H (--tpc-h) data from S3 (--load-data), creates tables (--load-data) and converts it to Parquet (--parquet): 

```

python prepare_benchmark.py --load-data --create-schema --impala --impala-host <master from prepare_cdh> --impala-slaves <list of slaves from prepare_cdh> --impala-identity-file <PEM for EC2 instances> --tpc-h --parquet -d <AWS key for S3> -k <AWS secret key for S3>

```  

Running Benchmarks
-------

Once the data is staged, the queries can be run and timed with:

```

python prepare_benchmark.py --query --impala --impala-host <master from prepare_cdh> --impala-slaves <list of slaves from prepare_cdh> --impala-identity-file <PEM for EC2 instances> --tpc-h --parquet -d <AWS key for S3> -k <AWS secret key for S3>


```
