Big Data Benchmark Scripts
======

Collection of scripts to create HDP and CDH clusters on EC2 and run benchmarks.

All of the scripts require some Python dependencies. These are in requirements.txt, so you can `pip install -r requirements.txt`

Creating Clusters
--------

To create a CDH cluster, use `prepare_cdh.py`, for HDP use `prepare_hdp.py`. They both take the same arguments. For a typical 5-node cluster, use:

```

python prepare_cdh.py launch --slaves 5 --key-pair <EC2 keypair name> --identity-file <PEM for the EC2 keypair> --instance-type i2.xlarge test_cluster 

```

To get the hostnames for all the instances you can use `python prepare_cdh.py info <cluster_name>` (ex. test_cluster).

Cluster startup can be restarted at various stages (like deploying the blueprints, installing RPMs, starting Ambari). See the actions provided in the help page.

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
