#!/usr/bin/env python
#
# Copyright (c) 2013-2014 Cloudera, Inc. All rights reserved.

# Deploys a CDH cluster and configures CM management services.

import socket, sys, time, ConfigParser, csv, pprint
from subprocess import Popen, PIPE, STDOUT
from math import log as ln
from cm_api.api_client import ApiResource
from cm_api.endpoints.services import ApiService
from cm_api.endpoints.services import ApiServiceSetupInfo

class ClouderaCluster:

  CDH_VERSION = "CDH5"

  TIMEOUT = 100000
  
  PARCELS = [
     { 'name' : "CDH", 'version' : "5.1.0-1.cdh5.1.0.p0.53" },
  ]
  
  HADOOP_DATA_DIR_PREFIX="/hadoop"
  
  ### ZooKeeper ###
  # ZK quorum will be the first three hosts 
  ZOOKEEPER_SERVICE_NAME = "ZOOKEEPER"
  ZOOKEEPER_SERVICE_CONFIG = {
     'zookeeper_datadir_autocreate': 'true',
  }
  ZOOKEEPER_ROLE_CONFIG = {
      'quorumPort': 2888,
      'electionPort': 3888,
      'dataLogDir': '/var/lib/zookeeper',
      'dataDir': '/var/lib/zookeeper',
      'maxClientCnxns': '1024',
  }
  
  ### HDFS ###
  HDFS_SERVICE_NAME = "HDFS"
  HDFS_SERVICE_CONFIG = {
    'dfs_replication': 3,
    'dfs_permissions': 'false',
    'dfs_block_local_path_access_user': 'impala,hbase,mapred,spark',
  }
  HDFS_NAMENODE_SERVICE_NAME = "nn"
  HDFS_NAMENODE_CONFIG = {
    'dfs_name_dir_list': HADOOP_DATA_DIR_PREFIX + '/namenode',
    'dfs_namenode_handler_count': 30, #int(ln(len(DATANODES))*20),
  }
  HDFS_SECONDARY_NAMENODE_CONFIG = {
    'fs_checkpoint_dir_list': HADOOP_DATA_DIR_PREFIX + '/namesecondary',
  }
  #dfs_datanode_du_reserved must be smaller than the amount of free space across the data dirs
  #Ideally each data directory will have at least 1TB capacity; they need at least 100GB at a minimum 
  #dfs_datanode_failed_volumes_tolerated must be less than the number of different data dirs (ie volumes) in dfs_data_dir_list
  HDFS_DATANODE_CONFIG = {
    'dfs_data_dir_list': HADOOP_DATA_DIR_PREFIX + '/datanode',
    'dfs_datanode_handler_count': 30,
    'dfs_datanode_du_reserved': 1073741824,
    'dfs_datanode_failed_volumes_tolerated': 0,
    'dfs_datanode_data_dir_perm': 755,
  }
  HDFS_GATEWAY_CONFIG = {
    'dfs_client_use_trash' : 'true'
  }
  
  ### MapReduce ###
  MAPRED_SERVICE_NAME = "MAPRED"
  MAPRED_SERVICE_CONFIG = {
    'hdfs_service': HDFS_SERVICE_NAME,
  }
  MAPRED_JT_CONFIG = {
    'mapred_jobtracker_restart_recover': 'true',
    'mapred_job_tracker_handler_count': 30, #int(ln(len(DATANODES))*20),
    'jobtracker_mapred_local_dir_list': HADOOP_DATA_DIR_PREFIX + '/mapred',
    'mapreduce_jobtracker_staging_root_dir': HADOOP_DATA_DIR_PREFIX + '/staging',
    'mapreduce_jobtracker_split_metainfo_maxsize': '100000000',
  }
  MAPRED_TT_CONFIG = {
    'tasktracker_mapred_local_dir_list': HADOOP_DATA_DIR_PREFIX + '/mapred',
    'mapred_tasktracker_map_tasks_maximum': 6,
    'mapred_tasktracker_reduce_tasks_maximum': 3,
    'override_mapred_child_java_opts_base': '-Xmx4g -Djava.net.preferIPv4Stack=true',
    'override_mapred_child_ulimit': 8388608,
    'override_mapred_reduce_parallel_copies': 5,
    'tasktracker_http_threads': 40,
    'override_mapred_output_compress': 'true',
    'override_mapred_output_compression_type': 'BLOCK',
    'override_mapred_output_compression_codec': 'org.apache.hadoop.io.compress.SnappyCodec',
    'override_mapred_compress_map_output': 'true',
    'override_mapred_map_output_compression_codec': 'org.apache.hadoop.io.compress.SnappyCodec',
    'override_io_sort_record_percent': '0.15',
  }
  MAPRED_GW_CONFIG = {
    'mapred_submit_replication': 3,
  }
  
  ### YARN ###
  YARN_SERVICE_NAME = "YARN"
  YARN_SERVICE_CONFIG = {
    'hdfs_service': HDFS_SERVICE_NAME,
  }
  YARN_NM_CONFIG = {
    'yarn_nodemanager_local_dirs': HADOOP_DATA_DIR_PREFIX + '/yarn/nm'
  }
  
  ### HBase ###
  HBASE_SERVICE_NAME = "HBASE"
  HBASE_SERVICE_CONFIG = {
    'hdfs_service': HDFS_SERVICE_NAME,
    'zookeeper_service': ZOOKEEPER_SERVICE_NAME,
  }
  HBASE_RS_CONFIG = {
    'hbase_hregion_memstore_flush_size': 1024000000,
    'hbase_regionserver_handler_count': 10,
    'hbase_regionserver_java_heapsize': 2048000000,
    'hbase_regionserver_java_opts': '',
  }
  HBASE_THRIFTSERVER_SERVICE_NAME = "HBASETHRIFTSERVER"
  
  ### Hive ###
  HIVE_SERVICE_NAME = "HIVE"
  HIVE_SERVICE_CONFIG = {
    'hive_metastore_database_name': 'hive',
    'hive_metastore_database_password': 'hive',
    'hive_metastore_database_port': 5432,
    'hive_metastore_database_type': 'postgresql',
    'zookeeper_service': ZOOKEEPER_SERVICE_NAME,
    'mapreduce_yarn_service': YARN_SERVICE_NAME,
  }
  HIVE_HMS_CONFIG = {
    'hive_metastore_java_heapsize': 85306784,
  }
  
  ### Impala ###
  IMPALA_SERVICE_NAME = "IMPALA"
  IMPALA_SERVICE_CONFIG = {
    'hdfs_service': HDFS_SERVICE_NAME,
    'hbase_service': HBASE_SERVICE_NAME,
    'hive_service': HIVE_SERVICE_NAME,
  }
 
  CLUSTER_NAME="CDH1" 

  def set_hosts(self, cm, master, slaves):
    self.cm = cm
    self.master = master
    self.slaves = slaves
 
  # Creates the cluster and adds hosts
  def _init_cluster(self):
     self.cluster = self.api.create_cluster(self.CLUSTER_NAME, self.CDH_VERSION)
     # Add the CM host to the list of hosts to add in the cluster so it can run the management services 
     self.cluster.add_hosts(self.slaves+[self.master])
  
  # Downloads and distributes parcels
  def _deploy_parcels(self):
     for parcel in self.PARCELS:
        p = self.cluster.get_parcel(parcel['name'], parcel['version'])
        p.start_download()
        while True:
           p = self.cluster.get_parcel(parcel['name'], parcel['version'])
           if p.stage != "DOWNLOADING":
              break
           if p.state.errors:
              raise Exception(str(p.state.errors))
           print "Downloading %s: %s / %s" % (parcel['name'], p.state.progress, p.state.totalProgress)
           time.sleep(15)
        print "Downloaded %s" % (parcel['name'])
        p.start_distribution()
        while True:
           p = self.cluster.get_parcel(parcel['name'], parcel['version'])
           if p.stage != "DISTRIBUTING":
              break
           if p.state.errors:
              raise Exception(str(p.state.errors))
           print "Distributing %s: %s / %s" % (parcel['name'], p.state.progress, p.state.totalProgress)
           time.sleep(15)
        print "Distributed %s" % (parcel['name'])
        p.activate()
  
  def _clean_up_services(self):
    for s in self.cluster.get_all_services():
      print "Deleting service %s" % s.name
      self.cluster.delete_service(s.name)
  
  def deploy_management(self):
     try:
         mgmt = self.manager.get_service()
     except Exception:
         mgmt = self.manager.create_mgmt_service(ApiServiceSetupInfo())
         mgmt.create_role("AMON-1", "ACTIVITYMONITOR", self.master)
         mgmt.create_role("APUB-1", "ALERTPUBLISHER", self.master)
         mgmt.create_role("ESERV-1", "EVENTSERVER", self.master)
         mgmt.create_role("HMON-1", "HOSTMONITOR", self.master)
         mgmt.create_role("SMON-1", "SERVICEMONITOR", self.master)
         mgmt.start().wait()  
  
  # Deploys and initializes ZooKeeper
  def deploy_zookeeper(self):
     zk = self.cluster.create_service(self.ZOOKEEPER_SERVICE_NAME, "ZOOKEEPER")
     zk.update_config(self.ZOOKEEPER_SERVICE_CONFIG)
     
     zk_id = 0
     for zk_host in self.slaves[:3]:
        zk_id += 1
        zk_role_conf = {'serverId':zk_id}
        while True:
          try:
            role = zk.create_role(self.ZOOKEEPER_SERVICE_NAME + "-" + str(zk_id), "SERVER", zk_host)
            break
          except Exception as e:
            print "Exception creating ZK role: %s" % e
        role.update_config(zk_role_conf)
     zk.init_zookeeper()
  
  def deploy_hdfs(self):
     hdfs_service = self.cluster.create_service(self.HDFS_SERVICE_NAME, "HDFS")
     hdfs_service.update_config(self.HDFS_SERVICE_CONFIG)
     
     nn_role_group = hdfs_service.get_role_config_group("{0}-NAMENODE-BASE".format(self.HDFS_SERVICE_NAME))
     nn_role_group.update_config(self.HDFS_NAMENODE_CONFIG)

     while True:
       try:
         hdfs_service.create_role("{0}-nn".format(self.HDFS_SERVICE_NAME), "NAMENODE", self.master)
         break
       except Exception as e:
         print "Exception creating NameNode role: %s" % e
     snn_role_group = hdfs_service.get_role_config_group("{0}-SECONDARYNAMENODE-BASE".format(self.HDFS_SERVICE_NAME))
     snn_role_group.update_config(self.HDFS_SECONDARY_NAMENODE_CONFIG)

     while True:
       try:
         hdfs_service.create_role("{0}-snn".format(self.HDFS_SERVICE_NAME), "SECONDARYNAMENODE", self.master)
         break
       except Exception as e:
         print "Exception creating Secondary NameNode role: %s" % e

     dn_role_group = hdfs_service.get_role_config_group("{0}-DATANODE-BASE".format(self.HDFS_SERVICE_NAME))
     dn_role_group.update_config(self.HDFS_DATANODE_CONFIG)
     
     gw_role_group = hdfs_service.get_role_config_group("{0}-GATEWAY-BASE".format(self.HDFS_SERVICE_NAME))
     gw_role_group.update_config(self.HDFS_GATEWAY_CONFIG)
     
     datanode = 0
     for host in self.slaves:
        datanode += 1
        while True:
          try:
            hdfs_service.create_role("{0}-dn-".format(self.HDFS_SERVICE_NAME) + str(datanode), "DATANODE", host)
            break
          except Exception as e:
            print "Exception creating DataNode role: %s" % e
     gateway = 0

     for host in [self.master]:
        gateway += 1
        while True:
          try:
            hdfs_service.create_role("{0}-gw-".format(self.HDFS_SERVICE_NAME) + str(gateway), "GATEWAY", host)
            break
          except Exception as e:
            print "Exception creating Gateway role: %s" % e
      
     self._init_hdfs(hdfs_service) 
  
  def _init_hdfs(self, hdfs_service):
     cmd = hdfs_service.format_hdfs("{0}-nn".format(self.HDFS_SERVICE_NAME))[0]
     if not cmd.wait(self.TIMEOUT).success:
        print "WARNING: Failed to format HDFS, attempting to continue with the setup" 
  
  def deploy_yarn(self):
     yarn_service = self.cluster.create_service(self.YARN_SERVICE_NAME, "YARN")
     yarn_service.update_config(self.YARN_SERVICE_CONFIG)

     while True:
       try:
         yarn_service.create_role("{0}-rm".format(self.YARN_SERVICE_NAME), "RESOURCEMANAGER", self.master)
         break
       except Exception as e:
         print "Exception creating ResourceManager role: %s" % e     
       
     while True:
       try:
         yarn_service.create_role("{0}-jhs".format(self.YARN_SERVICE_NAME), "JOBHISTORY", self.master)
         break
       except Exception as e:
         print "Exception creating JobHistory role: %s" % e 
  
     nm = yarn_service.get_role_config_group("{0}-NODEMANAGER-BASE".format(self.YARN_SERVICE_NAME))
     nm.update_config(self.YARN_NM_CONFIG)
 
     nodemanager = 0
     for host in self.slaves:
        nodemanager += 1
        while True:
          try:
            yarn_service.create_role("{0}-nm-".format(self.YARN_SERVICE_NAME) + str(nodemanager), "NODEMANAGER", host)
            break
          except Exception as e:
            print "Exception creating NodeManager role: %s" % e 
     
     gateway = 0
     for host in [self.master]:
        gateway += 1
        while True:
          try:
            yarn_service.create_role("{0}-gw-".format(self.YARN_SERVICE_NAME) + str(gateway), "GATEWAY", host)
            break
          except Exception as e:
            print "Exception creating YARN gateway role: %s" % e
  
  def deploy_hive(self):
     hive_service = self.cluster.create_service(self.HIVE_SERVICE_NAME, "HIVE")
     hive_service.update_config(self.HIVE_SERVICE_CONFIG)
     
     hms = hive_service.get_role_config_group("{0}-HIVEMETASTORE-BASE".format(self.HIVE_SERVICE_NAME))
     hms.update_config(self.HIVE_HMS_CONFIG)
     hive_service.create_role("{0}-hms".format(self.HIVE_SERVICE_NAME), "HIVEMETASTORE", self.master)
     
     hs2 = hive_service.get_role_config_group("{0}-HIVESERVER2-BASE".format(self.HIVE_SERVICE_NAME))
     hive_service.create_role("{0}-hs2".format(self.HIVE_SERVICE_NAME), "HIVESERVER2", self.master)
     
     whc = hive_service.get_role_config_group("{0}-WEBHCAT-BASE".format(self.HIVE_SERVICE_NAME))
     hive_service.create_role("{0}-whc".format(self.HIVE_SERVICE_NAME), "WEBHCAT", self.master)
     
     gw = hive_service.get_role_config_group("{0}-GATEWAY-BASE".format(self.HIVE_SERVICE_NAME))
     
     gateway = 0
     for host in [self.master]:
        gateway += 1
        hive_service.create_role("{0}-gw-".format(self.HIVE_SERVICE_NAME) + str(gateway), "GATEWAY", host)
    
     self._init_hive(hive_service) 
  
  def _init_hive(self, hive_service):
     hive_service.create_hive_metastore_database()
     hive_service.create_hive_metastore_tables()
     hive_service.create_hive_warehouse()
     #don't think that the create_hive_userdir call is needed as the create_hive_warehouse already creates it
     #hive_service.create_hive_userdir()
  
  
  def deploy_impala(self):
     impala_service = self.cluster.create_service(self.IMPALA_SERVICE_NAME, "IMPALA")
     impala_service.update_config(self.IMPALA_SERVICE_CONFIG)
     
     impala_service.create_role("{0}-ss".format(self.IMPALA_SERVICE_NAME), "STATESTORE", self.master)
     
     impala_service.create_role("{0}-cs".format(self.IMPALA_SERVICE_NAME), "CATALOGSERVER", self.master)
     
     impalad = 0
     for host in self.slaves:
        impalad += 1
        impala_service.create_role("{0}-id-".format(self.IMPALA_SERVICE_NAME) + str(impalad), "IMPALAD", host)
  
     return impala_service
     
  def create_cluster(self, cm_public_dns):
     self.api = ApiResource(cm_public_dns, version=5, username='admin', password='admin')
     self.manager = self.api.get_cloudera_manager()
     self.manager.update_config({})
     clusters = self.api.get_all_clusters()
     if len(clusters) > 0:
        print "Found existing cluster, re-using"
        self.cluster = clusters[0]
        self._clean_up_services()
     else:
        self._init_cluster()
     self._deploy_parcels()
