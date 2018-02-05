# Changes

Version 2.0.0 introduces uses wait_for_it script for the cluster startup

# Hadoop Docker

To deploy an example HDFS cluster, run:
```
  docker-compose up
```

Or deploy in swarm:
```
docker stack deploy -c docker-compose-v3.yml hadoop
```

The configuration parameters can be specified in the hadoop.env file or as environmental variables for specific services (e.g. namenode, datanode etc.):
```
  CORE_CONF_fs_defaultFS=hdfs://namenode:8020
```

CORE_CONF corresponds to core-site.xml. fs_defaultFS=hdfs://namenode:8020 will be transformed into:
```
  <property><name>fs.defaultFS</name><value>hdfs://namenode:8020</value></property>
```
To define dash inside a configuration parameter, use double underscore, such as YARN_CONF_yarn_log___aggregation___enable=true (yarn-site.xml):
```
  <property><name>yarn.log-aggregation-enable</name><value>true</value></property>
```

The available configurations are:
* /etc/hadoop/core-site.xml CORE_CONF
* /etc/hadoop/hdfs-site.xml HDFS_CONF
* /etc/hadoop/yarn-site.xml YARN_CONF
* /etc/hadoop/httpfs-site.xml HTTPFS_CONF
* /etc/hadoop/kms-site.xml KMS_CONF
* /etc/hadoop/mapred-site.xml  MAPRED_CONF

If you need to extend some other configuration file, refer to base/entrypoint.sh bash script.

After starting the example Hadoop cluster, you should be able to access interfaces of all the components (substitute domain names by IP addresses from ```network inspect dockerhadoop_default``` command):
* Namenode: http://namenode:50070/dfshealth.html#tab-overview
* History server: http://historyserver:8188/applicationhistory
* Datanode: http://datanode:50075/
* Nodemanager: http://nodemanager:8042/node
* Resource manager: http://resourcemanager:8088/
