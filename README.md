# Hadoop Docker

This repository provides Hadoop in Docker containers. You can either run Hadoop in a single node or create a cluster.

The deployed Hadoop uses data replication "2". To change it edit the hdfs-site.xml file.

All data are stored in /hdfs-data, so to store data in a host directory run the container using "-v /path/to/host:/hdfs-data".
By default the container formats the namenode directory only if not exists (hdfs namenode -format -nonInteractive).
If you want to mount an external directory that already contains a namenode directory and format it you have to first delete it manually.

## Single node mode

To deploy a single Hadoop node run

    docker run -h namenode bde2020/hadoop-base

To store data in a host directory run the container as as

    docker run -h namenode -v /path/to/host:/hdfs-data bde2020/hadoop-base

## Cluster mode

The namenode runs in a seperate container than the datanodes.

To start the namenode run 

    docker run --name namenode -h namenode bde2020/hadoop-namenode

To add a datanode to the cluster run

    docker run --link namenode:namenode bde2020/hadoop-datanode

Use the same command to add more datanodes to the cluster

More info is comming soon on how to deploy a Hadoop cluster using docker network and docker swarm

# access the namenode

The namenode listens on 

    hdfs://namenode:8020
    
To access the namenode from another container link it using "--link namenode:namenode" and then use the afformentioned URL.
More info on how to access it using docker network coming soon.
