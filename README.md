This is a Hadoop cluster running in docker containers. The namenode and datanodes run in different containers.

The cluster by default uses data replication "2". To change it edit the hdfs-site.xml file.

To start the namenode run 

    docker run --name namenode -h bde2020/hadoop-namenode

To start two datanodes on the same host run

    docker run --name datanode1 --link namenode:namenode bde2020/hadoop-datanode
    docker run --name datanode2 --link namenode:namenode bde2020/hadoop-datanode
    
More info is comming soon on how to run hadoop docker using docker network and docker swarm

All data are stored in /hdfs-data, so to store data in a host directory datanodes as

    docker run --name datanode1 --link namenode:namenode -v /path/to/host:/hdfs-data bde2020/hadoop-datanode
    docker run --name datanode2 --link namenode:namenode -v /path/to/host:/hdfs-data bde2020/hadoop-datanode

By default the namenode formats the namenode directory only if not exists (hdfs namenode -format -nonInteractive). 
If you want to mount an external directory that already contains a namenode directory and format it you have to first delete it manually.

Hadoop namenode listens on 

    hdfs://namenode:8020
    
To access the namenode from another container link it using "--link namenode:namenode" and then use the afformentioned URL.
More info on how to access it using docker network coming soon.
