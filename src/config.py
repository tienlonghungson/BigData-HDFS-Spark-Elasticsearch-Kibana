from pyspark.sql import SparkSession

class Config:
    def __init__(self,
                elasticsearch_host,
                elasticsearch_port,
                # hdfs_datanode_host,
                # hdfs_user
                 ):
        self.elasticsearch_conf = {
            'es.nodes': elasticsearch_host,
            'es.port': elasticsearch_port
        }
        # self.hdfs_datanode_host = hdfs_datanode_host
        # self.hdfs_user = hdfs_user
        self.spark_app = None
        

    def initialize_spark_session(self,appName):
        if self.spark_app == None :
            self.spark_app = (SparkSession
                        .builder.master("local[*]")
                        .appName(appName)
                        .config("spark.jars","/content/elasticsearch-hadoop-7.15.1.jar")
                        .config("spark.driver.extraClassPath","/content/elasticsearch-hadoop-7.15.1.jar")
                        .getOrCreate())
        return self.spark_app