from pyspark.sql import SparkSession

class Config:
    def __init__(self,
                elasticsearch_host,
                elasticsearch_port,
                elasticsearch_input_json,
                elasticsearch_nodes_wan_only,
                hdfs_namenode
                 ):
        self.elasticsearch_conf = {
            'es.nodes': elasticsearch_host,
            'es.port': elasticsearch_port,
            "es.input.json":elasticsearch_input_json,
            "es.nodes.wan.only": elasticsearch_nodes_wan_only
        }
        self.hdfs_namenode = hdfs_namenode
        self.spark_app = None
        
    def get_elasticsearch_conf(self):
        return self.elasticsearch_conf

    def get_hdfs_namenode(self):
        return self.hdfs_namenode

    def initialize_spark_session(self,appName):
        if self.spark_app == None :
            self.spark_app = (SparkSession
                        .builder.master("spark://spark-master:7077")
                        .appName(appName)
                        .config("spark.jars","/elasticsearch-hadoop-7.15.1.jar")
                        .config("spark.driver.extraClassPath","/elasticsearch-hadoop-7.15.1.jar")
                        .config("spark.es.nodes",self.elasticsearch_conf["es.nodes"])
                        .config("spark.es.port",self.elasticsearch_conf["es.port"])
                        .config("spark.es.nodes.wan.only",self.elasticsearch_conf["es.nodes.wan.only"])
                        .getOrCreate())
        return self.spark_app