import json
# def save_dataframes_to_hdfs(path,config,data_dfs,target_file_names):
#     """
#         Function to store dataframe in hdfs
        
#         Input:
        
#         path: the directory path to store dataframe to
#         config: Config object
#         data_dfs: list of PySpark DataFrames to write
#         target_file_names: list of file names to store dataframes by        
#     """

#     for data_df,target_file_name in zip(data_dfs,target_file_names):
#         print(f"Processing file: {target_file_name}")
#         data_df.write.format("csv").mode("overwrite").save(f"{config.hdfs_datanode_host}/{path}/{target_file_name}")

def save_dataframes_to_elasticsearch(dataframes,indices,es_write_config):
    """
       Helper function to save PySpark DataFrames to elasticsearch cluster

       Parameters
       ----------

       dataframes: list of all PySpark DataFrames
       indices: list of elasticsearch indices
       es_write_config: dict of elasticsearch write config
    """

    for dataframe,index in zip(dataframes,indices):
        print(f"Processing index:{index}")

        es_write_config['es.resource'] = index

        rdd_ = dataframe.rdd
        rdd_.map(lambda row: (None, \
                              json.dumps(row.asDict()))) \
                              .saveAsNewAPIHadoopFile(path='-', \
                              outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat", \
                              keyClass="org.apache.hadoop.io.NullWritable", \
                              valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable", \
                              conf=es_write_config)
