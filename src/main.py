import pyspark
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

from operator import add
import sys,os
from pyspark.sql.types import *
# import json
import patterns, udfs , queries, config, save_to_cluster

schema = StructType([
      StructField("name",StringType(),True),
      StructField("Mô tả công việc",StringType(),True),
      StructField("Yêu cầu ứng viên",StringType(),True),
      StructField("Quyền lợi",StringType(),True),
      StructField("Cách thức ứng tuyển",StringType(),True)
  ])

if __name__ == "__main__":
    
    APP_NAME="PreprocessData"
    # spark = SparkSession.builder.master("local[*]").appName(APP_NAME).config("spark.es.nodes","elasticsearch").config("spark.es.port","9200").config("spark.es.nodes.wan.only","true").getOrCreate()
    app_config = config.Config(elasticsearch_host="localhost",
                               elasticsearch_port="9200",
                               elasticsearch_input_json="yes",
                               elasticsearch_nodes_wan_only="False")
    spark = app_config.initialize_spark_session(APP_NAME)
    sc = spark.sparkContext
    sc.addPyFile(os.path.dirname(__file__)+"/patterns.py")
    
    raw_recruit_df = spark.read.schema(schema).option("multiline","true").json("/content/drive/MyDrive/BigData/Project/Data/*.json")
    extracted_recruit_df=raw_recruit_df.select(raw_recruit_df["name"].alias("CompanyName"),
          udfs.extract_framework_plattform("Mô tả công việc","Yêu cầu ứng viên").alias("FrameworkPlattforms"),
          udfs.extract_language("Mô tả công việc","Yêu cầu ứng viên").alias("Languages"),
          udfs.extract_design_pattern("Mô tả công việc","Yêu cầu ứng viên").alias("DesignPatterns"),
          udfs.extract_knowledge("Mô tả công việc","Yêu cầu ứng viên").alias("Knowledges"),
          udfs.normalize_salary("Quyền lợi").alias("Salaries")
          )
    extracted_recruit_df.cache()
    extracted_recruit_df.show(5)
    
    framework_plattform_df = queries.get_counted_framework_plattform(extracted_recruit_df)
    framework_plattform_df.cache()
    framework_plattform_df.show(5)

    design_pattern_df = queries.get_counted_design_pattern(extracted_recruit_df)
    design_pattern_df.cache()
    design_pattern_df.show(5)

    lang_df = queries.get_counted_language(extracted_recruit_df)
    lang_df.cache()
    lang_df.show(5)

    knowledge_df = queries.get_counted_knowledge(extracted_recruit_df)
    knowledge_df.cache()
    knowledge_df.show(5)

    udfs.broadcast_labeled_knowledges(sc,patterns.labeled_knowledges)
    grouped_knowledge_df = queries.get_grouped_knowledge(knowledge_df)
    grouped_knowledge_df.cache()
    grouped_knowledge_df.show()

    company_language_salary_df = queries.get_company_language_salary(extracted_recruit_df)
    company_language_salary_df.cache()
    company_language_salary_df.show(5)

    df_to_elasticsearch=(company_language_salary_df,
                         framework_plattform_df,
                         design_pattern_df,
                         lang_df,
                         knowledge_df,
                         grouped_knowledge_df,
                         sal_df)
    
    df_es_indices = ("companies_languages_salaries",
                     "frameworks_plattforms",
                     "design_patterns",
                     "languages",
                     "knowledges",
                     "grouped_knowledges")
    
    save_to_cluster.save_dataframes_to_elasticsearch(df_to_elasticsearch,df_es_indices,app_config.get_elasticsearch_conf())
