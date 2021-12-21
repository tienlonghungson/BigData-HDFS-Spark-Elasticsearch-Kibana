import pyspark
from pyspark.sql.functions import explode
import udfs

def get_counted_company(extracted_recruit_df:pyspark.sql.DataFrame):
# def get_counted_company(extracted_recruit_df):
    return extracted_recruit_df.groupBy(extracted_recruit_df["CompanyName"])\
          .count()\
          .orderBy("count", ascending=False)

def get_counted_framework_plattform(extracted_recruit_df:pyspark.sql.DataFrame):
# def get_counted_framework_plattform(extracted_recruit_df):
    return extracted_recruit_df.withColumn("FrameworkPlattform", explode("FrameworkPlattforms"))\
          .select("FrameworkPlattform")\
          .groupBy("FrameworkPlattform")\
          .count()\
          .orderBy("count",ascending=False)

def get_counted_design_pattern(extracted_recruit_df:pyspark.sql.DataFrame):
# def get_counted_design_pattern(extracted_recruit_df):
    return extracted_recruit_df.withColumn("DesignPattern", explode("DesignPatterns"))\
          .select("DesignPattern")\
          .groupBy("DesignPattern")\
          .count()\
          .orderBy("count",ascending=False)

def get_counted_language(extracted_recruit_df:pyspark.sql.DataFrame):
# def get_counted_language(extracted_recruit_df):
    return extracted_recruit_df.withColumn("Language", explode("Languages"))\
          .select("Language")\
          .groupBy("Language")\
          .count()\
          .orderBy("count",ascending=False)

def get_counted_knowledge(extracted_recruit_df:pyspark.sql.DataFrame):
# def get_counted_knowledge(extracted_recruit_df):
    return extracted_recruit_df.withColumn("Knowledge", explode("Knowledges"))\
          .select("Knowledge")\
          .groupBy("Knowledge")\
          .count()\
          .orderBy("count",ascending=False)

# from pyspark.sql import functions as f
def get_grouped_knowledge(knowledge_df:pyspark.sql.DataFrame):
    return knowledge_df.withColumn('category', udfs.labeling_knowledge('Knowledge'))\
          .groupBy('category').sum("count").filter("category!='null'")

def get_salary_distribution(extracted_recruit_df:pyspark.sql.DataFrame):
# def get_salary_distribution(extracted_recruit_df):
    nested_salary_df=extracted_recruit_df.withColumn("SalaryRange", explode("Salaries"))\
                      .select("SalaryRange")
    return nested_salary_df.withColumn("Salary",explode("SalaryRange"))\
          .select("Salary")\
          .groupBy("Salary")\
          .count()\
          .orderBy("count",ascending=False)

def get_lang_sal_distr(extracted_recruit_df:pyspark.sql.DataFrame):
# def get_lang_sal_distr(extracted_recruit_df):
    lang_sal_dict = extracted_recruit_df.select("Languages","Salaries").collect()
    lang_sal_dist = dict()
    for row in lang_sal_dict:
        row_list = list(row)
        for lang in row_list[0]:
            for sal_range in row_list[1]:
                LEN = len(sal_range)
                if LEN>0:
                    # LEN_1 = 1/LEN
                    for sal in sal_range:
                        try :
                            # lang_sal_dist[lang][str(sal)]+=LEN_1
                            lang_sal_dist[lang][str(sal)]+=1
                        except KeyError:
                            if not (lang in lang_sal_dist):
                                lang_sal_dist[lang] = dict()
                            # lang_sal_dist[lang][str(sal)] = LEN_1
                            lang_sal_dist[lang][str(sal)] = 1
    return lang_sal_dist