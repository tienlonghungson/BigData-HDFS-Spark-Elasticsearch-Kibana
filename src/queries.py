import pyspark
from pyspark.sql.functions import explode
import udfs

def get_counted_framework_plattform(extracted_recruit_df:pyspark.sql.DataFrame):
    '''
    Return a dataframe of 2 columns:
    Col1 (FrameworkPlattform): StringType, name of framework and plattform
    Col2 (count): IntegerType, number of appearances for each framework and plattform

    Parameters
    ----------
    extracted_recruit_df : orginal dataframe
    '''
    return extracted_recruit_df.withColumn("FrameworkPlattform", explode("FrameworkPlattforms"))\
          .select("FrameworkPlattform")\
          .groupBy("FrameworkPlattform")\
          .count()\
          .orderBy("count",ascending=False)

def get_counted_design_pattern(extracted_recruit_df:pyspark.sql.DataFrame):
    '''
    Return a dataframe of 2 columns:
    Col1 (DesignPattern): StringType, name of design pattern
    Col2 (count): IntegerType, number of appearances for each design pattern

    Parameters
    ----------
    extracted_recruit_df : orginal dataframe
    '''
    return extracted_recruit_df.withColumn("DesignPattern", explode("DesignPatterns"))\
          .select("DesignPattern")\
          .groupBy("DesignPattern")\
          .count()\
          .orderBy("count",ascending=False)

def get_counted_language(extracted_recruit_df:pyspark.sql.DataFrame):
    '''
    Return a dataframe of 2 columns:
    Col1 (Language): StringType, name of language
    Col2 (count): IntegerType, number of appearances for each language

    Parameters
    ----------
    extracted_recruit_df : orginal dataframe
    '''
    return extracted_recruit_df.withColumn("Language", explode("Languages"))\
          .select("Language")\
          .groupBy("Language")\
          .count()\
          .orderBy("count",ascending=False)

def get_counted_knowledge(extracted_recruit_df:pyspark.sql.DataFrame):
    '''
    Return a dataframe of 2 columns:
    Col1 (Knowledge): StringType, name of knowledge
    Col2 (count): IntegerType, number of appearances for each knowledge

    Parameters
    ----------
    extracted_recruit_df : orginal dataframe
    '''
    return extracted_recruit_df.withColumn("Knowledge", explode("Knowledges"))\
          .select("Knowledge")\
          .groupBy("Knowledge")\
          .count()\
          .orderBy("count",ascending=False)

def get_grouped_knowledge(knowledge_df:pyspark.sql.DataFrame):
    '''
    Return a dataframe of 2 columns:
    Col1 (Category): StringType, name of category of knowledge
    Col2 (count): IntegerType, number of appearances for each category of knowledge

    Parameters
    ----------
    knowledge_df : dataframe of knowledges
    '''
    return knowledge_df.withColumn('Category', udfs.labeling_knowledge('Knowledge'))\
          .groupBy('Category').sum("count").filter("Category!='null'")

def get_company_language_salary(extracted_recruit_df:pyspark.sql.DataFrame):
    '''
    Return a dataframe of 3 columns:
    Col1 (CompanyNames): StringType, name of company
    Col2 (Languages): StringType, name of programming language
    Col3 (Salaries) : ArrayType(IntegerType) : list of salary bins

    Parameters
    ----------
    knowledge_df : dataframe of knowledges
    '''
    return extracted_recruit_df.select("CompanyName","Languages","Salaries")