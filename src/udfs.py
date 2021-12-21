from pyspark.sql.functions import udf
from pyspark.sql.types import *
import re, unicodedata
import patterns
import math
@udf(returnType=ArrayType(StringType()))
def extract_framework_plattform(mo_ta_cong_viec: str,yeu_cau_ung_vien: str):
    return [framework for framework in patterns.framework_plattforms if re.search(framework, mo_ta_cong_viec + " " + yeu_cau_ung_vien, re.IGNORECASE)]

@udf(returnType=ArrayType(StringType()))
def extract_language(mo_ta_cong_viec: str,yeu_cau_ung_vien: str):
    return [language for language in patterns.languages if re.search(language.replace("+", "\+").replace("(", "\(").replace(")", "\)"), mo_ta_cong_viec + " " + yeu_cau_ung_vien, re.IGNORECASE)]

@udf(returnType=ArrayType(StringType()))
def extract_knowledge(mo_ta_cong_viec: str,yeu_cau_ung_vien: str):
    return [knowledge for knowledge in patterns.knowledges if re.search(knowledge, mo_ta_cong_viec + " " + yeu_cau_ung_vien, re.IGNORECASE)]

def broadcast_labeled_knowledges(sc,labeled_knowledges):
    global mapped_knowledge
    mapped_knowledge = sc.broadcast(labeled_knowledges)

@udf(returnType=StringType())
def labeling_knowledge(knowledge: str):
    try :
        return mapped_knowledge.value[knowledge]
    except :
        return None

@udf(returnType=ArrayType(StringType()))
def extract_design_pattern(mo_ta_cong_viec: str,yeu_cau_ung_vien: str):
    return [design_pattern for design_pattern in patterns.design_patterns if re.search(design_pattern, mo_ta_cong_viec + " " + yeu_cau_ung_vien, re.IGNORECASE)]

@udf(returnType=ArrayType(ArrayType(IntegerType())))
def normalize_salary(quyen_loi:str):
    def extract_salary(quyen_loi: str):
        salaries = []
        for pattern in patterns.salary_patterns:
            salaries.extend(re.findall(pattern, unicodedata.normalize('NFKC', quyen_loi), re.IGNORECASE))
        return salaries

    def dollar_to_vnd(dollar:int):
        return math.floor(dollar*23/1000)

    def dollar_handle(currency:str):
        if not currency.__contains__("$"):
            if not currency.__contains__("USD"):
                if not currency.__contains__("usd"):
                    return None
                else :
                    ext_curr= currency.replace("usd","")
            ext_curr = currency.replace("USD","")
        elif (currency.startswith("$")):
            ext_curr = currency[1:]
        else :
            ext_curr = currency[:-1]
        ext_curr= ext_curr.replace(".","")
        try :
            # print("try converting ",ext_curr)
            val_curr = int(ext_curr)
            return [dollar_to_vnd(val_curr)]
        except ValueError:
            return [0]

    def normalize_vnd(vnd:str):
        mill = "000000"
        norm_vnd = vnd.replace("triệu",mill).replace("Triệu",mill)\
        .replace("TRIỆU",mill).replace("m",mill).replace("M",mill)\
        .replace(".","").replace(" ","").replace(",","")
        try :
            # print("Norm = ",norm_vnd)
            vnd = math.floor(int(norm_vnd)/1000000)
            return vnd
        except ValueError:
            # print("Value Error while converting ",norm_vnd)
            return None

    def vnd_handle(ori_range_list:list):
        vnd_range_list=[]

        if (len(ori_range_list)==1):
            sal = normalize_vnd(ori_range_list[0])
            if sal!=None:
                vnd_range_list=[sal]
        else :
            try :
                start = int(ori_range_list[0].strip().replace(".","").replace(",",""))
                end = normalize_vnd(ori_range_list[1])
                if end!=None :
                    vnd_range_list=[sal for sal in range(start,end+1)]
                else :
                    print("Error converting end ",ori_range_list[1]," with start ",ori_range_list[0])
            except ValueError:
                print("Error Converting Start ",ori_range_list[0]," with end ",ori_range_list[1])
        return vnd_range_list

    def salary_handle(currency:str):
        range_val = dollar_handle(currency)
        # print("DollarHandle ",currency," and get ",range_val)
        if (range_val == None):
            splitted_currency = currency.strip().strip("-").split("-")
            range_val = vnd_handle(splitted_currency)
            # print("VNDHandle ",currency," and get ",range_val)
        return range_val

    salaries = extract_salary(quyen_loi)
    range_salary_set=set()
    for sal in salaries:
        range_sal = salary_handle(sal)
        if range_sal!=None and range_sal!=[0]:
            range_salary_set.add(tuple(range_sal))
    return [sal_range for sal_range in range_salary_set]