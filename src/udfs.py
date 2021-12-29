# coding=utf-8
from pyspark.sql.functions import udf
from pyspark.sql.types import *
import re, unicodedata
import patterns
import math
@udf(returnType=ArrayType(StringType()))
def extract_framework_plattform(mo_ta_cong_viec,yeu_cau_ung_vien):
    return [framework for framework in patterns.framework_plattforms if re.search(framework, mo_ta_cong_viec + " " + yeu_cau_ung_vien, re.IGNORECASE)]

@udf(returnType=ArrayType(StringType()))
def extract_language(mo_ta_cong_viec,yeu_cau_ung_vien):
    return [language for language in patterns.languages if re.search(language.replace("+", "\+").replace("(", "\(").replace(")", "\)"), mo_ta_cong_viec + " " + yeu_cau_ung_vien, re.IGNORECASE)]

@udf(returnType=ArrayType(StringType()))
def extract_knowledge(mo_ta_cong_viec,yeu_cau_ung_vien):
    return [knowledge for knowledge in patterns.knowledges if re.search(knowledge, mo_ta_cong_viec + " " + yeu_cau_ung_vien, re.IGNORECASE)]

def broadcast_labeled_knowledges(sc,labeled_knowledges):
    '''
    broadcast the mapped of labeled_knowledges to group data in knowledge field
    '''
    global mapped_knowledge
    mapped_knowledge = sc.broadcast(labeled_knowledges)

@udf(returnType=StringType())
def labeling_knowledge(knowledge):
    try :
        return mapped_knowledge.value[knowledge]
    except :
        return None

@udf(returnType=ArrayType(StringType()))
def extract_design_pattern(mo_ta_cong_viec,yeu_cau_ung_vien):
    return [design_pattern for design_pattern in patterns.design_patterns if re.search(design_pattern, mo_ta_cong_viec + " " + yeu_cau_ung_vien, re.IGNORECASE)]

@udf(returnType=ArrayType(IntegerType()))
def normalize_salary(quyen_loi):
    BIN_SIZE=5
    def extract_salary(quyen_loi):
        '''
        Return a list of salary patterns found in raw data

        Parameters
        ----------
        quyen_loi : quyen_loi field in raw data
        '''
        salaries = []
        for pattern in patterns.salary_patterns:
            salaries.extend(re.findall(pattern, unicodedata.normalize('NFKC', quyen_loi), re.IGNORECASE))
        return salaries

    def sal_to_bin_list(sal):
        '''
        Return a list of bin containing salary value

        Parameters
        ----------
        sal : salary value
        '''
        sal = int(sal/BIN_SIZE)
        if sal<int(100/BIN_SIZE):
            return [BIN_SIZE*sal]
        else :
            return [100]

    def range_to_bin_list(start, end):
        '''
        Return a list of bin containing salary range

        Parameters
        ----------
        start : the start of salary range
        end : the end of salary range
        '''
        start = int(start/BIN_SIZE)
        end = int(end/BIN_SIZE)
        if end >= int(100/BIN_SIZE):
            end=int(100/BIN_SIZE)
        return [BIN_SIZE*i for i in range(start,end+1)]


    def dollar_to_vnd(dollar):
        '''
        Return a list of bin containing salary value

        Parameters
        ----------
        dollar : salary value in dollar unit
        '''
        return sal_to_bin_list(math.floor(dollar*23/1000))

    def dollar_handle(currency):
        '''
        Handle currency
        If currency is in dollar unit, returns the salary bins
        Otherwise returns None

        Parameter
        ---------
        currency : string of salary pattern
        '''
        if not currency.__contains__("$"):
            if not currency.__contains__("USD"):
                if not currency.__contains__("usd"):
                    return None
                else :
                    ext_curr= currency.replace("usd","")
            else :
                ext_curr = currency.replace("USD","")
        elif (currency.startswith("$")):
            ext_curr = currency[1:]
        else :
            ext_curr = currency[:-1]
        ext_curr= ext_curr.replace(".","")
        try :
            val_curr = int(ext_curr)
            return dollar_to_vnd(val_curr)
        except ValueError:
            return None

    def normalize_vnd(vnd):
        '''
        Return normalized currency in VND unit
        Normalize currency is a string of currency in milion VND unit
        The postfix such as Triệu, triệu, M, m,... is removed

        Parameters
        ----------
        vnd : string of salary in vnd unit
        '''
        mill = "000000"
        norm_vnd = vnd.encode('utf-8').replace("triệu",mill).replace("Triệu",mill)\
        .replace("TRIỆU",mill).replace("m",mill).replace("M",mill)\
        .replace(".","").replace(" ","").replace(",","")
        try :
            vnd = math.floor(int(norm_vnd)/1000000)
            return vnd
        except ValueError:
            print("Value Error while converting ",norm_vnd)
            return None

    def vnd_handle(ori_range_list):
        '''
        Handle currency, returns the salary bins
        The currency must be preprocessed and returned None by dollar_handle()
        The currency must be stripped and splitted by "-" to become a list
        
        Parameters
        ----------
        ori_range_list : the range of salary (a list containing at most 2 element)
        '''
        if (len(ori_range_list)==1):
            sal = normalize_vnd(ori_range_list[0])
            if sal!=None:
                return sal_to_bin_list(sal)
        else :
            try :
                start = int(ori_range_list[0].strip().replace(".","").replace(",",""))
                end = normalize_vnd(ori_range_list[1])
                if end!=None :
                    return range_to_bin_list(start,end)
                else :
                    print("Error converting end ",ori_range_list[1]," with start ",ori_range_list[0])
            except ValueError:
                print("Error Converting Start ",ori_range_list[0]," with end ",ori_range_list[1])
        # return [0]*11
        return None

    def salary_handle(currency):
        '''
        Handle currency
        Return salary bin

        Parameters
        ----------
        currency : a string
        '''
        range_val = dollar_handle(currency)
        if (range_val == None):
            splitted_currency = currency.strip().strip("-").split("-")
            range_val = vnd_handle(splitted_currency)
        return range_val

    salaries = extract_salary(quyen_loi)
    bin_set = set()
    for sal in salaries:
        sal_bins = salary_handle(sal)
        if sal_bins!= None and sal_bins!=[]:
            bin_set = bin_set.union(tuple(sal_bins))
    return sorted(list(bin_set))