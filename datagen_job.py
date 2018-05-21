'''
PYSPARK APPLICATION FOR DISTRIBUTED RANDOM DATA GENERATION
'''

'''
Created by 
pyadla@hortonworks.com
'''

# import dependent modules
from pyspark import SparkConf
import numpy as np
import json
import os
from xeger import Xeger
from pyspark import SparkContext
import random
from pyspark.sql import *


# function for random integers
def int_gen(seed, nb, sub_schema):
    np.random.seed(seed)
    INT_LIST = []
    RANGE = sub_schema["range"]
    DISTRIBUTION = "uniform"
    if DISTRIBUTION == "uniform":
        INT_LIST = np.random.uniform(RANGE[0], RANGE[1], nb)
    for o in range(len(INT_LIST)):
        INT_LIST[o] = int(INT_LIST[o].item())
    return INT_LIST

# function for random floats
def float_gen(seed, nb,sub_schema):
    np.random.seed(seed)
    FLOAT_LIST = []
    mu =sub_schema["mean"]
    sigma = sub_schema["stddev"]
    DISTRIBUTION = "normal"
    if DISTRIBUTION == "normal":
        FLOAT_LIST = np.random.normal(mu, sigma, nb)

    to_native_type = lambda x: x.item()
    FINAL_FLOAT_LIST = [to_native_type(i) for i in FLOAT_LIST]
    return FINAL_FLOAT_LIST

# function for random bools
def bool_gen(seed, m):
    random.seed(seed)
    BOOL_LIST = []
    for j in range(m):
        x = random.getrandbits(1)
        if x==1:
            BOOL_LIST.append(True)
        else:
            BOOL_LIST.append(False)
    return BOOL_LIST

# function for random strings
def str_gen(seed,m, subschema):
    random.seed(seed)
    x = Xeger(limit=subschema["limit"])
    STR_LIST = []
    for j in range(m):
        STR_LIST.append(x.xeger(str(subschema["matching_regex"][0])))
    return STR_LIST

# read external configuration
def extract_schema():
    with open("datagen_schema_config.json") as json_data:
        schema = json.load(json_data)
    return schema

# read sub-configuration for each executor
def extract_subschema():
    with open("subschema.json") as json_data:
        sub_schema = json.load(json_data)
    return sub_schema

# build sub-configuration for each executor
def build_subschema():
    schema = extract_schema()
    # subschema for aggregate_function
    sub_schema = {}

    sub_schema["n_rows_per_exec"] = int(schema["table"]["n_rows"]/schema["Parallelism"]["no_of_executors"])
    sub_schema["column"] = schema["columns"]
    with open('subschema.json', 'w') as fp:
        json.dump(sub_schema, fp)


def row_convert_func(a, row_gen):
    temp = []
    for i in range(len(a)):
        temp.append(a[i].item())
    b = row_gen(*temp)
    return wrapper_class(b)

class wrapper_class:
    def __init__(self,row):
            self.row = row

# random data generating function
def aggregate_func(seed,sub_schema):
    ROW_LIST = []
    I = 0
    for x in sub_schema["column"]:
        if(x["name"] == "Integer"):
            COL1 = int_gen(seed, sub_schema["n_rows_per_exec"], sub_schema["column"][I])
            ROW_LIST.append(COL1.tolist())
            I += 1
        if(x["name"] == "Floating Point"):
            COL2 = float_gen(seed,sub_schema["n_rows_per_exec"],sub_schema["column"][I])
            ROW_LIST.append(COL2)
            I += 1
        if(x["name"] == "Strings"):
            COL3 = str_gen(seed,sub_schema["n_rows_per_exec"],sub_schema["column"][I])
            ROW_LIST.append(COL3)
            I += 1
        if(x["name"] == "Boolean"):
            COL4 = bool_gen(seed,sub_schema["n_rows_per_exec"])
            ROW_LIST.append(COL4)
            I += 1
    NAME_LIST = []
    for i in sub_schema["column"]:
        NAME_LIST.append(i["name"])
    INV_ROW_LIST = np.transpose(ROW_LIST)
    ROW_GEN = Row(*NAME_LIST)
    NP_ARRAY = np.array(INV_ROW_LIST)
    A = np.apply_along_axis(row_convert_func, 1, NP_ARRAY, ROW_GEN)
    unboxer = lambda t: t.row
    DF_ROW_LIST = [unboxer(i) for i in A]
    return DF_ROW_LIST

def main():
    conf = SparkConf().setAppName("DataGenerationApp")
    spark = SparkSession.builder \
        .appName("DataGenerationApp") \
        .enableHiveSupport() \
        .getOrCreate()

    build_subschema()
    sub_schema = extract_subschema()

    sch = extract_schema()

    seed = range(int(sch["Parallelism"]["no_of_executors"]))
    seed = spark.sparkContext.parallelize(seed)
    print(seed.count())
    RDD = seed.flatMap(lambda x: aggregate_func(x, sub_schema))
    print("RDD :- ")
    print(RDD.count())

    spark.sql('show databases').show()
    spark.sql('show tables').show()
    df = spark.createDataFrame(RDD)
    df.show()

    # df.write.csv('DATA_GEN.csv')
    df = df.withColumnRenamed("Floating Point", "FloatingPoint")
    HIVE_TABLE_NAME = sch["Name"]["hive_table_name"]
    df.write.mode("overwrite").saveAsTable(HIVE_TABLE_NAME)

    '''
    df_load = spark.sql('SELECT * FROM DATAGEN_TABLE')
    for row in df_load.collect():
        print(row)
    '''

if __name__ == "__main__":
    main()