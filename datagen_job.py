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
    int_list = []
    int_range = sub_schema["range"]
    distribution = sub_schema["distribution"]
    if distribution == "uniform":
        int_list = np.random.uniform(int_range[0], int_range[1], nb)
    for o in range(len(int_list)):
        int_list[o] = int(int_list[o].item())
    return int_list

# function for random floats
def float_gen(seed, nb,sub_schema):
    np.random.seed(seed)
    float_list = []
    mu =sub_schema["mean"]
    sigma = sub_schema["stddev"]
    distribution = sub_schema["distribution"]
    if distribution == "normal":
        float_list = np.random.normal(mu, sigma, nb)

    to_native_type = lambda x: x.item()
    final_float_list = [to_native_type(i) for i in float_list]
    return final_float_list

# function for random bools
def bool_gen(seed, m):
    random.seed(seed)
    bool_list = []
    for j in range(m):
        x = random.getrandbits(1)
        if x==1:
            bool_list.append(True)
        else:
            bool_list.append(False)
    return bool_list

# function for random strings
def str_gen(seed,m, subschema):
    random.seed(seed)
    x = Xeger(limit=subschema["limit"])
    str_list = []
    for j in range(m):
        str_list.append(x.xeger(str(subschema["matching_regex"][0])))
    return str_list

# read external configuration
def extract_schema():
    with open("datagen_schema_config_new.json") as json_data:
        schema = json.load(json_data)
    return schema

# build and return sub-configuration for each executor
def build_and_extract_subschema():
    schema = extract_schema()
    # subschema for aggregate_function
    sub_schema = {}

    sub_schema["n_rows_per_exec"] = int(schema["table"]["n_rows"]/schema["Parallelism"]["no_of_executors"])
    sub_schema["column"] = schema["columns"]
    return sub_schema

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
    row_list = []

    for x in sub_schema["column"]:
        if(x["type"] == "Int"):
            col = int_gen(seed, sub_schema["n_rows_per_exec"], x)
            row_list.append(col.tolist())
        if(x["type"] == "Float"):
            col = float_gen(seed,sub_schema["n_rows_per_exec"],x)
            row_list.append(col)
        if(x["type"] == "String"):
            col = str_gen(seed,sub_schema["n_rows_per_exec"],x)
            row_list.append(col)
        if(x["type"] == "Boolean"):
            col = bool_gen(seed,sub_schema["n_rows_per_exec"])
            row_list.append(col)

    name_list = []
    for i in sub_schema["column"]:
        name_list.append(i["name"])
    inv_row_list = np.transpose(row_list)
    row_gen = Row(*name_list)
    np_array = np.array(inv_row_list)
    A = np.apply_along_axis(row_convert_func, 1, np_array, row_gen)
    unboxer = lambda t: t.row
    df_row_list = [unboxer(i) for i in A]
    return df_row_list

def main():
    conf = SparkConf().setAppName("DataGenerationApp")
    spark = SparkSession.builder \
        .appName("DataGenerationApp") \
        .enableHiveSupport() \
        .getOrCreate()

    sub_schema = build_and_extract_subschema()
    sch = extract_schema()
    seed = range(int(sch["Parallelism"]["no_of_executors"]))
    seed = spark.sparkContext.parallelize(seed)                 # CREATE RDD OF SEEDS
    RDD = seed.flatMap(lambda x: aggregate_func(x, sub_schema))  #TRANSFORM EACH SEED TO row_list OF RANDOM DATA
    df = spark.createDataFrame(RDD)
    HIVE_TABLE_NAME = sch["Name"]["hive_table_name"]        # READ HIVE TABLE LOCATION
    df.write.mode("overwrite").saveAsTable(HIVE_TABLE_NAME)     # STORE RESULTS IN HIVE TABLE

if __name__ == "__main__":
    main()