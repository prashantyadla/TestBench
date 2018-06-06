"""
Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.

Except as expressly permitted in a written agreement between you or your company
and Hortonworks, Inc. or an authorized affiliate or partner thereof, any use,
reproduction, modification, redistribution, sharing, lending or other exploitation
of all or any part of the contents of this software is strictly prohibited.
"""

import numpy as np
import json
import exrex
import random
from pyspark.sql import *


def int_gen(seed, num_values_to_generate, col_schema):
    np.random.seed(seed)
    low, high = col_schema["range"]
    distribution = col_schema["distribution"]
    if distribution == "uniform":
        result_list = [elem.item() for elem in np.random.uniform(low, high, num_values_to_generate)]
        return result_list
    else:
        return [0] * num_values_to_generate


def float_gen(seed, num_values_to_generate, col_schema):
    np.random.seed(seed)
    mu = col_schema["mean"]
    sigma = col_schema["stddev"]
    distribution = col_schema["distribution"]
    if distribution == "normal":
        result_list = [elem.item() for elem in np.random.normal(mu, sigma, num_values_to_generate)]
        return result_list
    else:
        return [0.0] * num_values_to_generate


def bool_gen(seed, m):
    random.seed(seed)
    bool_list = []
    for j in range(m):
        x = random.getrandbits(1)
        if x == 1:
            bool_list.append(True)
        else:
            bool_list.append(False)
    return bool_list


def str_gen(seed, num_values_to_generate, col_schema):
    return [exrex.getone(col_schema["matching_regex"][0]) for i in range(num_values_to_generate)]


def extract_schema():
    with open("datagen_schema_config.json") as json_data:
        schema = json.load(json_data)
    return schema


def row_convert_func(a, row_gen):
    temp = []
    for i in range(len(a)):
        temp.append(a[i].item())
    b = row_gen(*temp)
    return WrapperClass(b)


class WrapperClass:
    def __init__(self,row):
            self.row = row


def aggregate_func(seed, schema, num_rows_per_executor):
    row_list = []

    for col_schema in schema["columns"]:

        if col_schema["type"] == "int":
            col = int_gen(seed, num_rows_per_executor, col_schema)
            row_list.append(col.tolist())

        elif col_schema["type"] == "float":
            col = float_gen(seed, num_rows_per_executor, col_schema)
            row_list.append(col)

        elif col_schema["type"] == "string":
            col = str_gen(seed, num_rows_per_executor, col_schema)
            row_list.append(col)

        elif col_schema["type"] == "boolean":
            col = bool_gen(seed, num_rows_per_executor)
            row_list.append(col)

    name_list = []
    for i in schema["columns"]:
        name_list.append(i["name"])

    inv_row_list = np.transpose(row_list)
    row_gen = Row(*name_list)
    np_array = np.array(inv_row_list)
    A = np.apply_along_axis(row_convert_func, 1, np_array, row_gen)
    unboxer = lambda t: t.row
    df_row_list = [unboxer(i) for i in A]
    return df_row_list




def main():
    spark = SparkSession.builder.appName("DataGenerationApp") \
        .enableHiveSupport().getOrCreate()

    schema = extract_schema()
    num_rows_per_executor = int(schema["table"]["number_of_rows"]/schema["num_executors"])
    seed = range(int(schema["num_executors"]))
    seed_rdd = spark.sparkContext.parallelize(seed).repartition(len(seed))
    rdd = seed_rdd.flatMap(lambda x: aggregate_func(x, schema, num_rows_per_executor))
    df = spark.createDataFrame(rdd)
    hive_table_name = schema["table"]["hive_table_name"]
    df.write.mode("overwrite").saveAsTable(hive_table_name)


if __name__ == "__main__":
    main()