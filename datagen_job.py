"""
Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.

Except as expressly permitted in a written agreement between you or your company
and Hortonworks, Inc. or an authorized affiliate or partner thereof, any use,
reproduction, modification, redistribution, sharing, lending or other exploitation
of all or any part of the contents of this software is strictly prohibited.
"""

import numpy as np
#import exrex
import random
from pyspark.sql import *
from util import Util


class Generator:
    def __init__(self):
        pass

    @staticmethod
    def generate_integer_list(seed, num_values_to_generate, col_schema):
        np.random.seed(seed)
        low, high = col_schema["range"]
        distribution = col_schema["distribution"]
        if distribution == "uniform":
            result_list = [elem.item() for elem in np.random.uniform(low, high, num_values_to_generate)]
            return result_list
        else:
            return [0] * num_values_to_generate

    @staticmethod
    def generate_float_list(seed, num_values_to_generate, col_schema):
        np.random.seed(seed)
        mu = col_schema["mean"]
        sigma = col_schema["stddev"]
        distribution = col_schema["distribution"]
        if distribution == "normal":
            result_list = [elem.item() for elem in np.random.normal(mu, sigma, num_values_to_generate)]
            return result_list
        else:
            return [0.0] * num_values_to_generate

    @staticmethod
    def generate_bool_list(seed, m):
        random.seed(seed)
        bool_list = []
        for j in range(m):
            x = random.getrandbits(1)
            if x == 1:
                bool_list.append(True)
            else:
                bool_list.append(False)
        return bool_list


    @staticmethod
    def generate_string_list(seed, num_values_to_generate, col_schema):
        return ["random" for i in range(num_values_to_generate)]
        #return [exrex.getone(col_schema["matching_regex"][0]) for i in range(num_values_to_generate)]


def aggregate_func(seed, schema, num_rows_per_executor):
    row_list = []
    for col_schema in schema["columns"]:

        if col_schema["type"] == "int":
            col = Generator.generate_integer_list(seed, num_rows_per_executor, col_schema)
            row_list.append(col)

        elif col_schema["type"] == "float":
            col = Generator.generate_float_list(seed, num_rows_per_executor, col_schema)
            row_list.append(col)

        elif col_schema["type"] == "string":
            col = Generator.generate_string_list(seed, num_rows_per_executor, col_schema)
            row_list.append(col)

        elif col_schema["type"] == "boolean":
            col = Generator.generate_bool_list(seed, num_rows_per_executor)
            row_list.append(col)

    name_list = []
    for i in schema["columns"]:
        name_list.append(i["name"])

    inv_row_list = np.transpose(row_list)
    row_gen = Row(*name_list)
    np_array = np.array(inv_row_list)
    A = np.apply_along_axis(Util.row_convert_func, 1, np_array, row_gen)
    unboxer = lambda t: t.row
    df_row_list = [unboxer(i) for i in A]
    return df_row_list

if __name__ == "__main__":
    spark = SparkSession.builder.appName("DataGenerationApp").enableHiveSupport().getOrCreate()

    schema = Util.extract_schema("datagen_schema_config.json")
    num_rows_per_executor = int(schema["table"]["number_of_rows"]/schema["parallelism"])
    seed = range(int(schema["parallelism"]))
    seed_rdd = spark.sparkContext.parallelize(seed).repartition(len(seed))
    rdd = seed_rdd.flatMap(lambda x: aggregate_func(x, schema, num_rows_per_executor))
    df = spark.createDataFrame(rdd)
    df.createOrReplaceTempView("tempTable")
    hive_table_name = schema["table"]["hive_table_name"]
    spark.sql("create table " + hive_table_name + " as select * from tempTable")
    spark.stop()
    #df.write.mode("overwrite").saveAsTable(hive_table_name)