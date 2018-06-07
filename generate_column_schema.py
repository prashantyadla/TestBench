import json
import copy
import argparse

def generate_columns(num_columns):
    file_directory = "/Users/visharma/Hortonworks/Data_Plane/TestBench/datagen_schema_config.json"
    with open(file_directory) as f:
        data = json.load(f)

    col_template = data["columns"]
    generated_columns = []

    j = 0
    for x in range(num_columns):
        col_copy = copy.deepcopy(col_template[j])
        col_copy["name"] = "random_col_" + str(x)
        generated_columns.append(col_copy)
        j = (j + 1) % 4

    data["columns"] = generated_columns

    with open("schema_scale_testing.json","w") as outfile:
        json.dump(data, outfile)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--num_columns", type=int, help="Number of columns to generate")
    args = parser.parse_args()
    num_columns = args.num_columns
    generate_columns(num_columns)