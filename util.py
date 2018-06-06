import json
class Util:
    def __init__(self):
        pass

    @staticmethod
    def extract_schema(file_name):
        with open(file_name) as json_data:
            schema = json.load(json_data)
        return schema

    @staticmethod
    def row_convert_func(a, row_gen):
        temp = []
        for i in range(len(a)):
            temp.append(a[i].item())
        b = row_gen(*temp)
        return WrapperClass(b)

class WrapperClass:
    def __init__(self,row):
            self.row = row
