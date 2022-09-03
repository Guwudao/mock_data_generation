import json
import uuid
import pandas as pd
import time
import random
import datetime
import os
import numpy as np


class Mock:

    def __init__(self, total_id, data, tables):
        self.hk_ids = [uuid.uuid4().hex for i in range(total_id)]
        self.data = data
        self.tables = tables
        self.temp_date = []

    def get_random_data(self, field):
        try:
            if type(field) is dict:
                return random.randint(int(field["start"]), int(field["end"]))
            elif field == "DATE":
                return self.get_random_date()
            elif field == "EARLY_DATE":
                return self.get_random_date(is_early=True)
            elif field == "TIME":
                return self.get_random_date(is_time=True)
            elif field == "LATE_DATE":
                base = self.temp_date.pop(0)
                return self.get_random_date(base=base)
            else:
                return field[np.random.randint(0, len(field))]
        except Exception as e:
            print("error with: ", field)
            print(e)

    def get_fields_and_count(self, table):
        return self.data[table]["fields"], self.data[table]["count"]

    def get_random_date(self, is_time=False, base=0, is_early=False):
        now = datetime.datetime.now()
        start_time = (2018, 1, 1, 0, 0, 0, 0, 0, 0)
        end_time = (now.year, now.month, now.day - 1, 23, 59, 59, 0, 0, 0)
        start = int(time.mktime(start_time))
        end = int(time.mktime(end_time))
        t = random.randint(start, end)
        if is_early:
            self.temp_date.append(t)

        if is_time:
            return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(t))

        if base == 0:
            return time.strftime("%Y%m%d", time.localtime(t))
        else:
            return time.strftime("%Y%m%d", time.localtime(base + random.randint(100000, 10000000)))

    def create_dataframe(self, *args):
        table_name, fields, values = args
        print("\n" + "=" * 30 + f" {table_name} " + "=" * 30)
        path = "./data/dmp"
        if not os.path.exists(path):
            os.makedirs(path)

        dataframe = {}
        for field, value in zip(fields, values):
            dataframe[field] = value

        print(dataframe)
        pd.DataFrame(dataframe).to_csv(f"{path}/{table_name}.csv", index=False)

    def start(self):
        for table in self.tables:
            fields, count = self.get_fields_and_count(table)
            values = [self.hk_ids[0:count]]
            for field in self.data[table]["fields"]:
                temp = []
                if field == "AC":
                    continue
                else:
                    for i in range(count):
                        temp.append(self.get_random_data(self.data[table][field]))

                values.append(temp)
            self.create_dataframe(table, fields, values)


def dmp_data_generation():
    with open("dmp_config.json", "r", encoding="utf-8") as f:
        content = json.load(f)
        mock = Mock(content["total_id"], content["data"], content["tables"])
        mock.start()


def read_cdp_data(file):
    path = "./data/dmp_csv"
    if not os.path.exists(path):
        os.makedirs(path)

    df = pd.read_csv(f"./dwh_raw_sample/{file}", header=None, error_bad_lines=False)
    new_df = {}
    for i, column in enumerate(df.loc[0, 0].split("|")):
        temp = []
        for data in df.loc[1:, 0]:
            value = ""
            if len(data.split("|")) > i:
                value = data.split("|")[i]
            temp.append(value)

        print(file, temp)
        new_df[column] = temp

    pd.DataFrame(new_df).to_csv(f"./{path}/{file}", index=False)


dmp_data_generation()

# for root, directories, files in os.walk("./dwh_raw_sample"):
#     for file in files:
#         print(file)
#         read_cdp_data(file)
