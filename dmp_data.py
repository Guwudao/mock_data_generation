import json
import uuid
import pandas as pd
import time
import random
import datetime
import os
import numpy as np


class Mock:

    def __init__(self, data, tables):
        self.hk_ids = [uuid.uuid4().hex for i in range(100)]
        self.data = data
        self.tables = tables

    def blacklist_history(self, table, fields, count):
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

    def default(self, table, fields, count):
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

    def get_random_data(self, field):
        if type(field) is dict:
            return random.randint(int(field["start"]), int(field["end"]))
        elif field == "DATE":
            return self.get_random_date()
        elif field == "TIME":
            return self.get_random_date(is_time=True)
        else:
            return field[np.random.randint(0, len(field))]

    def get_fields_and_count(self, table):
        return self.data[table]["fields"], self.data[table]["count"]

    def get_random_date(self, is_time=False):
        now = datetime.datetime.now()
        start_time = (2016, 1, 1, 0, 0, 0, 0, 0, 0)
        end_time = (now.year, now.month, now.day - 1, 23, 59, 59, 0, 0, 0)
        start = int(time.mktime(start_time))
        end = int(time.mktime(end_time))
        t = random.randint(start, end)

        r_date = time.strftime("%Y%m%d", time.localtime(t))
        # print(time.strftime("%Y%m%d", time.localtime(t + random.randint(100000, 10000000))))

        if is_time:
            # r_date = radar.random_datetime("1999-07-12T14:12:06", "1999-08-12T14:12:06")
            r_date = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(t))
        return r_date

    def create_dataframe(self, *args):
        path = "./data/dmp"
        if not os.path.exists(path):
            os.makedirs(path)

        dataframe = {}
        table_name, fields, values = args
        for field, value in zip(fields, values):
            dataframe[field] = value

        print("\n" + "=" * 30 + f" {table_name} " + "=" * 30)
        print(dataframe)
        pd.DataFrame(dataframe).to_csv(f"{path}/{table_name}.csv", index=False)

    def start(self):
        for table in self.tables:
            self.create_tables(table)

    def create_tables(self, table_name):
        fun = getattr(self, table_name, self.default)
        fields, count = self.get_fields_and_count(table_name)
        return fun(table_name, fields, count)


def dmp_data_generation():
    with open("dmp_config.json", "r", encoding="utf-8") as f:
        content = json.load(f)

        mock = Mock(content["data"], content["tables"])
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
