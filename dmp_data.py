import json
import uuid
import pandas as pd
import time
import random
import datetime
import os
import numpy as np


class Mock:

    def __init__(self):
        self.hk_ids = [uuid.uuid4().hex for i in range(100)]
        with open("./dmp_data_value.json", "r") as f:
            content = json.load(f)
            self.data = content["data"]
        self.values_key = "values"

    def payment(self, table, fields, count):
        pay_method_data = self.data[table]["pay_method"]
        pay_billed_dates, pay_methods, pay_amounts = [], [], []
        for i in range(count):
            pay_billed_dates.append(self.get_random_date())
            pay_methods.append(self.get_random_data(pay_method_data))
            pay_amounts.append(random.randint(100, 999))
        values = [self.hk_ids[0:count], pay_billed_dates, pay_methods, pay_amounts]
        self.create_dataframe(table, fields, values)

    def three_mall(self, table, fields, count):
        category_data = self.data[table]["category_name"]
        purchase_date, subtotal, store_code, category_name, quantity = [], [], [], [], []
        for i in range(count):
            purchase_date.append(self.get_random_date())
            subtotal.append(random.randint(100, 999))
            store_code.append(random.randint(1, 9999))
            quantity.append(random.randint(1, 999))
            category_name.append(self.get_random_data(category_data))
        values = [self.hk_ids[0:count], purchase_date, subtotal, store_code, category_name, quantity]
        self.create_dataframe(table, fields, values)

    def roaming_data_cdr(self, table, fields, count):
        values = [self.hk_ids[0:count]]
        for field in self.data[table]["fields"]:
            temp = []
            if field == "ID":
                continue
            else:
                for i in range(count):
                    temp.append(self.get_random_data(self.data[table][field]))
            values.append(temp)
        self.create_dataframe(table, fields, values)

    def blacklist_history(self, table, fields, count):
        print("--blacklist_history")

    def overdue_history(self, table, fields, count):
        print("--overdue_history")

    def top_districts_location(self, table, fields, count):
        values = [self.hk_ids[0:count]]
        for field in self.data[table]["fields"]:
            temp = []
            if field == "ID":
                continue
            else:
                for i in range(count):
                    if field == "district_duration":
                        temp.append(random.randint(1, 999))
                    else:
                        temp.append(self.get_random_data(self.data[table][field]))

            values.append(temp)
        self.create_dataframe(table, fields, values)

    def sales_transaction(self, table, fields, count):
        print("--sales_transaction")

    def campaign_response(self, table, fields, count):
        print("--campaign_response")

    def default(self, table, fields, count):
        print("????")

    def get_random_data(self, field):
        return field[np.random.randint(0, len(field))]

    def get_fields_and_count(self, table):
        return self.data[table]["fields"], self.data[table]["count"]

    def get_random_date(self):
        now = datetime.datetime.now()
        start_time = (2016, 1, 1, 0, 0, 0, 0, 0, 0)
        end_time = (now.year, now.month, now.day - 1, 23, 59, 59, 0, 0, 0)
        start = int(time.mktime(start_time))
        end = int(time.mktime(end_time))
        t = random.randint(start, end)
        date_tuple = time.localtime(t)
        r_date = time.strftime("%Y%m%d", date_tuple)
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

    def create_tables(self, table_name):
        fun = getattr(self, table_name, self.default)
        fields, count = self.get_fields_and_count(table_name)
        return fun(table_name, fields, count)


def dmp_data_generation():
    with open("dmp_data_structure_config.json", "r", encoding="utf-8") as f:
        content = json.load(f)

        mock = Mock()
        for data in content["mockData"]:
            mock.create_tables(data["table"])


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


# dmp_data_generation()

# for root, directories, files in os.walk("./dwh_raw_sample"):
#     for file in files:
#         print(file)
#         read_cdp_data(file)
