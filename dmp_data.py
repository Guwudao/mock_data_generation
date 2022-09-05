import json
import uuid
import pandas as pd
import time
import random
import datetime
import os
import numpy as np
import oss2
from activity_es_data_generation import get_time


class Mock:

    def __init__(self, total_id, data, tables):
        self.hk_ids = [uuid.uuid4().hex for i in range(total_id)]
        self.data = data
        self.tables = tables
        self.temp_date = []
        self.path = "./data/dmp_mock"

    def generate_email(self):
        list_sum = [i for i in range(10)] + ["a", "b", "c", "d", "e", "f", "g", "h", 'i', "j", "k",
                                             "l", "M", "n", "o", "p", "q", "r", "s", "t", "u", "v",
                                             "w", "x", "y", "z"]
        email_str = ""
        email_suffix = ["@163.com", "@qq.com", "@gmail.com", "@mail.hk.com", "＠yahoo.co.id", "@mail.com"]
        for i in range(10):
            a = str(random.choice(list_sum))
            email_str = email_str + a
        return email_str + random.choice(email_suffix)

    def generate_mobile_number(self):
        mobiles = ["139", "138", "137", "136", "135", "134", "159", "158", "157", "150", "151", "152", "188", "187", "182", "183", "184", "178", "130", "131", "132", "156", "155", "186", "185", "176", "133", "153", "189", "180", "181", "177"]
        number = "0123456789"
        mobile = random.choice(mobiles) + "".join(random.choice(number) for i in range(8))
        return mobile

    def generate_random_data(self, field):
        try:
            if type(field) is dict:
                if field["type"] == "int":
                    return random.randint(int(field["start"]), int(field["end"]))
                elif field["type"] == "float":
                    return random.uniform(1, 2)
            elif field == "DATE":
                return self.generate_date()
            elif field == "EARLY_DATE":
                return self.generate_date(is_early=True)
            elif field == "TIME":
                return self.generate_date(is_time=True)
            elif field == "LATE_DATE":
                base = self.temp_date.pop(0)
                return self.generate_date(base=base)
            elif field == "MOBILE":
                return self.generate_mobile_number()
            elif field == "EMAIL":
                return self.generate_email()
            else:
                return field[np.random.randint(0, len(field))]
        except Exception as e:
            print("error with: ", field, e)

    def generate_random_datafields_and_count(self, table):
        return self.data[table]["fields"], self.data[table]["count"]

    def generate_date(self, is_time=False, base=0, is_early=False):
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
            return time.strftime("%Y%m%d", time.localtime(base + random.randint(100000, 30000000)))

    def create_dataframe(self, *args):
        table_name, fields, values = args
        print("\n" + "=" * 30 + f" {table_name} " + "=" * 30)
        if not os.path.exists(self.path):
            os.makedirs(self.path)

        dataframe = {}
        for field, value in zip(fields, values):
            dataframe[field] = value

        # print(dataframe)
        pd.DataFrame(dataframe).to_csv(f"{self.path}/{table_name}.csv", index=False)

    def start(self):
        for table in self.tables:
            fields, count = self.generate_random_datafields_and_count(table)
            values = [self.hk_ids[0:count]]
            for field in self.data[table]["fields"]:
                temp = []
                if field == "AC" or field == "hk_id":
                    continue
                else:
                    for i in range(count):
                        temp.append(self.generate_random_data(self.data[table][field]))

                values.append(temp)
            self.create_dataframe(table, fields, values)

    def upload_to_oss(self):
        print("\n" + get_time() + "-" * 30 + "  begin to upload files " + "-" * 30)
        access_key_id = os.getenv('OSS_TEST_ACCESS_KEY_ID', 'LTAI5tAG24AcqCYzPvvw4ig8')
        access_key_secret = os.getenv('OSS_TEST_ACCESS_KEY_SECRET', 'BWZCSGdF3XUeZh50knJap1t6BZ7GiQ')
        bucket_name = os.getenv('OSS_TEST_BUCKET', 'apac-lab-ai-model')
        endpoint = os.getenv('OSS_TEST_ENDPOINT', 'oss-cn-shenzhen.aliyuncs.com')

        # 创建Bucket对象，所有Object相关的接口都可以通过Bucket对象来进行
        bucket = oss2.Bucket(oss2.Auth(access_key_id, access_key_secret), endpoint, bucket_name)
        tables = [csv for csv in os.listdir(self.path) if "csv" in csv]
        for table in tables:
            result = oss2.resumable_upload(bucket, f"CDP_mock_data/{table}", f"{self.path}/{table}")
            print(f"{get_time()} {table} ---> upload status: {result.status}")


def dmp_data_generation():
    with open("dmp_tables_config.json", "r", encoding="utf-8") as f:
        content = json.load(f)
        mock = Mock(content["total_id"], content["data"], content["tables"])
        mock.start()
        mock.upload_to_oss()


def read_cdp_data(file):
    path = "./data/dmp_sample_csv"
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
