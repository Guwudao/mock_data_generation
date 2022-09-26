import json
import uuid
import pandas as pd
import datetime
import os
import oss2
from demp_data_generation import Generator


def get_time():
    t = datetime.datetime.now()
    now = t.strftime("%Y-%m-%d %H:%M:%S")
    return now


class Mock:

    def __init__(self, total_id, data, tables):
        self.hk_ids = [uuid.uuid4().hex for _ in range(total_id)]
        self.data = data
        self.tables = tables
        self.temp_date = []
        self.path = "./data/dmp_mock"

    def get_fields_and_count(self, table):
        return self.data[table]["fields"], self.data[table]["count"]

    def create_dataframe(self, *args):
        table_name, fields, values = args
        if not os.path.exists(self.path):
            os.makedirs(self.path)

        dataframe = {}
        for field, value in zip(fields, values):
            dataframe[field] = value

        # print(dataframe)
        pd.DataFrame(dataframe).to_csv(f"{self.path}/{table_name}.csv", index=False)

    def start(self):
        for table in self.tables:
            fields, count = self.get_fields_and_count(table)
            print("\n" + "=" * 30 + f" {table} " + "=" * 30)
            values = [self.hk_ids[0:count]]
            generator = Generator()
            for field in self.data[table]["fields"]:
                temp = []
                if field == "AC" or field == "hk_id":
                    continue
                else:
                    for i in range(count):
                        temp.append(generator.generate(self.data[table][field]))
                values.append(temp)
            self.create_dataframe(table, fields, values)

    def upload_to_oss(self):
        print("\n" + get_time() + "-" * 30 + "  begin to upload files " + "-" * 30)
        access_key_id = os.getenv('OSS_TEST_ACCESS_KEY_ID', 'access_key_id')  # replace access_key_id
        access_key_secret = os.getenv('OSS_TEST_ACCESS_KEY_SECRET', 'access_key_secret')  # replace access_key_secret
        bucket_name = os.getenv('OSS_TEST_BUCKET', 'apac-lab-ai-model')
        endpoint = os.getenv('OSS_TEST_ENDPOINT', 'oss-cn-shenzhen.aliyuncs.com')

        # 创建Bucket对象，所有Object相关的接口都可以通过Bucket对象来进行
        bucket = oss2.Bucket(oss2.Auth(access_key_id, access_key_secret), endpoint, bucket_name)
        tables = [csv for csv in os.listdir(self.path) if "csv" in csv]
        for table in tables:
            result = oss2.resumable_upload(bucket, f"dmp_mock_data/{table}", f"{self.path}/{table}")
            print(f"{get_time()} {table} ---> upload status: {result.status}")


def dmp_data_generation():
    with open("dmp_tables_config.json", "r", encoding="utf-8") as f:
        content = json.load(f)
        mock = Mock(content["total_id"], content["data"], content["tables"])
        mock.start()
        mock.upload_to_oss()


dmp_data_generation()


# def convert_cdp_raw_data(file):
#     path = "./data/dmp_sample_csv"
#     if not os.path.exists(path):
#         os.makedirs(path)
#
#     df = pd.read_csv(f"./dwh_raw_sample/{file}", header=None, error_bad_lines=False)
#     new_df = {}
#     for i, column in enumerate(df.loc[0, 0].split("|")):
#         temp = []
#         for data in df.loc[1:, 0]:
#             value = ""
#             if len(data.split("|")) > i:
#                 value = data.split("|")[i]
#             temp.append(value)
#
#         print(file, temp)
#         new_df[column] = temp
#
#     pd.DataFrame(new_df).to_csv(f"./{path}/{file}", index=False)


# for root, directories, files in os.walk("./dwh_raw_sample"):
#     for file in files:
#         print(file)
#         convert_cdp_raw_data(file)
