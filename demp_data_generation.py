import random
import datetime
import time
import numpy as np


class Generator:

    def __init__(self):
        self.field = None
        self.temp_date = []

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

    def email(self):
        list_sum = [i for i in range(10)] + ["a", "b", "c", "d", "e", "f", "g", "h", 'i', "j", "k",
                                             "l", "M", "n", "o", "p", "q", "r", "s", "t", "u", "v",
                                             "w", "x", "y", "z"]
        email_str = ""
        email_suffix = ["@163.com", "@qq.com", "@gmail.com", "@mail.hk.com", "＠yahoo.co.id", "@mail.com"]
        for i in range(10):
            a = str(random.choice(list_sum))
            email_str = email_str + a
        return email_str + random.choice(email_suffix)

    def mobile(self):
        mobiles = ["139", "138", "137", "136", "135", "134", "159", "158", "157", "150", "151", "152", "188", "187",
                   "182", "183", "184", "178", "130", "131", "132", "156", "155", "186", "185", "176", "133", "153",
                   "189", "180", "181", "177"]
        number = "0123456789"
        mobile = random.choice(mobiles) + "".join(random.choice(number) for i in range(8))
        return mobile

    def date(self):
        return self.generate_date()

    def time(self):
        return self.generate_date(is_time=True)

    def early_date(self):
        return self.generate_date(is_early=True)

    def late_date(self):
        # late_date must be generated after early_date
        base = self.temp_date.pop(0)
        return self.generate_date(base=base)

    def default(self):
        print("no implement for this field: ", self.field)

    def list_type_generation(self):
        return self.field[np.random.randint(0, len(self.field))]

    def dict_type_generation(self):
        opera_type = self.field["type"]
        start = self.field["start"]
        end = self.field["end"]

        random_gen = {
            "int": random.randint,
            "float": random.uniform
        }
        generator = random_gen[opera_type]
        return generator(start, end)

    def str_type_generation(self):
        fun = getattr(self, self.field.lower(), self.default)
        return fun()

    def generate(self, f):
        self.field = f
        operator_gen = {
            list: self.list_type_generation,
            dict: self.dict_type_generation,
            str: self.str_type_generation
        }
        try:
            operator = operator_gen[type(f)]
            return operator()
        except Exception as e:
            print("error with: ", f, e)
