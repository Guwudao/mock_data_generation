import pandas as pd
import random
import datetime


def get_today():
    now = datetime.datetime.now()
    today = now.strftime("%Y-%m-%d")
    return today


def mock_training_data():
    time_period = pd.date_range('2016-1-1', get_today(), freq='MS').strftime("%Y-%m").tolist()
    expense, emission = [], []

    interval = 8
    for i in range(len(time_period)):
        if 0 <= i <= interval:
            expense.append(format(random.uniform(1000, 900), ".3f"))
            emission.append(format(random.uniform(220000, 160000), ".3f"))
        elif interval < i <= interval * 2:
            expense.append(format(random.uniform(1000, 800), ".3f"))
            emission.append(format(random.uniform(200000, 160000), ".3f"))
        elif interval * 2 < i <= interval * 3:
            expense.append(format(random.uniform(900, 700), ".3f"))
            emission.append(format(random.uniform(180000, 130000), ".3f"))
        elif interval * 3 < i <= interval * 4:
            expense.append(format(random.uniform(1000, 800), ".3f"))
            emission.append(format(random.uniform(150000, 90000), ".3f"))
        elif interval * 4 < i <= interval * 5:
            expense.append(format(random.uniform(800, 700), ".3f"))
            emission.append(format(random.uniform(160000, 110000), ".3f"))
        elif interval * 5 < i <= interval * 6:
            expense.append(format(random.uniform(900, 600), ".3f"))
            emission.append(format(random.uniform(150000, 80000), ".3f"))
        elif interval * 6 < i <= interval * 7:
            expense.append(format(random.uniform(800, 600), ".3f"))
            emission.append(format(random.uniform(140000, 70000), ".3f"))
        elif interval * 7 < i <= interval * 8:
            expense.append(format(random.uniform(800, 500), ".3f"))
            emission.append(format(random.uniform(120000, 80000), ".3f"))
        elif interval * 8 < i <= interval * 9:
            expense.append(format(random.uniform(900, 500), ".3f"))
            emission.append(format(random.uniform(110000, 70000), ".3f"))
        else:
            expense.append(format(random.uniform(800, 500), ".3f"))
            emission.append(format(random.uniform(100000, 60000), ".3f"))

    print(expense)
    print(emission)

    dataframe = {
        "Time_Period": time_period,
        "expense": expense,
        "emission": emission,
    }

    pd.DataFrame(dataframe).to_csv("./data/TAE.csv", index=False)


mock_training_data()
