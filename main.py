import pandas as pd
import numpy as np
import time
import random
import raw_data
import oss2
import os
import datetime
from enum import Enum
import json
from activity_es_data_generation import push_es_data, get_time
import uuid
from sqlalchemy import create_engine
from urllib import parse


class Category(Enum):
    logistics_upstream_transportation = "logi_upstreamTransportation"
    logistics_downstream_transportation = "logi_downstreamTransportation"
    logistics_upstream_distribution = "logi_upstreamDistribution"
    logistics_downstream_distribution = "logi_downstreamDistribution"
    waste_end_life = "waste_endLife"
    waste_operation = "waste_operation"
    travel_business_travel = "travel_businessTravel"
    travel_employee_commuting = "travel_employeeCommuting"
    scope1 = "scope1_scope1"
    scope2 = "scope2_scope2"
    assetsInvestments = "assetsInvestments_investments"
    processing_site_specific = "processing_siteSpecific"
    fuelEnergy_useofsold = "fuelEnergy_useofsold"
    purchases_capitalGoods = "purchases_capitalGoods"
    purchases_goodsAndServices = "purchases_goodsAndServices"
    franchises_refrigerant_consumption = "franchises_refrigerantConsumption"
    franchises_electricity_consumption = "franchises_electricityConsumption"


def get_random_date():
    now = datetime.datetime.now()
    start_time = (2016, 1, 1, 0, 0, 0, 0, 0, 0)
    end_time = (now.year, now.month, now.day-1, 23, 59, 59, 0, 0, 0)
    start = int(time.mktime(start_time))
    end = int(time.mktime(end_time))
    t = random.randint(start, end)
    date_tuple = time.localtime(t)
    r_date = time.strftime("%Y%m%d", date_tuple)
    return r_date


def get_today():
    now = datetime.datetime.now()
    today = now.strftime("%Y-%m-%d")
    return today


def get_random_data_quantity():
    return np.random.randint(200, 300)


def create_dataframe(activity_name=[], store_id=[], geography=[], special_activity_type=[], sector=[],
                     isic_classification=[], isic_section=[], time_period=[], scope=[], unit=[], values=[],
                     value_names=[], tickers=[], ticker_values=[], suppliers=[]):
    data = {}

    if len(activity_name) > 0:
        data["Activity_Name"] = activity_name

    if len(store_id) > 0:
        data["Store_id"] = store_id

    if len(geography) > 0:
        data["Geography"] = geography

    if len(special_activity_type) > 0:
        data["Special_Activity_Type"] = special_activity_type

    if len(sector) > 0:
        data["Sector"] = sector

    if len(isic_classification) > 0:
        data["ISIC_Classification"] = isic_classification

    if len(isic_section) > 0:
        data["ISIC_Section"] = isic_section

    if len(time_period) > 0:
        data["Time_Period"] = time_period

    if len(scope) > 0:
        data["Scope"] = scope

    if len(unit) > 0:
        data["Unit"] = unit

    names = []
    if "*" in value_names:
        names = value_names.split("*")
    else:
        names.append(value_names)

    if len(values) > 0 and len(values) == len(names):
        for value, name in zip(values, names):
            data[name] = value

    if len(tickers) > 0 and len(tickers) == len(ticker_values):
        for ticker, value in zip(tickers, ticker_values):
            data[ticker] = value

    if len(suppliers) > 0:
        data["Suppliers"] = suppliers

    return pd.DataFrame(data)


def generate_specific_es_data(category_name, sheet_list, value_names, unit_list):
    if not os.path.exists(json_path):
        os.makedirs(json_path)

    for sheet, value_name, unit_value in zip(sheet_list, value_names, unit_list):
        source = []
        if category_name in [Category.logistics_upstream_transportation.value,
                             Category.logistics_downstream_transportation.value,
                             Category.logistics_upstream_distribution.value,
                             Category.logistics_downstream_distribution.value]:

            for i in range(get_random_data_quantity()):
                source_dict = {
                    "activity_name": raw_data.logistics_activity_name[np.random.randint(0, 120)],
                    "store_id": np.random.randint(1, 50),
                    "id": uuid.uuid4().hex,
                    "activity": category_name + "_" + sheet,
                    "isic_section": "H - Transportation and storage",
                    "date_created": get_today(),
                    "special_activity_type": "ordinary transforming activity",
                    "unit": unit_value,
                    "pillar": "logistics",
                    "geography": raw_data.logistics_geography[np.random.randint(0, 19)],
                    "isic_classification": raw_data.logistics_isic_classification[np.random.randint(0, 8)],
                    "scope": "scope3",
                    "category": "transportation",
                    "status": "NEW",
                    "sector": "Transport",
                    "time_period": get_random_date()
                }
                if "*" in value_name:
                    # unit为 距离x重量 时value为20-50避免值太大影响图标比例
                    source_dict["data_fields"] = {}
                    for column in value_name.split("*"):
                        source_dict["data_fields"][column.lower()] = np.random.randint(20, 50)
                else:
                    # 其余时候value取值
                    source_dict["data_fields"] = {
                        value_name.lower(): np.random.randint(1000, 9999)
                    }
                source.append(source_dict)

        elif category_name == Category.travel_business_travel.value:
            for i in range(get_random_data_quantity()):
                source_dict = {
                    "activity_name": raw_data.travel_transport_activity_name[np.random.randint(0, 17)],
                    "store_id": np.random.randint(1, 50),
                    "id": uuid.uuid4().hex,
                    "activity": category_name + "_" + sheet,
                    "isic_section": "H - Transportation and storage",
                    "date_created": get_today(),
                    "special_activity_type": "ordinary transforming activity",
                    "unit": unit_value,
                    "pillar": "travel",
                    "geography": raw_data.logistics_geography[np.random.randint(0, 19)],
                    "isic_classification": raw_data.travel_transport_isic_classification[np.random.randint(0, 4)],
                    "scope": "scope3",
                    "category": "businessTravel",
                    "status": "NEW",
                    "sector": "Transport",
                    "time_period": get_random_date()
                }

                if "Nights" in value_name:
                    # unit为Nights时value为个位数
                    value = np.random.randint(1, 10)
                else:
                    # 其余时候value取值
                    value = np.random.randint(100, 999)

                if "*" in value_name:
                    # unit为 距离x重量 时value为20-50避免值太大影响图标比例
                    source_dict["data_fields"] = {}
                    for column in value_name.split("*"):
                        source_dict["data_fields"][column.lower()] = np.random.randint(20, 50)
                else:
                    source_dict["data_fields"] = {
                        value_name.lower(): value
                    }
                source.append(source_dict)

        elif category_name == Category.travel_employee_commuting.value:
            for i in range(get_random_data_quantity()):
                source_dict = {
                    "activity_name": raw_data.travel_accommodation_activity_name[np.random.randint(0, 4)],
                    "store_id": np.random.randint(1, 50),
                    "id": uuid.uuid4().hex,
                    "activity": category_name + "_" + sheet,
                    "isic_section": "I - Accommodation and food service activities",
                    "date_created": get_today(),
                    "special_activity_type": "ordinary transforming activity",
                    "unit": unit_value,
                    "pillar": "travel",
                    "geography": raw_data.purchase_geography[np.random.randint(0, 110)],
                    "isic_classification": "5510:Short term accommodation activities",
                    "scope": "scope3",
                    "category": "employeeCommuting",
                    "status": "NEW",
                    "sector": "Infrastructure & Machinery",
                    "time_period": get_random_date(),
                    "data_fields": {
                        value_name.lower(): np.random.randint(1000, 9999)
                    }
                }
                source.append(source_dict)

        elif category_name == Category.waste_operation.value:
            for i in range(get_random_data_quantity()):
                source_dict = {
                    "activity_name": raw_data.waste_activity_name[np.random.randint(0, 520)],
                    "store_id": np.random.randint(1, 50),
                    "id": uuid.uuid4().hex,
                    "activity": category_name + "_" + sheet,
                    "isic_section": raw_data.waste_isic_sector[np.random.randint(0, 4)],
                    "date_created": get_today(),
                    "special_activity_type": "ordinary transforming activity",
                    "unit": unit_value,
                    "pillar": "waste",
                    "geography": raw_data.waste_geography[np.random.randint(0, 40)],
                    "isic_classification": raw_data.scope1_isic_classification[np.random.randint(0, 3)],
                    "scope": "scope3",
                    "category": "operation",
                    "status": "NEW",
                    "sector": "Waste Treatment & Recycling",
                    "time_period": get_random_date()
                }
                value = np.random.randint(1000, 10000)
                if "*" in value_name:
                    source_dict["data_fields"] = {}
                    for column in value_name.split("*"):
                        source_dict["data_fields"][column.lower()] = value
                else:
                    source_dict["data_fields"] = {
                        value_name.lower(): value
                    }
                source.append(source_dict)

        elif category_name == Category.waste_end_life.value:
            for i in range(get_random_data_quantity()):
                source_dict = {
                    "activity_name": raw_data.waste_activity_name[np.random.randint(0, 520)],
                    "store_id": np.random.randint(1, 50),
                    "id": uuid.uuid4().hex,
                    "activity": category_name + "_" + sheet,
                    "isic_section": raw_data.waste_isic_sector[np.random.randint(0, 4)],
                    "date_created": get_today(),
                    "special_activity_type": "ordinary transforming activity",
                    "unit": unit_value,
                    "pillar": "waste",
                    "geography": raw_data.waste_geography[np.random.randint(0, 40)],
                    "isic_classification": raw_data.scope1_isic_classification[np.random.randint(0, 3)],
                    "scope": "scope3",
                    "category": "endLift",
                    "status": "NEW",
                    "sector": "Waste Treatment & Recycling",
                    "time_period": get_random_date()
                }
                if "*" in value_name:
                    source_dict["data_fields"] = {}
                    for column in value_name.split("*"):
                        source_dict["data_fields"][column.lower()] = np.random.randint(1000, 9999)
                else:
                    source_dict["data_fields"] = {
                        value_name.lower(): np.random.randint(1000, 9999)
                    }
                source.append(source_dict)

        elif category_name == Category.scope1.value:
            for i in range(get_random_data_quantity()):
                source_dict = {
                    "activity_name": raw_data.scope1_activity_name[np.random.randint(0, 176)],
                    "store_id": np.random.randint(1, 50),
                    "id": uuid.uuid4().hex,
                    "activity": category_name + "_" + sheet,
                    "isic_section": "D - Electricity; gas; steam and air conditioning supply",
                    "date_created": get_today(),
                    "special_activity_type": "ordinary transforming activity",
                    "unit": unit_value,
                    "pillar": "scope1",
                    "geography": raw_data.scope1_geography[np.random.randint(0, 40)],
                    "isic_classification": raw_data.scope1_isic_classification[np.random.randint(0, 3)],
                    "scope": "scope1",
                    "category": "scope1",
                    "status": "NEW",
                    "sector": "Transport",
                    "time_period": get_random_date(),
                    "data_fields": {
                        value_name.lower(): np.random.randint(50, 100)
                    }
                }

                # if sheet == "owned_building_fuel" or sheet == "owned_building_refrigerant":
                #     source_dict["data_fields"] = {
                #         value_name.lower(): np.random.randint(50, 100)
                #     }
                # else:
                #     source_dict["data_fields"] = {
                #         value_name.lower(): np.random.randint(1000, 4999)
                #     }
                source.append(source_dict)

        elif category_name == Category.scope2.value:
            for i in range(get_random_data_quantity()):
                source_dict = {
                    "activity_name": raw_data.scope2_activity_name[np.random.randint(0, 6)],
                    "store_id": np.random.randint(1, 50),
                    "id": uuid.uuid4().hex,
                    "activity": category_name + "_" + sheet,
                    "isic_section": "D - Electricity; gas; steam and air conditioning supply",
                    "date_created": get_today(),
                    "special_activity_type": "market activity",
                    "unit": unit_value,
                    "pillar": "scope2",
                    "geography": raw_data.scope2_geography[np.random.randint(0, 40)],
                    "isic_classification": raw_data.scope1_isic_classification[np.random.randint(0, 3)],
                    "scope": "scope2",
                    "category": "scope2",
                    "status": "NEW",
                    "sector": "Electricity",
                    "time_period": get_random_date(),
                    "data_fields": {
                        value_name.lower(): np.random.randint(1000, 9999)
                    }
                }
                source.append(source_dict)

        elif category_name == Category.processing_site_specific.value:
            for i in range(get_random_data_quantity()):
                source_dict = {
                    "activity_name": raw_data.scope2_activity_name[np.random.randint(0, 6)],
                    "store_id": np.random.randint(1, 50),
                    "id": uuid.uuid4().hex,
                    "activity": category_name + "_" + sheet,
                    "isic_section": "D - Electricity; gas; steam and air conditioning supply",
                    "date_created": get_today(),
                    "special_activity_type": "market activity",
                    "unit": unit_value,
                    "pillar": "Processing",
                    "geography": raw_data.scope2_geography[np.random.randint(0, 40)],
                    "isic_classification": raw_data.scope1_isic_classification[np.random.randint(0, 3)],
                    "scope": "scope3",
                    "category": "siteSpecific",
                    "status": "NEW",
                    "sector": "Processing",
                    "time_period": get_random_date(),
                    "data_fields": {
                        value_name.lower(): np.random.randint(1000, 9999)
                    }
                }
                source.append(source_dict)

        elif category_name == Category.fuelEnergy_useofsold.value:
            for i in range(get_random_data_quantity()):
                source_dict = {
                    "activity_name": raw_data.scope2_activity_name[np.random.randint(0, 6)],
                    "store_id": np.random.randint(1, 50),
                    "id": uuid.uuid4().hex,
                    "activity": category_name + "_" + sheet,
                    "isic_section": "D - Electricity; gas; steam and air conditioning supply",
                    "date_created": get_today(),
                    "special_activity_type": "market activity",
                    "unit": unit_value,
                    "pillar": "FuelAndEnergy",
                    "geography": raw_data.scope2_geography[np.random.randint(0, 40)],
                    "isic_classification": raw_data.scope1_isic_classification[np.random.randint(0, 3)],
                    "scope": "scope3",
                    "category": "useOfSold",
                    "status": "NEW",
                    "sector": "useOfSold",
                    "time_period": get_random_date(),
                    "data_fields": {
                        value_name.lower(): np.random.randint(1000, 9999)
                    }
                }
                source.append(source_dict)

        elif category_name in [Category.franchises_refrigerant_consumption.value,
                               Category.franchises_electricity_consumption]:
            for i in range(get_random_data_quantity()):
                source_dict = {
                    "activity_name": raw_data.scope2_activity_name[np.random.randint(0, 6)],
                    "store_id": np.random.randint(1, 50),
                    "id": uuid.uuid4().hex,
                    "activity": category_name + "_" + sheet,
                    "isic_section": "D - Electricity; gas; steam and air conditioning supply",
                    "date_created": get_today(),
                    "special_activity_type": "market activity",
                    "unit": unit_value,
                    "pillar": "franchises",
                    "geography": raw_data.scope2_geography[np.random.randint(0, 40)],
                    "isic_classification": raw_data.scope1_isic_classification[np.random.randint(0, 3)],
                    "scope": "scope3",
                    "category": "energyconsumption",
                    "status": "NEW",
                    "sector": "energyconsumption",
                    "time_period": get_random_date(),
                    "data_fields": {
                        value_name.lower(): np.random.randint(1000, 9999)
                    }
                }
                source.append(source_dict)

        elif category_name in [Category.purchases_capitalGoods.value,
                               Category.purchases_goodsAndServices.value]:
            for i in range(get_random_data_quantity()):
                source_dict = {
                    "activity_name": raw_data.scope2_activity_name[np.random.randint(0, 6)],
                    "store_id": np.random.randint(1, 50),
                    "id": uuid.uuid4().hex,
                    "activity": category_name + "_" + sheet,
                    "isic_section": "D - Electricity; gas; steam and air conditioning supply",
                    "date_created": get_today(),
                    "special_activity_type": "market activity",
                    "unit": unit_value,
                    "pillar": "purchases",
                    "geography": raw_data.scope2_geography[np.random.randint(0, 40)],
                    "isic_classification": raw_data.scope1_isic_classification[np.random.randint(0, 3)],
                    "scope": "scope3",
                    "category": "goodsandservice",
                    "status": "NEW",
                    "sector": "goodsandservice",
                    "time_period": get_random_date(),
                    "data_fields": {
                        value_name.lower(): np.random.randint(1000, 9999)
                    }
                }
                source.append(source_dict)

        elif category_name == Category.assetsInvestments.value:
            for i in range(1):
                source_dict = {
                    "activity_name": raw_data.scope2_activity_name[np.random.randint(0, 6)],
                    "store_id": np.random.randint(1, 50),
                    "id": uuid.uuid4().hex,
                    "activity": category_name + "_" + sheet,
                    "isic_section": "D - Electricity; gas; steam and air conditioning supply",
                    "date_created": get_today(),
                    "special_activity_type": "market activity",
                    "unit": unit_value,
                    "pillar": "assetsInvestments",
                    "geography": raw_data.scope2_geography[np.random.randint(0, 40)],
                    "isic_classification": raw_data.scope1_isic_classification[np.random.randint(0, 3)],
                    "scope": "scope3",
                    "category": "investments",
                    "status": "NEW",
                    "sector": raw_data.asset_investment_sector[np.random.randint(1, 10)],
                    "time_period": get_random_date()
                }
                tickers = value_names
                ticker_values = unit_list
                temp = {}
                for ticker, value in zip(tickers, ticker_values):
                    temp[ticker] = value
                source_dict["data_fields"] = temp
                source.append(source_dict)

        with open(f"{json_path}/{category_name}_{sheet}.json", "w+") as f:
            f.write(json.dumps(source, indent=4))

        # print(json.dumps(source, indent=4))
        # 创建es数据并推送到es服务器
        info = f"----> push data to es -- {category_name}_{sheet}"
        push_es_data(source, info)


def generate_specific_oss_data(category_name, sheet_list, value_names, unit_list):
    if not os.path.exists(xlsx_path):
        os.makedirs(xlsx_path)

    print("\n" + f"{get_time()} ----> start to generate --{category_name}-- mock data...")
    with pd.ExcelWriter(f"{xlsx_path}/{category_name}.xlsx") as xlsx:
        for sheet, value_name, unit_value in zip(sheet_list, value_names, unit_list):

            activity_uuid, activity_name, store_id, geography, special_activity_type, sector, isic_classification, isic_section, time_period, scope, product_uuid, product_group, product_name, unit, value1, value2, tickers, ticker_values, suppliers = [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], []

            counts = get_random_data_quantity()

            for i in range(counts):
                activity_uuid.append(uuid.uuid4().hex)
                store_id.append(np.random.randint(1, 50))
                time_period.append(get_random_date())
                unit.append(unit_value)
                scope.append("scope3")

            if category_name in [Category.logistics_upstream_transportation.value,
                                 Category.logistics_downstream_transportation.value,
                                 Category.logistics_upstream_distribution.value,
                                 Category.logistics_downstream_distribution.value]:
                for i in range(counts):
                    activity_name.append(raw_data.logistics_activity_name[np.random.randint(0, 120)])
                    geography.append(raw_data.logistics_geography[np.random.randint(0, 19)])
                    special_activity_type.append("ordinary transforming activity")
                    sector.append("Transport")
                    isic_classification.append(raw_data.logistics_isic_classification[np.random.randint(0, 8)])
                    isic_section.append("H - Transportation and storage")
                    suppliers.append(raw_data.suppliers[np.random.randint(0, 10)])

                    if "Distance*Weight" in value_name:
                        # unit为 距离x重量 时value为20-50避免值太大影响图标比例
                        value1.append(np.random.randint(20, 50))
                        value2.append(np.random.randint(20, 50))
                    else:
                        # 其余时候value取值
                        value1.append(np.random.randint(1000, 10000))
                        value2.append(np.random.randint(1000, 10000))

            elif category_name == Category.travel_business_travel.value:
                for i in range(counts):
                    activity_name.append(raw_data.travel_transport_activity_name[np.random.randint(0, 17)])
                    geography.append(raw_data.travel_transport_geography[np.random.randint(0, 7)])
                    special_activity_type.append("ordinary transforming activity")
                    sector.append("Transport")
                    isic_classification.append(raw_data.travel_transport_isic_classification[np.random.randint(0, 4)])
                    isic_section.append("H - Transportation and storage")

                    if "Nights" in value_name:
                        # unit为Nights时value为个位数
                        value1.append(np.random.randint(1, 10))
                        value2.append(np.random.randint(1, 10))
                    elif "Distance*Weight" in value_name:
                        # unit为 距离x重量 时value为20-50避免值太大影响图标比例
                        value1.append(np.random.randint(20, 50))
                        value2.append(np.random.randint(20, 50))
                    else:
                        # 其余时候value取值
                        value1.append(np.random.randint(100, 999))
                        value2.append(np.random.randint(100, 999))

            elif category_name == Category.travel_employee_commuting.value:
                for i in range(counts):
                    activity_name.append(raw_data.travel_accommodation_activity_name[np.random.randint(0, 4)])
                    geography.append(raw_data.purchase_geography[np.random.randint(0, 110)])
                    special_activity_type.append("ordinary transforming activity")
                    sector.append("Infrastructure & Machinery")
                    isic_classification.append("5510:Short term accommodation activities")
                    isic_section.append("I - Accommodation and food service activities")
                    value1.append(np.random.randint(100, 999))
                    value2.append(np.random.randint(100, 999))

            elif category_name == Category.waste_operation.value or category_name == Category.waste_end_life.value:
                for i in range(counts):
                    activity_name.append(raw_data.waste_activity_name[np.random.randint(0, 520)])
                    geography.append(raw_data.waste_geography[np.random.randint(0, 40)])
                    special_activity_type.append("ordinary transforming activity")
                    sector.append("Waste Treatment & Recycling")
                    isic_classification.append(raw_data.waste_isic_classification[np.random.randint(0, 13)])
                    isic_section.append(raw_data.waste_isic_sector[np.random.randint(0, 4)])
                    value1.append(np.random.randint(1000, 10000))
                    value2.append(np.random.randint(1000, 10000))

            elif category_name == Category.scope1.value:
                scope.clear()
                for i in range(counts):
                    activity_name.append(raw_data.scope1_activity_name[np.random.randint(0, 176)])
                    geography.append(raw_data.scope1_geography[np.random.randint(0, 230)])
                    special_activity_type.append("ordinary transforming activity")
                    sector.append("Transport")
                    isic_classification.append(raw_data.scope1_isic_classification[np.random.randint(0, 3)])
                    isic_section.append("D - Electricity; gas; steam and air conditioning supply")
                    scope.append("scope1")
                    value1.append(np.random.randint(1000, 9999))

            elif category_name == Category.scope2.value:
                scope.clear()
                for i in range(counts):
                    activity_name.append(raw_data.scope2_activity_name[np.random.randint(0, 6)])
                    geography.append(raw_data.scope2_geography[np.random.randint(0, 170)])
                    special_activity_type.append("market activity")
                    sector.append("Electricity")
                    isic_classification.append(raw_data.scope1_isic_classification[np.random.randint(0, 3)])
                    isic_section.append("D - Electricity; gas; steam and air conditioning supply")
                    scope.append("scope2")
                    value1.append(np.random.randint(1000, 9999))

            elif category_name == Category.assetsInvestments.value:
                tickers = value_names
                ticker_values = unit_list

                activity_uuid.clear()
                time_period.clear()
                store_id.clear()
                scope.clear()
                for i in range(1):
                    activity_uuid.append(uuid.uuid4().hex)
                    activity_name.append(raw_data.scope2_activity_name[np.random.randint(0, 6)])
                    store_id.append(np.random.randint(1, 50))
                    geography.append(raw_data.scope2_geography[np.random.randint(0, 170)])
                    special_activity_type.append("market activity")
                    sector.append(raw_data.asset_investment_sector[np.random.randint(1, 10)])
                    isic_classification.append(raw_data.scope1_isic_classification[np.random.randint(0, 3)])
                    isic_section.append("D - Electricity; gas; steam and air conditioning supply")
                    time_period.append(get_random_date())
                    scope.append("scope3")

            elif category_name in [Category.purchases_capitalGoods.value,
                                   Category.purchases_goodsAndServices.value,
                                   Category.processing_site_specific.value,
                                   Category.fuelEnergy_useofsold.value,
                                   Category.franchises_refrigerant_consumption.value,
                                   Category.franchises_electricity_consumption.value]:
                for i in range(counts):
                    activity_name.append(raw_data.scope1_activity_name[np.random.randint(0, 170)])
                    geography.append(raw_data.scope2_geography[np.random.randint(0, 170)])
                    special_activity_type.append("market activity")
                    sector.append("market activity")
                    isic_classification.append(raw_data.scope1_isic_classification[np.random.randint(0, 3)])
                    isic_section.append("D - Electricity; gas; steam and air conditioning supply")
                    value1.append(np.random.randint(1000, 9999))

            else:
                print("-------- enum mapping error --------")
                continue

            # 多unit追加多组数据（目前最多2个unit）
            value_list = [value1]
            if "*" in value_name:
                value_list.append(value2)

            df = create_dataframe(
                activity_name=activity_name,
                store_id=store_id,
                geography=geography,
                special_activity_type=special_activity_type,
                sector=sector,
                isic_classification=isic_classification,
                isic_section=isic_section,
                time_period=time_period,
                scope=scope,
                unit=unit,
                values=value_list,
                value_names=value_name,
                tickers=tickers,
                ticker_values=ticker_values,
                suppliers=suppliers
            )
            df.to_excel(xlsx, sheet_name=sheet, index=False)
            print(f"{get_time()} generate random data success! --{category_name}-- --{sheet}--")


def generate_mock_data():
    with open("pm_config.json", "r", encoding="utf-8") as f:
        content = json.load(f)
        oss_enabled = content["ossMockupEnabled"]
        es_enabled = content["esMockupEnabled"]

        for data in content["mockData"]:
            pillar = data["pillar"]
            for category in data["categories"]:
                file_name = pillar + "_" + category["name"]
                # print(file_name)

                if oss_enabled and file_name == Category.assetsInvestments.value:
                    # assets_investments_investments
                    tickers_names, tickers_values = category["value_name"], category["unit"]
                    for i in range(np.random.randint(8, 12)):
                        tickers_names.append(raw_data.ticker[np.random.randint(1, 30)])
                        tickers_values.append(np.random.randint(1000, 9999))
                    generate_specific_oss_data(file_name,
                                               category["activities"],
                                               tickers_names,
                                               tickers_values)
                else:
                    if oss_enabled:
                        # generate oss xlsx files
                        generate_specific_oss_data(file_name,
                                                   category["activities"],
                                                   category["value_name"],
                                                   category["unit"])
                    if es_enabled:
                        # generate es data
                        generate_specific_es_data(file_name,
                                                  category["activities"],
                                                  category["value_name"],
                                                  category["unit"])


def upload_excels_to_oss():
    print("\n" + get_time() + "-" * 30 + "  begin to upload files " + "-" * 30)
    access_key_id = os.getenv('OSS_TEST_ACCESS_KEY_ID', 'access_key_id')    # replace access_key_id
    access_key_secret = os.getenv('OSS_TEST_ACCESS_KEY_SECRET', 'access_key_secret')  # replace access_key_secret
    bucket_name = os.getenv('OSS_TEST_BUCKET', 'apac-lab-process-mining')
    endpoint = os.getenv('OSS_TEST_ENDPOINT', 'oss-cn-shenzhen.aliyuncs.com')

    # 创建Bucket对象，所有Object相关的接口都可以通过Bucket对象来进行
    bucket = oss2.Bucket(oss2.Auth(access_key_id, access_key_secret), endpoint, bucket_name)

    xlsx_list = [xlsx for xlsx in os.listdir(xlsx_path) if "xlsx" in xlsx and "~$" not in xlsx]
    for xlsx in xlsx_list:
        result = oss2.resumable_upload(bucket, f"input/{get_today()}/{xlsx}", f"{xlsx_path}/{xlsx}")
        print(f"{get_time()} {xlsx} ---> upload status: {result.status}")


def mysql_operation():
    username = "root"
    password = "1qaz@WSX"
    host = "localhost"
    db = "jeecg-boot"

    # sqlalchemy package connect
    pwd = parse.quote_plus(password)
    engine = create_engine(f'mysql+pymysql://{username}:{pwd}@{host}:3306/{db}?charset=utf8')

    table_name_sql = "SELECT TABLE_NAME FROM information_schema.tables WHERE TABLE_NAME LIKE '%%t_dws%%';"
    table_names = pd.read_sql(table_name_sql, engine)
    print(table_names.values.flatten())

    with pd.ExcelWriter("./t_dws.xlsx") as xlsx:
        for name in table_names.values.flatten():
            if len(name.split("_")) > 5:
                continue
            sql = f"select * from {name}"
            data = pd.read_sql(sql, engine)
            data.to_excel(xlsx, sheet_name=name, index=False)


if __name__ == '__main__':
    json_path = "./data/json"
    xlsx_path = "./data/xlsx"

    generate_mock_data()
    upload_excels_to_oss()
    # mysql_operation()
