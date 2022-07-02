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
import configuration


class Category(Enum):
    logistics_transportation = "logi_transportation"
    logistics_distribution = "logi_distribution"
    waste_end_life = "waste_endLife"
    waste_operation = "waste_operation"
    travel_business_travel = "travel_businessTravel"
    travel_employee_commuting = "travel_employeeCommuting"
    scope1 = "scope1_scope1"
    scope2 = "scope2_scope2"
    assetsInvestments = "assetsInvestments_investments"


def get_random_date():
    start_time = (2016, 1, 1, 0, 0, 0, 0, 0, 0)
    end_time = (2022, 5, 31, 23, 59, 59, 0, 0, 0)
    start = time.mktime(start_time)
    end = time.mktime(end_time)
    t = random.randint(start, end)
    date_touple = time.localtime(t)
    r_date = time.strftime("%Y%m%d", date_touple)
    return r_date


def get_today():
    now = datetime.datetime.now()
    today = now.strftime("%Y-%m-%d")
    return today


def get_random_data_quantity():
    return np.random.randint(200, 300)
    # return np.random.randint(1, 3)


def create_dataframe(activity_name=[], store_id=[], geography=[], special_activity_type=[], sector=[],
                     isic_classification=[], isic_section=[], time_period=[], scope=[], unit=[], values=[],
                     value_names=[], tickers=[], ticker_values=[]):
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

    return pd.DataFrame(data)


def generate_specific_es_data(category_name, sheet_list, value_names, unit_list):
    if not os.path.exists("json"):
        os.mkdir("./json")

    for sheet, value_name, unit_value in zip(sheet_list, value_names, unit_list):
        source = []
        if category_name is Category.logistics_transportation:
            for i in range(get_random_data_quantity()):
                source_dict = {
                    "activity_name": raw_data.logistics_activity_name[np.random.randint(0, 120)],
                    "store_id": np.random.randint(1, 50),
                    "id": uuid.uuid4().hex,
                    "activity": category_name.value + "_" + sheet,
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

                if "Distance*Weight" in value_name:
                    # unit为 距离x重量 时value为20-50避免值太大影响图标比例
                    v1 = np.random.randint(20, 50)
                    v2 = np.random.randint(20, 50)
                else:
                    # 其余时候value取值
                    v1 = np.random.randint(1000, 10000)
                    v2 = np.random.randint(1000, 10000)

                if "*" in value_name:
                    source_dict["data_fields"] = {
                        unit_value: v1 * v2
                    }
                else:
                    source_dict["data_fields"] = {
                        unit_value: v1
                    }
                source.append(source_dict)

        elif category_name is Category.travel_business_travel:
            for i in range(get_random_data_quantity()):
                source_dict = {
                    "activity_name": raw_data.travel_transport_activity_name[np.random.randint(0, 17)],
                    "store_id": np.random.randint(1, 50),
                    "id": uuid.uuid4().hex,
                    "activity": category_name.value + "_" + sheet,
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
                    v1 = np.random.randint(1, 10)
                    v2 = np.random.randint(1, 10)
                elif "Distance*Weight" in value_name:
                    # unit为 距离x重量 时value为20-50避免值太大影响图标比例
                    v1 = np.random.randint(20, 50)
                    v2 = np.random.randint(20, 50)
                else:
                    # 其余时候value取值
                    v1 = np.random.randint(100, 999)
                    v2 = np.random.randint(100, 999)

                if "*" in value_name:
                    source_dict["data_fields"] = {
                        unit_value: v1 * v2
                    }
                else:
                    source_dict["data_fields"] = {
                        unit_value: v1
                    }
                source.append(source_dict)

        elif category_name is Category.travel_employee_commuting:
            for i in range(get_random_data_quantity()):
                source_dict = {
                    "activity_name": raw_data.travel_accommodation_activity_name[np.random.randint(0, 4)],
                    "store_id": np.random.randint(1, 50),
                    "id": uuid.uuid4().hex,
                    "activity": category_name.value + "_" + sheet,
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
                    "time_period": get_random_date()
                }
                source.append(source_dict)

        elif category_name is Category.waste_operation:
            for i in range(get_random_data_quantity()):
                source_dict = {
                    "activity_name": raw_data.waste_activity_name[np.random.randint(0, 520)],
                    "store_id": np.random.randint(1, 50),
                    "id": uuid.uuid4().hex,
                    "activity": category_name.value + "_" + sheet,
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
                v1 = np.random.randint(1000, 10000)
                v2 = np.random.randint(1000, 10000)
                if "*" in value_name:
                    source_dict["data_fields"] = {
                        unit_value: v1 * v2
                    }
                else:
                    source_dict["data_fields"] = {
                        unit_value: v1
                    }
                source.append(source_dict)

        elif category_name is Category.waste_end_life:
            for i in range(get_random_data_quantity()):
                source_dict = {
                    "activity_name": raw_data.waste_activity_name[np.random.randint(0, 520)],
                    "store_id": np.random.randint(1, 50),
                    "id": uuid.uuid4().hex,
                    "activity": category_name.value + "_" + sheet,
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
                v1 = np.random.randint(1000, 9999)
                v2 = np.random.randint(1000, 9999)
                if "*" in value_name:
                    source_dict["data_fields"] = {
                        unit_value: v1 * v2
                    }
                else:
                    source_dict["data_fields"] = {
                        unit_value: v1
                    }
                source.append(source_dict)

        elif category_name is Category.scope1:
            for i in range(get_random_data_quantity()):
                source_dict = {
                    "activity_name": raw_data.scope1_activity_name[np.random.randint(0, 176)],
                    "store_id": np.random.randint(1, 50),
                    "id": uuid.uuid4().hex,
                    "activity": category_name.value + "_" + sheet,
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
                    "time_period": get_random_date()
                }
                v1 = np.random.randint(1000, 9999)
                source_dict["data_fields"] = {
                    unit_value: v1
                }
                source.append(source_dict)

        elif category_name is Category.scope2:
            for i in range(get_random_data_quantity()):
                source_dict = {
                    "activity_name": raw_data.scope2_activity_name[np.random.randint(0, 6)],
                    "store_id": np.random.randint(1, 50),
                    "id": uuid.uuid4().hex,
                    "activity": category_name.value + "_" + sheet,
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
                    "time_period": get_random_date()
                }
                v1 = np.random.randint(1000, 9999)
                source_dict["data_fields"] = {
                    unit_value: v1
                }
                source.append(source_dict)

        elif category_name is Category.assetsInvestments:
            for i in range(1):
                source_dict = {
                    "activity_name": raw_data.scope2_activity_name[np.random.randint(0, 520)],
                    "store_id": np.random.randint(1, 50),
                    "id": uuid.uuid4().hex,
                    "activity": category_name.value + "_" + sheet,
                    "isic_section": "D - Electricity; gas; steam and air conditioning supply",
                    "date_created": get_today(),
                    "special_activity_type": "market activity",
                    "unit": unit_value,
                    "pillar": "scope3",
                    "geography": raw_data.scope2_geography[np.random.randint(0, 40)],
                    "isic_classification": raw_data.scope1_isic_classification[np.random.randint(0, 3)],
                    "scope": "scope3",
                    "category": "scope3",
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

        with open(f"./json/{category_name.value}_{sheet}.json", "w+") as f:
            f.write(json.dumps(source, indent=4))

        # print(json.dumps(source, indent=4))
        # 创建es数据并推送到es服务器
        info = f"----> push -- {category_name.value}_{sheet} -- data to es"
        push_es_data(source, info)


def generate_specific_oss_data(category_name, sheet_list, value_names, unit_list):
    if not os.path.exists("xlsx"):
        os.mkdir("./xlsx")

    print("\n" + f"{get_time()} ----> start to generate --{category_name.value}-- data...")
    with pd.ExcelWriter(f"./xlsx/{category_name.value}.xlsx") as xlsx:
        for sheet, value_name, unit_value in zip(sheet_list, value_names, unit_list):

            activity_uuid, activity_name, store_id, geography, special_activity_type, sector, isic_classification, isic_section, time_period, scope, product_uuid, product_group, product_name, unit, value1, value2, tickers, ticker_values = [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], []

            if category_name is Category.logistics_distribution or category_name is Category.logistics_transportation:
                for i in range(get_random_data_quantity()):

                    activity_uuid.append(raw_data.logistics_activity_uuid[np.random.randint(0, 220)])
                    activity_name.append(raw_data.logistics_activity_name[np.random.randint(0, 120)])
                    store_id.append(np.random.randint(1, 50))
                    geography.append(raw_data.logistics_geography[np.random.randint(0, 19)])
                    special_activity_type.append("ordinary transforming activity")
                    sector.append("Transport")
                    isic_classification.append(raw_data.logistics_isic_classification[np.random.randint(0, 8)])
                    isic_section.append("H - Transportation and storage")
                    time_period.append(get_random_date())
                    scope.append("scope3")
                    unit.append(unit_value)

                    if "Distance*Weight" in value_name:
                        # unit为 距离x重量 时value为20-50避免值太大影响图标比例
                        value1.append(np.random.randint(20, 50))
                        value2.append(np.random.randint(20, 50))
                    else:
                        # 其余时候value取值
                        value1.append(np.random.randint(1000, 10000))
                        value2.append(np.random.randint(1000, 10000))

            elif category_name is Category.travel_business_travel:
                for i in range(get_random_data_quantity()):
                    activity_uuid.append(raw_data.purchase_activity_uuid[np.random.randint(0, 3700)])
                    activity_name.append(raw_data.travel_transport_activity_name[np.random.randint(0, 17)])
                    store_id.append(np.random.randint(1, 50))
                    geography.append(raw_data.travel_transport_geography[np.random.randint(0, 7)])
                    special_activity_type.append("ordinary transforming activity")
                    sector.append("Transport")
                    isic_classification.append(raw_data.travel_transport_isic_classification[np.random.randint(0, 4)])
                    isic_section.append("H - Transportation and storage")
                    time_period.append(get_random_date())
                    scope.append("scope3")
                    unit.append(unit_value)

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

            elif category_name is Category.travel_employee_commuting:
                for i in range(get_random_data_quantity()):
                    activity_uuid.append(raw_data.purchase_activity_uuid[np.random.randint(0, 3700)])
                    activity_name.append(raw_data.travel_accommodation_activity_name[np.random.randint(0, 4)])
                    store_id.append(np.random.randint(1, 50))
                    geography.append(raw_data.purchase_geography[np.random.randint(0, 110)])
                    special_activity_type.append("ordinary transforming activity")
                    sector.append("Infrastructure & Machinery")
                    isic_classification.append("5510:Short term accommodation activities")
                    isic_section.append("I - Accommodation and food service activities")
                    time_period.append(get_random_date())
                    scope.append("scope3")
                    unit.append(unit_value)
                    value1.append(np.random.randint(100, 999))
                    value2.append(np.random.randint(100, 999))

            elif category_name is Category.waste_operation or category_name is Category.waste_end_life:
                for i in range(get_random_data_quantity()):
                    activity_uuid.append(raw_data.purchase_activity_uuid[np.random.randint(0, 3700)])
                    activity_name.append(raw_data.waste_activity_name[np.random.randint(0, 520)])
                    store_id.append(np.random.randint(1, 50))
                    geography.append(raw_data.waste_geography[np.random.randint(0, 40)])
                    special_activity_type.append("ordinary transforming activity")
                    sector.append("Waste Treatment & Recycling")
                    isic_classification.append(raw_data.waste_isic_classification[np.random.randint(0, 13)])
                    isic_section.append(raw_data.waste_isic_sector[np.random.randint(0, 4)])
                    time_period.append(get_random_date())
                    scope.append("scope3")
                    unit.append(unit_value)
                    value1.append(np.random.randint(1000, 10000))
                    value2.append(np.random.randint(1000, 10000))

            elif category_name is Category.scope1:
                for i in range(get_random_data_quantity()):
                    activity_uuid.append(raw_data.purchase_activity_uuid[np.random.randint(0, 3700)])
                    activity_name.append(raw_data.scope1_activity_name[np.random.randint(0, 176)])
                    store_id.append(np.random.randint(1, 50))
                    geography.append(raw_data.scope1_geography[np.random.randint(0, 230)])
                    special_activity_type.append("ordinary transforming activity")
                    sector.append("Transport")
                    isic_classification.append(raw_data.scope1_isic_classification[np.random.randint(0, 3)])
                    isic_section.append("D - Electricity; gas; steam and air conditioning supply")
                    time_period.append(get_random_date())
                    scope.append("scope1")
                    unit.append(unit_value)
                    value1.append(np.random.randint(1000, 9999))

            elif category_name is Category.scope2:
                for i in range(get_random_data_quantity()):
                    activity_uuid.append(raw_data.purchase_activity_uuid[np.random.randint(0, 3700)])
                    activity_name.append(raw_data.scope2_activity_name[np.random.randint(0, 6)])
                    store_id.append(np.random.randint(1, 50))
                    geography.append(raw_data.scope2_geography[np.random.randint(0, 170)])
                    special_activity_type.append("market activity")
                    sector.append("Electricity")
                    isic_classification.append(raw_data.scope1_isic_classification[np.random.randint(0, 3)])
                    isic_section.append("D - Electricity; gas; steam and air conditioning supply")
                    time_period.append(get_random_date())
                    scope.append("scope2")
                    unit.append(unit_value)
                    value1.append(np.random.randint(1000, 9999))

            elif category_name is Category.assetsInvestments:
                tickers = value_names
                ticker_values = unit_list
                for i in range(1):
                    activity_uuid.append(raw_data.purchase_activity_uuid[np.random.randint(0, 3700)])
                    activity_name.append(raw_data.scope2_activity_name[np.random.randint(0, 6)])
                    store_id.append(np.random.randint(1, 50))
                    geography.append(raw_data.scope2_geography[np.random.randint(0, 170)])
                    special_activity_type.append("market activity")
                    sector.append(raw_data.asset_investment_sector[np.random.randint(1, 10)])
                    isic_classification.append(raw_data.scope1_isic_classification[np.random.randint(0, 3)])
                    isic_section.append("D - Electricity; gas; steam and air conditioning supply")
                    time_period.append(get_random_date())
                    scope.append("scope3")

            else:
                print("-------- enum mapping error --------")

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
                ticker_values=ticker_values
            )
            df.to_excel(xlsx, sheet_name=sheet, index=False)
            print(f"{get_time()} generate --{category_name.value}-- --{sheet}-- random data success!")


# EFDB sector mapping
def sector_mapping(raw_sectors):
    sectors = []

    for sector in raw_sectors:
        if "Fishing & Aquaculture" in sector or "Forestry" in sector or "Wood" in sector or "Land Use" in sector:
            sectors.append("Agriculture/Hunting/Forestry/Fishing")

        elif "Infrastructure & Machinery" in sector:
            sectors.append("Buildings and Infrastructure")

        elif "Pulp & Paper" in sector or "Textiles" in sector or "Textiles; Agriculture & Animal Husbandry" in sector:
            sectors.append("Materials")

        elif "Electricity" in sector or "Heat" in sector or "Fuels" in sector:
            sectors.append("energy")

        elif "Electronics" in sector:
            sectors.append("Equipment")

        elif "Agriculture & Animal Husbandry" in sector:
            sectors.append("Livestock Farming")

        elif "Agriculture & Animal Husbandry" in sector or "Cement & Concrete" in sector:
            sectors.append("Manufacturing")

        elif "Chemicals" in sector or "Metals" in sector or "Minerals" in sector or "Resource Extraction" in sector:
            sectors.append("Materials")

        elif "Transport" in sector:
            sectors.append("Transport")

        elif "Waste Treatment & Recycling" in sector:
            sectors.append("Waste")

        elif "Water Supply" in sector:
            sectors.append("water")

        else:
            print("invalid sector: ", sector)

    return sectors


def generate_oss_data():
    # logistics_transportation
    generate_specific_oss_data(Category.logistics_transportation,
                               configuration.logistics_transportation_activity,
                               configuration.logistics_transportation_value_names,
                               configuration.logistics_transportation_unit)

    # logistics_distribution activity list
    # generate_specific_template_data(Category.logistics_distribution,
    #                                 logistics_distribution,
    #                                 logistics_distribution_value_names,
    #                                 logistics_distribution_unit)

    # waste_end_life
    generate_specific_oss_data(Category.waste_end_life,
                               configuration.waste_end_life_activity,
                               configuration.waste_end_life_value_names,
                               configuration.waste_end_life_unit)

    # waste_operation
    generate_specific_oss_data(Category.waste_operation,
                               configuration.waste_operation_activity,
                               configuration.waste_operationt_value_names,
                               configuration.waste_operation_unit)

    # travel_business_travel
    generate_specific_oss_data(Category.travel_business_travel,
                               configuration.travel_business_travel_activity,
                               configuration.travel_business_travel_value_names,
                               configuration.travel_business_travel_unit)

    # travel_employee_commuting
    generate_specific_oss_data(Category.travel_employee_commuting,
                               configuration.travel_employee_commuting_activity,
                               configuration.travel_employee_commuting_value_names,
                               configuration.travel_employee_commuting_unit)

    # scope1
    generate_specific_oss_data(Category.scope1,
                               configuration.scope1_activity,
                               configuration.scope1_value_names,
                               configuration.scope1_unit)

    # scope2
    generate_specific_oss_data(Category.scope2,
                               configuration.scope2_activity,
                               configuration.scope2_value_names,
                               configuration.scope2_unit)

    # assets_investments_investments
    for i in range(np.random.randint(8, 12)):
        configuration.assets_investments_investments_tickers_names.append(raw_data.ticker[np.random.randint(1, 30)])
        configuration.assets_investments_investments_tickers_values.append(np.random.randint(1000, 9999))
    generate_specific_oss_data(Category.assetsInvestments,
                               configuration.assets_investments_investments_activity,
                               configuration.assets_investments_investments_tickers_names,
                               configuration.assets_investments_investments_tickers_values)


def generate_es_data():
    # logistics_transportation
    generate_specific_es_data(Category.logistics_transportation,
                              configuration.logistics_transportation_activity,
                              configuration.logistics_transportation_value_names,
                              configuration.logistics_transportation_unit)

    # waste_end_life
    generate_specific_es_data(Category.waste_end_life,
                              configuration.waste_end_life_activity,
                              configuration.waste_end_life_value_names,
                              configuration.waste_end_life_unit)

    # waste_operation
    generate_specific_es_data(Category.waste_operation,
                              configuration.waste_operation_activity,
                              configuration.waste_operationt_value_names,
                              configuration.waste_operation_unit)

    # travel_business_travel
    generate_specific_es_data(Category.travel_business_travel,
                              configuration.travel_business_travel_activity,
                              configuration.travel_business_travel_value_names,
                              configuration.travel_business_travel_unit)

    # travel_employee_commuting
    generate_specific_es_data(Category.travel_employee_commuting,
                              configuration.travel_employee_commuting_activity,
                              configuration.travel_employee_commuting_value_names,
                              configuration.travel_employee_commuting_unit)

    # scope1
    generate_specific_es_data(Category.scope1,
                              configuration.scope1_activity,
                              configuration.scope1_value_names,
                              configuration.scope1_unit)

    # scope2
    generate_specific_es_data(Category.scope2,
                              configuration.scope2_activity,
                              configuration.scope2_value_names,
                              configuration.scope2_unit)


def upload_files_to_oss():
    print("\n" + get_time() + "-" * 30 + " begin to upload files " + "-" * 30)
    access_key_id = os.getenv('OSS_TEST_ACCESS_KEY_ID', 'LTAI5tAG24AcqCYzPvvw4ig8')
    access_key_secret = os.getenv('OSS_TEST_ACCESS_KEY_SECRET', 'BWZCSGdF3XUeZh50knJap1t6BZ7GiQ')
    bucket_name = os.getenv('OSS_TEST_BUCKET', 'apac-lab-process-mining')
    endpoint = os.getenv('OSS_TEST_ENDPOINT', 'oss-cn-shenzhen.aliyuncs.com')

    # 创建Bucket对象，所有Object相关的接口都可以通过Bucket对象来进行
    bucket = oss2.Bucket(oss2.Auth(access_key_id, access_key_secret), endpoint, bucket_name)

    xlsx_list = [xlsx for xlsx in os.listdir("./xlsx") if "xlsx" in xlsx and "~$" not in xlsx]
    for xlsx in xlsx_list:
        result = oss2.resumable_upload(bucket, f"input/{get_today()}/{xlsx}", f"./xlsx/{xlsx}")
        print(f"{get_time()} {xlsx} ---> upload status: {result.status}")


if __name__ == '__main__':
    generate_es_data()
    generate_oss_data()
    upload_files_to_oss()
