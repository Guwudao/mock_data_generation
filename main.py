import pandas as pd
import numpy as np
import time
import random
import raw_data
import oss2
import os
import datetime
from enum import Enum


class Category(Enum):
    logistics_transportation = "logistics_transportation"
    logistics_distribution = "logistics_distribution"
    waste_end_lift = "waste_endLift"
    waste_operation = "waste_operation"
    travel_business_travel = "travel_businessTravel"
    travel_employee_commuting = "travel_employeeCommuting"


def get_random_date():
    start_time = (2016, 1, 1, 0, 0, 0, 0, 0, 0)
    end_time = (2022, 5, 31, 23, 59, 59, 0, 0, 0)
    start = time.mktime(start_time)
    end = time.mktime(end_time)
    t = random.randint(start, end)
    date_touple = time.localtime(t)
    r_date = time.strftime("%Y%m%d", date_touple)
    return r_date


def create_dataframe(activity_uuid=[], activity_name=[], store_id=[], geography=[], special_activity_type=[], sector=[], isic_classification=[], isic_section=[], time_period=[], scope=[], product_uuid=[], product_group=[], product_name=[], unit=[], values=[], value_names=[]):
    data = {}

    # if len(activity_uuid) > 0:
    #     data["Activity_UUID"] = activity_uuid

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

    # if len(product_uuid) > 0:
    #     data["Product_UUID"] = product_uuid
    #
    # if len(product_group) > 0:
    #     data["Product_Group"] = product_group
    #
    # if len(product_name) > 0:
    #     data["Product_Name"] = product_name

    if len(unit) > 0:
        data["Unit"] = unit

    names = []
    if "*" in value_names:
        names = value_names.split("*")
    else:
        names.append(value_names)

    for value, name in zip(values, names):
        data[name] = value

    return pd.DataFrame(data)


def generate_specific_template_data(category_name, sheet_list, value_names, unit_list, length):
    print(f"----> start to generate --{category_name.value}-- data...")
    with pd.ExcelWriter(f"./xlsx/{category_name.value}.xlsx") as xlsx:
        for sheet, value_name, unit_value in zip(sheet_list, value_names, unit_list):

            activity_uuid, activity_name, store_id, geography, special_activity_type, sector, isic_classification, isic_section, time_period, scope, product_uuid, product_group, product_name, unit, value1, value2 = [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], []

            if category_name is Category.logistics_distribution \
                    or category_name is Category.logistics_transportation:
                for i in range(length):
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
                    product_uuid.append(raw_data.logistics_product_uuid[np.random.randint(0, 98)])
                    product_group.append("ReferenceProduct")
                    product_name.append(raw_data.purchase_product_name[np.random.randint(0, 98)])
                    unit.append(unit_value)
                    value1.append(np.random.randint(100, 10000))
                    value2.append(np.random.randint(100, 1000))

            elif category_name is Category.travel_business_travel:
                for i in range(length):
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
                    product_uuid.append(raw_data.purchase_product_uuid[np.random.randint(0, 2400)])
                    product_group.append("ReferenceProduct")
                    product_name.append(raw_data.travel_transport_product_name[np.random.randint(0, 13)])
                    unit.append(unit_value)

                    if "Nights" in value_name:
                        value1.append(np.random.randint(1, 10))
                        value2.append(np.random.randint(1, 10))
                    else:
                        value1.append(np.random.randint(100, 999))
                        value2.append(np.random.randint(100, 999))

            elif category_name is Category.travel_employee_commuting:
                for i in range(length):
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
                    product_uuid.append(raw_data.purchase_product_uuid[np.random.randint(0, 2400)])
                    product_group.append("ReferenceProduct")
                    product_name.append(raw_data.travel_accommodation_product_name[np.random.randint(0, 4)])
                    unit.append(unit_value)
                    value1.append(np.random.randint(100, 999))
                    value2.append(np.random.randint(100, 999))

            elif category_name is Category.waste_operation or category_name is Category.waste_end_lift:
                for i in range(length):
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
                    product_uuid.append(raw_data.purchase_product_uuid[np.random.randint(0, 2400)])
                    product_group.append("ReferenceProduct")
                    product_name.append(raw_data.waste_product_name[np.random.randint(0, 220)])
                    unit.append(unit_value)
                    value1.append(np.random.randint(100, 1000))
                    value2.append(np.random.randint(100, 1000))
            else:
                print("-------- enum mapping error --------")

            value_list = [value1]
            if "*" in value_name:
                value_list.append(value2)

            df = create_dataframe(
                activity_uuid=activity_uuid,
                activity_name=activity_name,
                store_id=store_id,
                geography=geography,
                special_activity_type=special_activity_type,
                sector=sector,
                isic_classification=isic_classification,
                isic_section=isic_section,
                time_period=time_period,
                scope=scope,
                product_uuid=product_uuid,
                product_group=product_group,
                product_name=product_name,
                unit=unit,
                values=value_list,
                value_names=value_name
            )
            df.to_excel(xlsx, sheet_name=sheet, index=False)
            print(f"generate --{category_name.value}-- --{sheet}-- random data success!")


def sector_mapping(raw_sectors):
    # EFDB sector mapping
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


def generate_random_data():
    data_quantity = np.random.randint(800, 1000)

    # logistics_transportation activity list
    logistics_transportation = ["fuelBased_fuel", "fuelBased_electricity", "fuelBased_refrigerant", "distanceBased_air", "distanceBased_road", "distanceBased_sea", "moneyBased_amount"]
    logistics_transportation_value_names = ["Fuel", "Electricity", "Refrigerant", "Weight*Distance", "Weight*Distance", "Weight*Distance", "Amount"]
    logistics_transportation_unit = ["kg", "kWh", "kg", "tonne-km", "tonne-km", "tonne-km", "CNY"]
    generate_specific_template_data(Category.logistics_transportation,
                                    logistics_transportation,
                                    logistics_transportation_value_names,
                                    logistics_transportation_unit,
                                    data_quantity)

    # logistics_distribution activity list
    logistics_distribution = ["siteSpecific_fuel", "siteSpecific_electricity", "siteSpecific_refrigerant",
                              "siteSpecific_space"]
    logistics_distribution_value_names = ["Fuel", "Electricity", "Refrigerant", "Square"]
    logistics_distribution_unit = ["kg", "kWh", "kg", "m3"]
    generate_specific_template_data(Category.logistics_distribution,
                                    logistics_distribution,
                                    logistics_distribution_value_names,
                                    logistics_distribution_unit,
                                    data_quantity)

    # waste_end_lift activity list
    waste_end_lift = ["typeSepecific_incinerated", "typeSepecific_recycled", "typeSepecific_landfilled"]
    waste_end_lift_value_names = ["Weight", "Weight", "Weight"]
    waste_end_lift_unit = ["kg", "kg", "kg"]
    generate_specific_template_data(Category.waste_end_lift,
                                    waste_end_lift,
                                    waste_end_lift_value_names,
                                    waste_end_lift_unit,
                                    data_quantity)

    # waste_operation activity list
    waste_operation = ["typeSepecific_incinerated", "typeSepecific_recycled", "typeSepecific_landfilled"]
    waste_operationt_value_names = ["Weight", "Weight", "Weight"]
    waste_operation_unit = ["kg", "kg", "kg"]
    generate_specific_template_data(Category.waste_operation,
                                    waste_operation,
                                    waste_operationt_value_names,
                                    waste_operation_unit,
                                    data_quantity)

    # travel_business_travel activity list
    travel_business_travel = ["fuelBased_fuel", "fuelBased_electricity", "fuelBased_refrigerant", "distanceBased_air", "distanceBased_road", "distanceBased_accomodation", "moneyBased_ammount"]
    travel_business_travel_value_names = ["Fuel", "Electricity", "Refrigerant", "Distance", "Distance", "Nights", "Counts*Nights"]
    travel_business_travel_unit = ["kg", "kWh", "kg", "km", "km", "night", "person-night"]
    generate_specific_template_data(Category.travel_business_travel,
                                    travel_business_travel,
                                    travel_business_travel_value_names,
                                    travel_business_travel_unit,
                                    data_quantity)

    travel_employee_commuting = ["fuelBased_fuel", "fuelBased_electricity", "fuelBased_refrigerant",
                                 "distanceBased_road"]
    travel_employee_commuting_value_names = ["Fuel", "Electricity", "Refrigerant", "Distance"]
    travel_employee_commuting_unit = ["kg", "kWh", "kg", "km"]
    generate_specific_template_data(Category.travel_employee_commuting,
                                    travel_employee_commuting,
                                    travel_employee_commuting_value_names,
                                    travel_employee_commuting_unit,
                                    data_quantity)

    # generate_purchases("purchases", data_quantity)
    # generate_scope1("scope1", data_quantity)
    # generate_scope2("scope2", data_quantity)


def upload_files():
    access_key_id = os.getenv('OSS_TEST_ACCESS_KEY_ID', 'LTAI5tAG24AcqCYzPvvw4ig8')
    access_key_secret = os.getenv('OSS_TEST_ACCESS_KEY_SECRET', 'BWZCSGdF3XUeZh50knJap1t6BZ7GiQ')
    bucket_name = os.getenv('OSS_TEST_BUCKET', 'apac-lab-process-mining')
    endpoint = os.getenv('OSS_TEST_ENDPOINT', 'oss-cn-shenzhen.aliyuncs.com')

    # 创建Bucket对象，所有Object相关的接口都可以通过Bucket对象来进行
    bucket = oss2.Bucket(oss2.Auth(access_key_id, access_key_secret), endpoint, bucket_name)

    now = datetime.datetime.now()
    today = now.strftime("%Y-%m-%d")
    xlsxs = [xlsx for xlsx in os.listdir("./xlsx") if "xlsx" in xlsx]
    for xlsx in xlsxs:
        result = oss2.resumable_upload(bucket, f"input/{today}/{xlsx}", f"./xlsx/{xlsx}")
        print(f"{xlsx} ---> upload status: {result.status}")


if __name__ == '__main__':
    generate_random_data()
    upload_files()
