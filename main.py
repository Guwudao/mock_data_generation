import pandas as pd
import numpy as np
import time
import random
import raw_data
import oss2
import os
import datetime
from time import strftime

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)


def get_random_date():
    start_time = (2016, 1, 1, 0, 0, 0, 0, 0, 0)
    end_time = (2022, 5, 31, 23, 59, 59, 0, 0, 0)
    start = time.mktime(start_time)
    end = time.mktime(end_time)
    t = random.randint(start, end)
    date_touple = time.localtime(t)
    r_date = time.strftime("%Y%m%d", date_touple)
    return r_date


def create_dataframe(activity_uuid=[],
                     activity_name=[],
                     store_id=[],
                     geography=[],
                     special_activity_type=[],
                     sector=[],
                     isic_classification=[],
                     isic_section=[],
                     time_period=[],
                     product_uuid=[],
                     product_group=[],
                     product_name=[],
                     unit=[],
                     value=[]):
    data = {}

    if len(activity_uuid) > 0:
        data["Activity_UUID"] = activity_uuid

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

    if len(value) > 0:
        data["Value"] = value

    return pd.DataFrame(data)


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


def generate_purchases(category_name, length):
    activity_uuid, activity_name, store_id, geography, special_activity_type, sector, isic_classification, isic_section, time_period, product_uuid, product_group, product_name, unit, value = [], [], [], [], [], [], [], [], [], [], [], [], [], []

    currencies = ["CNY", "USD"]
    for i in range(length):
        activity_uuid.append(raw_data.purchase_activity_uuid[np.random.randint(0, 3700)])
        activity_name.append(raw_data.purchase_activity_name[np.random.randint(0, 2400)])
        store_id.append(np.random.randint(1, 50))
        geography.append(raw_data.purchase_geography[np.random.randint(0, 110)])
        special_activity_type.append("market activity")
        sector.append(raw_data.purchase_sector[np.random.randint(0, 75)])

        isic_classification.append(raw_data.purchase_isic_classification[np.random.randint(0, 150)])
        isic_section.append(raw_data.purchase_isic_section[np.random.randint(0, 10)])
        time_period.append(get_random_date())
        product_uuid.append(raw_data.purchase_product_uuid[np.random.randint(0, 2400)])
        product_group.append("ReferenceProduct")
        product_name.append(raw_data.purchase_product_name[np.random.randint(0, 2400)])
        unit.append(currencies[np.random.randint(0, 2)])
        value.append(np.random.randint(100, 1000))

    sector = sector_mapping(sector)

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
        product_uuid=product_uuid,
        product_group=product_group,
        product_name=product_name,
        unit=unit,
        value=value
    )

    df.to_excel(f"./xlsx/{category_name}.xlsx", sheet_name=category_name, index=False)
    print(f"generate --{category_name}-- random data success!")


def generate_logistics(category_name, sheet_list, length):
    print(f"----> start to generate --{category_name}-- data...")
    with pd.ExcelWriter(f"./xlsx/{category_name}.xlsx") as xlsx:
        for sheet in sheet_list:
            activity_uuid, activity_name, store_id, geography, special_activity_type, sector, isic_classification, isic_section, time_period, product_uuid, product_group, product_name, unit, value = [], [], [], [], [], [], [], [], [], [], [], [], [], []

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
                product_uuid.append(raw_data.logistics_product_uuid[np.random.randint(0, 98)])
                product_group.append("ReferenceProduct")
                product_name.append(raw_data.purchase_product_name[np.random.randint(0, 98)])
                unit.append("ton*km")
                value.append(np.random.randint(1000, 10000))

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
                product_uuid=product_uuid,
                product_group=product_group,
                product_name=product_name,
                unit=unit,
                value=value
            )
            df.to_excel(xlsx, sheet_name=sheet, index=False)
            print(f"generate --{category_name}-- --{sheet}-- random data success!")


def generate_waste(category_name, sheet_list, length):
    print(f"----> start to generate --{category_name}-- data...")
    with pd.ExcelWriter(f"./xlsx/{category_name}.xlsx") as xlsx:
        for sheet in sheet_list:
            activity_uuid, activity_name, store_id, geography, special_activity_type, sector, isic_classification, isic_section, time_period, product_uuid, product_group, product_name, unit, value = [], [], [], [], [], [], [], [], [], [], [], [], [], []

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
                product_uuid.append(raw_data.purchase_product_uuid[np.random.randint(0, 2400)])
                product_group.append("ReferenceProduct")
                product_name.append(raw_data.waste_product_name[np.random.randint(0, 220)])
                unit.append("kg")
                value.append(np.random.randint(100, 1000))

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
                product_uuid=product_uuid,
                product_group=product_group,
                product_name=product_name,
                unit=unit,
                value=value
            )
            df.to_excel(xlsx, sheet_name=sheet, index=False)
            print(f"generate --{category_name}-- --{sheet}-- random data success!")


def generate_travel_employee_commuting(category_name, sheet_list, length):
    print(f"----> start to generate --{category_name}-- data...")
    with pd.ExcelWriter(f"./xlsx/{category_name}.xlsx") as xlsx:
        for sheet in sheet_list:
            activity_uuid, activity_name, store_id, geography, special_activity_type, sector, isic_classification, isic_section, time_period, product_uuid, product_group, product_name, unit, value = [], [], [], [], [], [], [], [], [], [], [], [], [], []

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
                product_uuid.append(raw_data.purchase_product_uuid[np.random.randint(0, 2400)])
                product_group.append("ReferenceProduct")
                product_name.append(raw_data.travel_accommodation_product_name[np.random.randint(0, 4)])
                unit.append("RMB")
                value.append(np.random.randint(100, 999))

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
                product_uuid=product_uuid,
                product_group=product_group,
                product_name=product_name,
                unit=unit,
                value=value
            )
            df.to_excel(xlsx, sheet_name=sheet, index=False)
            print(f"generate --{category_name}-- --{sheet}-- random data success!")


def generate_travel_business_travel(category_name, sheet_list, length):
    print(f"----> start to generate --{category_name}-- data...")
    with pd.ExcelWriter(f"./xlsx/{category_name}.xlsx") as xlsx:
        for sheet in sheet_list:
            activity_uuid, activity_name, store_id, geography, special_activity_type, sector, isic_classification, isic_section, time_period, product_uuid, product_group, product_name, unit, value = [], [], [], [], [], [], [], [], [], [], [], [], [], []

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
                product_uuid.append(raw_data.purchase_product_uuid[np.random.randint(0, 2400)])
                product_group.append("ReferenceProduct")
                product_name.append(raw_data.travel_transport_product_name[np.random.randint(0, 13)])
                unit.append("person*km")
                value.append(np.random.randint(100, 999))

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
                    product_uuid=product_uuid,
                    product_group=product_group,
                    product_name=product_name,
                    unit=unit,
                    value=value
                )
            df.to_excel(xlsx, sheet_name=sheet, index=False)
            print(f"generate --{category_name}-- --{sheet}-- random data success!")


def generate_scope1(category_name, length):
    activity_uuid, activity_name, store_id, geography, special_activity_type, sector, isic_classification, isic_section, time_period, product_uuid, product_group, product_name, unit, value = [], [], [], [], [], [], [], [], [], [], [], [], [], []

    sectors = ["Fuels AND Fuels", "Electricity"]
    for i in range(length):
        activity_uuid.append(raw_data.purchase_activity_uuid[np.random.randint(0, 3700)])
        activity_name.append(raw_data.scope1_activity_name[np.random.randint(0, 6)])
        store_id.append(np.random.randint(1, 50))
        geography.append(raw_data.scope1_geography[np.random.randint(0, 230)])
        special_activity_type.append("market activity")
        sector.append(sectors[random.randint(0, 1)])
        isic_classification.append("3510:Electric power generation; transmission and distribution")
        isic_section.append("D - Electricity; gas; steam and air conditioning supply")
        time_period.append(get_random_date())
        product_uuid.append(raw_data.purchase_product_uuid[np.random.randint(0, 2400)])
        product_group.append("ReferenceProduct")
        product_name.append(raw_data.purchase_product_name[np.random.randint(0, 2400)])
        unit.append("kWh")
        value.append(np.random.randint(1000, 9999))

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
            product_uuid=product_uuid,
            product_group=product_group,
            product_name=product_name,
            unit=unit,
            value=value
        )

    df.to_excel(f"./xlsx/{category_name}.xlsx", index=False)
    print(f"generate --{category_name}-- random data success!")


def generate_scope2(category_name, length):
    activity_uuid, activity_name, store_id, geography, special_activity_type, sector, isic_classification, isic_section, time_period, product_uuid, product_group, product_name, unit, value = [], [], [], [], [], [], [], [], [], [], [], [], [], []

    for i in range(length):
        activity_uuid.append(raw_data.purchase_activity_uuid[np.random.randint(0, 3700)])
        activity_name.append(raw_data.scope2_activity_name[np.random.randint(0, 176)])
        store_id.append(np.random.randint(1, 50))
        geography.append(raw_data.scope2_geography[np.random.randint(0, 170)])
        special_activity_type.append("ordinary transforming activity")
        sector.append("Electricity")
        isic_classification.append(raw_data.scope1_isic_classification[np.random.randint(0, 3)])
        isic_section.append("D - Electricity; gas; steam and air conditioning supply")
        time_period.append(get_random_date())
        product_uuid.append(raw_data.purchase_product_uuid[np.random.randint(0, 2400)])
        product_group.append("ReferenceProduct")
        product_name.append(raw_data.purchase_product_name[np.random.randint(0, 2400)])
        unit.append("kWh")
        value.append(np.random.randint(1000, 9999))

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
            product_uuid=product_uuid,
            product_group=product_group,
            product_name=product_name,
            unit=unit,
            value=value
        )

    df.to_excel(f"./xlsx/{category_name}.xlsx", index=False)
    print(f"generate --{category_name}-- random data success!")


def generate_random_data():
    data_quantity = np.random.randint(800, 1000)

    # generate_purchases("purchases", data_quantity)

    # logistics_transportation_list activity list
    logistics_transportation_list = ["fuelBased_fuel", "fuelBased_electricity", "fuelBased_refrigerant", "distanceBased_air", "distanceBased_road", "distanceBased_sea", "moneyBased_amount"]
    generate_logistics("logistics_transportation", logistics_transportation_list, data_quantity)

    # logistics_distribution activity list
    logistics_distribution = ["siteSpecific_fuel", "siteSpecific_electricity", "siteSpecific_refrigerant", "siteSpecific_space"]
    generate_logistics("logistics_distribution", logistics_distribution, data_quantity)

    # waste_end_lift activity list
    waste_end_lift = ["typeSepecific_incinerated", "typeSepecific_recycled", "typeSepecific_landfilled"]
    generate_waste("waste_endLift", waste_end_lift, data_quantity)

    # waste_operation_list activity list
    waste_operation_list = ["supplierSpecific", "typeSepecific_incinerated", "typeSepecific_recycled", "typeSepecific_landfilled"]
    generate_waste("waste_operation", waste_operation_list, data_quantity)

    # travel_business_travel activity list
    travel_business_travel = ["fuelBased_fuel", "fuelBased_electricity", "fuelBased_refrigerant", "distanceBased_air", "distanceBased_road", "distanceBased_accomodation", "moneyBased_ammount"]
    generate_travel_business_travel("travel_businessTravel", travel_business_travel, data_quantity)

    travel_employee_commuting = ["fuelBased_fuel", "fuelBased_electricity", "fuelBased_refrigerant", "distanceBased_road"]
    generate_travel_employee_commuting("travel_employeeCommuting", travel_employee_commuting, data_quantity)


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
