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


def get_random_data_quantity():
    return np.random.randint(200, 300)


def create_dataframe(activity_uuid=[], activity_name=[], store_id=[], geography=[], special_activity_type=[], sector=[], isic_classification=[], isic_section=[], time_period=[], scope=[], product_uuid=[], product_group=[], product_name=[], unit=[], values=[], value_names=[], tickers=[], ticker_values=[]):
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

    if len(values) == len(names):
        for value, name in zip(values, names):
            data[name] = value

    if len(tickers) > 0 and len(tickers) == len(ticker_values):
        for ticker, value in zip(tickers, ticker_values):
            data[ticker] = value

    return pd.DataFrame(data)


def generate_specific_template_data(category_name, sheet_list, value_names, unit_list):
    print(f"----> start to generate --{category_name.value}-- data...")
    with pd.ExcelWriter(f"./xlsx/{category_name.value}.xlsx") as xlsx:
        for sheet, value_name, unit_value in zip(sheet_list, value_names, unit_list):

            activity_uuid, activity_name, store_id, geography, special_activity_type, sector, isic_classification, isic_section, time_period, scope, product_uuid, product_group, product_name, unit, value1, value2, tickers, ticker_values = [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], []

            if category_name is Category.logistics_distribution \
                    or category_name is Category.logistics_transportation:
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
                    product_uuid.append(raw_data.logistics_product_uuid[np.random.randint(0, 98)])
                    product_group.append("ReferenceProduct")
                    product_name.append(raw_data.purchase_product_name[np.random.randint(0, 98)])
                    unit.append(unit_value)
                    if "Distance*Weight" in value_name or "Weight*Distance" in value_name:
                        value1.append(np.random.randint(20, 50))
                        value2.append(np.random.randint(20, 50))
                    else:
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
                    product_uuid.append(raw_data.purchase_product_uuid[np.random.randint(0, 2400)])
                    product_group.append("ReferenceProduct")
                    product_name.append(raw_data.travel_transport_product_name[np.random.randint(0, 13)])
                    unit.append(unit_value)

                    if "Nights" in value_name:
                        value1.append(np.random.randint(1, 10))
                        value2.append(np.random.randint(1, 10))
                    elif "Distance*Weight" in value_name or "Weight*Distance" in value_name:
                        value1.append(np.random.randint(20, 50))
                        value2.append(np.random.randint(20, 50))
                    else:
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
                    product_uuid.append(raw_data.purchase_product_uuid[np.random.randint(0, 2400)])
                    product_group.append("ReferenceProduct")
                    product_name.append(raw_data.travel_accommodation_product_name[np.random.randint(0, 4)])
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
                    product_uuid.append(raw_data.purchase_product_uuid[np.random.randint(0, 2400)])
                    product_group.append("ReferenceProduct")
                    product_name.append(raw_data.waste_product_name[np.random.randint(0, 220)])
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
                for i in range(get_random_data_quantity() + 800):
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
                value_names=value_name,
                tickers=tickers,
                ticker_values=ticker_values
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
    # logistics_transportation activity list
    logistics_transportation = ["fuelbased_fuel", "fuelbased_electricity", "fuelbased_refrigerant", "distancebased_air", "distancebased_road", "distancebased_sea", "moneybased_amount"]
    logistics_transportation_value_names = ["Fuel", "Electricity", "Refrigerant", "Weight*Distance", "Weight*Distance", "Weight*Distance", "Amount"]
    logistics_transportation_unit = ["kg", "kWh", "kg", "tonne-km", "tonne-km", "tonne-km", "CNY"]
    generate_specific_template_data(Category.logistics_transportation,
                                    logistics_transportation,
                                    logistics_transportation_value_names,
                                    logistics_transportation_unit)

    # logistics_distribution activity list
    # logistics_distribution = ["siteSpecific_fuel", "siteSpecific_electricity", "siteSpecific_refrigerant",
    #                           "siteSpecific_space"]
    # logistics_distribution_value_names = ["Fuel", "Electricity", "Refrigerant", "Square"]
    # logistics_distribution_unit = ["kg", "kWh", "kg", "m3"]
    # generate_specific_template_data(Category.logistics_distribution,
    #                                 logistics_distribution,
    #                                 logistics_distribution_value_names,
    #                                 logistics_distribution_unit)

    # waste_end_life activity list
    waste_end_life = ["typespecific_incinerated", "typespecific_recycled", "typespecific_landfilled"]
    waste_end_life_value_names = ["Weight", "Weight", "Weight"]
    waste_end_life_unit = ["kg", "kg", "kg"]
    generate_specific_template_data(Category.waste_end_life,
                                    waste_end_life,
                                    waste_end_life_value_names,
                                    waste_end_life_unit)

    # waste_operation activity list
    waste_operation = ["typespecific_incinerated", "typespecific_recycled", "typespecific_landfilled"]
    waste_operationt_value_names = ["Weight", "Weight", "Weight"]
    waste_operation_unit = ["kg", "kg", "kg"]
    generate_specific_template_data(Category.waste_operation,
                                    waste_operation,
                                    waste_operationt_value_names,
                                    waste_operation_unit)

    # travel_business_travel activity list
    travel_business_travel = ["fuelbased_fuel", "fuelbased_electricity", "fuelbased_refrigerant", "distancebased_air", "distancebased_road", "distancebased_accommodation", "moneybased_amount"]
    travel_business_travel_value_names = ["Fuel", "Electricity", "Refrigerant", "Distance*Weight", "Distance*Weight", "Count*Nights", "Amount"]
    travel_business_travel_unit = ["kg", "kWh", "kg", "tonne-km", "tonne-km", "person-night", "CNY"]
    generate_specific_template_data(Category.travel_business_travel,
                                    travel_business_travel,
                                    travel_business_travel_value_names,
                                    travel_business_travel_unit)

    travel_employee_commuting = ["fuelBased_fuel", "fuelBased_electricity", "fuelBased_refrigerant",
                                 "distanceBased_road"]
    travel_employee_commuting_value_names = ["Fuel", "Electricity", "Refrigerant", "Distance"]
    travel_employee_commuting_unit = ["kg", "kWh", "kg", "km"]
    generate_specific_template_data(Category.travel_employee_commuting,
                                    travel_employee_commuting,
                                    travel_employee_commuting_value_names,
                                    travel_employee_commuting_unit)

    scope1 = ["owned_building_fuel", "owned_building_refrigerant", "owned_vehicle_fuel", "owned_vehicle_refrigerant"]
    scope1_value_names = ["Fuel", "Refrigerant", "Fuel", "Refrigerant"]
    scope1_unit = ["kg", "kg", "kg", "kg"]
    generate_specific_template_data(Category.scope1,
                                    scope1,
                                    scope1_value_names,
                                    scope1_unit)

    scope2 = ["electricity_with_eac"]
    scope2_value_names = ["Electricity"]
    scope2_unit = ["kWh"]
    generate_specific_template_data(Category.scope2,
                                    scope2,
                                    scope2_value_names,
                                    scope2_unit)

    assets_investments_investments = ["listedEquityCorpBonds_shares"]
    assets_investments_investments_tickers_names = []
    assets_investments_investments_tickers_values = []
    for i in range(np.random.randint(1, 10)):
        assets_investments_investments_tickers_names.append(raw_data.ticker[np.random.randint(1, 30)])
        assets_investments_investments_tickers_values.append(np.random.randint(1000, 9999))
    generate_specific_template_data(Category.assetsInvestments,
                                    assets_investments_investments,
                                    assets_investments_investments_tickers_names,
                                    assets_investments_investments_tickers_values)

    # generate_purchases("purchases", data_quantity)


def upload_files():
    print("-" * 30 + "begin to upload files" + "-" * 30)
    access_key_id = os.getenv('OSS_TEST_ACCESS_KEY_ID', 'LTAI5tAG24AcqCYzPvvw4ig8')
    access_key_secret = os.getenv('OSS_TEST_ACCESS_KEY_SECRET', 'BWZCSGdF3XUeZh50knJap1t6BZ7GiQ')
    bucket_name = os.getenv('OSS_TEST_BUCKET', 'apac-lab-process-mining')
    endpoint = os.getenv('OSS_TEST_ENDPOINT', 'oss-cn-shenzhen.aliyuncs.com')

    # 创建Bucket对象，所有Object相关的接口都可以通过Bucket对象来进行
    bucket = oss2.Bucket(oss2.Auth(access_key_id, access_key_secret), endpoint, bucket_name)

    now = datetime.datetime.now()
    today = now.strftime("%Y-%m-%d")
    xlsx_list = [xlsx for xlsx in os.listdir("./xlsx") if "xlsx" in xlsx and "~$" not in xlsx]
    for xlsx in xlsx_list:
        result = oss2.resumable_upload(bucket, f"input/{today}/{xlsx}", f"./xlsx/{xlsx}")
        print(f"{xlsx} ---> upload status: {result.status}")


if __name__ == '__main__':
    generate_random_data()
    upload_files()
