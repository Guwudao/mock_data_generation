import pandas as pd
from openpyxl import load_workbook


def get_purchase(data):
    purchases = data.loc[(data["Special_Activity_Type"] == "market activity")
                         & (data["Product_Group"] == "ReferenceProduct")
                         & ((data["Unit"] == "USD") | (data["Unit"] == "CNY"))].dropna()
    # .loc[
    #     data["Sector"].apply(lambda x: "Waste Treatment & Recycling" not in x)].loc[
    #     data["Sector"].apply(lambda x: "Electricity" not in x)].loc[
    #     data["Sector"].apply(lambda x: "Transport" not in x)].dropna()

    purchases[(~purchases["Sector"].str.contains("Electricity"))]
    # & (~purchases["Sector"].str.contains("Waste Treatment & Recycling"))
    # & (~purchases["Sector"].str.contains("Transport"))]

    purchases.to_csv("./xlsx/purchases.csv", index=False)


def get_logistics(data):
    logistics = data.loc[(data["Special_Activity_Type"] == "ordinary transforming activity")
                         & (data["Sector"] == "Transport")
                         & (data["Product_Group"] == "ReferenceProduct")
                         & (data["Unit"] == "metric ton*km")].loc[
        data["ISIC_Section"].apply(lambda x: " Transportation and storage" in x)].dropna()
    logistics.to_csv("./xlsx/logistics.csv", index=False)


def get_waste(data):
    waste = data.loc[(data["Product_Group"] == "ReferenceProduct")
                     & (data["Sector"] == "Waste Treatment & Recycling")
                     & (data["Unit"] == "kg")
                     ].dropna()
    waste.to_csv("./xlsx/waste.csv", index=False)


def get_travel_transport(data):
    travel_transport = data.loc[(data["Special_Activity_Type"] == "ordinary transforming activity")
                                & (data["Product_Group"] == "ReferenceProduct")
                                & (data["Sector"] == "Transport")
                                & (data["Unit"] == "person*km")].dropna()
    travel_transport.to_csv("./xlsx/travel_transport.csv", index=False)


def get_travel_accommodation(data):
    travel_accommodation = data.loc[(data["Special_Activity_Type"] == "ordinary transforming activity")
                                    & (data["Product_Group"] == "ReferenceProduct")
                                    & (data["Sector"] == "Infrastructure & Machinery")
                                    & ((data["Unit"] == "USD") | (data["Unit"] == "RMB"))].dropna()
    travel_accommodation.to_csv("./xlsx/travel_accommodation.csv", index=False)


def csv_separation():
    print("start csv_separation ...")
    data = pd.read_excel(excel_name, sheet_name="Activity Log")
    # print(data.head())

    # get_purchase(data)
    # get_logistics(data)
    # get_waste(data)
    # get_travel_transport(data)
    # get_travel_accommodation(data)

    scope1 = data.loc[(data["Special_Activity_Type"] == "ordinary transforming activity")
                      # & (data["Product_Group"] == "ReferenceProduct")
                      & ((data["Sector"] == "Fuels AND Fuels") | (data["Sector"] == "Electricity"))
                      & ((data["ISIC_Classification"] == "3510:Electric power generation; transmission and distribution")
                         | (data["ISIC_Classification"] == "4922:Other passenger land transport")
                         | (data["ISIC_Classification"] == "4921:Urban and suburban passenger land transport"))
                      & ((data["ISIC_Section"] == "D - Electricity; gas; steam and air conditioning supply")
                         | (data["ISIC_Section"] == "H - Transportation and storage"))
                      & ((data["Unit"] == "kWh") | (data["Unit"] == "MJ") | (data["Unit"] == "km"))]
    # scope1.to_csv("./csv/scope1.csv", index=False)
    scope1.to_excel("./xlsx/scope1.xlsx", index=False)

    scope2 = data.loc[(data["Special_Activity_Type"] == "market activity")
                      # & (data["Product_Group"] == "ReferenceProduct")
                      & (data["Sector"] == "Electricity")
                      & (data["ISIC_Classification"] == "3510:Electric power generation; transmission and distribution")
                      & (data["ISIC_Section"] == "D - Electricity; gas; steam and air conditioning supply")
                      & (data["Unit"] == "kWh")]
    # scope2.to_csv("./csv/scope2.x", index=False)
    scope2.to_excel("./xlsx/scope2.xlsx", index=False)

    print("end csv_separation ...")
    # data.to_excel("mock_data_v2.xlsx", index=False)


def replace_commas():
    data = load_workbook(excel_name)
    sheet = data["Sheet1"]
    for row in sheet.rows:
        for cell in row:
            print(cell.value)
            if cell.value is str and "," in cell.value:
                cell.value = cell.value.replace(",", ";")

    # 列名去除空格
    for cell in sheet["1"]:
        cell.value = cell.value.replace(" ", "_")

    data.save(excel_name)


excel_name = "ecoinvent mock data 6-7-22 version 2.0.xlsx"
# replace_commas()
# csv_separation()
