import requests
from lxml import etree
import pandas as pd

headers = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.2 Safari/605.1.15",
    "referer": "https://www.climatiq.io/explorer?source=EXIOBASE"
}
prefix = "https://www.climatiq.io/explorer/_next/data/A8J4ZuuzVkZNOEPBC4ROO/emission-factor/"


def get_explore_data():
    url_list = [8, 41, 59, 133, 223, 307, 343, 498, 566, 574, 594]
    names, sources, years, regions, sectors, categories, unit_types, activity_ids, beis = [], [], [], [], [], [], [], [], []

    for i in url_list:
        try:
            url = f"https://www.climatiq.io/explorer?page={i+1}"
            resp = requests.get(url, headers=headers, timeout=30)
            html = etree.HTML(resp.text)

            # names += html.xpath("//a/span[@class='mr-2']/text()")
            # sources += html.xpath("//tr/td[@class='align-top factors-table-cell factors-table-cell--source']/text()")
            # years += html.xpath("//tr/td[@class='align-top factors-table-cell factors-table-cell--year']/text()")
            # regions += html.xpath("//tr/td[@class='align-top factors-table-cell factors-table-cell--region']/text()")
            # sectors += html.xpath("//tr/td[@class='align-top factors-table-cell factors-table-cell--sector']/text()")
            # categories += html.xpath("//tr/td[@class='align-top factors-table-cell factors-table-cell--category']/text()")
            # unit_types += html.xpath("//tr/td[@class='align-top factors-table-cell factors-table-cell--unit_type']/text()")

            activity_ids += html.xpath("//div[@class='flex items-start break-all']/code/text()")
            beis = html.xpath("//tr/td/a[@class='no-underline']/@href")

            with open("data_urls.txt", "a+") as f:
                for bei in beis:
                    f.write(bei.split("/")[-1] + "\n")

            print(f"爬取完成第{i+1}页")
        except Exception as e:
            error_page.append(i)
            print(f"error：爬取第{i+1}页异常", e)

    # dataframe = {
    #     "NAME": names,
    #     "SOURCE": sources,
    #     "YEAR": years,
    #     "REGION": regions,
    #     "SECTOR": sectors,
    #     "CATEGORY": categories,
    #     "UNIT_TYPE": unit_types
    # }
    # pd.DataFrame(dataframe).to_excel("data_explorer4.xlsx", index=False)


def get_all_data():
    _ids, names, sources, years, regions, region_names, sectors, categories, unit_types, units, lca_activitys, activity_ids, descriptions, factors, factor_calculation_methods, factor_calculation_origins, slugs = [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], []
    file = open("data_urls.txt", "r")
    context = file.readlines()

    for url in context:
        full_url = prefix + url.replace("\n", ".json")

        try:
            resp = requests.get(full_url, headers=headers, timeout=30)
            # print(resp.json())
            factor = resp.json()["pageProps"]["factors"][0]

            activity_ids.append(factor["activity_id"])
            _ids.append(factor["id"])
            names.append(factor["name"])
            categories.append(factor["category"])
            sectors.append(factor["sector"])
            sources.append(factor["source"])
            years.append(factor["year"])
            regions.append(factor["region"])
            region_names.append(factor["region_name"])
            descriptions.append(factor["description"])
            unit_types.append(factor["unit_type"][0])
            units.append(factor["unit"])
            # lca_activitys.append(factor["lca_activity"])
            factors.append(factor["factor"])
            factor_calculation_methods.append(factor["factor_calculation_method"])
            factor_calculation_origins.append(factor["factor_calculation_origin"])
            slugs.append(factor["slug"])
            print(factor)
        except Exception as e:
            error_page.append(url)
            print("error: ", e)

    dataframe = {
        "Activity_id": activity_ids,
        "ID": _ids,
        "Name": names,
        "Source": sources,
        "Year": years,
        "Region": regions,
        "Region_name": region_names,
        "Sector": sectors,
        "Category": categories,
        "Unit": units,
        "Unit_type": unit_types,
        "Factor": factors,
        "Factor_calculation_method": factor_calculation_methods,
        "Factor_calculation_origin": factor_calculation_origins,
        "Slug": slugs,
        "Description": descriptions
    }
    pd.DataFrame(dataframe).to_excel("d_e_sp.xlsx", index=False)


error_page = []
# get_explore_data()
# get_all_data()
print(error_page)

df1 = pd.read_excel("data_explorer_fulfill.xlsx")
df2 = pd.read_excel("d_e_sp.xlsx")
frames = [df1, df2]
pd.concat(frames).to_excel("data_explorer.xlsx", index=False)
