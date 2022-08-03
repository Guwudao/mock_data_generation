
# scope1
scope1_activity = ["owned_building_fuel", "owned_building_refrigerant", "owned_vehicle_fuel", "owned_vehicle_refrigerant"]
scope1_value_names = ["Fuel", "Refrigerant", "Fuel", "Refrigerant"]
scope1_unit = ["kg", "kg", "kg", "kg"]

# scope2
scope2_activity = ["electricity_without_eac"]
scope2_value_names = ["Electricity"]
scope2_unit = ["kWh"]

# logistics_transportation
logistics_transportation_activity = ["fuelbased_fuel", "fuelbased_electricity", "fuelbased_refrigerant", "distancebased_air",
                            "distancebased_road", "distancebased_sea", "moneybased_amount"]
logistics_transportation_value_names = ["Fuel", "Electricity", "Refrigerant", "Distance*Weight", "Distance*Weight",
                                        "Distance*Weight", "Amount"]
logistics_transportation_unit = ["kg", "kWh", "kg", "tonne-km", "tonne-km", "tonne-km", "CNY"]

# logistics_distribution
logistics_distribution_activity = ["siteSpecific_fuel", "siteSpecific_electricity", "siteSpecific_refrigerant",
                          "siteSpecific_space"]
logistics_distribution_value_names = ["Fuel", "Electricity", "Refrigerant", "Square"]
logistics_distribution_unit = ["kg", "kWh", "kg", "m3"]

# waste_end_life
waste_end_life_activity = ["typespecific_incinerated", "typespecific_recycled", "typespecific_landfilled"]
waste_end_life_value_names = ["Weight", "Weight", "Weight"]
waste_end_life_unit = ["kg", "kg", "kg"]

# waste_operation
waste_operation_activity = ["typespecific_incinerated", "typespecific_recycled", "typespecific_landfilled"]  # sheet name / activity name
waste_operationt_value_names = ["Weight", "Weight", "Weight"]
waste_operation_unit = ["kg", "kg", "kg"]

# travel_business_travel
travel_business_travel_activity = ["fuelbased_fuel", "fuelbased_electricity", "fuelbased_refrigerant", "distancebased_air",
                          "distancebased_road", "distancebased_accommodation", "moneybased_amount"]
travel_business_travel_value_names = ["Fuel", "Electricity", "Refrigerant", "Distance", "Distance", "Count*Nights",
                                      "Amount"]
travel_business_travel_unit = ["kg", "kWh", "kg", "km", "km", "person-night", "CNY"]

# travel_employee_commuting
travel_employee_commuting_activity = ["fuelBased_fuel", "fuelBased_electricity", "fuelBased_refrigerant",
                             "distanceBased_road"]
travel_employee_commuting_value_names = ["Fuel", "Electricity", "Refrigerant", "Distance"]
travel_employee_commuting_unit = ["kg", "kWh", "kg", "km"]

# assets_investments_investments
assets_investments_investments_activity = ["listedEquityCorpBonds_shares"]
assets_investments_investments_tickers_names = ["Unit"]
assets_investments_investments_tickers_values = ["kg"]
