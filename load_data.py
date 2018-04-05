# -*- coding: utf-8 -*-
import sys
import os
import logging
import json

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir)))

import pandas as pd
import numpy as np

from onedot.utils import ExcelUtils, DBUtils, BusinessRules
import config

# Set up python logging format
log_format = '%(asctime)s %(levelname)s %(message)s'
logging.basicConfig(format=log_format, level=logging.INFO)

# Remove pandas warning
pd.set_option('chained_assignment', None)


def load_supplier_file_into_dataframe(filepath):
    """
    Load the supplier json file into a dataframe
    :param filepath: path of the json file
    :return: the dataframe with the data of the json file
    """
    with open(filepath) as f:
        supplier_data_str = f.readlines()
    supplier_data_json = [json.loads(x.strip()) for x in supplier_data_str]

    # Load supplier data into a dataframe
    supplier_data = pd.DataFrame(supplier_data_json)
    supplier_data = supplier_data.sort_values(by="ID")

    # Set the values of column "Attribute Names" as different columns : one column per value
    supplier_data_as_table = supplier_data.pivot(index="ID", columns="Attribute Names",
                                                 values="Attribute Values").reset_index()

    # Add general information
    supplier_data_as_table = pd.merge(supplier_data_as_table, supplier_data[
        ["ID", "MakeText", "ModelText", "ModelTypeText", "TypeName", "TypeNameFull"]].drop_duplicates(subset="ID"),
                                      on="ID", how="left")

    # Replace value "null" by the pandas null value : Nan
    supplier_data_as_table.fillna(value=np.NaN, inplace=True)
    supplier_data_as_table = supplier_data_as_table.replace("null", np.nan)
    supplier_data_as_table.head()

    return supplier_data_as_table


def normalize_supplier_data(cars):
    """
    Normalize the supplier data
    :param cars: dataframe generated from the json supplier file
    :return: normalized dataframe
    """
    # Set to NaN colums where 0 is used instead
    for col in ["Doors", "FirstRegMonth", "Hp", "Seats"]:
        cars.loc[cars[col] == '0', col] = np.NaN

    # Define new values for the transmission attribute
    transmission_aggregation = {
        'Automat': 'Automat',
        'Schaltgetriebe': 'Schaltgetriebe',
        'Schaltgetriebe manuell': 'Schaltgetriebe',
        'Automat sequentiell': 'Automat sequentiell',
        'Automatik-Getriebe': 'Automat',
        'Automatisiertes Schaltgetriebe': 'Automat',
        'Automat stufenlos': 'Automat stufenlos',
        'Automat stufenlos, sequentiell': 'stufenlos',
        np.NaN: np.NaN}

    cars["NewTransmissionTypeText"] = cars.apply(axis=1,
                                                 func=lambda x: transmission_aggregation[x["TransmissionTypeText"]])

    return cars


def extract_useful_columns(cars):
    """
    Extract the columns that can be useful to compute attributes of the target database 
    :param cars: all information of cars
    :return: cars with only useful columns
    """
    cars_extracted = cars[['ID', 'BodyColorText', 'BodyTypeText', 'Properties', 'Seats', 'ConditionTypeText',
                           'City', 'MakeText', 'Km', 'ModelText', 'ModelTypeText']]
    return cars_extracted


def integrate_cars(cars):
    """
    Change the format of the data to be compatible with the target schema
    :param cars: dataframe of cars to integrate
    :return: a dataframe of cars in the target format
    """
    # Initialization of objects and data
    db = DBUtils()
    user = config.USER
    passw = config.PASSWORD
    host = config.HOST
    dbname = config.DBNAME

    br = BusinessRules()
    makes = db.get_distinct_values_from_db("make", user, passw, dbname, host)
    df_models_variants = db.get_distinct_model_and_variant(user, passw, dbname, host)

    cars_integrated = pd.DataFrame()

    # Construction of the data into the target format
    cars_integrated["carType"] = cars.apply(axis=1, func=lambda x: br.compute_car_type(x, br.type_mapping))
    cars_integrated["color"] = cars.apply(axis=1, func=lambda x: br.color_mapping[x["BodyColorText"]])
    cars_integrated["condition"] = cars.apply(axis=1, func=lambda x: br.condition_mapping[x["ConditionTypeText"]])
    cars_integrated["currency"] = np.NaN
    cars_integrated["drive"] = np.NaN
    cars_integrated["city"] = cars["City"]
    cars_integrated["country"] = "CH"
    cars_integrated["make"] = cars.apply(axis=1, func=lambda x: br.compute_make(x, makes))
    cars["make"] = cars_integrated["make"]
    cars_integrated["manufacture_year"] = np.NaN
    cars_integrated["mileage"] = cars.apply(axis=1, func=lambda x: str(x["Km"]) + ".0")
    cars_integrated["mileage_unit"] = "kilometer"
    cars_integrated["model"] = cars.apply(axis=1, func=lambda x: br.compute_model(x, df_models_variants))
    cars["model"] = cars_integrated["model"]
    cars_integrated["model_variant"] = cars.apply(axis=1, func=lambda x: br.compute_variant(x))
    cars_integrated["price_on_request"] = np.NaN
    cars_integrated["type"] = "car"
    cars_integrated["zip"] = np.NaN
    cars_integrated["manufacture_month"] = np.NaN
    cars_integrated["fuel_consumption_unit"] = np.NaN

    cars_integrated.fillna("null", inplace=True)
    return cars_integrated


if __name__ == '__main__':
    # Initialize variables
    supplier_data_path = config.SUPPLIER_DATA_PATH
    result_file = config.RESULT_FILE
    eu = ExcelUtils()

    # Loading of supplier file
    logging.info("******** Start of loading supplier file ********")
    cars_df = load_supplier_file_into_dataframe(supplier_data_path)
    logging.info("******** End of loading supplier file ********")

    # Normalization of the data
    logging.info("******** Start of normalizing supplier file ********")
    cars_normalized = normalize_supplier_data(cars_df)
    writer = eu.create_excel_file(result_file, 'normalized_data', cars_normalized)
    logging.info("******** End of normalizing supplier file ********")

    # Extraction of the data
    logging.info("******** Start of extracting supplier file ********")
    cars_extracted = extract_useful_columns(cars_normalized)
    eu.add_excel_sheet(result_file, 'extrated_data', cars_extracted)
    logging.info("******** End of extracting supplier file ********")

    # Integretion of the data
    logging.info("******** Start of integrating supplier file ********")
    cars_integrated = integrate_cars(cars_extracted)
    eu.add_excel_sheet(result_file, 'integrated_data', cars_integrated)
    logging.info("******** End of integrating supplier file ********")
