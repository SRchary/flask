import os
from bson import json_util


def export_pole_species_data(database, path):
    """
            This function will export pole_species data from pole_species.json to mongoDB collection pole_species_data
            :param database: Pymongo DB object
            :param job_year: Job run year
            :param path: pole_species.json file path
            :return:
            """
    pole_species_collection = database.pole_species_data
    pole_species_collection.delete_many({})
    file_name = "pole_species.json"
    file_path = os.path.join(path, file_name)

    with open(file_path, encoding='ascii') as json_file:
        content = json_file.read()
        print("Number Of records in Pole Species File", len(json_util.loads(content)))
        print("Processing Started......")
        for each in json_util.loads(content):
            pole_species_collection.insert(each)
        print("Pole Species Data Processed")


def export_pole_treatment_data(database, path):
    """
                This function will export pole_treatment data from pole_treatment.json to mongoDB collection pole_treatment_data
                :param database: Pymongo DB object
                :param job_year: Job run year
                :param path: pole_treatment.json file path
                :return:
                """
    pole_treatment_collection = database.pole_treatment_data
    pole_treatment_collection.delete_many({})
    file_name = "pole_treatment.json"
    file_path = os.path.join(path, file_name)

    with open(file_path, encoding='ascii') as json_file:
        content = json_file.read()
        print("Number Of records in Pole Treatment File", len(json_util.loads(content)))
        print("Processing Started......")
        for each in json_util.loads(content):
            pole_treatment_collection.insert(each)
        print("Pole Treatment Data Processed")


def export_pole_height_data(database, path):
    """
                This function will export pole_height data from pole_height.json to mongoDB collection pole_height_data
                :param database: Pymongo DB object
                :param job_year: Job run year
                :param path: pole_height.json file path
                :return:
                """
    pole_height_collection = database.pole_height_data
    pole_height_collection.delete_many({})
    file_name = "pole_height.json"
    file_path = os.path.join(path, file_name)

    with open(file_path, encoding='ascii') as json_file:
        content = json_file.read()
        print("Number Of records in Pole Height File", len(json_util.loads(content)))
        print("Processing Started......")
        for each in json_util.loads(content):
            pole_height_collection.insert(each)
        print("Pole Height Data Processed")


def export_pole_class_data(database, path):
    """
                This function will export pole_class data from pole_class.json to mongoDB collection pole_class_data
                :param database: Pymongo DB object
                :param job_year: Job run year
                :param path: pole_class.json file path
                :return:
                """
    pole_class_collection = database.pole_class_data
    pole_class_collection.delete_many({})
    file_name = "pole_class.json"
    file_path = os.path.join(path, file_name)

    with open(file_path, encoding='ascii') as json_file:
        content = json_file.read()
        print("Number Of records in Pole Class File", len(json_util.loads(content)))
        print("Processing Started......")
        for each in json_util.loads(content):
            pole_class_collection.insert(each)
        print("Pole Class Data Processed")


def export_data(database, path):
    export_pole_species_data(database, path)
    export_pole_treatment_data(database, path)
    export_pole_height_data(database, path)
    export_pole_class_data(database, path)