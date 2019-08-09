import os
from bson import json_util


def load_radial_data(database, file, year):
    """
        This function will export radial data from radial.json to mongoDB collection radial_data
        :param database: Pymongo DB object
        :param job_year: Job run year
        :param path: radial.json file path
        :return:
        """
    if not os.path.exists(file):
        print(file + " Not Exists")
        return
    radial_data = database.radial_data
    radial_data.delete_many({"YEAR": year})

    with open(file, encoding='ascii') as json_file:
        content = json_file.read()
        print("Number Of records in Radial File", len(json_util.loads(content)))
        print("Processing Started......")
        for each in json_util.loads(content):
            radial_data.insert(each)


def load_null_data(database, file, year):
    """
        This function will export null data from null.json to mongoDB collection null_data
        :param database: Pymongo DB object
        :param job_year: Job run year
        :param path: null.json file path
        :return:
        """
    null_data = database.null_data
    null_data.delete_many({"YEAR": year})
    with open(file, encoding='ascii') as json_file:
        content = json_file.read()
        print("Number Of records in Null File", len(json_util.loads(content)))
        print("Processing Started......")
        for each in json_util.loads(content):
            null_data.insert(each)


def export_data(database, job_year, prev_job_year, path):
    file_name = "radial_old.json"
    file_path = os.path.join(path, file_name)
    load_radial_data(database, file_path, job_year)

    file_name = "radial_new.json"
    file_path = os.path.join(path, file_name)
    load_radial_data(database, file_path, prev_job_year)

    file_name = "null_old.json"
    file_path = os.path.join(path, file_name)
    load_null_data(database, file_path, job_year)

    file_name = "null_new.json"
    file_path = os.path.join(path, file_name)
    load_null_data(database, file_path, prev_job_year)