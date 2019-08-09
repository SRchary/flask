import os
import traceback

from bson import json_util


def export_data(database, job_year, path):
    """
        This function will export ferc data from ferc.json to mongoDB collection ferc_data
        :param database: Pymongo DB object
        :param job_year: Job run year
        :param path: ferc.json file path
        :return:
    """
    try:
        file_name = "ferc.json"
        file_path = os.path.join(path, file_name)
        if not os.path.exists(file_path):
            print(file_path + " Not Exists")
            return
        ferc_data = database.ferc_data

        print("Removing existing records for avoiding duplicates.")
        ferc_data.delete_many({"YEAR": job_year})

        with open(file_path, encoding='ascii') as json_file:
            content = json_file.read()
            print("Number Of records in FERC File", len(json_util.loads(content)))
            print("Processing Started......")
            for each in json_util.loads(content):
                ferc_data.insert(each)
            print("FERC Processing Completed......")
    except Exception as e:
        print(e)
        traceback.print_exc()