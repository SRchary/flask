import os
from bson import json_util

import conf


def export_data(database, job_year, path):
    """
        This function will export conductor data from conductor.json to mongoDB collection conductor_data
        :param database: Pymongo DB object
        :param job_year: Job run year
        :param path: conductor.json file path
        :return:
        """
    conductor_data = database.conductor_data

    # Removing existing records for avoiding duplicates.
    conductor_data.delete_many({"IDYEAR": {'$gte': job_year}})

    file_name = "conductor.json"
    file_path = os.path.join(path, file_name)

    with open(file_path, encoding='ascii') as json_file:
        content = json_file.read()
        print("Number Of records in Conductor File", len(json_util.loads(content)))
        print("Processing Started......")
        for each in json_util.loads(content):
            conductor_data.insert(each)
