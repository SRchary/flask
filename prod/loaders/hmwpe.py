import os
from bson import json_util


def export_data(database, job_year, path):
    """
        This function will export conductor data from conductor.json to mongoDB collection conductor_data
        :param database: Pymongo DB object
        :param job_year: Job run year
        :param path: conductor.json file path
        :return:
        """
    hmwpe_data = database.hmwpe_data

    # Removing existing records for avoiding duplicates.
    hmwpe_data.delete_many({"$expr": {"$eq": [{"$year": "$TESTDATE"}, job_year]}})

    file_name = "hmwpe.json"
    file_path = os.path.join(path, file_name)

    with open(file_path, encoding='ascii') as json_file:
        content = json_file.read()
        print("Number Of records in HMWPE File", len(json_util.loads(content)))
        print("Processing Started......")
        for each in json_util.loads(content):
            hmwpe_data.insert(each)
