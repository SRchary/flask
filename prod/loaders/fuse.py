from datetime import datetime
import os
from bson import json_util


def export_data(database, job_year, path):
    """
    This function will export fuse data from fuse.json to mongoDB collection fuse_data
    :param database: Pymongo DB object
    :param job_year: Job run year
    :param path: fuse.json file path
    :return:
    """
    fuse_data = database.fuse_data
    job_date = "{}-12-31".format(job_year - 1)
    installation_date = datetime.strptime(job_date, "%Y-%m-%d")
    x = fuse_data.delete_many({"IYEAR": {'$gte': job_year}})
    print(x.deleted_count, " documents deleted from fuse.")
    file_name = "fuse.json"
    file_path = os.path.join(path, file_name)

    with open(file_path, encoding='ascii') as json_file:
        content = json_file.read()
        print("Number Of records in Fuse File", len(json_util.loads(content)))
        print("Processing Started......")
        for each in json_util.loads(content):
            fuse_data.insert(each)