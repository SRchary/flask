import os

from bson import json_util

import conf


def load_grasshopper_data(database, file, year):
    if not os.path.exists(file):
        print(file + " Not Exists")
        return
    grasshopper_data = database.grasshopper_data
    grasshopper_data.delete_many({"YEAR": year})

    with open(file, encoding='ascii') as json_file:
        content = json_file.read()
        print("Number Of records in Radial File", len(json_util.loads(content)))
        print("Processing Started......")
        for each in json_util.loads(content):
            grasshopper_data.insert(each)


def export_data(database, job_year, prev_year, path):
    file_name = "grasshopper_old.json".format(prev_year)
    file_path = os.path.join(path, file_name)
    load_grasshopper_data(database, file_path, prev_year)

    file_name = "grasshopper_new.json".format(job_year)
    file_path = os.path.join(path, file_name)
    load_grasshopper_data(database, file_path, job_year)
