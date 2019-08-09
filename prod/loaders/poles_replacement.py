import os
from datetime import  datetime
from bson import json_util


def export_data(database, job_year, path):
    """
            This function will export poles replacements data from poles_replacements.json to mongoDB collection poles_replacements_data
            :param database: Pymongo DB object
            :param job_year: Job run year
            :param path: poles_replacements.json file path
            :return:
            """
    poles_data = database.poles_data
    job_date = "{}-12-31".format(job_year-1)
    installation_date = datetime.strptime(job_date, "%Y-%m-%d")
    x = poles_data.delete_many({"IDYEAR": {"$gte": job_year}})
    print(x.deleted_count, " documents deleted.")
    file_name = "pole_replacement.json"
    file_path = os.path.join(path, file_name)

    with open(file_path, encoding='ascii') as json_file:
        content = json_file.read()
        print("Number Of records in Poles File", len(json_util.loads(content)))
        print("Processing Started......")
        for each in json_util.loads(content):
            poles_data.insert(each)