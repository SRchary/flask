import os

from bson import json_util
import conf


def export_data(database, year):
    try:
        radial_data = database.line_3b
        radial_data.delete_many({"YEAR": year})

        file_name = "line3b.json"
        file_path = os.path.join(conf.OUTPUT_DIR, file_name)

        with open(file_path, encoding='ascii') as json_file:
            content = json_file.read()
            print("Number Of records in line3b File", len(json_util.loads(content)))
            for each in json_util.loads(content):
                radial_data.insert(each)
    except Exception as e:
        print(e)
