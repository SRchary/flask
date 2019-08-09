import os

from bson import json_util


def load_support_structure(database, path):
    """
        This function will export support structure data from support_structure.json to mongoDB collection support_structure_data
        :param database: Pymongo DB object
        :param job_year: Job run year
        :param path: support_structure.json file path
        :return:
        """
    support_structure_collection = database.support_structure_data
    file_name = "support_structure.json"
    file_path = os.path.join(path, file_name)
    support_structure_collection.delete_many({})
    with open(file_path, encoding='ascii') as json_file:
        content = json_file.read()
        print("Number Of records in Support Structure File", len(json_util.loads(content)))
        print("Processing Started......")
        for each in json_util.loads(content):
            support_structure_collection.insert(each)
        print("Support Structure Data Processed")


# def load_pri_oh_conductor(database, path):
#     """
#         This function will export prioh conductor data from pri_oh.json to mongoDB collection pri_oh_conductor_data
#         :param database: Pymongo DB object
#         :param job_year: Job run year
#         :param path: pri_oh_conductor.json file path
#         :return:
#         """
#     pri_oh_conductor_collection = database.pri_oh_conductor_data
#     pri_oh_conductor_collection.delete_many({})
#     file_name = "pri_oh_conductor.json"
#     file_path = os.path.join(path, file_name)
#     with open(file_path, encoding='ascii') as json_file:
#         content = json_file.read()
#         print("Number Of records in Pri Oh Conductor File", len(json_util.loads(content)))
#         print("Processing Started......")
#         for each in json_util.loads(content):
#             pri_oh_conductor_collection.insert(each)
#         print("Pri oh Conductor Data Processed")
#
#
# def load_pri_ug_conductor(database, path):
#     """
#             This function will export pri_ug conductor data from pri_ug.json to mongoDB collection pri_ug_conductor_data
#             :param database: Pymongo DB object
#             :param job_year: Job run year
#             :param path: pri_ug_conductor.json file path
#             :return:
#             """
#     pri_ug_conductor_collection = database.pri_ug_conductor_data
#     pri_ug_conductor_collection.delete_many({})
#     file_name = "pri_ug_conductor.json"
#     file_path = os.path.join(path, file_name)
#     with open(file_path, encoding='ascii') as json_file:
#         content = json_file.read()
#         print("Number Of records in PRI UG Conductor File", len(json_util.loads(content)))
#         print("Processing Started......")
#         for each in json_util.loads(content):
#             pri_ug_conductor_collection.insert(each)
#         print("Pri UG Conductor Data Processed")


def export_data(database, path):
    load_support_structure(database, path)
    # load_pri_oh_conductor(database, path)
    # load_pri_ug_conductor(database, path)
