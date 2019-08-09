import os
from datetime import datetime

from pymongo import MongoClient

import conf
from loaders import poles_replacement
from loaders import pole_age_histogram
from loaders import pole_stats
from loaders import pilc
from loaders import line3b
from loaders import conductor
from loaders import grasshopper
from loaders import fuse
from loaders import hmwpe
from loaders import ferc
from loaders import ferc_ug


def connection():
    """
    Will create MongoDB conncetion using Configurations defied in conf file.
    Will use replicaset if the value assigned in conf file.
    :return:
    It returns the mongodb connection
    """
    if conf.MONGO_REPLICASET:
        client = MongoClient(conf.MONGO_DATABASE_URI, replicaset=conf.MONGO_REPLICASET)
    else:
        client = MongoClient(conf.MONGO_DATABASE_URI)
    return client.ReportingDetails


def add_audit_log(database, start_timestamp=None):
    audit_data = database.audit_data
    audit_data.insert({"start_timestamp": start_timestamp, "completed_timestamp": datetime.now()})


def export_data(folder_path="20180101", start_timestamp=None):
    """
    This module will load data from Json files to MongoDB.
    Will create required collections in database and process the all required json files.
    :param file_ext:
    :return:
    """
    print("Exporting Data from JSON files to Mongo DB Started......")
    print("Creating Mongo Connection....")
    database = connection()

    print("Mongo Connection Created....")

    # ImportingLine 1A Data
    poles_replacement.export_data(database, conf.JOB_YEAR, folder_path)
    
    # Importing Line 1B Data
    pole_age_histogram.export_data(database, folder_path)

    # Importing Line 1C Data
    pole_stats.export_data(database, folder_path)

    # Importing Line2 Data
    pilc.export_data(database, conf.JOB_YEAR, conf.JOB_YEAR-1, folder_path)

    # Importing Line3A Data
    # This Tab Data is Loaded in Above. We are using same data for Loading in Tab.

    # Importing Line3B Data
    hmwpe.export_data(database, conf.JOB_YEAR - 1, folder_path)

    # Importing Line4 Data
    conductor.export_data(database, conf.JOB_YEAR, folder_path)

    # Importing Line5 Data
    grasshopper.export_data(database, conf.JOB_YEAR, conf.JOB_YEAR-1, folder_path)

    # Importing Line6 Data
    fuse.export_data(database, conf.JOB_YEAR, folder_path)

    # Importing Ferc Data
    ferc.export_data(database, conf.JOB_YEAR, folder_path)

    # Importing Ferc_ug Data
    ferc_ug.export_data(database, conf.JOB_YEAR, folder_path)

    add_audit_log(database, start_timestamp)


if __name__ == "__main__":
    folder_path = os.path.join(conf.OUTPUT_DIR, datetime.now().strftime('%Y%m%d%H'))
    print("Output Folder Path ", folder_path)
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
    print("Output Folder Created ", folder_path)
    export_data(folder_path)
