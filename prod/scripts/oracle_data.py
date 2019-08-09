import os
from datetime import datetime

import conf
from scripts import poles_replacement
from scripts import pole_age_histogram
from scripts import pole_stats
from scripts import pilc
from scripts import conductor
from scripts import grasshopper
from scripts import fuse
from scripts import hmwpe
from scripts import ferc
from scripts import ferc_ug


def import_data(path="20180101"):
    """
    This module will load data from oracle database to local json files.
    we are offloading data from oracle to JSON Files. this files will be
    created every new execution of this script. Each execution files will
    be created in same folder with added timestamp of current day.
    Once successful completion of this script, Data is ready to load on
    mongo Database.
    """

    # Importing Line4 Data
    conductor.import_data(conf.JOB_YEAR, path)

    # Importing Line 1A Data
    poles_replacement.import_data(conf.JOB_YEAR, path)
    
    # Importing Line 1B Data
    pole_age_histogram.import_data(path)

    # Importing Line 1C Data
    pole_stats.import_data(path)

    # Importing Line2 Data
    pilc.import_data(conf.JOB_YEAR, conf.JOB_YEAR-1, path)

    # Importing Line3A Data
    # This Tab Data is Loaded in Above. We are using same data for Loading in Tab.

    # Importing Line3B Data
    hmwpe.import_data(conf.JOB_YEAR - 1, path)

    # Importing Line5 Data
    grasshopper.import_data(conf.JOB_YEAR, conf.JOB_YEAR-1, path)

    # Importing Line6 Data
    fuse.import_data(conf.JOB_YEAR, path)

    # Importing Ferc Data
    ferc.import_data(conf.JOB_YEAR, path)

    # Import Ferc_ug Data
    ferc_ug.import_data(conf.JOB_YEAR, path)


if __name__ == "__main__":
    try:
        folder_path = os.path.join(conf.OUTPUT_DIR, datetime.now().strftime('%Y%m%d%H%M'))
        print("Output Folder Path ", folder_path)
        os.makedirs(folder_path)
        print("Output Folder Created ", folder_path)
        import_data(folder_path)
    except Exception as e:
        print(e)
