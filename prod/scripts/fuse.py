import os
import json
from bson import json_util

import pyodbc
import pandas as pd

import conf


def import_data(job_year, path):
    """
       This function will export data from Oracle DB to json file
       :param job_year:
       :param path: json file creation folder path
       :return:This will create fuse.json file in output folder
       """
    try:
        connection = pyodbc.connect("{} {}".format(conf.ORACLE_CONNECTION_URI,
                                                   conf.ORACLE_CONNECTION_PARAMS))
        sql = '''
            SELECT
                fu.installjobprefix as JOBPREFIX,
                fu.installjobnumber as INSTALLJOBNUMBER,
                fu.installjobyear as INSTALLJOBYEAR,
                fu.installationdate as INSTALLATIONDATE,
                NVL(EXTRACT(YEAR FROM installationdate),installjobyear) as IYEAR,
                to_char(SUBSTR(pmo.mat, 1, 2)) as MWC,
                to_char(pmo.mat) as MAT
            FROM
                EDGIS.FUSE fu
            LEFT OUTER JOIN webr.pge_pmorder pmo on fu.installjobnumber = pmo.installjobnumber
            WHERE
                NVL(EXTRACT(YEAR FROM fu.installationdate),fu.installjobyear) > '{}'
                AND fu.STATUS in (5,30) -- In Service, Idle
                AND fu.CUSTOMEROWNED <> 'Y'
                AND fu.installjobnumber <> 'FICTITIOUS';
        '''
        sql = sql.format(job_year-1)

        print("SQL Query.....", sql)

        print("Reading SQL Data from Database......")
        data = pd.read_sql(sql,connection)
        data["INSTALLATIONDATE"].fillna("", inplace=True)

        print("SQL Data Database Converting to Python Data DICT")
        fuse_data = data.to_dict(orient='records')

        print("Fuse JSON File Creating.....")
        file_name = "fuse.json"
        file_path = os.path.join(path, file_name)
        with open(file_path, 'w') as json_file:
            json.dump(fuse_data, json_file, default=json_util.default)
        print("Fuse JSON File Created Successfully.")
    except Exception as e:
        print(e)