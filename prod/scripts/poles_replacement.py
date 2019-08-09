import os
import json

from bson import json_util
import traceback
import pyodbc
import pandas as pd

import conf


def import_data(year, path):
    """
       This function will export data from Oracle DB to json file
       :param job_year:
       :param path: json file creation folder path
       :return:This will create poles_replacements.json file in output folder
       """
    try:
        connection = pyodbc.connect("{} {}".format(conf.ORACLE_CONNECTION_URI,
                                                   conf.ORACLE_CONNECTION_PARAMS))
        sql = '''
            SELECT
                ss.installjobnumber as Installjobnumber,
                ss.installationdate as InstallationDate,
                NVL(EXTRACT(YEAR FROM installationdate),installjobyear) AS IDYEAR,
                to_char(SUBSTR(pmo.mat, 1, 2)) as MWC,
                to_char(pmo.mat) as MAT
            FROM 
                EDGIS.supportstructure ss
            LEFT OUTER JOIN webr.pge_pmorder pmo on ss.installjobnumber = pmo.installjobnumber
            WHERE 
                NVL(EXTRACT(YEAR FROM installationdate),installjobyear) > '{}' AND
                ss.status = 5 AND
                (ss.customerowned = 'N' or ss.customerowned is null);
        '''.format(str(year-1))
        print("SQL Query.....", sql)

        print("Reading SQL Data from Database......")
        data = pd.read_sql(sql, connection)
        data["YEAR"] = data["INSTALLATIONDATE"].apply(lambda x: x.year if x else x)
        print("SQL Data Database Converting to Python Data DICT")
        data["INSTALLATIONDATE"].fillna("", inplace=True)
        data["INSTALLATIONDATE"] = data["INSTALLATIONDATE"].apply(lambda x: pd.to_datetime(x, errors='coerce') if x else x)
        #data["INSTALLATIONDATE"] = data["INSTALLATIONDATE"].apply(lambda x: "" if x==pd.NaT else x)
        data["INSTALLATIONDATE"] = data["INSTALLATIONDATE"].fillna("")
        data["MWC"] = data["MWC"].fillna("NULL")
        poles_data = data.to_dict(orient='records')
        print("Poles JSON File Creating.....")
        
        file_name = "pole_replacement.json"
        file_path = os.path.join(path, file_name)
        with open(file_path, 'w') as json_file:
            json.dump(poles_data, json_file, default=json_util.default)
        print("Poles JSON File Created Successfully.")
    except Exception as e:
        print("Unable to load Poles Replacement Data from Oracle to JSON file.")
        print(str(e))
        traceback.print_exc()
