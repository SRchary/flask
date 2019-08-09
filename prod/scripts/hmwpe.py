import datetime
import json
import os

from bson import json_util

import pyodbc
import pandas as pd

import conf


def import_data(job_year, path):
    """
    This function will export data from Oracle DB to json file
    :param job_year:
    :param path: json file creation folder path
    :return:This will create conductor.json file in output folder
    """
    hmwpe_sql = """
                SELECT 
                    DISTINCT PUG.INSTALLJOBNUMBER,
                    CT.TESTDATE as TESTDATE,
                    CS.FEEDERTYPE as CIRCUIT_TYPE,
                    SDE.ST_LENGTH(PUG.SHAPE)/5280 as MILES,
                    PUG.STATUS, 
                    PUGI.PGE_CONDUCTORCODE,
                    PUG.CUSTOMEROWNED 
                FROM 
                    EDGIS.PRIUGCONDUCTOR PUG
                LEFT OUTER JOIN EDGIS.PRIUGCONDUCTORINFO PUGI on PUGI.GLOBALID = PUG.GLOBALID
                LEFT OUTER JOIN EDGIS.CABLETEST CT on CT.CONDUCTORGUID = PUG.GLOBALID
                LEFT OUTER JOIN EDGIS.CIRCUITSOURCE CS on CS.CIRCUITID = PUG.CIRCUITID
                WHERE 
                    CT.SUBTYPECD = '2' AND 
                    CT.TESTDATE > '31-DEC-{}' AND
                    PUGI.PGE_CONDUCTORCODE is not null
                    AND CS.FEEDERTYPE in (1,2,3);
            """
    connection = pyodbc.connect("{} {}".format(conf.ORACLE_CONNECTION_URI,
                                               conf.ORACLE_CONNECTION_PARAMS))
    hmwpe_sql = hmwpe_sql.format(job_year)
    print("Reading HMWPE SQL Data from Database......")
    result_data = pd.read_sql(hmwpe_sql, connection)
    print(result_data.shape)
    result_data = result_data.to_dict(orient='records')
    print("Hmwpe JSON File Creating.....")
    file_name = "hmwpe.json"
    file_path = os.path.join(path, file_name)

    with open(file_path, 'w') as json_file:
        json.dump(result_data, json_file, default=json_util.default)
