import os
import json
from bson import json_util
import traceback

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
    try:
        connection = pyodbc.connect("{} {}".format(conf.ORACLE_CONNECTION_URI,
                                                   conf.ORACLE_CONNECTION_PARAMS))
        sql = '''
                SELECT 
                cond.installjobprefix as installjobprefix,
                cond.installjobnumber as installjobnumber,
                cond.installjobyear as installjobyear,
                NVL(EXTRACT(YEAR FROM installationdate),installjobyear) AS IDYEAR,
                installationdate as installationdate,
                pmo.mat as mat,
                sum(sde.st_length(shape))/5280 as length
            FROM 
                EDGIS.priohconductor cond LEFT OUTER JOIN EDGIS.priohconductorinfo info 
            ON 
                info.conductorguid = cond.globalid
            LEFT OUTER JOIN 
                webr.PGE_PMORDER pmo
            ON
                PMO.installjobnumber = cond.installjobnumber
    
            WHERE 
                NVL(EXTRACT(YEAR FROM installationdate),installjobyear) > '{}' AND
                NVL(EXTRACT(YEAR FROM installationdate),installjobyear) < EXTRACT(YEAR FROM SYSDATE) AND
                cond.status in (5,30) AND 
                info.phasedesignation < 8 AND 
                cond.customerowned <> 'Y'
            GROUP BY 
                cond.installjobprefix,
                cond.installjobnumber,
                cond.installjobyear,
                pmo.mat,
                installationdate;
        '''
        sql = sql.format(job_year-1)

        print("SQL Query.....")
        print(sql)

        print("Reading SQL Data from Database......")
        data = pd.read_sql(sql, connection)
        print(data["INSTALLATIONDATE"])
        data["INSTALLATIONDATE"].fillna("", inplace=True)

        print("SQL Data Database Converting to Python Data DICT")
        conductor_data = data.to_dict(orient='records')

        print("Conductor JSON File Creating.....")
        file_name = "conductor.json"
        file_path = os.path.join(path, file_name)
        with open(file_path, 'w') as json_file:
            json.dump(conductor_data, json_file, default=json_util.default)
        print("Conductor JSON File Created Successfully.")
    except Exception as e:
        print(e)
        traceback.print_exc()