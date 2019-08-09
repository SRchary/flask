import json
import os

from bson import json_util

import pyodbc
import pandas as pd

import conf


def create_json_file(connection, sql, file_name):
    """
       This function will export data from Oracle DB to json file.
       We are not using the queries for prioh and priug conductors we are not showing those tables in excel sheet.
       :param job_year:
       :param path: json file creation folder path
       :return:This will create pole_age_histogram.json file in output folder
       """
    print("SQL Query.....")
    print(sql)

    print("Reading SQL Data from Database......")
    data = pd.read_sql(sql, connection)

    print("SQL Data Database Converting to Python Data DICT")
    conductor_data = data.to_dict(orient='records')

    print("Conductor JSON File Creating.....")
    with open(file_name, 'w') as json_file:
        json.dump(conductor_data, json_file, default=json_util.default)
    print("{} JSON File Created Successfully.".format(file_name))


support_structure_sql = '''
    SELECT 
        NVL(EXTRACT(YEAR FROM installationdate),installjobyear) AS INSTALLATIONYEAR,
        count(*) AS COUNT
    FROM 
        EDGIS.supportstructure
    WHERE 
        subtypecd IN (1,4,5) /* Pole, Guy Stub, Push Brace */
        AND poleuse in (4,5) /* Distribution, Transmission with Distribution underbuild */
        AND customerowned <> 'Y'
        AND status <> 30  /* Not proposed install */
    GROUP BY 
        NVL(EXTRACT(YEAR FROM installationdate),installjobyear)
    ORDER BY 
        NVL(EXTRACT(YEAR FROM installationdate),installjobyear);        
'''

# pri_oh_conductor_sql = '''
#     SELECT
#         info.pge_conductorcode as CONDUCTORCODE,
#         NVL(EXTRACT(YEAR FROM info.installationdate),cond.installjobyear) as INSTALLJOBYEAR,
#         sum(sde.st_length(cond.shape))/5280 as MILES
#     FROM
#         EDGIS.priohconductor cond
#     LEFT OUTER JOIN
#         EDGIS.priohconductorinfo info ON info.conductorguid = cond.globalid
#     WHERE
#         info.phasedesignation < 8 /* Phase conductor only */
#         AND cond.customerowned <> 'Y'
#         AND cond.status IN (5,30)
#     GROUP BY
#         info.pge_conductorcode,NVL(EXTRACT(YEAR FROM info.installationdate),cond.installjobyear)
#     ORDER BY
#         NVL(EXTRACT(YEAR FROM info.installationdate),cond.installjobyear);
# '''
#
# pri_ug_conductor_sql = '''
#     SELECT
#         info.pge_conductorcode as CONDUCTORCODE,
#         NVL(EXTRACT(YEAR FROM info.installationdate),cond.installjobyear) as INSTALLJOBYEAR,
#         sum(sde.st_length(cond.shape))/5280 as MILES
#     FROM
#         EDGIS.priugconductor cond
#     LEFT OUTER JOIN
#         EDGIS.priugconductorinfo info ON info.conductorguid = cond.globalid
#     WHERE
#         info.phasedesignation < 8 /* Phase conductor only */
#         AND cond.customerowned <> 'Y'
#         AND cond.status IN (5,30)
#     GROUP BY
#         info.pge_conductorcode,NVL(EXTRACT(YEAR FROM info.installationdate),cond.installjobyear)
#     ORDER BY
#         NVL(EXTRACT(YEAR FROM info.installationdate),cond.installjobyear);
# '''


def import_data(path):
    try:
        connection = pyodbc.connect("{} {}".format(conf.ORACLE_CONNECTION_URI,
                                                   conf.ORACLE_CONNECTION_PARAMS))
        file_name = "support_structure.json"
        file_path = os.path.join(path, file_name)
        create_json_file(connection, support_structure_sql, file_path)

        # file_name = "pri_oh_conductor.json"
        # file_path = os.path.join(path, file_name)
        # create_json_file(connection, pri_oh_conductor_sql, file_path)
        #
        # file_name = "pri_ug_conductor.json"
        # file_path = os.path.join(path, file_name)
        # create_json_file(connection, pri_ug_conductor_sql, file_path)
    except Exception as e:
        print(e)
