"""
   This function will export data from Oracle DB to json file
   :param job_year:
   :param path: json file creation folder path
   :return:This will create grasshopper.json file in output folder
   """

import os
import json
from datetime import datetime
from bson import json_util

import pyodbc
import pandas as pd

import conf

sql = '''
    SELECT 
        count(*) AS RECORDS
    FROM 
        EDGIS.switch
    WHERE 
        status in (5,30) 
        AND attachmenttype = 2 
        AND installjobnumber <> 'FICTITIOUS';
'''


def create_json_file(connection, sql, file_name, year):
    print("SQL Query.....")
    print(sql)
    
    print("Reading SQL Data from Database......")
    data = pd.read_sql(sql,connection)
    
    print("Adding Year to Records")
    data["YEAR"] = year
    
    print("DATAFRAME Converting to Python Data DICT")
    grasshopper_data = data.to_dict(orient='records')

    print("Grasshopper JSON File Creating.....")

    with open(file_name, 'w') as json_file:
        json.dump(grasshopper_data, json_file, default=json_util.default)
    print("{} JSON File Created Successfully.".format(file_name))


def verify_datemodified(connection, year):
    end_date = "15-01-{}".format(year + 1)
    start_date = "01-11-{}".format(year)
    end_date = datetime.strptime(end_date, "%d-%m-%Y").date()
    start_date = datetime.strptime(start_date, "%d-%m-%Y").date()
    sql = "select max(datemodified) as MAX_DATE from edgis.switch;"
    data = pd.read_sql(sql, connection)
    data = data.to_dict(orient='records')

    date_modified = data[0].get("MAX_DATE") if data else None
    date_modified = date_modified.date() if date_modified else None
    if not (date_modified and start_date <= date_modified <= end_date):
        print("Last Modified Crossed then Expected.")
        return False
    return True


def import_data(job_year, previous_year, path):
    try:
        connection_latest = pyodbc.connect("{} {}".format(conf.ORACLE_CONNECTION_URI_CYEAR,
                                                          conf.ORACLE_CONNECTION_PARAMS_CYEAR))
        connection_prev = pyodbc.connect("{} {}".format(conf.ORACLE_CONNECTION_URI_PYEAR,
                                                        conf.ORACLE_CONNECTION_PARAMS_PYEAR))
        if not verify_datemodified(connection_latest, job_year):
            return
        if not verify_datemodified(connection_prev, previous_year):
            return

        file_name = "grasshopper_old.json".format(previous_year)
        file_path = os.path.join(path, file_name)
        create_json_file(connection_prev, sql, file_path, previous_year)

        file_name = "grasshopper_new.json".format(job_year)
        file_path = os.path.join(path, file_name)
        create_json_file(connection_latest, sql, file_path, job_year)
    except Exception as e:
        print(e)
