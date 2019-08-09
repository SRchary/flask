import os
import json
from bson import json_util

import pyodbc
import pandas as pd


def import_data():
    """
    This function will export data from Oracle DB to json file
    :param job_year:
    :param path: json file creation folder path
    :return:This will create conductor.json file in output folder
    """
    try:
        connection = pyodbc.connect('Driver={Oracle in OraClient11g_home1}; DBQ=DEETAPPDBOLC005.comp.pge.com:1521/GMED1D; '
                                    'Uid=CADRAPP; Pwd=cadrapp_2016; CHARSET=UTF8;')
        cursor = connection.cursor()
        exceldata = pd.ExcelFile("../static/excel/test_data.xlsx")
        df = exceldata.parse("CABLETEST")
        print(df)
        sql = """
                INSERT INTO "CADRAPP"."CABLETESTX" (
                GLOBALID, CREATIONUSER, 
                DATECREATED, DATEMODIFIED, LASTUSER, 
                CONVERSIONID, CONVERSIONWORKPACKAGE, 
                SUBTYPECD, TESTDATE, CONDUCTORGUID) 
            VALUES (
                '{GLOBALID}', '{CREATIONUSER}', 
                TO_DATE('{DATECREATED}', 'YYYY-MM-DD HH24:MI:SS'), 
                TO_DATE('{DATEMODIFIED}', 'YYYY-MM-DD HH24:MI:SS'), 
                '{LASTUSER}', '{CONVERSIONID}', '{CONVERSIONWORKPACKAGE}', 
                '{SUBTYPECD}', 
                TO_DATE('{TESTDATE}', 'YYYY-MM-DD HH24:MI:SS'), '{CONDUCTORGUID}'
            );
            """

        print(df.columns)
        df = df.fillna("")
        data = df.to_dict(orient='records')
        for row in data:
            print(sql.format(**row))
            insert_query = sql.format(**row)
            try:
                cursor.execute(insert_query)
                connection.commit()
            except:
                pass
        cursor.close()
        connection.close()
    except Exception as e:
        print(e)




import_data()
