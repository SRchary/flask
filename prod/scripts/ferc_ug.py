import datetime
import json
import os
import traceback

from bson import json_util

import pyodbc
import pandas as pd
import conf

ugl_sql = '''
            SELECT
                DISTINCT UGL.SAP_FUNC_LOC_NO as SAP_FUNC_LOC_NO,
                UGL.TLINE_NO as TLINE_NO,
                UGL.TLINE_NM as TLINE_NM,
                UGL.NOMINAL_VOLTAGE as NOMINAL_VOLTAGE,
                SDE.ST_LENGTH(UGL.SHAPE)/5280 as CIRCUIT_MI,
                UGL.RATEDKV as RATEDKV,
                UGL.STATUS as STATUS
            FROM
                etgis.T_UGLINESEGMENT UGL
            WHERE
                UGL.STATUS <> 'PRP';
        '''

ugc_sql = '''
            SELECT
                UGC.SAP_FUNC_LOC_NO,
                UGC.CABLE_TYPE as CONDUCTOR_TYPE,
                UGC.CABLE_SIZE as CONDUCTOR_SIZE
            FROM
                etgis.T_UGCONDUCTORINFO UGC
            WHERE
                UGC.SAP_FUNC_LOC_NO IN (
                SELECT 
                    SAP_FUNC_LOC_NO
                FROM 
                    etgis.T_UGLINESEGMENT
                WHERE
                    SAP_FUNC_LOC_NO IS NOT NULL 
            );
        '''


def verify_datemodified(connection, year):
    end_date = "15-01-{}".format(year + 1)
    start_date = "01-11-{}".format(year)
    end_date = datetime.datetime.strptime(end_date, "%d-%m-%Y").date()
    start_date = datetime.datetime.strptime(start_date, "%d-%m-%Y").date()
    sql = "select max(datemodified) as MAX_DATE from edgis.t_uglinesegment;"
    data = pd.read_sql(sql, connection)
    data = data.to_dict(orient='records')

    date_modified = data[0].get("MAX_DATE") if data else None
    date_modified = date_modified.date() if date_modified else None
    if not (date_modified and start_date <= date_modified <= end_date):
        print("Last Modified Crossed then Expected.")
        return False
    return True


def import_data(job_year, path):
    """
    This function will export data from Oracle DB to json file
    :param job_year:
    :param path: json file creation folder path
    :return:This will create ferc.json file in output folder
    """
    try:
        connection = pyodbc.connect("{} {}".format(conf.ORACLE_CONNECTION_URL_ET,
                                                   conf.ORACLE_CONNECTION_PARAMS_ET))
        if not verify_datemodified(connection, job_year):
            return
        print("Reading UGL SQL Data from Database......")
        ugl_data = pd.read_sql(ugl_sql, connection)
        ugl_data["RATEDKV"].fillna("", inplace=True)
        ugl_data["TLINE_NO"].fillna("", inplace=True)
        ugl_data_extra = ugl_data[["SAP_FUNC_LOC_NO", "TLINE_NO", "NOMINAL_VOLTAGE", "RATEDKV", "STATUS"]]
        ugl_data_extra = ugl_data_extra.drop_duplicates()
        ugl_data_extra["STRUC_TYPE"] = "N/A"
        ugl_data = ugl_data.groupby(['SAP_FUNC_LOC_NO', 'TLINE_NM', 'RATEDKV'])['CIRCUIT_MI'].sum().reset_index()
        ugl_data = ugl_data.drop_duplicates()
        print("Reading UGC SQL Data from Database......")
        ugc_data = pd.read_sql(ugc_sql, connection)
        ugc_data["CONDUCTOR_SEGMENT"] = ugc_data[["CONDUCTOR_SIZE", "CONDUCTOR_TYPE"]].apply(lambda x: " - ".join(filter(None, x)), axis=1)
        ugc_data = ugc_data.groupby(['SAP_FUNC_LOC_NO'])['CONDUCTOR_SEGMENT'].apply(lambda x: ' '.join(set(x))).reset_index()

        ugl_data = pd.merge(ugl_data, ugl_data_extra, on=['SAP_FUNC_LOC_NO', 'RATEDKV'], how='left')
        result_data = pd.merge(ugl_data, ugc_data, on='SAP_FUNC_LOC_NO', how='left')
        result_data["YEAR"] = job_year
        result_data = result_data.to_dict(orient='records')
        file_name = "ferc_ug.json"
        file_path = os.path.join(path, file_name)
        print("Ferc_UG json creating.....")
        with open(file_path, 'w') as json_file:
            json.dump(result_data, json_file, default=json_util.default)
        print("Ferc_UG json file created Successfully....")
    except Exception as e:
        print(e)
        traceback.print_exc()
