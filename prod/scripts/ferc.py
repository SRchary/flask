import datetime
import json
import os
import traceback

from bson import json_util

import pyodbc
import pandas as pd
import conf

POLE_STRUCTURE_TYPE = {
    "LAMINATED WOOD POLE": "SWP",
    "LATTICE STEEL POLE": "SSP",
    "LATTICE STEEL TOWER": "T",
    "LIGHT DUTY STEEL POLE": "SSP",
    "SINGLE WOOD POLE": "SWP",
    "TUBULAR STEEL": "SSP",
    "LDSP": "SSP",
    "TSP": "SSP",
    "OTHER": "OTHER"
}

TOWER_STRUCTURE_TYPE = {
    "LAMINATED WOOD POLE": "SWP",
    "LATTICE STEEL POLE": "SSP",
    "LATTICE STEEL TOWER": "T",
    "LIGHT DUTY STEEL POLE": "SSP",
    "SINGLE WOOD POLE": "SWP",
    "TUBULAR STEEL": "SSP",
    "LDSP": "SSP",
    "TSP": "SSP"
}

ohl_sql = '''
            SELECT
                OHL.SAP_FUNC_LOC_NO as SAP_FUNC_LOC_NO,
                OHL.TLINE_NO as TLINE_NO,
                OHL.TLINE_NM as TLINE_NM,
                OHL.NOMINAL_VOLTAGE as NOMINAL_VOLTAGE,
                SDE.ST_LENGTH(OHL.SHAPE)/5280 as CIRCUIT_MI,
                OHL.RATEDKV as RATEDKV,
                OHL.STATUS as STATUS
            FROM
                etgis.T_OHLINESEGMENT OHL
            WHERE 
                OHL.subtypecd <> '2'
                AND OHL.STATUS <> 'PRP';
        '''

ohc_sql = """
            SELECT
                DISTINCT OHC.SAP_FUNC_LOC_NO,
                OHC.CONDUCTOR_TYPE as CONDUCTOR_TYPE,
                OHC.CONDUCTOR_SIZE as CONDUCTOR_SIZE,
                OHC.CONDUCTOR_GROUP as CONDUCTOR_GROUP
            FROM
                etgis.T_OHCONDUCTORINFO OHC
            WHERE
                OHC.SAP_FUNC_LOC_NO IN (
                SELECT 
                    SAP_FUNC_LOC_NO
                FROM 
                    etgis.T_OHLINESEGMENT
                WHERE
                    SAP_FUNC_LOC_NO IS NOT NULL 
            );
        """

tts_sql = """
            SELECT
                TTS.SAP_FUNC_LOC_NO,
                TTS.STRUCTURE_TYPE as TOWER_STRUCTURE_TYPE
            FROM
         etgis.T_TOWERSTRUCTURE TTS       
            WHERE
                TTS.SAP_FUNC_LOC_NO IN (
                    SELECT 
                        SAP_FUNC_LOC_NO
                    FROM 
                        etgis.T_OHLINESEGMENT
                    WHERE
                        SAP_FUNC_LOC_NO IS NOT NULL 
                )
            ORDER BY TTS.SAP_FUNC_LOC_NO;
        """

tps_sql = """
            SELECT  
                DISTINCT TPS.SAP_FUNC_LOC_NO,  
                TPS.STRUCTURE_TYPE as POLE_STRUCTURE_TYPE
            FROM  
                etgis.T_POLESTRUCTURE TPS  
            WHERE  
                TPS.SAP_FUNC_LOC_NO IN (  
                    SELECT  
                    SAP_FUNC_LOC_NO  
                    FROM  
                    etgis.T_OHLINESEGMENT  
                    WHERE  
                    SAP_FUNC_LOC_NO IS NOT NULL  
                ) 
            ORDER BY 
                TPS.SAP_FUNC_LOC_NO;
        """


def verify_datemodified(connection, year):
    end_date = "15-01-{}".format(year + 1)
    start_date = "01-11-{}".format(year)
    end_date = datetime.datetime.strptime(end_date, "%d-%m-%Y").date()
    start_date = datetime.datetime.strptime(start_date, "%d-%m-%Y").date()
    sql = "select max(datemodified) as MAX_DATE from edgis.t_ohlinesegmnet;"
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

        print("Reading OHL Data.....")
        ohl_data = pd.read_sql(ohl_sql, connection)
        ohl_data["RATEDKV"].fillna("", inplace =True)
        ohl_data["SAP_FUNC_LOC_NO"] = ohl_data["SAP_FUNC_LOC_NO"].apply(lambda x: x.strip() if x else x)
        ohl_data_extra = ohl_data[["SAP_FUNC_LOC_NO", 'TLINE_NM', 'TLINE_NO', "NOMINAL_VOLTAGE", "STATUS", "RATEDKV"]]
        ohl_data_extra = ohl_data_extra.drop_duplicates()
        ohl_data = ohl_data.groupby(['SAP_FUNC_LOC_NO', "TLINE_NO", "TLINE_NM", "RATEDKV", "STATUS"])['CIRCUIT_MI'].sum().reset_index()
        ohl_data = ohl_data.drop_duplicates()
        ohl_data = ohl_data[pd.notnull(ohl_data['SAP_FUNC_LOC_NO'])]
        print("Reading OHC Data.....")
        ohc_data = pd.read_sql(ohc_sql, connection)
        ohc_data["SAP_FUNC_LOC_NO"] = ohc_data["SAP_FUNC_LOC_NO"].apply(lambda x: x.strip() if x else x)
        ohc_data["CONDUCTOR_SEGMENT"] = ohc_data[["CONDUCTOR_SIZE", "CONDUCTOR_TYPE", "CONDUCTOR_GROUP"]].apply(lambda x: " - ".join(filter(None,x)), axis=1)
        ohc_data = ohc_data.groupby(['SAP_FUNC_LOC_NO'])['CONDUCTOR_SEGMENT'].apply(lambda x: ' '.join(set(x))).reset_index()

        print("Reading TTS Data.....")
        tts_data = pd.read_sql(tts_sql, connection)
        tts_data["TOWER_STRUCTURE_TYPE"] = tts_data["TOWER_STRUCTURE_TYPE"].apply(lambda x: TOWER_STRUCTURE_TYPE.get(x, "OTHERS"))
        tts_data = tts_data.drop_duplicates()
        tts_data = tts_data.rename(columns={"TOWER_STRUCTURE_TYPE": "STRUC_TYPE"})

        print("Reading TPS Data.....")
        tps_data = pd.read_sql(tps_sql, connection)
        tps_data["POLE_STRUCTURE_TYPE"] = tps_data["POLE_STRUCTURE_TYPE"].apply(lambda x: POLE_STRUCTURE_TYPE.get(x, "OTHERS"))
        tps_data = tps_data.drop_duplicates()
        tps_data = tps_data.rename(columns={"POLE_STRUCTURE_TYPE": "STRUC_TYPE"})
        struc_data = pd.concat([tts_data, tps_data])
        struc_data = struc_data.groupby(['SAP_FUNC_LOC_NO'])['STRUC_TYPE'].apply(lambda x: ' '.join(set(x))).reset_index()
        ohl_data = pd.merge(ohl_data, ohl_data_extra, on=['SAP_FUNC_LOC_NO', 'TLINE_NO', 'TLINE_NM', 'RATEDKV', 'STATUS'], how='left')
        ohl_data = pd.merge(ohl_data, ohc_data, on='SAP_FUNC_LOC_NO', how='left')
        result_data = pd.merge(ohl_data, struc_data, on='SAP_FUNC_LOC_NO', how='left')
        result_data = result_data.apply(lambda x: x.str.strip() if x.dtype == "object" else x)
        result_data.drop_duplicates(inplace=True)
        result_data["YEAR"] = job_year
        result_data = result_data.to_dict(orient='records')
        file_name = "ferc.json"
        file_path = os.path.join(path, file_name)
        print("Ferc json creating.....")
        with open(file_path, 'w') as json_file:
            json.dump(result_data, json_file, default=json_util.default)
        print("Ferc json created Successfully...")
    except Exception as e:
        print(e)
        traceback.print_exc()
