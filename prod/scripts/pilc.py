"""
   This function will export data from Oracle DB to json file
   :param job_year:
   :param path: json file creation folder path
   :return:This will create radial.json and null.json file in output folder
   """

import os
import json
from datetime import datetime
from bson import json_util

import pyodbc
import pandas as pd

import conf


conductor_code_map = {
    "50": "PILC", "51": "PILC", "52": "PILC", "53": "PILC", "54": "PILC", "55": "PILC", "56": "PILC",
    "57": "PILC", "58": "PILC", "59": "PILC", "60": "PILC", "61": "PILC", "62": "PILC", "63": "PILC",
    "64": "PILC", "65": "PILC", "66": "PILC", "67": "PILC", "68": "PILC", "69": "PILC", "77": "PILC",
    "79": "PILC", "80": "PILC", "91": "Other", "92": "Other", "93": "Other", "94": "Other", "95": "Other",
    "96": "Other", "100": "PILC", "101": "PILC", "102": "PILC", "103": "PILC", "104": "PILC", "105": "PILC",
    "106": "PILC", "107": "PILC", "108": "PILC", "109": "PILC", "110": "PILC", "111": "PILC", "112": "Other",
    "113": "Other", "114": "Other", "115": "Other", "116": "Other", "117": "Other", "118": "Other", "119": "Other",
    "120": "HMWPE", "121": "HMWPE", "122": "HMWPE", "123": "PILC", "124": "PILC", "125": "PILC", "130": "Other",
    "131": "Other", "132": "Other", "133": "Other", "134": "Other", "135": "Other", "136": "Other", "137": "Other",
    "138": "Other", "139": "Other", "140": "HMWPE", "141": "HMWPE", "142": "HMWPE", "143": "HMWPE",
    "144": "HMWPE", "145": "HMWPE", "150": "XLPE", "151": "XLPE", "152": "XLPE", "153": "XLPE",
    "154": "XLPE", "155": "XLPE", "161": "XLPE", "162": "XLPE", "163": "XLPE", "164": "XLPE", "165": "XLPE",
    "166": "HMWPE", "170": "HMWPE", "171": "HMWPE", "180": "XLPE", "181": "XLPE", "182": "XLPE", "183": "XLPE",
    "184": "XLPE", "185": "XLPE", "186": "XLPE", "187": "EPR", "188": "EPR", "189": "EPR", "190": "EPR",
    "191": "XLPE", "192": "XLPE", "193": "XLPE", "194": "XLPE", "195": "XLPE", "196": "XLPE", "199": "Other",
    "201": "XLPE", "202": "XLPE", "203": "XLPE", "204": "XLPE", "205": "XLPE", "206": "XLPE", "207": "XLPE",
    "211": "XLPE", "212": "XLPE", "213": "XLPE", "214": "XLPE", "215": "XLPE", "216": "XLPE", "217": "XLPE",
    "218": "XLPE", "219": "XLPE", "220": "XLPE", "221": "HMWPE", "222": "HMWPE", "231": "HMWPE", "232": "HMWPE",
    "233": "HMWPE", "234": "HMWPE", "235": "HMWPE", "236": "HMWPE", "237": "HMWPE", "241": "HMWPE", "242": "HMWPE",
    "243": "HMWPE", "244": "HMWPE", "251": "XLPE", "252": "XLPE", "253": "XLPE", "254": "XLPE", "255": "XLPE", "261": "HMWPE",
    "262": "HMWPE", "263": "HMWPE", "269": "XLPE", "270": "XLPE", "271": "XLPE", "272": "XLPE", "273": "XLPE", "274": "XLPE",
    "275": "XLPE", "276": "XLPE", "277": "XLPE", "278": "XLPE", "279": "XLPE", "280": "XLPE", "281": "XLPE", "282": "XLPE",
    "283": "XLPE", "284": "XLPE", "285": "XLPE", "286": "XLPE", "287": "XLPE", "288": "EPR", "289": "EPR", "290": "EPR",
    "291": "XLPE", "292": "XLPE", "293": "EPR", "294": "EPR", "298": "Other", "299": "Other", "390": "XLPE", "391": "XLPE",
    "9999": "Other", "265": "EPR", "392": "", "393": "XLP", "394": "XLP", "395": "EPR", "396": "", "397": "XLP", "398": "EPR",
    "399": "PILC", "400": "EPR", "777": "Other", "888": "Other"
}


def create_json_file(connection, sql, file_name, year):
    print("SQL Query.....")
    print(sql)
    print("Reading SQL Data from Database......")
    data=pd.read_sql(sql,connection)
    print(data)
    data["CONDUCTORCODE"] = data["CONDUCTORCODE"].apply(lambda x: conductor_code_map.get(x, "N/A") if x else "N/A")
    data = data.groupby('CONDUCTORCODE', as_index=False).agg({'COUNT': 'sum'})
    print("CONDUCTOR CODE GROUPED")
    data["YEAR"] = year
    print(data)
    print("DATAFRAME Converting to Python Data DICT")
    conductor_data = data.to_dict(orient='records')

    print("Conductor JSON File Creating.....")
    with open(file_name, 'w') as json_file:
        json.dump(conductor_data, json_file, default=json_util.default)
    print("{} JSON File Created Successfully.".format(file_name))


radial_sql = '''
    SELECT 
        to_char(info.pge_conductorcode) as "CONDUCTORCODE",
        sum(sde.st_length(cond.shape))/5280 as "COUNT"
    FROM 
        EDGIS.priugconductor cond
    LEFT OUTER JOIN 
        EDGIS.priugconductorinfo info ON info.conductorguid = cond.globalid
    LEFT OUTER JOIN 
        EDGIS.circuitsource cs ON cond.circuitid = cs.circuitid
    WHERE 
        cond.status in (5,30) --in service, idle
        AND info.phasedesignation < 8 --basically phase conductor
        AND cond.customerowned <> 'Y' -- PGE and nulls only
        AND cs.feedertype in (1,3)  -- Radial, Tie
    GROUP BY 
        info.pge_conductorcode;  
'''
null_sql = '''
    SELECT 
        to_char(info.pge_conductorcode) as CONDUCTORCODE,
        sum(sde.st_length(cond.shape))/5280 as "COUNT"
    FROM 
        EDGIS.priugconductor cond
    LEFT OUTER JOIN 
        EDGIS.priugconductorinfo info ON info.conductorguid = cond.globalid
    WHERE 
        circuitid is null
        AND cond.status in (5,30)
        AND info.phasedesignation < 8
        AND cond.customerowned <> 'Y'
    GROUP BY 
        info.pge_conductorcode;
'''


def verify_datemodified(connection, year):
    end_date = "15-01-{}".format(year + 1)
    start_date = "01-11-{}".format(year)
    end_date = datetime.strptime(end_date, "%d-%m-%Y").date()
    start_date = datetime.strptime(start_date, "%d-%m-%Y").date()
    sql = "select max(datemodified) as MAX_DATE from edgis.priugconductor;"
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

        file_name = "radial_old.json"
        file_path = os.path.join(path, file_name)
        create_json_file(connection_latest, radial_sql, file_path, job_year)

        file_name = "radial_new.json"
        file_path = os.path.join(path, file_name)
        create_json_file(connection_prev, radial_sql, file_path, previous_year)

        file_name = "null_old.json"
        file_path = os.path.join(path, file_name)
        create_json_file(connection_latest, null_sql, file_path, job_year)

        file_name = "null_new.json"
        file_path = os.path.join(path, file_name)
        create_json_file(connection_prev, null_sql, file_path, previous_year)
    except Exception as e:
        print(e)
