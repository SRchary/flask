import os
import json
from bson import json_util

import pyodbc
import pandas as pd

import conf


def create_json_file(connection, sql, file_name):
    """
       This function will export data from Oracle DB to json file
       :param job_year:
       :param path: json file creation folder path
       :return:This will create pole_stats.json file in output folder
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


pole_species_sql = '''
    SELECT 
        ps.description AS "SPECIES", NVL(EXTRACT(YEAR FROM ss.INSTALLATIONDATE), ss.INSTALLJOBYEAR) as "YEAR",
        count(*) AS "COUNT"
    FROM 
        EDGIS.supportstructure ss
    LEFT OUTER JOIN 
        (
            SELECT 
                * 
            FROM 
                EDGIS.pge_codes_and_descriptions 
            WHERE 
                domain_name = 'Pole Species'
        ) ps ON ps.code = ss.species
    WHERE 
        subtypecd IN (1,4,5) /* Pole, Guy Stub, Push Brace */
        AND (poleuse not in (1) or poleuse is null) /* Distribution, Transmission with Distribution underbuild */
        AND (customerowned <> 'Y' or customerowned is null)
        AND status in (5,30)  /* Not proposed install */
        --AND installjobnumber <> 'FICTITIOUS'
    GROUP BY 
        ps.description, NVL(EXTRACT(YEAR FROM ss.INSTALLATIONDATE), ss.INSTALLJOBYEAR)
    ORDER BY 
        ps.description;         
'''


pole_treatment_sql = '''
     SELECT 
        pt.description AS "TREATMENT", NVL(EXTRACT(YEAR FROM ss.INSTALLATIONDATE), ss.INSTALLJOBYEAR) as "YEAR",
        count(*) AS "COUNT"
    FROM 
        EDGIS.supportstructure ss
    LEFT OUTER JOIN 
        (
            SELECT 
                * 
            FROM 
                EDGIS.pge_codes_and_descriptions 
            WHERE 
                domain_name = 'Pole Treatment Type - Wood'
        ) pt ON pt.code = ss.originaltreatmenttype
    WHERE 
        subtypecd IN (1,4,5) /* Pole, Guy Stub, Push Brace */
        AND (poleuse not in (1) or poleuse is null) /* Distribution, Transmission with Distribution underbuild */
        AND (customerowned <> 'Y' or customerowned is null)
        AND status in (5,30)  /* Not proposed install */
        --AND installjobnumber <> 'FICTITIOUS'
        GROUP BY pt.description, NVL(EXTRACT(YEAR FROM ss.INSTALLATIONDATE), ss.INSTALLJOBYEAR) 
        ORDER BY pt.description;
'''

pole_height_sql = '''
    SELECT 
        ht.description AS "HEIGHT", NVL(EXTRACT(YEAR FROM ss.INSTALLATIONDATE), ss.INSTALLJOBYEAR) as "YEAR",
        count(*) AS "COUNT"
    FROM 
        EDGIS.supportstructure ss
    LEFT OUTER JOIN 
        (
            SELECT 
                * 
            FROM 
                EDGIS.pge_codes_and_descriptions 
            WHERE 
                domain_name = 'Pole Height'
        ) ht on ht.code = ss.height
    WHERE 
        subtypecd IN (1,4,5) /* Pole, Guy Stub, Push Brace */
        AND (poleuse not in (1) or poleuse is null) /* Distribution, Transmission with Distribution underbuild */
        AND (customerowned <> 'Y' or customerowned is null)
        AND status in (5,30)  /* Not proposed install */
        --AND installjobnumber <> 'FICTITIOUS'
    GROUP BY 
        ht.description, NVL(EXTRACT(YEAR FROM ss.INSTALLATIONDATE), ss.INSTALLJOBYEAR)
    ORDER BY 
        ht.description;
'''

pole_class_sql = '''
    SELECT 
        CLASS AS "CLASS", NVL(EXTRACT(YEAR FROM ss.INSTALLATIONDATE), ss.INSTALLJOBYEAR) as "YEAR",
        count(*) AS "COUNT"
    FROM 
        EDGIS.supportstructure ss
    WHERE 
        subtypecd IN (1,4,5) /* Pole, Guy Stub, Push Brace */
        AND (poleuse not in (1) or poleuse is null) /* Distribution, Transmission with Distribution underbuild */
        AND (customerowned <> 'Y' or customerowned is null)
        AND status in (5,30)  /* Not proposed install */
        --AND installjobnumber <> 'FICTITIOUS'
    GROUP BY 
        CLASS, NVL(EXTRACT(YEAR FROM ss.INSTALLATIONDATE), ss.INSTALLJOBYEAR)
    ORDER BY 
        CLASS;
'''


def import_data(path):
    try:
        connection = pyodbc.connect("{} {}".format(conf.ORACLE_CONNECTION_URI,
                                                   conf.ORACLE_CONNECTION_PARAMS))

        file_path = os.path.join(path, "pole_species.json")
        create_json_file(connection, pole_species_sql, file_path)

        file_path = os.path.join(path, "pole_treatment.json")
        create_json_file(connection, pole_treatment_sql, file_path)

        file_path = os.path.join(path, "pole_height.json")
        create_json_file(connection, pole_height_sql, file_path)

        file_path = os.path.join(path, "pole_class.json")
        create_json_file(connection, pole_class_sql, file_path)
    except Exception as e:
        print(e)
