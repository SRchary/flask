import os
import base64
import datetime

SECRET_KEY = ""

#base64.b64encode("".encode("utf-8")).decode("utf-8")
# GeoMart:
# connection = pyodbc.connect('Driver={Oracle in OraClient11g_home1}; DBQ=pretappdbolc006-vip:1521/GMED1P_P.comp.pge.com; '
#                             'Uid=geomart_ro; CHARSET=UTF8;')
ENC_ORACLE_CONNECTION_URI = 'RHJpdmVyPXtPcmFjbGUgaW4gT3JhQ2xpZW50MTFnX2hvbWUxfTsgREJRPXByZXRhcHBkYm9sYzAwNi12aXA6MTUyMS9HTUVEMVBfUC5jb21wLnBnZS5jb207'
ENC_ORACLE_CONNECTION_PARAMS = 'VWlkPWdlb21hcnRfcm87IFB3ZD1nZW9tYXJ0X3JvOyBDSEFSU0VUPVVURjg7'
# ED DataMart:
# connection = pyodbc.connect('Driver={Oracle in OraClient11g_home1}; DBQ=edgisdboraprd05.comp.pge.com:1521/EDDATM_PRD; '
#                             'Uid=geomart_ro; CHARSET=UTF8;')
#ENC_ORACLE_CONNECTION_URI = 'RHJpdmVyPXtPcmFjbGUgaW4gT3JhQ2xpZW50MTFnX2hvbWUxfTsgREJRPWVkZ2lzZGJvcmFwcmQwNS5jb21wLnBnZS5jb206MTUyMS9FRERBVE1fUFJEOw=='
#ENC_ORACLE_CONNECTION_PARAMS = 'VWlkPWdlb21hcnRfcm87IFB3ZD1HRU9NQVJUOTk5OTEzNDsgQ0hBUlNFVD1VVEY4Ow=='


# connection_old = pyodbc.connect('Driver={Oracle in OraClient11g_home1}; DBQ=pritgisdbolx001.comp.pge.com:1521/EDHIST1P; '
#                             'Uid=geomart_ro; CHARSET=UTF8;')
ENC_ORACLE_CONNECTION_URI_CYEAR = 'RHJpdmVyPXtPcmFjbGUgaW4gT3JhQ2xpZW50MTFnX2hvbWUxfTsgREJRPXByaXRnaXNkYm9seDAwMS5jb21wLnBnZS5jb206MTUyMS9FREhJU1QxUDs='
ENC_ORACLE_CONNECTION_PARAMS_CYEAR = 'VWlkPWdlb21hcnRfcm87IFB3ZD1nZW9tYXJ0X3JvOyBDSEFSU0VUPVVURjg7'


# connection_new = pyodbc.connect('Driver={Oracle in OraClient11g_home1}; DBQ=pritgisdbolx001.comp.pge.com:1521/EDHIST2P; '
#                             'Uid=geomart_ro; CHARSET=UTF8;')
ENC_ORACLE_CONNECTION_URI_PYEAR = 'RHJpdmVyPXtPcmFjbGUgaW4gT3JhQ2xpZW50MTFnX2hvbWUxfTsgREJRPXByaXRnaXNkYm9seDAwMS5jb21wLnBnZS5jb206MTUyMS9FREhJU1QyUDs='
ENC_ORACLE_CONNECTION_PARAMS_PYEAR = 'VWlkPWdlb21hcnRfcm87IFB3ZD1nZW9tYXJ0X3JvOyBDSEFSU0VUPVVURjg7'

ORACLE_CONNECTION_URI = base64.b64decode(ENC_ORACLE_CONNECTION_URI.encode("utf-8")).decode("utf-8")
ORACLE_CONNECTION_PARAMS = base64.b64decode(ENC_ORACLE_CONNECTION_PARAMS.encode("utf-8")).decode("utf-8")

ORACLE_CONNECTION_URI_CYEAR = base64.b64decode(ENC_ORACLE_CONNECTION_URI_CYEAR.encode("utf-8")).decode("utf-8")
ORACLE_CONNECTION_PARAMS_CYEAR = base64.b64decode(ENC_ORACLE_CONNECTION_PARAMS_CYEAR.encode("utf-8")).decode("utf-8")

ORACLE_CONNECTION_URI_PYEAR = base64.b64decode(ENC_ORACLE_CONNECTION_URI_PYEAR.encode("utf-8")).decode("utf-8")
ORACLE_CONNECTION_PARAMS_PYEAR = base64.b64decode(ENC_ORACLE_CONNECTION_PARAMS_PYEAR.encode("utf-8")).decode("utf-8")


MONGO_DATABASE_URI = "mongodb://10.31.168.96:27017/ReportingDetails"
MONGO_DATABASE_NAME = "ReportingDetails"
MONGO_REPLICASET = None
#MONGO_REPLICASET = "mongors"

JOB_YEAR = (datetime.datetime.now().year)-1


APP_ROOT = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(ROOT_DIR, "output")

# GRC_FILE_DIR = "static/"
# GRC_FILE_NAME = "GRC_4_2_2_2.xlsx"

GRC_FILE_DIRS = ["\\\\PRETAPPIISWC006\\GeoMart\\CADR_CSV\\",
                 "\\\\PRETAPPIISWC007\\GeoMart\\CADR_CSV\\",
                 "\\\\PRGTAPPIISWC007\\GeoMart\\CADR_CSV\\",
                 "\\\\PRGTAPPIISWC008\\GeoMart\\CADR_CSV\\"]
GRC_FILE_NAME = "GRC_4_2_2_2"
FERC_FILE_NAME = "FERC422-423"
FERC_INTR_FILE_NAME = "INTERMEDIATE_FERC422-423"
FERC_TEMPLATE_FILE = os.path.join(os.path.join(ROOT_DIR, "static"), "FERC_Form_1_Pages_422-423_Template.xlsx")