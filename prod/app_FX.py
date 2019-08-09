import datetime

from flask import send_from_directory
from flask_pymongo import PyMongo
from flask import Flask, jsonify, request


import helper
import conf
from ferc import helper as ferc_helper
import process


app = Flask(__name__)
app.config['MONGO_DBNAME'] = conf.MONGO_DATABASE_NAME
app.config['MONGO_URI'] = conf.MONGO_DATABASE_URI

mongo = PyMongo(app, replicaset="mongors") if conf.MONGO_REPLICASET else PyMongo(app)


@app.route("/grc")
def home():
    """
    This function is just for testing api
    :return: This function will return the text message
    """
    return "GRC PROD Homepage (FXIOC)"


@app.errorhandler(405)
def method_not_allowed(e):
    """
    This function will executed if any api called other than GET and Post http methods
    :param e:
    :return: Json response status code 405
    """
    return jsonify(error=405, text=str(e)), 405


@app.route('/grc/v1/report/data/', methods=['GET',])
def grc_report_data():
    """
    This will call the grc summary data.
    Request method: GET
    Request params:report type, start date and end date
    :return: will get the grc summary data in json format
    """
    response_data = dict()
    try:
        end_year = request.args['end_date']
        end_year = int(end_year)
        if end_year < 1900:
            raise Exception("Please Select Valid Year.")
        start_date = "01/01/{}".format(end_year)
        end_date = "12/31/{}".format(end_year)

        start_date = datetime.datetime.strptime(start_date, "%m/%d/%Y")
        end_date = datetime.datetime.strptime(end_date, "%m/%d/%Y")
    except Exception as e:
        response_data["status"] = "FAIL"
        response_data["message"] = "Please Select Valid Year."
        response_data["data"] = []
        return jsonify(response_data), 400
    report_data = dict()
    report_data["report_type"] = "grc"
    report_data["report_name"] = "General Rate Case"
    report_data["report_year"] = end_year
    report_data["start_date"] = start_date.date()
    report_data["end_date"] = end_date.date()
    report_data["report_data"] = helper.get_grc_report_data(mongo, start_date, end_date)
    response_data["status"] = "SUCCESS"
    response_data["message"] = "Report Data Generated Successfully"
    response_data["data"] = report_data
    return jsonify(response_data), 200


@app.route('/grc/v1/report/download/', methods=['GET', ])
def generate_excel_report_download():
    """
    This function will generate excel file and will store in specified path we mentioned
    Request method: GET
    Request params:report type, start date and end date
    :return: This will return the excel file as a attachment
    """
    response_data = dict()
    try:
        end_year = request.args['end_date']
        end_year = int(end_year)
        if end_year < 1900:
            raise Exception("Please Select Valid Year.")
        start_date = "01/01/{}".format(end_year)
        end_date = "12/31/{}".format(end_year)

        start_date = datetime.datetime.strptime(start_date, "%m/%d/%Y")
        end_date = datetime.datetime.strptime(end_date, "%m/%d/%Y")
    except Exception as e:
        response_data["status"] = "FAIL"
        response_data["message"] = "Please Select Valid Year."
        response_data["data"] = []
        return jsonify(response_data), 400
    file_path = helper.generate_grc_report_xlsx(mongo, start_date, end_date)
    return send_from_directory(directory=conf.GRC_FILE_DIRS[0],
                               filename=file_path, as_attachment=True)


@app.route('/ferc/v1/report/download/', methods=['GET', ])
def generate_ferc_excel_report_download():
    """
    This function will generate excel file and stored in specified path we mentioned
    Request method: GET
    :return: This will return excel file as a attachment
    """
    response_data = dict()
    try:
        end_year = request.args['end_date']
        end_year = int(end_year)
        if end_year < 1900:
            raise Exception("Please Select Valid Year.")
        start_date = "01/01/{}".format(end_year)
        end_date = "12/31/{}".format(end_year)

        start_date = datetime.datetime.strptime(start_date, "%m/%d/%Y")
        end_date = datetime.datetime.strptime(end_date, "%m/%d/%Y")
    except Exception as e:
        response_data["status"] = "FAIL"
        response_data["message"] = "Please Select Valid Year."
        response_data["data"] = []
        return jsonify(response_data), 400
    file_path = ferc_helper.generate_excel(mongo, start_date, end_date)
    print(file_path)
    return send_from_directory(directory=conf.GRC_FILE_DIRS[0],filename=file_path, as_attachment=True)


@app.route('/report/refresh/', methods=['GET', ])
def refresh_mongodata():
    """
    This function will refresh the db when the db is not up to date.
    Request method: GET
    """
    try:
        process.run()
        return "Data Updated"
    except Exception as e:
        return str(e)


if __name__ == "__main__":
    app.run(debug=True)