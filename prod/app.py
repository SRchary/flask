import datetime

from flask import send_from_directory
from flask_pymongo import PyMongo
from flask import Flask, jsonify, request


import helper
import conf
from ferc import helper as ferc_helper
import process
import json
from bson.objectid import ObjectId


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
    return "GRC PROD Homepage (RCIOC)"


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

@app.route( "/project_initiation", methods=['POST'])
def project_initiation():

    return_data = {"error": 1, "message": "Please Try Again"}
    data = request.data
    dataDict = json.loads(data)
    job_number = ''
    if dataDict.get("job_number", -1) != -1:
        job_number = str(dataDict.get("job_number"))
    else:
        return_data["job_number_error"] = "job_number Is Required"

    job_owner = ''
    if dataDict.get("job_owner", -1) != -1:
        job_owner = str(dataDict.get("job_owner"))
    else:
        return_data["job_owner_error"] = "job_owner Is Required"

    transmission_line_name = ''
    if dataDict.get("transmission_line_name", -1) != -1:
        transmission_line_name = str(dataDict["transmission_line_name"])
    else:
        return_data["transmission_line_name_error"] = "transmission_line_name Is Required"

    job_type = -1
    if dataDict.get("job_type", -1) != -1:
        job_type = int(dataDict.get("job_type"))
    else:
        return_data["job_type_error"] = "job_type Is Required"

    oh_estimated_completion_date = ''
    if dataDict.get("oh_estimated_completion_date", -1) != -1:
        oh_estimated_completion_date = dataDict.get("oh_estimated_completion_date")
        oh_estimated_completion_date = datetime.datetime.strptime(oh_estimated_completion_date, "%m/%d/%Y")

    ug_estimated_completion_date = ''
    if dataDict.get("og_estimated_completion_date", -1) != -1:
        ug_estimated_completion_date = dataDict.get("og_estimated_completion_date")
        ug_estimated_completion_date = datetime.datetime.strptime(ug_estimated_completion_date, "%m/%d/%Y")

    is_itlocked = 0;  # Default Value =0 not locked
    if( job_number != '' and job_owner != '' and job_owner != '' and transmission_line_name != '' and job_type >= 0  ):
        try:
            result = helper.find_project(mongo ,{"job_number":job_number})
            if result['total_records'] <=0:
                insert_data ={}
                insert_data['job_number'] =job_number
                insert_data['job_owner'] =job_owner
                insert_data['transmission_line_name'] =transmission_line_name
                insert_data['job_type'] =job_type
                insert_data['oh_estimated_completion_date'] =oh_estimated_completion_date
                insert_data['ug_estimated_completion_date'] =ug_estimated_completion_date
                result = helper.insert_data(mongo ,insert_data)
                if result !='':
                    return_data['error'] = 0
                    return_data['message'] ="Successfully Insereted!"
                else:
                    return_data['error'] =0
                    return_data['message'] ="Please Try Again..!"

            else:
                return_data['error']= 1
                return_data['message'] ="Given Job Number Already Exists"
        except Exception as e:
            return_data['error'] = 1
            return_data['message'] = str(e)
    else:
        return_data["error"] = 1
        return_data['message'] ="Invalid Inputs!"

    return jsonify(return_data)

@app.route( "/project_initiation_update", methods=['POST'])
def project_initiation_update():

    return_data = {"error": 1, "message": "Please Try Again"}
    data = request.data
    dataDict = json.loads(data)
    id = ''
    if dataDict.get("id", -1) != -1:
        id = str(dataDict.get("id"))
    else:
        return_data["id_error"] = "Document id Is Required"


    job_number = ''
    if dataDict.get("job_number", -1) != -1:
        job_number = str(dataDict.get("job_number"))
    else:
        return_data["job_number_error"] = "job_number Is Required"

    job_owner = ''
    if dataDict.get("job_owner", -1) != -1:
        job_owner = str(dataDict.get("job_owner"))
    else:
        return_data["job_owner_error"] = "job_owner Is Required"

    transmission_line_name = ''
    if dataDict.get("transmission_line_name", -1) != -1:
        transmission_line_name = str(dataDict["transmission_line_name"])
    else:
        return_data["transmission_line_name_error"] = "transmission_line_name Is Required"

    job_type = -1
    if dataDict.get("job_type", -1) != -1:
        job_type = int(dataDict.get("job_type"))
    else:
        return_data["job_type_error"] = "job_type Is Required"

    oh_estimated_completion_date = ''
    if dataDict.get("oh_estimated_completion_date", -1) != -1:
        oh_estimated_completion_date = dataDict.get("oh_estimated_completion_date")
        oh_estimated_completion_date = datetime.datetime.strptime(oh_estimated_completion_date, "%m/%d/%Y")

    ug_estimated_completion_date = ''
    if dataDict.get("og_estimated_completion_date", -1) != -1:
        ug_estimated_completion_date = dataDict.get("og_estimated_completion_date")
        ug_estimated_completion_date = datetime.datetime.strptime(ug_estimated_completion_date, "%m/%d/%Y")

    is_itlocked = 0;  # Default Value =0 not locked
    if(id !='' and job_number != '' and job_owner != '' and job_owner != '' and transmission_line_name != '' and job_type >= 0 ):
        try:
            result = helper.find_project(mongo ,{"_id":ObjectId(id)})
            if result['total_records'] >0:
                #check post job number is already exists other than this Selected Document
                job_number_result = helper.find_project(mongo ,{"_id":{"$ne":ObjectId(id)} ,"job_number":job_number })
                if job_number_result['total_records'] >0:
                    return_data['error']= 1
                    return_data['message'] ="Selected Job Number Already Exists"

                else:
                    result['documents'] = list(result['documents'])[0]
                    document_job_type = result['documents']['job_type']
                    if document_job_type != job_type:

                        #Job type change from 2 to job type one (underground TO overhead )
                        if job_type == 1 and document_job_type ==2:
                            up_res1 =helper.project_update_rename(mongo ,{"_id":ObjectId(id)} ,{"underground":"overhead"}, upsert_value =False ,multi_value =False)
                            up_res2 =helper.project_update(mongo ,{"_id":ObjectId(id)} ,{"job_type":1 ,"overhead.type":1}, upsert_value =False ,multi_value =False)

                            #unsetting underground
                            if document_job_type ==3 :
                                up_res1 =helper.project_update_unset(mongo ,{"_id":ObjectId(id),"underground":{"$exists":1}} ,{"underground":1}, upsert_value =False ,multi_value =False)


                        #Job type change from 1 to job type 2 ( overhead TO underground  )
                        elif job_type == 2 and  document_job_type == 1:

                            up_res1 =helper.project_update_rename(mongo ,{"_id":ObjectId(id)} ,{"overhead":"underground"}, upsert_value =False ,multi_value =False)
                            up_res2 =helper.project_update(mongo ,{"_id":ObjectId(id)} ,{"job_type":2 ,"underground.type":2 }, upsert_value =False ,multi_value =False)
                            if document_job_type ==3:
                                up_res1 =helper.project_update_unset(mongo ,{"_id":ObjectId(id),"overhead":{"$exists":1}} ,{"overhead":1}, upsert_value =False ,multi_value =False)

                    update_data ={}
                    update_data['job_number'] =job_number
                    update_data['job_owner'] =job_owner
                    update_data['transmission_line_name'] =transmission_line_name
                    update_data['job_type'] =job_type
                    update_data['oh_estimated_completion_date'] =oh_estimated_completion_date
                    update_data['ug_estimated_completion_date'] =ug_estimated_completion_date
                    update_res =helper.project_update(mongo ,{"_id":ObjectId(id)} ,update_data, upsert_value =False ,multi_value =False)
                    if update_res["n"] >0 :
                        return_data["error"] =0
                        return_data["message"] = "Update Successfully..!"
                    else:
                        return_data["error"] =1
                        return_data["message"] ="Not Updated Anything..!"



            else:
                return_data['error']= 1
                return_data['message'] ="Selected Record not Found..!"
        except Exception as e:
            return_data['error'] = 1
            return_data['message'] = str(e)
    else:
        return_data["error"] = 1
        return_data['message'] ="Invalid Inputs!"

    return jsonify(return_data)


@app.route( "/search", methods=['GET'])
def search():
    return_data = {"error":1,"message":"please Try again"}
    wher_query = {}
    return_data["search_result"] = []
    if "id" in request.args:
        id = str(request.args.get("id").strip())
        if id !='':
            wher_query["_id"] = ObjectId(id)
    else:
        if 'job_number' in request.args:
            job_number = str(request.args.get("job_number").strip())
            if job_number != '':
                wher_query['job_number'] = {"$regex": job_number, '$options': 'i'}
        if 'job_owner' in request.args:
                job_owner = str(request.args.get("job_owner").strip())
                if job_owner != '':
                    wher_query['job_owner'] = {"$regex": job_owner, '$options': 'i'}
        if 'transmission_line_name' in request.args:
            transmission_line_name = str(request.args.get("transmission_line_name").strip())
            if transmission_line_name != '':
                wher_query['transmission_line_name'] = {"$regex": transmission_line_name, '$options': 'i'}

    if len(wher_query) > 0:
        try:
            result = helper.find_project(mongo ,wher_query)
            #print(result)
            #return_data["search_result"] = result['documents']
            return_data["result_count"] = result['total_records']
            docs = []
            for doc in result['documents']:
                doc['id'] = str(doc['_id'])
                # doc.pop('_id')
                return_data["search_result"].append(doc)
            return_data['error'] = 0
            return_data['message'] = "Success fully Searched"


        except Exception as e:
            return_data['error'] = 1
            return_data['message'] = str(e)
    else:
        return_data['error'] = 1
        return_data['message'] = "Invalid search inputs"

    return jsonify(return_data)


@app.route( "/project_update", methods=['POST'])
def project_update():
    #1 = Overhead (OH) , 2 = Underground(UG) ,3= Both
    return_data = {"error":1,"message":"please Try again"}
    update_data = {}
    where_condation = {}
    data = request.data
    post_data = json.loads(data)
    document_id =''
    if post_data.get("id", -1) != -1:
        id = str(post_data.get("id"))
        where_condation["_id"] = ObjectId(id)
        try:
            res = helper.find_project(mongo ,where_condation)

            if res['total_records'] >0:
                document = list(res['documents'])[0]



                date_time_str = str(document['oh_estimated_completion_date'])
                date_time_obj = datetime.datetime.strptime(date_time_str, '%Y-%m-%d %H:%M:%S')
                oh_estimated_completion_year = date_time_obj.year
                this_oh_year_locked = helper.check_islocked(mongo,oh_estimated_completion_year,1)
                if this_oh_year_locked >0:
                    return_data["ug_error"] = "Overhead Project is Locked in this ("+ str(oh_estimated_completion_year) +") Year"

                #overhead DATA PREPARATION  1 = overhead
                if post_data.get("overhead", -1) != -1 and (document["job_type"] == 1 or document["job_type"] == 3 ) and this_oh_year_locked <=0 :

                    update_data["overhead.type"] = 1
                    update_data["overhead.estimated_completion_date"] = document['oh_estimated_completion_date']
                    update_data["overhead.is_itlocked"] = 0
                    overhead_data =post_data['overhead']

                    if overhead_data.get("addition", -1) != -1:
                        overhead_addition_data = overhead_data['addition']
                        if overhead_addition_data.get("proj_completion", -1) != -1:
                            overhead_addition_proj_completion_data = overhead_addition_data['proj_completion']

                            for key in  overhead_addition_proj_completion_data:
                                update_data["overhead.addition.proj_completion."+key] =overhead_addition_proj_completion_data[key]


                        if overhead_addition_data.get("business_finance", -1) != -1:
                            overhead_addition_business_finance_data = overhead_addition_data['business_finance']
                            for key in  overhead_addition_business_finance_data:
                                update_data["overhead.addition.business_finance."+key] =overhead_addition_business_finance_data[key]

                    if overhead_data.get("reconductor", -1) != -1:
                        overhead_reconductor_data = overhead_data['reconductor']
                        if overhead_reconductor_data.get("proj_completion", -1) != -1:
                            overhead_reconductor_proj_completion_data = overhead_reconductor_data['proj_completion']

                            for key in  overhead_reconductor_proj_completion_data:
                                update_data["overhead.reconductor.proj_completion."+key] =overhead_reconductor_proj_completion_data[key]


                        if overhead_reconductor_data.get("business_finance", -1) != -1:
                            overhead_reconductor_business_finance_data = overhead_reconductor_data['business_finance']
                            for key in  overhead_reconductor_business_finance_data:
                                update_data["overhead.reconductor.business_finance."+key] =overhead_reconductor_business_finance_data[key]

                    if overhead_data.get("removal", -1) != -1:
                        overhead_removal_data = overhead_data['removal']
                        if overhead_removal_data.get("proj_completion", -1) != -1:
                            overhead_removal_proj_completion_data = overhead_removal_data['proj_completion']

                            for key in  overhead_removal_proj_completion_data:
                                update_data["overhead.removal.proj_completion."+key] =overhead_removal_proj_completion_data[key]


                        if overhead_removal_data.get("business_finance", -1) != -1:
                            overhead_removal_business_finance_data = overhead_removal_data['business_finance']
                            for key in  overhead_removal_business_finance_data:
                                update_data["overhead.removal.business_finance."+key] =overhead_removal_business_finance_data[key]

                #UNDERGROUND DATA PRAPARATION 2 == underground
                date_time_str = str(document['ug_estimated_completion_date'])
                date_time_obj = datetime.datetime.strptime(date_time_str, '%Y-%m-%d %H:%M:%S')
                ug_estimated_completion_year = date_time_obj.year
                this_ug_year_locked = helper.check_islocked(mongo,ug_estimated_completion_year,2)
                if this_ug_year_locked >0:
                    return_data["ug_error"] = "Ungerground Project is Locked in this ("+ str(ug_estimated_completion_year) +") Year"

                if post_data.get("underground", -1) != -1 and (document["job_type"] == 2 or document["job_type"] == 3 ) and this_ug_year_locked <= 0:

                    update_data["underground.type"] = 2
                    update_data["underground.estimated_completion_date"] = document['ug_estimated_completion_date']
                    update_data["underground.is_itlocked"] = 0
                    underground_data =post_data['underground']


                    if underground_data.get("addition", -1) != -1:
                        underground_addition_data = underground_data['addition']
                        if underground_addition_data.get("proj_completion", -1) != -1:
                            underground_addition_proj_completion_data = underground_addition_data['proj_completion']

                            for key in  underground_addition_proj_completion_data:
                                update_data["underground.addition.proj_completion."+key] =underground_addition_proj_completion_data[key]


                        if underground_addition_data.get("business_finance", -1) != -1:
                             underground_addition_business_finance_data = underground_addition_data['business_finance']
                             for key in  underground_addition_business_finance_data:
                                 update_data["underground.addition.business_finance."+key] =underground_addition_business_finance_data[key]

                    if underground_data.get("reconductor", -1) != -1:
                        underground_reconductor_data = underground_data['reconductor']
                        if underground_reconductor_data.get("proj_completion", -1) != -1:
                            underground_reconductor_proj_completion_data = underground_reconductor_data['proj_completion']

                            for key in  underground_reconductor_proj_completion_data:
                                update_data["underground.reconductor.proj_completion."+key] =underground_reconductor_proj_completion_data[key]


                        if underground_reconductor_data.get("business_finance", -1) != -1:
                            underground_reconductor_business_finance_data = underground_reconductor_data['business_finance']
                            for key in  underground_reconductor_business_finance_data:
                                update_data["underground.reconductor.business_finance."+key] =underground_reconductor_business_finance_data[key]

                    if underground_data.get("removal", -1) != -1:
                        underground_removal_data = underground_data['removal']
                        if underground_removal_data.get("proj_completion", -1) != -1:
                            underground_removal_proj_completion_data = underground_removal_data['proj_completion']

                            for key in  underground_removal_proj_completion_data:
                                update_data["underground.removal.proj_completion."+key] =underground_removal_proj_completion_data[key]

                        if underground_removal_data.get("business_finance", -1) != -1:
                            underground_removal_business_finance_data = underground_removal_data['business_finance']
                            for key in  underground_removal_business_finance_data:
                                update_data["underground.removal.business_finance."+key] =underground_removal_business_finance_data[key]

                if len(update_data) >0:
                    result = helper.project_update(mongo, where_condation ,update_data, upsert_value =False ,multi_value =True)
                    if result["n"] >0 :
                        return_data["error"] =0
                        return_data["message"] = "Update Successfully..!"
                    else:
                        return_data["error"] =1
                        return_data["message"] ="Not Updated Anything..!"

            else:
                return_data["error"] =1
                return_data["message"] ="In Valid Project Seleted"

        except Exception as e:
            return_data["error"] =1
            return_data["message"] =str(e)



    return jsonify(return_data)
'''
@app.route( "/project_update", methods=['POST'])
def project_update():
    #1 = Overhead (OH) , 2 = Underground(UG) ,3= Both
    return_data = {"error":1,"message":"please Try again"}
    update_data = {}
    where_condation = {}
    data = request.data
    post_data = json.loads(data)
    document_id =''
    if post_data.get("id", -1) != -1:
        id = str(post_data.get("id"))
        try:
            res = helper.find_project(mongo ,{"_id":ObjectId(id)})


            if res['total_records'] >0:
                document = list(res['documents'])[0]


                #prepare update data for overhead
                update_data["overhead.type"] = 1
                update_data["overhead.estimated_completion_date"] = document['oh_estimated_completion_date']
                update_data["overhead.is_itlocked"] = 0

                update_data["overhead.addition.proj_completion.from"] = "FROM"
                update_data["overhead.addition.proj_completion.to"] = "TO"
                update_data["overhead.addition.proj_completion.length"] = 12
                update_data["overhead.addition.proj_completion.which_type"] = "Tryangle"
                update_data["overhead.addition.proj_completion.ave"] = 12.5
                update_data["overhead.addition.proj_completion.present"] = 100
                update_data["overhead.addition.proj_completion.ultimate"] = 101
                update_data["overhead.addition.proj_completion.size"] = 102
                update_data["overhead.addition.proj_completion.spec"] = "SPEC"
                update_data["overhead.addition.proj_completion.config"] = "config"
                update_data["overhead.addition.proj_completion.voltage"] = "voltage"

                update_data["overhead.addition.business_finance.from"] = "FROM"
                update_data["overhead.addition.business_finance.to"] = "TO"
                update_data["overhead.addition.business_finance.length"] = 12
                update_data["overhead.addition.business_finance.which_type"] = "Tryangle"
                update_data["overhead.addition.business_finance.ave"] = 12.5
                update_data["overhead.addition.business_finance.present"] = 100
                update_data["overhead.addition.business_finance.ultimate"] = 101
                update_data["overhead.addition.business_finance.size"] = 102
                update_data["overhead.addition.business_finance.spec"] = "SPEC"
                update_data["overhead.addition.business_finance.config"] = "config"
                update_data["overhead.addition.business_finance.voltage"] = "voltage"

                update_data["overhead.reconductor.proj_completion.from"] = "FROM"
                update_data["overhead.reconductor.proj_completion.to"] = "TO"
                update_data["overhead.reconductor.proj_completion.length"] = 12
                update_data["overhead.reconductor.proj_completion.which_type"] = "Tryangle"
                update_data["overhead.reconductor.proj_completion.ave"] = 12.5
                update_data["overhead.reconductor.proj_completion.present"] = 100
                update_data["overhead.reconductor.proj_completion.ultimate"] = 101,
                update_data["overhead.reconductor.proj_completion.size"] = 102
                update_data["overhead.reconductor.proj_completion.spec"] = "SPEC"
                update_data["overhead.reconductor.proj_completion.config"] = "config"
                update_data["overhead.reconductor.proj_completion.voltage"] = "voltage"

                update_data["overhead.reconductor.business_finance.from"] = "FROM"
                update_data["overhead.reconductor.business_finance.to"] = "TO"
                update_data["overhead.reconductor.business_finance.length"] = 12
                update_data["overhead.reconductor.business_finance.which_type"] = "Tryangle"
                update_data["overhead.reconductor.business_finance.ave"] = 12.5
                update_data["overhead.reconductor.business_finance.present"] = 100
                update_data["overhead.reconductor.business_finance.ultimate"] = 101
                update_data["overhead.reconductor.business_finance.size"] = 102
                update_data["overhead.reconductor.business_finance.spec"] = "SPEC"
                update_data["overhead.reconductor.business_finance.config"] = "config"
                update_data["overhead.reconductor.business_finance.voltage"] = "voltage"

                update_data["overhead.removal.proj_completion.from"] = "FROM"
                update_data["overhead.removal.proj_completion.to"] = "TO"
                update_data["overhead.removal.proj_completion.length"] = 12
                update_data["overhead.removal.proj_completion.which_type"] = "Tryangle"
                update_data["overhead.removal.proj_completion.ave"] = 12.5
                update_data["overhead.removal.proj_completion.present"] = 100
                update_data["overhead.removal.proj_completion.ultimate"] = 101
                update_data["overhead.removal.proj_completion.size"] = 102
                update_data["overhead.removal.proj_completion.spec"] = "SPEC"
                update_data["overhead.removal.proj_completion.config"] = "config"
                update_data["overhead.removal.proj_completion.voltage"] = "voltage"

                update_data["overhead.removal.business_finance.from"] = "FROM"
                update_data["overhead.removal.business_finance.to"] = "TO"
                update_data["overhead.removal.business_finance.length"] = 12
                update_data["overhead.removal.business_finance.which_type"] = "Tryangle"
                update_data["overhead.removal.business_finance.ave"] = 12.5
                update_data["overhead.removal.business_finance.present"] = 100
                update_data["overhead.removal.business_finance.ultimate"] = 101
                update_data["overhead.removal.business_finance.size"] = 10000
                update_data["overhead.removal.business_finance.spec"] = "SPEC"
                update_data["overhead.removal.business_finance.config"] = "config"
                update_data["overhead.removal.business_finance.voltage"] = "voltage"


                #prepare update data for underground
                update_data["underground.type"] = 2
                update_data["underground.estimated_completion_date"] = document['oh_estimated_completion_date']
                update_data["underground.is_itlocked"] = 1
                update_data["underground.addition.proj_completion.from"] = "FROM"
                update_data["underground.addition.proj_completion.to"] = "TO"
                update_data["underground.addition.proj_completion.length"] = 12
                update_data["underground.addition.proj_completion.which_type"] = "Tryangle"
                update_data["underground.addition.proj_completion.ave"] = 12.5
                update_data["underground.addition.proj_completion.present"] = 100
                update_data["underground.addition.proj_completion.ultimate"] = 101
                update_data["underground.addition.proj_completion.size"] = 102
                update_data["underground.addition.proj_completion.spec"] = "SPEC"
                update_data["underground.addition.proj_completion.config"] = "config"
                update_data["underground.addition.proj_completion.voltage"] = "voltage"

                update_data["underground.addition.business_finance.from"] = "FROM"
                update_data["underground.addition.business_finance.to"] = "TO"
                update_data["underground.addition.business_finance.length"] = 12
                update_data["underground.addition.business_finance.which_type"] = "Tryangle"
                update_data["underground.addition.business_finance.ave"] = 12.5
                update_data["underground.addition.business_finance.present"] = 100
                update_data["underground.addition.business_finance.ultimate"] = 101
                update_data["underground.addition.business_finance.size"] = 102
                update_data["underground.addition.business_finance.spec"] = "SPEC"
                update_data["underground.addition.business_finance.config"] = "config"
                update_data["underground.addition.business_finance.voltage"] = "voltage"

                update_data["underground.reconductor.proj_completion.from"] = "FROM"
                update_data["underground.reconductor.proj_completion.to"] = "TO"
                update_data["underground.reconductor.proj_completion.length"] = 12
                update_data["underground.reconductor.proj_completion.which_type"] = "Tryangle"
                update_data["underground.reconductor.proj_completion.ave"] = 12.5
                update_data["underground.reconductor.proj_completion.present"] = 100
                update_data["underground.reconductor.proj_completion.ultimate"] = 101,
                update_data["underground.reconductor.proj_completion.size"] = 102
                update_data["underground.reconductor.proj_completion.spec"] = "SPEC"
                update_data["underground.reconductor.proj_completion.config"] = "config"
                update_data["underground.reconductor.proj_completion.voltage"] = "voltage"

                update_data["underground.reconductor.business_finance.from"] = "FROM"
                update_data["underground.reconductor.business_finance.to"] = "TO"
                update_data["underground.reconductor.business_finance.length"] = 12
                update_data["underground.reconductor.business_finance.which_type"] = "Tryangle"
                update_data["underground.reconductor.business_finance.ave"] = 12.5
                update_data["underground.reconductor.business_finance.present"] = 100
                update_data["underground.reconductor.business_finance.ultimate"] = 101
                update_data["underground.reconductor.business_finance.size"] = 102
                update_data["underground.reconductor.business_finance.spec"] = "SPEC"
                update_data["underground.reconductor.business_finance.config"] = "config"
                update_data["underground.reconductor.business_finance.voltage"] = "voltage"

                update_data["underground.removal.proj_completion.from"] = "FROM"
                update_data["underground.removal.proj_completion.to"] = "TO"
                update_data["underground.removal.proj_completion.length"] = 12
                update_data["underground.removal.proj_completion.which_type"] = "Tryangle"
                update_data["underground.removal.proj_completion.ave"] = 12.5
                update_data["underground.removal.proj_completion.present"] = 100
                update_data["underground.removal.proj_completion.ultimate"] = 101
                update_data["underground.removal.proj_completion.size"] = 102
                update_data["underground.removal.proj_completion.spec"] = "SPEC"
                update_data["underground.removal.proj_completion.config"] = "config"
                update_data["underground.removal.proj_completion.voltage"] = "voltage"

                update_data["underground.removal.business_finance.from"] = "FROM"
                update_data["underground.removal.business_finance.to"] = "TO"
                update_data["underground.removal.business_finance.length"] = 12
                update_data["underground.removal.business_finance.which_type"] = "Tryangle"
                update_data["underground.removal.business_finance.ave"] = 12.5
                update_data["underground.removal.business_finance.present"] = 100
                update_data["underground.removal.business_finance.ultimate"] = 101
                update_data["underground.removal.business_finance.size"] = 10000
                update_data["underground.removal.business_finance.spec"] = "SPEC"
                update_data["underground.removal.business_finance.config"] = "config"
                update_data["underground.removal.business_finance.voltage"] = "voltage"


                result = helper.project_update(mongo, where_condation ,update_data, upsert_value =False ,multi_value =True)
                print(result)
            else:
                return_data["error"] =1
                return_data["message"] ="In Valid Project Seleted"

        except Exception as e:
            return_data["error"] =1
            return_data["message"] =str(e)



    return jsonify(return_data)

'''

@app.route( "/check_lock_status", methods=['POST'])
def check_lock_status():
    return_data = {"error":1,"message":"please Try again" ,"locked_count":0}
    data = request.data
    post_data = json.loads(data)
    selected_year = ""
    selected_type = 0
    if post_data.get("year" ,-1) != -1 :
        selected_year = post_data['year']
    if post_data.get("job_type" ,-1) != -1 :
        selected_type = post_data['job_type']


    try:
        result = helper.check_islocked(mongo,selected_year,selected_type)
        return_data['locked_count'] =result

    except Exception as e:
        return_data['message'] =str(e)

    return jsonify(return_data)


@app.route( "/lock_year", methods=['POST'])
def lock_year():
    return_data = {"error":1,"message":"please Try again" ,"locked_count":0}
    data = request.data
    post_data = json.loads(data)
    selected_year = ""
    selected_type = 0
    if post_data.get("year" ,-1) != -1 :
        selected_year = post_data['year']
    if post_data.get("job_type" ,-1) != -1 :
        selected_type = post_data['job_type']


    try:
        result = helper.lock_year(mongo,selected_year,selected_type)
        return_data['locked_count'] =100#result

    except Exception as e:
        return_data['message'] =str(e)

    return jsonify(return_data)

@app.route( "/remove_document", methods=['POST'])
def remove_document():
    return_data = {"error":1,"message":"please Try again"}
    data = request.data
    post_data = json.loads(data)
    selected_year = ""
    selected_type = 0
    if post_data.get("id" ,-1) != -1 :
        id = str(post_data['id'])
        result = helper.remove_document(mongo ,ObjectId(id))
        if result.deleted_count >0:
            return_data['error'] =0
            return_data['message'] ="Deleted Success"
        else:
            return_data['error'] =1
            return_data['message'] ="Document is Not Found..!"


    else:
        return_data['error'] =1
        return_data['message'] ="Project id Is Required"

    return jsonify(return_data)





if __name__ == "__main__":
    app.run(debug=True)
