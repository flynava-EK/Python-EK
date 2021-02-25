
from flask import Flask, request, jsonify, make_response
from werkzeug.utils import secure_filename
from openpyxl import load_workbook
import datetime
import os
import re
import pandas as pd
import pymongo
from datetime import date
from datetime import datetime
from bson.objectid import ObjectId
from flask_cors import CORS
from gfs import parse_text,validation
import json
UPLOAD_FOLDER = "files/"
app = Flask(__name__)
CORS(app)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
def _build_cors_prelight_response():
    response = make_response()
    response.headers.add("Access-Control-Allow-Origin", "*")
    response.headers.add("Access-Control-Allow-Headers", "*")
    response.headers.add("Access-Control-Allow-Methods", "*")
    return response
def _corsify_actual_response(response):
    response.headers.add("Access-Control-Allow-Origin", "*")
    return response
@app.route("/validation", methods=["GET", "POST", "OPTIONS"])
def validation_template():
    if request.method == 'OPTIONS':
        print('options method call')
        return _build_cors_prelight_response()
    if request.method == "POST":
        try:
            first_call = True # Required for multi-file processing
            form=request.form.to_dict()
            json1=json.loads(form['data'])
            file_name_li = json1["file_name"]
            batch_count = len(file_name_li)

            response = []
            idx = 0
            for each_file in file_name_li:

                file = request.files[each_file]
                file_name = secure_filename(file.filename)
                print(file)
                print(file_name)
                file.save(os.path.join("./", file_name))
                txt_file = os.path.join("./", file_name)
                corporate_name = json1["corporate_name"]
                # print(corporate_name)

                file_date = json1["file_date"][idx]
                print(file_date)
                file_time = json1["file_time"][idx]
                system_date = json1["system_date"][idx]
                system_time = json1["system_time"][idx]

                file_upload_date = json1["file_upload_date"][idx]
                file_upload_time = json1["file_upload_time"][idx]
                uploaded_by = json1["uploaded_by"]

                client = pymongo.MongoClient(
                        '3.6.201.161:27022',
                        username='data.EK',
                        password='data@123',
                        authSource='admin',
                        authMechanism='SCRAM-SHA-1')
                db=client['rawDB_prod']
                if corporate_name == None or file_date == None or file_time == None:
                    return _corsify_actual_response(jsonify({"error": "Valdation requires corporate_name, file_time and file_date params"}))
                if first_call == True:
                    id1=parse_text(file_name,system_date,system_time,db,file_date,file_time)
                    res = validation(corporate_name,id1,db, None, batch_count, file_upload_date, file_upload_time, uploaded_by)
                    exist_id = res['validation_record_id']
                    response.append(res)
                    first_call = False
                else:
                    id1 = parse_text(file_name, system_date, system_time, db, file_date, file_time)
                    res_ = response.append(validation(corporate_name, id1, db, exist_id, batch_count, file_upload_date, file_upload_time, uploaded_by))
                    response.append(res_)
                os.remove(txt_file)
                idx = idx + 1
        except Exception as e:
            print(e)
        return _corsify_actual_response(jsonify(response))
if __name__ == "__main__":
    print("Server Started...")
    print("To stop the Server press CTRL+C")
    # http_server = WSGIServer(('', 5001), app)
    # http_server.serve_forever()
    app.run(host="0.0.0.0", port=5055)




