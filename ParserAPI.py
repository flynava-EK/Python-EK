from flask import Flask, request, jsonify, make_response
from werkzeug.utils import secure_filename
from openpyxl import load_workbook
import datetime
import os
from bson.objectid import ObjectId
# from gevent.pywsgi import WSGIServer
from flask_cors import CORS 

from CorporateTemplateProcessor import parse, transform

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

@app.route("/parse", methods=["GET", "POST", "OPTIONS"])
def parse_template():
    if request.method == 'OPTIONS':
        print('options method call')
        return _build_cors_prelight_response()
    if request.method == "POST":
        file = request.files["file"]
        filename = datetime.datetime.now().strftime("%Y_%m_%d_%H_%M_%S") + secure_filename(file.filename)
        if not os.path.isdir(app.config['UPLOAD_FOLDER']):
            os.mkdir(app.config['UPLOAD_FOLDER']) 
        file.save(os.path.join(app.config['UPLOAD_FOLDER'], filename))
        excel_file = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        response = parse(excel_file)
        print("removing the excel file from server")
        try:
            os.remove(excel_file)
            print("removed the excel file from the server")
        except Exception as e:
            print("exception occured when trying the delete the excel file")
            print(e)

    return jsonify(response)

@app.route("/transform", methods=["GET", "POST", "OPTIONS"])
def transform_template():
    if request.method == 'OPTIONS':
        print('options method call')
        return _build_cors_prelight_response()
    if request.method == "POST":
        data = request.json 
        corporate_name = data.get("corporate_name", None)
        raw_record_id = data.get("raw_record_id", None)
        file_date = data.get("file_date", None)
        file_time = data.get("file_time", None)
        system_date = datetime.date.today().strftime("%Y-%m-%d")
        system_time = datetime.datetime.now().strftime("%H:%M:%S")

        raw_record_id = ObjectId(raw_record_id)

        if corporate_name == None or file_date == None or file_time == None:
            return _corsify_actual_response(jsonify({"error": "transformation requires corporate_name, file_time and file_date params"}))
        response = transform(corporate_name, raw_record_id, file_date, file_time, system_date, system_time)

    return _corsify_actual_response(jsonify(response))


if __name__ == "__main__":
    print("Server Started...")
    print("To stop the Server press CTRL+C")
    # http_server = WSGIServer(('', 5001), app)
    # http_server.serve_forever()
    app.run(host="0.0.0.0", port=5001)