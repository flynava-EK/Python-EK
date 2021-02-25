#!/usr/bin/env python
# coding: utf-8

# ## API to transfor image 

# In[68]:


from IPython.display import Image
# from gevent.pywsgi import WSGIServer
from flask import Flask, request, jsonify, make_response
from flask_cors import CORS
import json
import base64

UPLOAD_FOLDER = "D:\Flynava\EK\RawFileReading\\file"


# In[79]:


from mongoengine import connect, Document, fields, disconnect

# connect to mongodb
# connect(db = 'Test_DB', host = '127.0.0.1', port = 27017)
disconnect()
host = '3.6.201.161'
port = 27022
username='data.EK'
password='data@123'
authSource='admin'
authMechanism='SCRAM-SHA-1'
db_name = 'POC'         
connect( host = host, port = port, db = db_name, username=username, password=password, authentication_source=authSource)


# In[70]:



class raw_files(Document):  
    # assign a collection
    meta = {"collection": "Corporate_file"}    
    corporate_id = fields.StringField(required=True)
    raw_image = fields.ImageField(thumbnail_size=(150,150, False))
    


# In[71]:


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


# In[72]:


from PIL import Image as Image1 
import io
from base64 import encodebytes
def get_response_image(image_path):
    pil_img = Image1.open(image_path, mode='r') # reads the PIL image
    byte_arr = io.BytesIO()
    pil_img.save(byte_arr, format='PNG') # convert the PIL image to byte array
    encoded_img = encodebytes(byte_arr.getvalue()).decode('ascii') # encode as base64

    #Need for UI to decode
#     image_code  = base64.b64decode(encoded_img)
#     with open('test.png', 'wb') as f:
#         f.write(image_code )
    
    return encoded_img


# In[73]:


## Beta testing with multiple id files 

@app.route("/raw_file", methods=["GET", "POST", "OPTIONS"])
def raw_file():
    if request.method == 'OPTIONS':
        print('options method call')
        return _build_cors_prelight_response()
    if request.method == "POST":
        try:
            first_call = True
            print("-------------- Request started ------------------")
            form=request.form.to_dict()
            print(form)
            json1=json.loads(form['data'])
            print(json1)
            list_encode_img = list()
            img_na = list()
            for idx in range(len(json1['field'])):
                name = json1['field'][idx]
                raw_id = json1['values'][idx]
                
                print("raw id -{}- from API request ".format(raw_id))
                ## show image
                rec_obj = raw_files.objects(corporate_id = raw_id).first()

                img = Image(rec_obj.raw_image.read())
                image_file_name = str(raw_id+'.png')
                open(UPLOAD_FOLDER+"\\"+image_file_name, 'wb').write(img.data)
                print("Image retrived from DB in {} as {}".format(UPLOAD_FOLDER, image_file_name))

                # server side code
    #             image_path = 'output.png' # point to your image location
                encoded_img = get_response_image(UPLOAD_FOLDER+"\\"+image_file_name)
                list_encode_img.append(encoded_img)
                img_na.append(raw_id)
            response =  { 'Status' : 'Success', 'ImageBytes': list_encode_img, 'ImgNameList' : img_na}
            print("Response : ", response['Status'])
            print("-------------- Request completed ------------------")
            
        except Exception as e:
            print(e)

    return jsonify(response)


# In[80]:


if __name__ == "__main__":
    print("Server Started...")
    print("To stop the Server press CTRL+C")
#     http_server = WSGIServer(('', 5055), app)
#     http_server.serve_forever()
    app.run(host="0.0.0.0", port=5002)


# In[ ]:




