
import pymongo
import datetime
import time
import pprint
import re
def func(key,a,td,tc,db,db1,db2,rule):
    value={"value":a,"category":"","tbl_no":"","transformed_value":a,"pre_validation":"","cat_override_tag":"X","rule_db_flag":False,"sfi_flag":False}
    master=list(db2.coversheet.master.find({"field":key},{"category":1,"_id":0,"col_name":1,"query":1}))
    query={}
    if len(master)>0:
        category=master[0]
        value['category']=category['category']
        collection=category['col_name']
        query=category['query']
        if(collection!="" and query!="") and a!=None:
            cat_no=str(value['category'])
            if "50" in str(value['category']) and "corporate" in key:
                value['class_of_service']=" "
                value['journey_type']=" "
                value['note_text']=" "
                value['sfi_flag']=True
                value['defined']='defined'
                value['transformed_value']=a
                value['pre_validation']=a.upper()
                temp="CORPORATE\s*(.*)"+value['pre_validation']+"$"
                query["APPLICATION_TITLE"]={"$regex":temp}
                value['query']={"appl_title":"CORPORATE FARES FOR "+value['pre_validation'],'dom':"INTL"}
            if "15" in str(value['category']) or "14" in str(value['category']):
                value=value.copy()
                b,c,d={},{},{}
                value['sfi_flag']=True
                value['defined']='defined'
                for key in a:
                    if a[key]==None or a[key]=='No restriction' or a[key]=="-" or a[key]=="":
                        if key=='from':
                            b[key]=""
                            c[key]=""
                            d[key]="1900-01-01"
                        if key=="to" or key=="completed_by":
                            b[key]=""
                            c[key]=""
                            d[key]="2099-12-31"
                    elif (re.search('open', a[key], flags=re.IGNORECASE) is not None)  :
                            b[key]="OPEN"
                            c[key]="OPEN"
                            d[key]="2099-12-31"
                    elif "base" in a[key] or"fare" in a[key]  or "Base" in a[key]:
                        value["cat_override_tag"]="B"
                        value['sfi_flag']=False
                        value['defined']='defined'
                        b[key]=a[key]
                        c[key]=""
                        d[key]=""
                    else:
                        if "(" in a[key]:
                            a[key]="30-09-2021" 
                        y=a[key].split("-")
                        b[key]=y[2]+"-"+y[1]+"-"+y[0]
                        d[key]=y[2]+"-"+y[1]+"-"+y[0]
                        month=datetime.date(int(y[2]),int(y[1]),int(y[0])).strftime('%b')
                        c[key]=y[0]+month.upper()+" "+y[2][2:4]
                        if "99" in c[key]:
                            c[key]="OPEN"
                    value['transformed_value']=b
                    value['pre_validation']=c
                    value['quer']=d
                if value['category']=="14":
                    query["TRAVEL_DATES_COMM_1"] = d['from']
                    query["TRAVEL_DATES_EXP_1"]=d['to']
                    query["TRAVEL_DATES_COMMENCE_1"]=d['completed_by']
                    value['RI']=" "
                    value['return_travel']=" "
                    value['abbreviated_text']=" "
                    value['return_by']={"date":" ","time":" "}    
                if value['category']=="15":
                    query["SALE_DATES_EARLIEST_TKTG_1"] = d['from']
                    query["SALE_DATES_LATEST_TKTG_1"]=d['to']
                    value['RI']=" "
                    value['reservations_must_be']={"on_after":" ","on_before":" "}
                    value['abr_txt']=" "
            if "pnr" in key and "50" in str(value["category"]):
                query['TEXT']=a
            if "35" in str(value["category"]):
                if key=="fare_amount_box":
                    value['transformed_value']=a[0:3].upper()
                query["TICKETING_TICKET_DESIGNATOR"]=td
                query["TICKETING_TOUR_CAR_VALUE_CODE"]=tc
            print(query)
            q=db1.JUP_DB_ATPCO_Record_2_Cat_All.find_one({"RULE_NO":rule,"CAT_NO":value['category'].zfill(3)})
            if q!=None:
                value['rule_db_flag']=True
            query1 = list(db1.get_collection(collection).aggregate([{"$match":query},{"$group":{"_id":None,
                                                                                                "tbl":{"$addToSet":"$TBL_NO"}}}]))
            print(query1)
            if len(query1) > 0:
                if  "50" in str(value['category']):
                    result=db1.JUP_DB_ATPCO_Record_2_Cat_All.find_one({"DATA_TABLE_STRING_TBL_NO_1":{"$in":query1[0]['tbl']},"CAT_NO":"050"})
                    if result:
                        value["tbl_no"] = result["DATA_TABLE_STRING_TBL_NO_1"]
                    else:
                        result=db1.JUP_DB_ATPCO_Record_2_Cat_All.find_one({"DATA_TABLE_STRING_TBL_NO_2":{"$in":query1[0]['tbl']},"CAT_NO":"050"})
                        if result:
                            value["tbl_no"] = result["DATA_TABLE_STRING_TBL_NO_2"]
                        else:
                            query1[0]['tbl'].sort()
                            value["tbl_no"]=query1[0]['tbl'][-1]
                else:
                    value["tbl_no"] = query1[0]['tbl']
                    if len(query1[0]['tbl'])==1:
                        value["tbl_no"] = query1[0]['tbl'][0]
            else:
                for key in query:
                    if key!="APPLICATION_TITLE":
                        value[key] = query[key]
        if "18" in str(value['category']):
            if "WP" not in str(value['value']):
                value['endoresement_text']=[value['value']]
                value['text']=[value['value']]
                value['note_text']=""
                value['sfi_flag']=False
                value['defined']='defined'
                value['undefined_flag']='defined'
                value["cat_override_tag"]="B"
        if "19" in str(value['category']) :
            if value['value']!=None:
                cat_no=str(value['category'])
                value['text']=[a]
                value['no_appl']="X"
                value['transformed_value'] = "No"
                value['pre_validation'] = "NONE UNLESS OTHERWISE SPECIFIED"
                value['sfi_flag']=True
                value['defined']='defined'
                value['undefined_flag']='defined'
            else:value['text']=[a]
        if a==None:
            value["cat_override_tag"]="B"
    print(value,query)
    return value

def transform_cs(db,db1,db2,corporate_name, raw_record_id, file_date, file_time, system_date, system_time):
    resul = \
        (list(db.corporate.instruction.find({"_id": raw_record_id},
                                            {"cover_sheet": 1, "_id": 0})))[0]
    result = resul['cover_sheet']
    print(result)
    osi = result['details_of_ticketing_instructions']['osi_field_entry_in_pnr']
    td = result['details_of_ticketing_instructions']['ticket_designator']
    tc = result['details_of_ticketing_instructions']['tour_code']
    transformed_cs = [{"corporate_name": result['details_of_agreement']['corporate_name']},
                      {"travel_period": result['details_of_agreement']['travel_period']},
                      {"ticketing_sales_period": result['details_of_agreement']['sales_period']},
                      {"disclaimer": result['details_of_ticketing_instructions']['disclaimer']},
                      {"osi_field_entry_in_pnr": result['details_of_ticketing_instructions']['osi_field_entry_in_pnr']},
                      {"ticket_designator": result['details_of_ticketing_instructions']['ticket_designator']},
                      {"tour_code": result['details_of_ticketing_instructions']['tour_code']},
                      {"fare_amount_box": result['details_of_ticketing_instructions']['fare_amount_box']},
                      {"child_infant_discounts": result['details_of_ticketing_instructions']["child_infant_discounts"]},
                      # {"fare_rules": result['details_of_ticketing_instructions']['fare_rules']}
                      ]
    print(transformed_cs)
    print(str(transformed_cs[0]['corporate_name'].lower().replace(" ","_").replace(".","_").replace(",","_").replace("-","_")))
    qu=db.JUP_DB_Corporate_Info.find_one({"corporate_name":str(transformed_cs[0]['corporate_name'].lower().replace(" ","_").replace(".","_").replace(",","_").replace("-","_"))})
    if qu!=None:
        rule=qu['rule'][0]
    else:
        rule=""
    cat_list=[]
    cover_cat=[]
    for i in range(len(transformed_cs)):
        for key in transformed_cs[i]:
            a = transformed_cs[i][key]
            transformed_cs[i][key] = func(key, a, tc, td, db, db1, db2,rule)
            print("done",transformed_cs[i][key])
            if transformed_cs[i][key]["value"]!=None:
                cover_cat.append(transformed_cs[i][key]["category"])
                print(cover_cat)
                cat_list.append({"cat":int(transformed_cs[i][key]["category"]),"tag":transformed_cs[i][key]["cat_override_tag"]})
    cover_cat=list(set(cover_cat))
    cover_cat=cover_cat
    #cover_sheet_cat=db.temp.find_one({"corporate_name":corporate_name.strip()})['cover_sheet_cat']
    db.corporate.sfi.insert_one({"corporate_name": corporate_name, "file_date": file_date, "file_time": file_time, \
                             "transformed_cover_sheet": transformed_cs,"cover_sheet_cat":cover_cat,"sury":cover_cat,
                "cover_sheet_tag":cat_list}) 
    #db.corporate.sfi.update_many({"corporate_name":corporate_name \
     #                         },{"$set":{"transformed_cover_sheet": transformed_cs,"cover_sheet_cat":cover_cat,
      #          "cover_sheet_tag":cat_list}})

if __name__=="__main__":
    from bson.objectid import ObjectId
    st=time.time()
    client = pymongo.MongoClient(
        "3.6.201.161:27022",
        username="data.EK",
        password="data@123",
        authSOurce="admin",
        authMechanism='SCRAM-SHA-1'
    )
    db = client['rawDB_prod']
    db1=client['PDSS_EK']
    db2=client["ATPCO_EK"]
    corporate_name ="PT Wira Cipta Perkasa"
    raw_record_id = ObjectId("602e0956bda5bb14e00cfe89")
    transform_cs(db,db1,db2,corporate_name, raw_record_id, "", "", "", "")
    print("time ",time.time()-st)
