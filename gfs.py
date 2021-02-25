import re
import pandas as pd
import pymongo
import copy
from datetime import date
from datetime import datetime
from bson.objectid import ObjectId
import Utils
logger = Utils.get_logger("Pre-GFS validation")
def parse_text(file_name,system_date,system_time,db,file_date,file_time):
    f = open(file_name,"r")
    logger.info("parsing txt file {} started ".format(file_name))
    dict1=[]
    list1=[]
    listtemp=[]
    cats_dict=[]
    cat_data={}
    cat_final={}
    import re
    for i in f:
        text=i.replace("*","")
        text1=text.strip()
        dict1.append(text1)
    for i in dict1:
        if i=="":
            pass
        else:
            list1.append(i)
    for i in range(len(list1)):
        key={"tariff":"TARIFF:","cxr":"CXR:","rule":"RULE:"}
        if list1[i].strip().startswith("FARE CLASSES:"):
            fare_class=list1[i].replace("FARE CLASSES:","").strip().split("/")
        if list1[i].strip().startswith("BATCH ID:"):
            batch_id=list1[i].replace("BATCH ID:","").strip().split("/")
        if list1[i].strip().startswith("TARIFF") and list1[i].strip()!="TARIFF:":
            t_r_c=list1[i].split("  ")
            key1=key.copy()
            for j in range(len(t_r_c)):
                for k in key:
                    if t_r_c[j].strip().startswith(key[k]):
                        key1[k]=t_r_c[j].replace(key[k],"").strip().split(",")
        if list1[i].strip().startswith("CATEGORY"):
            cats_dict.append(list1[i])
        if len(cats_dict) > 0:
            x = list1.index(cats_dict[0])
            for val in list1[x:]:
                if val in cats_dict:
                    lst = []
                    lst.append(val)
                    cat_data[val] = lst
                else:
                    lst.append(val)
    for i in cat_data:
        cat_final[str("Cat_"+str([int(s) for s in i.split() if s.isdigit()][0]))]=cat_data[i]
    df=pd.DataFrame([{"Tariff":key1['tariff'],"Carrier":key1['cxr'][0],"Rule_id":key1['rule'],"Fare_class":fare_class,"Batch_id":batch_id,"cat_data":cat_final,"file_date":file_date,"file_name":file_name,"file_time":file_time}])
    df=df.to_dict('records')
    db.JUP_DB_Text_Parsing.insert_many(df)
    for rec in df:
        id1 = str(rec["_id"])
    logger.info("parsing txt file {} completed ".format(file_name))
    print(id1)
    return id1

def validation(corporate_name,id1,db, ext_id, batch_count, file_upload_date, file_upload_time, uploaded_by):
    logger.info("validation of {} started ".format(corporate_name))
    text_parsing = db.JUP_DB_Text_Parsing.find_one({"_id":ObjectId(id1)})
    corp_inst =db.corporate.instruction.find_one({"corporate_name" :corporate_name}, sort=[("_id", pymongo.DESCENDING)])
    sfi = db.corporate.sfi.find_one({"corporate_name" :corporate_name}, sort=[("_id", pymongo.DESCENDING)])
    cat_18_dict,cat_15_dict,cat_14_dict,cat_19_dict,cat_25_dict,cat_35_dict,cat_50_dict={},{},{},{},{},{},{}

    def deep_index(lst, w):
        ss = [True if w.lower() in key.lower() else False for key in lst  ]
        if True in ss:
            ssd = True
        else:
            ssd = False
        return ssd

    def cat_01_validation(cat_bat, cat, cat_acc):
        cat_dict = dict()
        li_cat1 = [e for e in cat_bat if 'VALID FOR ADULT WITH ID. THIS FARE IS VALID FOR A' in e]
        df = pd.DataFrame(cat)
        if re.search('refer', cat_acc, flags = re.IGNORECASE):
            cat_acc = 'TBA'
        try: 
            acc_code =list(set(list(df['Account Code'].unique()) + [cat_acc]))  
            if '' in acc_code:
                acc_code.remove('')  
            if len(acc_code)>1 and 'TBA' in acc_code:
                acc_code.remove('TBA')
        except Exception as error:
            acc_code = [cat_acc]
        if len(acc_code) == len(li_cat1):
            cat_dict['validation'] = True
        else:
            cat_dict['validation'] = False

        cat_dict['cat_name'] = cat_bat[0]
        cat_dict['corporate_instruction'] = acc_code
        cat_dict['batch_file'] = cat_bat
        return cat_dict

    def cat_50_validation(cat_50_bat, cat_50, Sinohydro_corporate_name):
        cat_dict = dict()
        cat_list=[]
        inner_dict = dict()
        inner_dict['Corporate_Name'] = cat_50['pre_validation']
        cat_list.append(inner_dict)
        cat_50['pre_validation'] = re.sub(r"\s+", "", cat_50['pre_validation'])
        if cat_50['pre_validation'] in re.sub(r"\s+", "", ''.join(cat_50_bat)):
            inner_dict['validity']= True
        else:
            inner_dict['validity'] = False

        cat_dict['cat_name'] = cat_50_bat[0]
        cat_dict['corporate_instruction'] =cat_list
        cat_dict['batch_file'] = cat_50_bat
        cat_dict['validation'] = inner_dict['validity']
        # print(cat_dict)
        return cat_dict

    def cat_14_validation(cat_bat, cat):
        cat_dict = dict()
        cat_list = list()
        cat_dict['cat_name'] = cat_bat[0]
        inner_dict = dict()
        inner_dict_1 = dict()
        inner_dict['travel_on_or_before'] = cat['pre_validation']['to']
        if "open" in str(cat['pre_validation']['to']).lower():
            inner_dict['validity'] = True
        else:
            inner_dict['validity'] = deep_index(cat_bat, cat['pre_validation']['to'])
        inner_dict_1['travel_on_or_after'] = cat['pre_validation']['from']
        inner_dict_1['validity'] = deep_index(cat_bat, cat['pre_validation']['from'])
        cat_list.append(inner_dict)
        cat_list.append(inner_dict_1)
        cat_dict['corporate_instruction'] = cat_list
        cat_dict['batch_file'] = cat_bat
        cat_dict['validation'] = inner_dict['validity'] and inner_dict_1['validity']
        # #print cat
        # #print cat_dict
        # #print cat_bat
        return cat_dict

    def cat_15_validation(cat_bat, cat):
        cat_dict = dict()
        cat_list = list()
        cat_dict['cat_name'] = cat_bat[0]
        inner_dict = dict()
        inner_dict_1 = dict()
        inner_dict['SALES_ON_OR_BEFORE'] = cat['pre_validation']['to']
        if "open" in str(cat['pre_validation']['to']).lower():
            inner_dict['validity'] = True
        else:
            inner_dict['validity'] = deep_index(cat_bat, cat['pre_validation']['to'])

        inner_dict_1['SALES_ON_OR_AFTER'] = cat['pre_validation']['from']
        inner_dict_1['validity'] = deep_index(cat_bat, cat['pre_validation']['from'])
        cat_list.append(inner_dict)
        cat_list.append(inner_dict_1)
        cat_dict['corporate_instruction'] = cat_list
        cat_dict['validation'] = inner_dict['validity'] and inner_dict_1['validity']
        cat_dict['batch_file'] = cat_bat
        # #print cat
        # #print cat_dict
        # #print cat_bat
        return cat_dict

    def cat_19_validation(cat_bat, cat):
        cat_dict = dict()
        cat_list = list()
        cat_dict['cat_name'] = cat_bat[0]
        inner_dict = dict()
        inner_dict_1 = dict()
        inner_dict['child_infant'] = cat['pre_validation']
        inner_dict['validity'] = deep_index(cat_bat, cat['pre_validation'])

        cat_list.append(inner_dict)
        cat_dict['corporate_instruction'] = cat_list
        cat_dict['batch_file'] = cat_bat
        cat_dict['validation'] = inner_dict['validity']
        # #print cat_dict
        # #print cat_bat
        return cat_dict

    def cat_35_validation(cat_bat, cat, haj_doc, cat_acc):
        logger.info('cat 35 validation started ')
        try:
            cat_dict = dict()
            cat_list = list()
            li_from_to = [e for e in cat_bat if 'FROM/TO' in e]
            li_iata = [e for e in cat_bat if 'SALE IS RESTRICTED TO SPECIFIC AGENTS' in e]
            cat_dict['cat_name'] = cat_bat[0]
            inner_dict = dict()
            inner_dict_1 = dict()
            # phase 1 for Haj data
            if len(haj_doc) > 0:
                try:
                    general_note = haj_doc
                    exclude_field_value = ""
                    if general_note is not None:
                        for each in general_note:
                            if each['field_key'] == "general_exclusions":
                                exclude_field_value = each['field_value']

                    else:
                        general_note = []
                except KeyError as error:
                    exclude_field_value = ""

                if exclude_field_value == "":
                    print("General_exclusions Nothing is there for exclude")
                else:
                    if 'Exclude "HAJ" fares' in exclude_field_value:
                        fare_family = ["-HAS", "-HAP"]
                        print("There is no location so we can skip this validation")
                        ## There is no loc1 and loc2 so we can skip this
                    elif "HAJ" in exclude_field_value:
                        # We can hard code fare family for now since we don't have any other condition
                        print("surya")
                        fare_family = ["-HAS", "-HAP"]
                        arr = exclude_field_value.split(" ")
                        arr = arr[0]
                        arr = arr.split("/")
                        # check wit all the combination of tags
                        for e_fare in fare_family:
                            for e_array in arr:
                                # if e_fare in li_from_to and e_array in li_from_to:
                                to_remove = [e for e in li_from_to if e_fare in e and e_array in e][0]
                                # print(to_remove)
                                # li_from_to.remove(to_remove)
                                li_from_to = [i for i in li_from_to if i not in to_remove]
                                # else:
                                #     print("no match")
                                # print(e_fare, e_array)
                        # exclude_destination = arr
                        # print('remaining {}'.format(li_from_to))
            else:
                pass
            list2 = []
            for i in range(len(cat)):
                x = [k for k in [(k, v) for k, v in cat[i].items()] if k[1]!='' and k[0] in ['Galileo', 'Sabre', 'Amadeus', 'Worldspan', 'FLX', 'Travelsky', 'Apollo', 'Abacus']]
                if len(x)>0 :
                    if 'Account Code' not in list(cat[i].keys()) or cat[i]['Account Code']=='':
                        if re.search('refer', cat_acc, flags = re.IGNORECASE):
                            cat_acc = 'TBA'
                    else:
                        cat_acc = cat[i]['Account Code']
                        # cat[i]['Account Code'] = cat_acc
                    list2.extend([{m[0]:cat_acc} for m in x])
            if len(list2)==0:
                df = pd.DataFrame(cat)
                print('h')
                list3 = df[['GDS', 'Account Code']]
                print('hi')
                list3 = list3[list3['Account Code']!='']
                list3 = list3.to_dict("records")
                list2 = [{list(v.values())[0]:list(v.values())[1]} for v in list3]
                
                print(list2)
                # acc = df['Account Code'] 
            list2 = [dict(t) for t in {tuple(d.items()) for d in list2}]
            if len(li_from_to) == 0:
                inner_dict['General_exclusion_validation'] = True
            else:
                inner_dict['General_exclusion_validation'] = False

            if len(list2) == len(li_iata):
                inner_dict['GDS_validation'] = True
            else:
                inner_dict['GDS_validation'] = False

            if inner_dict['General_exclusion_validation'] and inner_dict['GDS_validation']:
                cat_dict['validation'] = True # For now
            else:
                cat_dict['validation'] = False  # For now

            cat_dict['corporate_instruction'] = list2
            cat_dict['batch_file'] = cat_bat
            logger.info('cat 35 validation completed')
            return cat_dict

        except Exception as error:
            print('Error in cat 35 validation')
            print(error)
            cat_dict['validation'] = False
            return cat_dict


    def cat_18_validation(cat_bat, cat):
        cat_dict = dict()
        cat_list = list()
        cat_dict['cat_name'] = cat_bat[0]
        inner_dict = dict()
        inner_dict_1 = dict()
        inner_dict['pre_validation'] = cat['pre_validation']
        inner_dict['validity'] = deep_index(cat_bat, cat['pre_validation'])
        cat_list.append(inner_dict)
        cat_dict['corporate_instruction'] = cat_list
        cat_dict['batch_file'] = cat_bat
        cat_dict['validation'] =inner_dict['validity']
        # #print cat_dict
        # #print cat_bat
        return cat_dict

    def cat_25_validation(cat_bat, cat):
        cat_dict = dict()
        cat_list = list()
        cat_dict['cat_name'] = cat_bat[0]
        inner_dict = dict()
        inner_dict_1 = dict()
        tbl_list = list()
        header_list = list()
        for i in range(len(cat)):
            sheet_data = cat[i]['sheet_data']
            table_data = cat[i]['table_data']
            sheet_name = cat[i]['sheet_name']
            #print('------------table data--------------')
            for each_sheet in sheet_data:
                print("hi")
                # #print each_sheet
                tbl_dt = dict()
                tbl_dt['origin'] = each_sheet['origin']
                tbl_dt['destination'] = each_sheet['destination']
                rbd_li = dict()
                rbd_li['origin'] = each_sheet['origin']
                rbd_li['destination'] = each_sheet['destination']
                rbd_li["ow_rt"] =  ""
                rbd_li["disc_applicable"] =  ""
                rbd_li["pax_type"] =  ""
                rbd_li["tkt_designator"] =  ""
                rbd_li["tier"] =  ""
                rbd_li["revenue_range"] =  ""
                rbd_li["tkt_fbc_code"] =  ""
                rbd_li["fare_box"] =  ""
                rbd_li["validity"] =  True
                # #print each_sheet
                for each_count in each_sheet['discount_details']:
                    ll = each_count['rbd'].split(" / ")
                    for ll_ in ll:
                        if (isinstance(each_count['discount'], str)) == True:
                            if '|' in each_count['discount']:
                                d = each_count['discount'].split('|')
                                d = int(d[0].replace("%", ""))
                                rbd_li[ll_] = d
                            else:
                                rbd_li[ll_] = "N/A"
                        else:
                            rbd_li[ll_] = each_count['discount']
                    # #print each_count
                    # #print ll
                # #print rbd_li
                tbl_list.append(rbd_li)

        # #print tbl_list
        #print('------------table headers--------------')
            for each_doc in table_data['table_data']:
                print("hiii")
                header_dict = dict()
                header_dict["rbdlist"] = each_doc['rbdlist']
                header_dict["header"]= each_doc['header']
                header_dict["subheaders"]= each_doc['subheaders']
                header_dict["span"] = each_doc['span']
                header_dict["rowspan"] = [1]
                header_list.append(header_dict)
            # #print header_dict

        cat_dict = dict()
        cat_list = list()
        cat_dict['cat_name'] = cat_bat[0]

        inner_dict = dict()
        inner_dict_1 = dict()
        inner_dict_1['tableheaders'] = header_list
        inner_dict_1['tableData'] = tbl_list
        inner_dict['corporate_instruction'] = inner_dict_1
        inner_dict['batch_file'] = cat_bat
        # inner_dict['validity'] = deep_index(cat_bat, cat['pre_validation'])
        cat_list.append(inner_dict)
        cat_dict['sequences'] = inner_dict
        cat_dict['override_tag'] = {}
        cat_dict['validation'] = True
        # #print cat_dict
        return cat_dict


    Sinohydro_corporate_name = "Sinohydro  Corporation" # only exceptional files
    # print("----------   Cat 50  ------------")
    try:
        cat_50_bat = text_parsing['cat_data']['Cat_50']
        # #print cat_50_bat
        cat_50 = sfi['transformed_cover_sheet'][0]['corporate_name']
        cat_50_dict = cat_50_validation(cat_50_bat, cat_50, Sinohydro_corporate_name)

    except Exception as error:
        # #print error
        cat_50_bat = None
        cat_15 = []

    #print("----------   Cat 14  ------------")
    try:
        cat_14_bat = text_parsing['cat_data']['Cat_14']
        cat_14 = sfi['transformed_cover_sheet'][1]['travel_period']
        cat_14_dict = cat_14_validation(cat_14_bat, cat_14)

    except Exception as error:
        # #print error
        cat_14_bat = []
        cat_14 = []

    #print("----------   Cat 15  ------------")
    try:
        cat_15_bat = text_parsing['cat_data']['Cat_15']
        cat_15 = sfi['transformed_cover_sheet'][2]['ticketing_sales_period']
        cat_15_dict = cat_15_validation(cat_15_bat, cat_15)

    except Exception as error:
        # #print error
        cat_15_bat = []
        cat_15 = []
        cat_15_dict = dict()
        cat_15_dict['validation'] = False

    #print("----------   Cat 19  ------------")
    try:
        cat_19_bat = text_parsing['cat_data']['Cat_19']
        cat_19 = sfi['transformed_cover_sheet'][8]['child_infant_discounts']
        cat_19_dict = cat_19_validation(cat_19_bat, cat_19)

    except Exception as error:
        # #print error
        cat_19_bat = []
        cat_19 = []
        cat_19_dict = dict()
        cat_19_dict['validation'] = False

    #print("----------   Cat 25  ------------")
    try:
        print("hi")
        cat_25_bat = text_parsing['cat_data']['Cat_25']
        cat_25 = corp_inst['fare_sheet']['sheets']
        print("gi")
        cat_25_dict = cat_25_validation(cat_25_bat, cat_25)
        # #print cat_25

    except Exception as error:
        print (error)
        cat_25_bat = []
        cat_25 = []
        cat_25_dict = dict()
        cat_25_dict['validation'] = False


    #print("----------   Cat 35  ------------")
    try:
        cat_35_bat = text_parsing['cat_data']['Cat_35']
        cat_35_gds_detail = copy.deepcopy(corp_inst['IATA_List']['IATA_data'])  
        cat_35_haj = corp_inst['rules_notes']['general_notes']
        cat_35_acc = corp_inst['cover_sheet']["details_required_for_private_filing"]['account_code']
        if cat_35_acc == None:
            cat_35_acc = 'TBA'
        cat_35_dict = cat_35_validation(cat_35_bat, cat_35_gds_detail, cat_35_haj, cat_35_acc)

    except Exception as error:
        print(error)
        cat_35_dict = dict()
        cat_35_dict['validation'] = False

    #print("----------   Cat 18  ------------")
    try:
        cat_18_bat = text_parsing['cat_data']['Cat_18']
        cat_18 = sfi['transformed_cover_sheet'][3]['disclaimer']
        cat_18_dict = cat_18_validation(cat_18_bat, cat_18)

    except Exception as error:
        # #print error
        cat_18_bat = []
        cat_18 = []
        cat_18_dict = dict()
        cat_18_dict['validation'] = False
    # 8
    try:
        cat_16_bat = text_parsing['cat_data']['Cat_16']
        # #print cat_50_bat
        # cat_50 = sfi['transformed_cover_sheet'][0]['corporate_name']
        cat_16_dict = {'validation': False, 'cat_name': cat_16_bat[0], 'corporate_instruction': [''], 'batch_file': cat_16_bat }

    except Exception as error:
        # #print error
        cat_16_dict = dict()
        cat_16_dict['validation'] = False
    
    try:
        cat_01_bat = text_parsing['cat_data']['Cat_1']
        cat_01 = corp_inst['IATA_List']['IATA_data']
        cat_01_acc = corp_inst['cover_sheet']["details_required_for_private_filing"]['account_code']
        if cat_01_acc == None:
            cat_01_acc = 'TBA'
        cat_01_dict = cat_01_validation(cat_01_bat, cat_01, cat_01_acc)
        # #print cat_50_bat
        # cat_50 = sfi['transformed_cover_sheet'][0]['corporate_name']
        #cat_01_dict = {'validation': False, 'cat_name': cat_01_bat[0], 'corporate_instruction': [''], 'batch_file': cat_01_bat }
    except Exception as error:
        print (error)
        cat_01_dict = dict()

        cat_01_dict['validation'] = False

    try:
        cat_05_bat = text_parsing['cat_data']['Cat_5']
        # #print cat_50_bat
        # cat_50 = sfi['transformed_cover_sheet'][0]['corporate_name']
        cat_05_dict = {'validation': False, 'cat_name': cat_05_bat[0], 'corporate_instruction': [''], 'batch_file': cat_05_bat }

    except Exception as error:
        # #print error
        cat_05_dict = dict()

        cat_05_dict['validation'] = False
    try:
        cat_06_bat = text_parsing['cat_data']['Cat_6']
        # #print cat_50_bat
        # cat_50 = sfi['transformed_cover_sheet'][0]['corporate_name']
        cat_06_dict = {'validation': False, 'cat_name': cat_06_bat[0], 'corporate_instruction': [''], 'batch_file': cat_06_bat }

    except Exception as error:
        # #print error
        cat_06_dict = dict()

        cat_06_dict['validation'] = False

    try:
        cat_07_bat = text_parsing['cat_data']['Cat_7']
        # #print cat_50_bat
        # cat_50 = sfi['transformed_cover_sheet'][0]['corporate_name']
        cat_07_dict = {'validation': False, 'cat_name': cat_07_bat[0], 'corporate_instruction': [''], 'batch_file': cat_07_bat }

    except Exception as error:
        # #print error
        cat_07_dict = dict()

        cat_07_dict['validation'] = False

    try:
        cat_10_bat = text_parsing['cat_data']['Cat_10']
        # #print cat_50_bat
        # cat_50 = sfi['transformed_cover_sheet'][0]['corporate_name']
        cat_10_dict = {'validation': False, 'cat_name': cat_10_bat[0], 'corporate_instruction': [''], 'batch_file': cat_10_bat }

    except Exception as error:
        # #print error
        cat_10_dict = dict()

        cat_10_dict['validation'] = False

    try:
        cat_26_bat = text_parsing['cat_data']['Cat_26']
        # #print cat_50_bat
        # cat_50 = sfi['transformed_cover_sheet'][0]['corporate_name']
        cat_26_dict = {'validation': False, 'cat_name': cat_26_bat[0], 'corporate_instruction': [''], 'batch_file': cat_26_bat }

    except Exception as error:
        # #print error
        cat_26_dict = dict()

        cat_26_dict['validation'] = False

    try:
        cat_31_bat = text_parsing['cat_data']['Cat_31']
        # #print cat_50_bat
        # cat_50 = sfi['transformed_cover_sheet'][0]['corporate_name']
        cat_31_dict = {'validation': False, 'cat_name': cat_31_bat[0], 'corporate_instruction': [''], 'batch_file': cat_31_bat }

    except Exception as error:
        # #print error
        cat_31_dict = dict()

        cat_31_dict['validation'] = False

    try:
        cat_33_bat = text_parsing['cat_data']['Cat_33']
        # #print cat_50_bat
        # cat_50 = sfi['transformed_cover_sheet'][0]['corporate_name']
        cat_33_dict = {'validation': False, 'cat_name': cat_33_bat[0], 'corporate_instruction': [''], 'batch_file': cat_33_bat }

    except Exception as error:
        # #print error
        cat_33_dict = dict()

        cat_33_dict['validation'] = False

    try:
        cat_04_bat = text_parsing['cat_data']['Cat_4']
        cat_04_dict = {'validation': False, 'cat_name': cat_04_bat[0], 'corporate_instruction': [''], 'batch_file': cat_04_bat }

    except Exception as error:
        # #print error
        cat_04_dict = dict()

        cat_04_dict['validation'] = False

    validation_array = list()
    validation = dict()
    validation['Cat_18'] = cat_18_dict
    validation['Cat_15'] = cat_15_dict
    validation['Cat_14'] = cat_14_dict
    validation['Cat_50'] = cat_50_dict
    validation['Cat_19'] = cat_19_dict
    validation['Cat_25'] = cat_25_dict
    validation['Cat_35'] = cat_35_dict
    validation['Cat_01'] = cat_01_dict
    validation['Cat_04'] = cat_04_dict
    validation['Cat_16'] = cat_16_dict
    validation['Cat_05'] = cat_05_dict
    validation['Cat_06'] = cat_06_dict
    validation['Cat_10'] = cat_10_dict
    validation['Cat_07'] = cat_07_dict
    validation['Cat_33'] = cat_33_dict
    validation['Cat_31'] = cat_31_dict
    validation['Cat_26'] = cat_26_dict

    try:
        validation['validation_flag'] = cat_18_dict['validation'] and cat_15_dict['validation'] \
                                        and cat_14_dict['validation'] and cat_50_dict['validation'] \
                                        and cat_19_dict['validation'] and cat_25_dict['validation'] \
                                        and cat_35_dict['validation'] and cat_01_dict['validation'] and cat_16_dict['validation'] \
                                        and cat_05_dict['validation'] and cat_06_dict['validation'] and cat_10_dict['validation']\
                                        and cat_07_dict['validation'] and cat_26_dict['validation'] and cat_31_dict['validation']\
                                        and cat_33_dict['validation'] and cat_04_dict['validation']

    except KeyError as error:
        print("Key error occured")
        print(cat_18_dict['validation'])
        print(cat_15_dict['validation'])
        print(cat_14_dict['validation'])
        print(cat_50_dict['validation'])
        print(cat_19_dict['validation'])
        print(cat_25_dict['validation'])
        print(cat_35_dict['validation'])

    validation['Tariff'] = text_parsing['Tariff']
    validation['rule_id'] = text_parsing['Rule_id']
    validation_array.append(validation)

    df1={
        "batch_count" : batch_count,
        "file_upload_date" : file_upload_date,
        "file_upload_time" : file_upload_time,
        "uploaded_by" : uploaded_by,
        "Batch_id" : text_parsing['Batch_id'],
        "Carrier" : text_parsing['Carrier'],
        "validation" : validation_array,
        "file_date":text_parsing['file_date'],
        "file_time":text_parsing['file_time'],
        "file_name":text_parsing['file_name']
    }
    # check if file match with cat 50 if match then upsert else skip append the data
    # print(corporate_name.upper())
    # print(str(cat_50_dict['corporate_instruction'][0]['Corporate_Name']).upper())
    # Sinohydro_corporate_name
    # if [i for i in cat_50_bat if corporate_name.replace(', Inc.', '').replace('-DMM', '').lower() in i.lower()]:
    if ext_id != None:
        db.Pre_GFS_Validation_1.update({"_id" : ObjectId(ext_id)},
                            { "$push": { "data" : df1}
                                })
        id2 = ObjectId(ext_id)

    else:
        _id = db.Pre_GFS_Validation_1.insert_one({"data" : [df1]}).inserted_id
        id2=_id
        print(id2)
    logger.info("validation of {} completed ".format(corporate_name))
    return {
        "msg": "validation done",
        "validation_record_id": str(id2),
        "validation_error": None
    }
    

if  __name__=="__main__":
    import time
    from bson.objectid import ObjectId
    st=time.time()
    client = pymongo.MongoClient(
    '3.6.201.161:27022',
    username='data.EK',
    password='data@123',
    authSource='admin',
    authMechanism='SCRAM-SHA-1')
    db=client['rawDB_prod']
    corporate_name="Essilor Middle East"
    file_name="DMMDXB-3004.txt"
    system_date="31-12-2099"
    system_time="5:30"
    file_date="21-12-1000"
    file_time="3:30"
    id1=parse_text(file_name,system_date,system_time,db,file_date,file_time)
    validation(corporate_name,id1,db, None)
    print(time.time()-st)   