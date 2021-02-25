import itertools
from operator import itemgetter

import copy
import pymongo
import gridfs
from bson.objectid import ObjectId
import re
import json
import xlrd
import pandas as pd
import glob
import datetime
import pprint

def get_raw_db():
    client = pymongo.MongoClient('3.6.201.161:27022')
    client.the_database.authenticate("data.EK", "data@123", source="admin")
    # client = pymongo.MongoClient('localhost:27017')
    db = client["rawDB_prod"]
    return db


# Function to convert
def listToString(s):
    # initialize an empty string
    str1 = ""

    # traverse in the string
    for ele in s:
        str1 += ele

        # return string
    return str1


def get_ATPCO_EK_db():
    client = pymongo.MongoClient('3.6.201.161:27022')
    client.the_database.authenticate("data.EK", "data@123", source="admin")
    # client = pymongo.MongoClient('localhost:27017')
    db = client["ATPCO_EK"]
    return db

def get_pdss_db():
    client = pymongo.MongoClient('3.6.201.161:27022')
    client.the_database.authenticate("data.EK", "data@123", source="admin")
    # client = pymongo.MongoClient('localhost:27017')
    db = client["PDSS_EK"]
    return db

def get_record_3_Table_983(corporate_name, db, ATPCO_db, pdss_db, record_2_json, get_record_3_Cat_1_json, raw_id, file_date, file_time):

    """
    :param corporate_name:
    :param db:
    :param ATPCO_db:
    :param record_2_json:
    :param get_record_3_Cat_1_json:
    :return:

    possibility of revisit this code because collection structure is not great to update all 4 part of data in one array of object
    """
    # print("---------------------- record_2_json -------------------------------")
    # print record_2_json
    print("---------------------- Get_record_3_Cat_1_json -------------------------------")
    print(get_record_3_Cat_1_json)

    final_list = list()
    cat_1_data = list()

    data_doc = dict()
    data_doc["amadeus"] = "1A"
    data_doc["galileo"] = "1G"
    data_doc["sabre"] = "1S"
    data_doc["worldspan"] = "1P"
    data_doc["apollo"] = "1V"
    data_doc["axcess"] = "1J"
    data_doc["abacus"] = "1B"
    data_doc["travelsky"] = "1E"
    data_doc["topas"] = "80"
    data_doc["infini"] = "1F"
    data_doc["ita"] = "1U"
    data_doc["electronic_data_system"] = "1Y"
    data_doc["expedia"] = "43"
    data_doc["switchworks"] = "G2"
    data_doc["vayant"] = "62"
    data_doc["flx"] = "F1"

    # update the defauld values
    rec3_tbl_983 = list()
    # Check the pcc in the IATA list
    cur = db.corporate.instruction.find_one({"_id" : raw_id})
    
    comission_str = ""
    comission_str_query = ""

    if cur is not None:
        try:
            # print cur['cover_sheet']
            try:
                fare_ind = cur['cover_sheet']['details_of_ticketing_instructions']['fare_amount_box']
                tour_val = cur['cover_sheet']['details_of_ticketing_instructions']['tour_code']
                ticket_desi = cur['cover_sheet']['details_of_ticketing_instructions']['ticket_designator']
                market_commission = cur["cover_sheet"]["details_of_ticketing_instructions"]["market_comission"]
            except KeyError as error:
                fare_ind = cur['cover_sheet']['details_of_ticket_instructions']['fare_amount_box']
                tour_val = cur['cover_sheet']['details_of_ticket_instructions']['tour_code']
                ticket_desi = cur['cover_sheet']['details_of_ticket_instructions']['ticket_designator']
                market_commission = ""

            try:
                fares_data = cur['fare_sheet']['sheets'][0]['sheet_desc'].replace('\n', ' ')
            except KeyError as error:
                fares_data = ""

            if '(NON COMMISSIONABLE)' in fares_data:
                comission = ""
            elif '%  COMMISSIONABLE'  in fares_data:
                s = fares_data
                c = '('
                array_of_match = [pos for pos, char in enumerate(s) if char == c]
                len_array = len(array_of_match)
                com_substring = fares_data[array_of_match[len_array-1] : ]

                # getting numbers from string
                temp = re.findall(r'\d+', com_substring)
                com_match = re.search(r'\d+(.\d+)?', com_substring)
                comission_str = ""
                if com_match is not None:
                    comission_str = com_match.group()
                res = list(map(int, temp))

                print(res)
                print(com_substring)
                print("comission string", comission_str)
                comission = list("-------")
                for idx, num in enumerate(res):
                    comission[idx+2] = res[idx]
                # comission[2] = res[0]
                print("updated comission", comission)
                comission_str_query = "".join(str(x) for x in comission)
                comission_str_query = str(comission_str_query.replace('-', '0'))
                print("formatted commission string query", comission_str_query)
                # pass
            elif market_commission != "":
                print("checking for commission in market_commission", market_commission)
                # getting numbers from string
                temp = re.findall(r'\d+', market_commission)
                com_match = re.search(r'\d+(.\d+)?', market_commission)
                comission_str = ""
                if com_match is not None:
                    comission_str = com_match.group()
                res = list(map(int, temp))

                print(res)
                print("comission string", comission_str)
                comission = list("-------")
                for idx, num in enumerate(res):
                    comission[idx+2] = res[idx]
                # comission[2] = res[0]
                print("updated comission", comission)
                comission_str_query = "".join(str(x) for x in comission)
                comission_str_query = str(comission_str_query.replace('-', '0'))
                print("formatted commission string query", comission_str_query)
                

            print(fare_ind)
        except Exception as error:
            print(error)
            fare_ind = "Net Fares"
            comission_str = ""

        if fare_ind is None:
            fare_ind = "Net Fares"
        # print(fares_data)
        print(fare_ind)
        try:
            IATA_list_ = cur['IATA_List']['security_tbl']
            print('using security_tbl contents')
        except Exception as error:
            try:
                print("error")
                print(error)
                IATA_list_ = cur['IATA_list']

            except Exception as error:
                IATA_list_ = cur['IATA_List']['IATA_data']

        print("---------------- ---------------")
        print("---------------- Account code group and breakup---------------")
        IATA_list_break = list()
        pcc_missing_records = list()
        
        print("no. of records in security tbl block : ", str(len(IATA_list_)))
        print("no. of records in record_2 : ", str(len(record_2_json)))

        for each_list in IATA_list_:

            if "Locales Code" not in each_list or each_list["Locales Code"] == None or each_list["Locales Code"] == "":
                pcc_missing_records.append({
                    "pcc": each_list.get("Locales Code", ""),
                    "gds": each_list.get("CXR/CRS Code", ""),
                    "account_code": each_list.get("Account Code", "")
                })
                continue

            if "Account Code" not in each_list or each_list["Account Code"] == None or each_list["Account Code"] == "":
                raw_rec_cur = db.corporate.instruction.find_one({"_id": raw_id}, {"cover_sheet.details_required_for_private_filing.account_code": 1})
                acc_code = ""
                try:
                    acc_code = raw_rec_cur["cover_sheet"]["details_required_for_private_filing"]["account_code"]
                    if acc_code == None or acc_code == "" or "ref" in acc_code.lower():
                        raise Exception("exception, could not find account code details, populating with default 'TBA'")
                except Exception as error:
                    print(error)
                    acc_code = "TBA"
                    
                each_list["Account Code"] = acc_code
            
            IATA_list_break.append(each_list)

        print("no. of records after omitting empty pcc records and break up on account code : ", str(len(IATA_list_break)))

        print("---------------- Travelport update ------------------------")
        IATA_list_break_ = list()
        for e in IATA_list_break:
            try:
                if "CXR/CRS Code" in e:
                    dd = e["CXR/CRS Code"].split(" - ")[0].strip()
                    if len(dd) == 2:
                        e["CXR/CRS"] = dd
                    else:
                        if e["CXR/CRS Code"].lower().strip() in data_doc:
                            e["CXR/CRS"] = data_doc[e["CXR/CRS Code"].lower().strip()]
                        else:
                            e["CXR/CRS"] = ""
            except Exception as e:
                print("exception occured while getting GDS code")
                print(e)
                e["CXR/CRS"] = "" 
            # try:
            #     if "GDS/CRS" in e:
            #         dd = e['GDS/CRS'].split(' - ')[0].split(' ')[0]
            #         if len(dd) > 2:
            #             e["CXR/CRS"] = data_doc[dd.lower()]
            #     elif "GDS" in e:
            #         dd = e['GDS'].split(' - ')[0].split(' ')[0]
            #         if len(dd) > 2:
            #             e["CXR/CRS"] = data_doc[dd.lower()]

            # except KeyError as error:
            #     print(error)
            #     # print each_list['GDS/CRS']
            #     try:
            #         e["CXR/CRS"] = e['GDS/CRS'].split(' - ')[0].split(' ')[0]
            #     except Exception as error:
            #         e["CXR/CRS"] = ""

            e["LOC_1"] = e["LOC 1"]

            # if 'travelport' in e['GDS']:
            if 'travelport' in e["CXR/CRS Code"]:
                e1 = copy.deepcopy(e)
                e2 = copy.deepcopy(e)
                e3 = copy.deepcopy(e)
                e1['GDS'] = 'apollo'
                e1["LOC_1"] = ""
                e2["LOC_1"] = ""
                e3["LOC_1"] = ""
                e1["CXR/CRS"] = data_doc['apollo']
                IATA_list_break_.append(e1)
                e2['GDS'] = 'galileo'
                e2["CXR/CRS"] = data_doc['galileo']
                IATA_list_break_.append(e2)
                e3['GDS'] = 'worldspan'
                e3["CXR/CRS"] = data_doc['worldspan']
                IATA_list_break_.append(e3)
            else:
                IATA_list_break_.append(e)

        print("no. of records after travelport update : ", str(len(IATA_list_break_)))
        print("---------------- GROUPING IATA list ------------------------")

        # Sort students data by `class` key.
        students = sorted(IATA_list_break_, key=itemgetter('Account Code'))

        cumulative_list = list()
        # Display data grouped by `class`
        print("group by with account code")
        for key, value in itertools.groupby(students, key=itemgetter('Account Code')):
            list_ = list()
            for i in value:
                list_.append(i)
            print("account code : ", key)
            # print("corresponding records : ")
            # pprint.pprint(list_)

            list_sorted = sorted(list_, key=itemgetter("CXR/CRS Code"))

            print("groupby with GDS code")            
            for k, kk in itertools.groupby(list_sorted, key=itemgetter("CXR/CRS Code")):
                print("gds code : ", k)
                print("corresponding records : ")
                query_builder = list()
                local_list = list()
                for j in kk:
                    print(j)

                    duplicate_record = False

                    if "CXR/CRS" not in j:
                        j["CXR/CRS"] = ""
                    j["Tbl_No"] = ""
                    # j["CXR/CRS"] = ""  # GDS
                    j["APPL"] = ""
                    # j["LOC_1"] = ""  # LOC_1

                    # j["Locales_Type"] = "T"

                    j["Update"] = "N"
                    j["Locales_Code"] = ""  # PCC
                    j["Redistribute"] = "N"
                    j["Sell"] = "Y"
                    j["Ticket"] = "Y"
                    # j["seller_ID"] = "Y"
                    j["seller_ID"] = "000000000000000000" 
                    zeros = "000000000000000000"
                    # if "RU_Replace" in j:
                    #     if j['RU_Replace'] == True:
                    #         zeros = zeros[:-6]+j['Account Code']
                            # j["seller_ID"] = zeros[:-6]+j['Account Code']
                    

                    if "PCC" in j:
                        j['PCC'] = j['PCC'].replace(".0", "")
                        j["Locales_Code"] = j['PCC']
                    if "LOC_1" in j:
                        j["LOC_1"] = j['LOC_1']

                    if j["CXR/CRS"] != "":
                        j["combine_column"] = j["CXR/CRS"] + "" + j["APPL"] + "" + \
                                                      j["LOC_1"] + "" + \
                                                      j["Locales Type"] + "" + \
                                                      j["Locales Code"] + "" + \
                                                      j["Update"] + "" + \
                                                      j["Redistribute"] + "" + \
                                                      j["Ticket"] + "" + \
                                                      j["Sell"] + zeros

                        query_builder_ = j["CXR/CRS"] + "" + j["APPL"] + "" + \
                                            j["LOC_1"] + "" + \
                                            j["Locales Type"] + "" + \
                                            j["Locales Code"] + "" + \
                                            j["Update"] + "" + \
                                            j["Redistribute"] + "" + \
                                            j["Ticket"] + "" + \
                                            j["Sell"] + zeros
                        if query_builder_ in query_builder:
                            duplicate_record = True
                        else:
                            query_builder.append(query_builder_)

                        print("individual query : ")
                        print(query_builder_)
                        
                    if not duplicate_record:
                        local_list.append(j)

                print("query builder : ")
                print(query_builder)

                each_dict = dict()
                each_dict['GDS'] = k
                each_dict['Account Code'] = str(key).strip()
                each_dict['iata_doc'] = local_list
                each_dict['combine_column'] = query_builder
                cumulative_list.append(each_dict)
            list_ = list_[:]


        print("no. of records after grouping on account code and gds : ", str(len(cumulative_list)))

        i = 0

        if ticket_desi == None:
            ticket_desi = ""
        for each_list in cumulative_list:

            for td in ticket_desi.split(","):
                data_dict_1 = dict()

                
                # for rec_3_cat_35
                data_dict_1["SEC_TBL_NO"] = ""
                data_dict_1["Tbl_No"] = ""
                # data_dict_1["PTC"] = ""
                if 'net' in fare_ind.lower():
                    data_dict_1["COMMISSION_NET_GROSS_INDICATOR"] = "N"  # N for Net, B for Percent
                else:
                    data_dict_1["COMMISSION_NET_GROSS_INDICATOR"] = "G"
                data_dict_1["COMMISSION_PERCENT"] = comission_str
                data_dict_1["TICKETING_IND"] = ""
                data_dict_1['COMMISSION_CURRENCY']=""
                data_dict_1['COMMISSION_AMOUNT']=""
                 # Required for type 2
                if 'amadeus' in each_list['GDS'].lower():
                    data_dict_1["TICKETING_TYPE"] = "C"   # C for amedious T for other GDS
                else:
                    data_dict_1["TICKETING_TYPE"] = "T"
                data_dict_1["TICKETING_TOUR_CAR_VALUE_CODE"] = tour_val
                data_dict_1["TICKETING_TICKET_DESIGNATOR"] = td
                data_dict_1["TICKETING_CXR"] = "EK"

                # each_list['combine_column']
                cur = ATPCO_db.summary_983.find_one({
                    "combine_column": {"$all" : each_list['combine_column']}, 
                    "combine_column_size": len(each_list["combine_column"])
                })
                # print([query_builder])
                # print(cur)
                if cur is not None:
                    print("table number records fetched for combine column:")
                    print(cur)
                    print(cur["combine_column"])
                    # data_dict_1["Tbl_No"] = cur['tbl_no']
                    data_dict_1["combine_column"] = each_list['combine_column']
                    data_dict_1["SEC_TBL_NO"] = cur['tbl_no']

                data_dict_1["combine_column"] = each_list['combine_column']

                record_3_cat_35_query = {
                        "SEC_TBL_NO": data_dict_1["SEC_TBL_NO"],
                        "PTC": "",
                        "FARE_CRTR_TBL_NO": "00000000",
                        "METHOD_TYPE" : "",
                        "COMMISSION_NET_GROSS_INDICATOR" : data_dict_1["COMMISSION_NET_GROSS_INDICATOR"],
                        "COMMISSION_PERCENT" : comission_str_query if comission_str_query != "" else "9999999",
                        "COMMISSION_AMT_1" : "0000000",
                        "COMMISSION_CUR_1" : "",
                        "COMMISSION_DEC_1" : "0",
                        "FILLER" : "",
                        "DATE_TBL_NO" : "00000000",
                        "TEXT_TBL_NO" : "00000000",
                        "TICKETING_IND" : "",
                        "TICKETING_CXR_1" : "EK",
                        "TICKETING_AUDITOR_PASSENGER_COUPON" : "",
                        "TICKETING_TYPE" : data_dict_1["TICKETING_TYPE"],
                        "TICKETING_TOUR_CAR_VALUE_CODE" : data_dict_1["TICKETING_TOUR_CAR_VALUE_CODE"],
                        "TICKETING_TICKET_DESIGNATOR" : data_dict_1["TICKETING_TICKET_DESIGNATOR"],
                        "TICKETING_TICKETED_FARE_DATA" : "",
                        "TICKETING_OW_RT" : "",
                        "TICKETING_GLOBAL_INDICATOR" : "",
                        "TICKETING_RULE_TARIFF_NUMBER" : "000",
                        "TICKETING_CXR_2" : "",
                        "TICKETING_RULE_NUMBER" : "",
                        "TICKETING_FARE_CLASS_FARE_FAMILY_CODE" : "",
                        "TICKETING_FARE_TYPE" : "",
                        "TICKETING_FARE_BOX_TEXT" : "",
                        "TICKETING_FARE_BOX_AMT" : "0000000",
                    }
                print("record 3 cat 35 query", record_3_cat_35_query)

                record_3_cat_35_cur = ATPCO_db.JUP_DB_ATPCO_Record_3_Cat_35.find_one(record_3_cat_35_query, {"_id": 0, "Tbl_No": 1})
                print("record 3 cat 35 cur", record_3_cat_35_cur)

                if record_3_cat_35_cur is not None:
                    data_dict_1["Tbl_No"] = record_3_cat_35_cur["Tbl_No"]

                # print(data_dict)
                for each in each_list['iata_doc']:

                    get_record_3_Cat_1_json['Account_Code'] = str(each['Account Code']).strip()
                    each["Tbl_No"] = data_dict_1["SEC_TBL_NO"]

                    cat_1_tbl_no_query = {
                            "PSGR_TYPE" : "ADT", 
                            "ID" : get_record_3_Cat_1_json["ID"],
                            "AGE_MIN" : "", 
                            "AGE_MAX" : "", 
                            "PSGR_OCC_FIRST" : "000", 
                            "PSGR_OCC_LAST" : "000", 
                            "DATE_TBL_NO" : "00000000", 
                            "TEXT_TBL_NO" : get_record_3_Cat_1_json["tabl_no_996"],
                            "UNAVAIL" : "", 
                            "APPL" : "", 
                            "PSGR_STATUS" : "", 
                            "LOC" : "", 
                            "ACCOUNT_CODE" : get_record_3_Cat_1_json['Account_Code']
                    }
                    print("record 3 cat 1 query", cat_1_tbl_no_query)
                    cat_1_cur = ATPCO_db.JUP_DB_ATPCO_Record_3_Cat_01.find_one(cat_1_tbl_no_query, {"_id": 0, "TBL_NO": 1})
                    
                    print("record 3 cat 1 tbl no")
                    print(cat_1_cur)
                    if cat_1_cur != None:
                            get_record_3_Cat_1_json["Tbl_No"] = cat_1_cur["TBL_NO"]
                    for rec2 in record_2_json:
                        final_dict = dict()
                        q = rec2.copy()
                        if 'commission_per' in q:
                            del q['commission_per']
                        if 'commission_val' in q:
                            del q['commission_val']
                        if 'commission_amt' in q:
                            del q['commission_amt']
                        if 'commission_cur' in q:
                            del q['commission_cur']
                        final_dict["Record_2"]=q
                        if "commission_per" in rec2 or "commission_val" in rec2:
                            record_3_cat_35_query = {
                                "SEC_TBL_NO": data_dict_1["SEC_TBL_NO"],
                                "PTC": "",
                                "FARE_CRTR_TBL_NO": "00000000",
                                "METHOD_TYPE" : "",
                                "COMMISSION_NET_GROSS_INDICATOR" : data_dict_1["COMMISSION_NET_GROSS_INDICATOR"],
                                "COMMISSION_PERCENT" : "9999999",
                                "COMMISSION_AMT_1" : "0000000",
                                "COMMISSION_CUR_1" : "",
                                "COMMISSION_DEC_1" : "0",
                                "FILLER" : "",
                                "DATE_TBL_NO" : "00000000",
                                "TEXT_TBL_NO" : "00000000",
                                "TICKETING_IND" : "",
                                "TICKETING_CXR_1" : "EK",
                                "TICKETING_AUDITOR_PASSENGER_COUPON" : "",
                                "TICKETING_TYPE" : data_dict_1["TICKETING_TYPE"],
                                "TICKETING_TOUR_CAR_VALUE_CODE" : data_dict_1["TICKETING_TOUR_CAR_VALUE_CODE"],
                                "TICKETING_TICKET_DESIGNATOR" : data_dict_1["TICKETING_TICKET_DESIGNATOR"],
                                "TICKETING_TICKETED_FARE_DATA" : "",
                                "TICKETING_OW_RT" : "",
                                "TICKETING_GLOBAL_INDICATOR" : "",
                                "TICKETING_RULE_TARIFF_NUMBER" : "000",
                                "TICKETING_CXR_2" : "",
                                "TICKETING_RULE_NUMBER" : "",
                                "TICKETING_FARE_CLASS_FARE_FAMILY_CODE" : "",
                                "TICKETING_FARE_TYPE" : "",
                                "TICKETING_FARE_BOX_TEXT" : "",
                                "TICKETING_FARE_BOX_AMT" : "0000000",
                            }
                            if "commission_per" in rec2:
                                print("query with commission percent")
                                data_dict_1["COMMISSION_PERCENT"] = rec2["commission_per"]
                                data_dict_1["COMMISSION_NET_GROSS_INDICATOR"] = "G"
                                data_dict_1["COMMISSION_AMOUNT"] = ""
                                data_dict_1["COMMISSION_CURRENCY"] = ""
                                comission = list("-------")
                                for idx, num in enumerate(rec2["commission_per"]):
                                    comission[idx+2] = rec2["commission_per"][idx]
                                comission_str_query = "".join(str(x) for x in comission)
                                comission_str_query = str(comission_str_query.replace('-', '0'))
                                record_3_cat_35_query["COMMISSION_PERCENT"] = comission_str_query
                            else:
                                print("query with commission value")
                                data_dict_1["COMMISSION_AMOUNT"] = rec2["commission_val"]
                                data_dict_1["COMMISSION_CURRENCY"] = rec2["commission_cur"]
                                data_dict_1["COMMISSION_NET_GROSS_INDICATOR"] = "G"
                                data_dict_1["COMMISSION_PERCENT"] = ""
                                currency_cur = pdss_db.JUP_DB_Currency_Decimal_Master.find_one({"currency": rec2["commission_cur"].upper()})
                                if currency_cur != None:
                                    record_3_cat_35_query["COMMISSION_CUR_1"] = rec2["commission_cur"].upper()
                                    record_3_cat_35_query["COMMISSION_DEC_1"] = currency_cur["decimal"]
                                    commission_amt = ""
                                    commission_amt = rec2["commission_val"] + "0" * int(currency_cur["decimal"])
                                    commission_amt = "0" * ( 7 - len(commission_amt)) + commission_amt
                                    record_3_cat_35_query["COMMISSION_AMT_1"] = commission_amt
                        
                        print("record 3 cat 35 query", record_3_cat_35_query)

                        record_3_cat_35_cur = ATPCO_db.JUP_DB_ATPCO_Record_3_Cat_35.find_one(record_3_cat_35_query, {"_id": 0, "Tbl_No": 1})
                        print("record 3 cat 35 cur", record_3_cat_35_cur)

                        if record_3_cat_35_cur is not None:
                            data_dict_1["Tbl_No"] = record_3_cat_35_cur["Tbl_No"]
                        else:
                            data_dict_1["Tbl_No"] = ""

                        if final_dict["Record_2"]["Appl"] == "NA":
                            final_dict["Record_2"]["Appl"] = "X"

                            final_dict["Record_3_Table_983"] = {
                                "Application Tag" : "",
                                "Travel Agency" : "",
                                "CXR/CRS Code" : "",
                                "Duty/Function Code" : "",
                                "LOC 1" : "",
                                "LOC 2" : "",
                                "Locales Type" : "",
                                "Locales Code" : "",
                                "Account Code" : "",
                                "Filing Type" : "",
                                "Changes Only" : "",
                                "Secondary Seller Control" : "",
                                "CXR/CRS" : "",
                                "LOC_1" : "",
                                "Tbl_No" : "",
                                "APPL" : "",
                                "Locales_Type" : "",
                                "Update" : "",
                                "Locales_Code" : "",
                                "Redistribute" : "",
                                "Sell" : "",
                                "Ticket" : "",
                                "seller_ID" : "",
                                "combine_column" : ""
                            }
                            final_dict['Record_3_Cat_35'] = {
                                "SEC_TBL_NO" : "",
                                "Tbl_No" : "",
                                "TICKETING_CXR": "",
                                "COMMISSION_NET_GROSS_INDICATOR" : "",
                                "COMMISSION_PERCENT" : "",
                                "COMMISSION_CURRENCY":"",
                                "COMMISSION_AMOUNT":"",
                                "TICKETING_IND" : "",
                                "TICKETING_TYPE" : "",
                                "TICKETING_TOUR_CAR_VALUE_CODE" : "",
                                "TICKETING_TICKET_DESIGNATOR" : "",
                                "combine_column" : [],

                            }
                            final_dict['Record_3_Cat_01'] = {
                                "Tbl_No" : "",
                                "tabl_no_996" : "",
                                "Account_Code" : "",
                                "ID" : "",
                                "Note_text" : ""
                            }
                        else:
                            final_dict['Record_3_Table_983'] = copy.deepcopy(each)
                            final_dict['Record_3_Cat_35'] = copy.deepcopy(data_dict_1)
                            if "mars" in final_dict["Record_3_Table_983"]["CXR/CRS Code"].lower().strip():
                                final_dict['Record_3_Cat_01'] = copy.deepcopy(get_record_3_Cat_1_json)
                            else:
                                final_dict['Record_3_Cat_01'] = {
                                    "Tbl_No" : "",
                                    "tabl_no_996" : "",
                                    "Account_Code" : "",
                                    "ID" : "",
                                    "Note_text" : ""
                                }

                                cat_1_data.append(
                                    {
                                        "Record_2": rec2,
                                        "Record_3_Cat_01": {
                                            "Tbl_No": get_record_3_Cat_1_json["Tbl_No"],
                                            "Account_Code": get_record_3_Cat_1_json["Account_Code"],
                                            "ID": get_record_3_Cat_1_json["ID"]
                                        },
                                        "Record_3_tbl_996": {
                                            "tabl_no_996": get_record_3_Cat_1_json["tabl_no_996"],
                                            "Note_text": get_record_3_Cat_1_json["Note_text"]
                                        }
                                    }
                                )
                        final_list.append(final_dict)

    print("no. of records after record_2, record_3 combination : ", str(len(final_list)))
    print("no. of records excluded from sfi", str(len(pcc_missing_records)))

    # Insert into SFI collectionx
    cat1_data=[]
    for k in range(len(cat_1_data)):
        if cat_1_data[k]["Record_2"]['Fare_Family']=="":
            cat1_data.append(cat_1_data[k])
    try:
        id = db.corporate.sfi.update_one(
            {"corporate_name": corporate_name, "file_date": file_date, "file_time": file_time},
            # {"_id": ObjectId("5f660c799fd23ce46d080ae5")},
            {"$set": {
                "iata_gridfs": False,
                "transformed_IATA_list" : final_list,
                "transformed_IATA_excluded_list": pcc_missing_records,
                "cat_1": cat1_data   
            }},
            upsert=True
        )
    except pymongo.errors.DocumentTooLarge:
        print("document size too large, could not insert into sfi collection")
        print("inserting the record into grid fs collection")
        fs = gridfs.GridFS(db, "corporate.sfi.grid")
        grid_id = fs.put(
            json.dumps({"transformed_IATA_list": final_list, "tranformed_IATA_excluded_list": pcc_missing_records, "cat_1": cat1_data}),
            encoding="utf-8")
        print("grid fs id")
        print(grid_id)
        # data = fs.get(grid_id)
        # print(data.read())
        db.corporate.sfi.update_one(
            {"corporate_name": corporate_name, "file_date": file_date, "file_time": file_time},
            {"$set": {
                "iata_gridfs": True,
                "grid_id": grid_id 
            }}
        )

    # print id
    # return id
    return None


def get_record_3_Cat_1(corporate_name, db, ATPCO_db, raw_id):
    data_dict = dict()
    # check cat-1 record present or not in the excel
    data_dict["Tbl_No"] = ""
    data_dict["tabl_no_996"] = "00000000"
    data_dict["Account_Code"] = ""
    data_dict["ID"] = ""
    data_dict["Note_text"] = ""
    
    cur = db.corporate.instruction.find_one({"_id" : raw_id})
    check_cat_1_eligibility = False
    cat_1_Note = False
    skip_Notes = False
    Notes_array = list()
    cat_1_arrya = list()
    try:
        array_of_notes = cur['rules_notes']['pos_sheet_rules']
        x=[i for i in array_of_notes if "Cat 01. " in i['field_value']]
        # print()) 
        cat_1_arrya=[]
        for i in array_of_notes[array_of_notes.index(x[0]):]:
            cat_1_arrya.append(i['field_value'])
            if re.search("cat\s*",i['field_value'],flags=re.IGNORECASE) and "01" not in i['field_value']:
                del cat_1_arrya[-1]
                break 
        # print(list2)
        Notes_array=[i for i in cat_1_arrya if  re.search("^note(.*)",i ,flags=re.IGNORECASE)]
        Notes_array=cat_1_arrya[cat_1_arrya.index(Notes_array[0])+1:]
        cat_1_arrya=cat_1_arrya[:cat_1_arrya.index(Notes_array[0])]
        # print(stri
    except Exception as error:
        pass
    if cat_1_arrya is not None:
        check_id = any("ID" in string for string in cat_1_arrya)
        if check_id == True:
            data_dict["ID"] = "X"
        else:
            data_dict["ID"] = ""
    # data_dict["ID"] = ""cat_1_arrya
    str_notes = ""
    for i in range(len(Notes_array)):
        if len(Notes_array[i])<15:
            break
        str_notes+=Notes_array[i]
    data_dict["Note_text"] = str_notes
    print("cat 1 notes text: ", str_notes)
    # check the table number
    cur = ATPCO_db.Summary_996.find_one({"TEXT" : str_notes})
    if cur is not None:
        # data_dict["Tbl_No"] = cur['TABLE']
        data_dict["tabl_no_996"] = cur['TABLE']

    # print(cat_1_arrya)
    # print(Notes_array)
    print(data_dict)
    return data_dict

def get_record_2(corporate_name, db, ATPCO_db,raw_id, file_date, file_time):
    Json_list = list()
    # get corporate info
    # query_corp_name = corporate_name.lower().replace(" ", "_")
    # cur = db.JUP_DB_Corporate_Info.find_one({"corporate_name": query_corp_name.lower().replace(" ", "_")})
    # print(cur)
    try:
        # rule = cur['rule']
        # tariff = cur['tariff']
        tariff = db.corporate.sfi.distinct(
                "record_3.tariff_name", 
                {"corporate_name": corporate_name, "file_date": file_date, "file_time": file_time}
            )
        rule = db.corporate.sfi.distinct(
                "record_3.rule_id", 
                {"corporate_name": corporate_name, "file_date": file_date, "file_time": file_time}
            )
    except Exception as error:
        rule = [""]
        tariff = [""]
    if len(rule) == 0:
        rule = [""]
    if len(tariff) == 0:
        tariff = [""]

    print(tariff)
    print(rule)

    commission_exists = False

    # get the farefamily
    exclude_field_value = ""
    rule_cur = db.corporate.instruction.find_one({"_id" : raw_id})
    if rule_cur is not None:
        try:
            general_note = rule_cur['rules_notes']["general_notes"]
            print("general notes : ")
            pprint.pprint(general_note)
            exclude_field_value = ""
            if general_note is not None:
                for each in general_note:
                    if each['field_key'] == "general_exclusions":
                        exclude_field_value = each['field_value']
                        print("found general exclusion key : ", exclude_field_value)

            else:
                general_note = []
        except KeyError as error:
            print("exception occured while getting general exclusion")
            print(error)
            exclude_field_value = ""
    
        # checking for commission in rules
        try:
            pos_sheet_rules = rule_cur["rules_notes"]["pos_sheet_rules"]
            commission_details = []
            for ru in pos_sheet_rules:
                print("rule :", ru)
                if commission_exists:
                    commission_text = ru["field_value"]
                    commission_defs = commission_text.split("and")
                    for commission_def in commission_defs:
                        commission_per = ""
                        commission_val = ""
                        commission_cur = ""
                        commission_detail = {}

                        print("commission def :", commission_def)
                        commission_applied_idx = commission_def.split(" ").index("commission")
                        commission_applied = ""
                        if commission_applied_idx != -1:
                            commission_applied = commission_def.split(" ")[commission_applied_idx - 1] 
                        print("commission applied :", commission_applied)
                        if "economy" in commission_applied.lower():
                            commission_details.append({"fare_families":[""],"commission_per":""})
                            commission_detail["fare_families"] = list()
                            eco=db.corporate.sfi.distinct(
                                "record_3.rbd", 
                                {"corporate_name": corporate_name, "file_date": file_date, "file_time": file_time})
                            economy_rbds = ["Y", "E", "R", "W", "M", "B", "U"]
                            economy_rbds=list(set(economy_rbds).intersection(set(eco)))
                            # economy_rbds = ["Y", "E", "R", "W", "M", "B", "U"]
                            fare_family = ru["field_key"].split(" ")[1]
                            for rbd in economy_rbds:
                                commission_detail["fare_families"].append(rbd + "-" + fare_family + "1")
                        elif "/" in commission_applied:
                            commission_detail["fare_families"] = list()
                            rbds = commission_applied.split("/")
                            eco=db.corporate.sfi.distinct(
                                "record_3.rbd", 
                                {"corporate_name": corporate_name, "file_date": file_date, "file_time": file_time})
                            rbds=list(set(rbds).intersection(set(eco)))
                            fare_family = ru["field_key"].split(" ")[1]
                            for rbd in rbds:
                                commission_detail["fare_families"].append(rbd + "-" + fare_family + "1")
                        else:
                            commission_detail["rbds"] = list()
                            fare_family = ru["field_key"].split(" ")[1]
                            commission_detail["fare_families"].append(rbd + "-" + fare_family + "1")

                        if "%" in commission_def:
                            commission_match = re.search(r"\d+", commission_def)
                            print("commission match", commission_match)
                            if commission_match is not None:
                                commission_per = commission_match.group()
                                commission_detail["commission_per"] = commission_per
                            print("commission percent :", commission_per)
                        else:
                            commission_match = re.search(r"\d+(\.)?", commission_def)
                            if commission_match is not None:
                                commission_val = commission_match.group()
                                commission_val_idx = commission_def.split(" ").index(commission_val)
                                commission_cur = commission_def.split(" ")[commission_val_idx - 1]
                                if commission_val[-1] == ".":
                                    commission_val = commission_val[:-1]
                                commission_detail["commission_val"] = commission_val
                                commission_detail["commission_cur"] = commission_cur
                                print("commission val :", commission_val)
                                print("commission cur :", commission_cur)
                        commission_details.append(commission_detail)            
                    break
                if "commissions:" in ru["field_value"].lower():
                    commission_exists = True
        except Exception as error:
            print(error)
    # analyse exclusion field
    Skip_exclude = False
    exclude_destination = []
    fare_family = []
    if exclude_field_value == "":
        Skip_exclude = True
        print("Nothing is there for exclude")
    else:
        if "HAJ" in exclude_field_value:
            # We can hard code fare family for now since we don't have any other condition
            fare_family = ["-HAS", "-HAP"]
            arr = exclude_field_value.replace("Exclude","").split(" ")
            arr = arr[0]
            arr = arr.split("/")
            exclude_destination = arr
            # print arr

    tariff_list = []
    # Get Tariff for excluded loc
    if Skip_exclude == True:
        pass
    else:
        # for each in exclude_destination:
        #     zone_doc = ATPCO_db.JUP_DB_ATPCO_Zone_Master.find_one({"CITY_CODE" : each })

        #     if zone_doc is not None:
        #         CITY_AREA = zone_doc['CITY_AREA']
        #     else:
        #         CITY_AREA = "9999"

        #     # tariff_list = db.private.tariff.master.distinct('tarrif_name', {"destination_area" : CITY_AREA, })
        tariff_list = tariff

    print("tariff_list")
    print(tariff_list)
    print("tariff")
    print(tariff)

    tariff_cur = list(db.private.tariff.master.find({"tarrif_name": { "$in": tariff_list + tariff}}, {"tarrif_name": 1, "tarrif_code": 1, "_id": 0}))
    tariff_code_dict = dict()
    print("tariff cur : ", tariff_cur)
    for tariff_rec in tariff_cur:
        tariff_code_dict[tariff_rec["tarrif_name"]] = tariff_rec.get("tarrif_code", "")
    
    # forming json doc
    if commission_exists:
        for each_tt in tariff:
            for r in rule:
                for commission_detail in commission_details:
                    for ff in commission_detail["fare_families"]:
                        record_2_json = dict()
                        record_2_json["Tariff"] = each_tt
                        record_2_json["Tariff_code"] = tariff_code_dict.get(each_tt, "")
                        record_2_json["Rule_No"] = r
                        record_2_json['Appl'] = ""
                        record_2_json['Loc_1'] = ""
                        record_2_json['Loc_2'] = ""
                        record_2_json['Fare_Family'] = ff
                        record_2_json['OW_RT'] = ""
                        if "commission_per" in commission_detail:
                            record_2_json["commission_per"] = commission_detail["commission_per"]
                        elif "commission_val" in commission_detail:
                            record_2_json["commission_val"] = commission_detail["commission_val"]
                            record_2_json["commission_cur"] = commission_detail["commission_cur"]
                        Json_list.append(record_2_json)
    elif Skip_exclude == True:
        for each_tt in tariff:
            for r in rule:
                record_2_json = dict()
                record_2_json['Tariff'] = each_tt
                record_2_json["Tariff_code"] = tariff_code_dict.get(each_tt, "")
                record_2_json['Rule_No'] = r
                record_2_json['Appl'] = ""
                record_2_json['Loc_1'] = ""
                record_2_json['Loc_2'] = ""
                record_2_json['Fare_Family'] = ""
                record_2_json['OW_RT'] = ""
                Json_list.append(record_2_json)
            # print record_2_json
    else:
        # Exclude part
        for each_tt in tariff_list:
            for each_location in exclude_destination:
                for each_fare in fare_family:
                    for r in rule:
                        record_2_json = dict()
                        record_2_json['Tariff'] = each_tt
                        record_2_json["Tariff_code"] = tariff_code_dict.get(each_tt, "")
                        record_2_json['Rule_No'] = r
                        record_2_json['Appl'] = "NA"
                        record_2_json['Loc_1'] = ""
                        record_2_json['Loc_2'] = each_location
                        record_2_json['Fare_Family'] = each_fare
                        record_2_json['OW_RT'] = ""
                        Json_list.append(record_2_json)
                        print(record_2_json)
        # actual tariff
        print("tariff for loop")
        print(tariff)
        for each_tt in tariff:
            for r in rule:
                record_2_json = dict()
                record_2_json['Tariff'] = each_tt
                record_2_json["Tariff_code"] = tariff_code_dict.get(each_tt, "")
                record_2_json['Rule_No'] = r
                record_2_json['Appl'] = ""
                record_2_json['Loc_1'] = ""
                record_2_json['Loc_2'] = ""
                record_2_json['Fare_Family'] = ""
                record_2_json['OW_RT'] = ""
                Json_list.append(record_2_json)
    print(Skip_exclude)
    print("record_2 json")
    pprint.pprint(Json_list)

    return Json_list 


def trans_iata(raw_db, atpco_db, pdss_db, corporate_name, raw_record_id, file_date, file_time, system_date, system_time):
    record_2_json = get_record_2(corporate_name, raw_db, atpco_db, raw_record_id, file_date, file_time)
    record_2_json=pd.DataFrame(record_2_json)
    def rec(b,a):
        if b == a:
            return 0
        else:
            return 1
    def rec1(b,a):
        if b != a:
            return 0
        else:
            return 1
      
    record_2_json["A"]=record_2_json.apply(lambda x:rec("NA",x['Appl']),axis=1)
    record_2_json["B"]=record_2_json.apply(lambda x:rec1('',x['Fare_Family']),axis=1)
    record_2_json.sort_values(by=['Tariff_code','A','B'], inplace=True)
    record_2_json = record_2_json.drop(["A","B"],1)
    record_2_json = record_2_json.fillna("")
    print(type(record_2_json))
    record_2_json = record_2_json.to_dict("records")
    print(record_2_json)
    # record_2_json= sorted(record_2_json, key = lambda i: i["Tariff_code"],reverse=False)
    # print(record_2_json)
    
    # record_2_json.sort_values(by=[], inplace=True)
    get_record_3_Cat_1_json = get_record_3_Cat_1(corporate_name, raw_db, atpco_db, raw_record_id)
    get_record_3_Table_983(corporate_name, raw_db, atpco_db, pdss_db, record_2_json, get_record_3_Cat_1_json, \
                             raw_record_id, file_date, file_time)



if __name__ == "__main__":
    raw_id = ObjectId("5fbb5cd317f675c2150b268e")
    corporate_name = "Utair"
    file_date = "2020-11-23"
    file_time = "12:25:00"

    trans_iata(get_raw_db(), get_ATPCO_EK_db(), get_pdss_db(), corporate_name, raw_id, file_date, file_time, "", "")
    # record_2_json = get_record_2(corporate_name, get_raw_db(), get_ATPCO_EK_db(),raw_id)
    # # print("record_2_json")
    # # print(record_2_json)

    # get_record_3_Cat_1_json = get_record_3_Cat_1(corporate_name, get_raw_db(), get_ATPCO_EK_db(), raw_id)
    # # print (get_record_3_Cat_1_json)
    # get_record_3_Table_983(corporate_name, get_raw_db(), get_ATPCO_EK_db(), record_2_json, get_record_3_Cat_1_json, raw_id)


