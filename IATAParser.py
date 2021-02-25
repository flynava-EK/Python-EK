import pymongo
import json
import xlrd
import pandas as pd
import glob
import datetime
import pprint
import copy
import re

SYSTEM_DATE = "2020-08-05"
SYSTEM_DATE = datetime.date.today().strftime("%Y-%m-%d")

def get_raw_db():
    client = pymongo.MongoClient('3.6.201.161:27022')
    client.the_database.authenticate("data.EK", "data@123", source="admin")
    # client = pymongo.MongoClient('localhost:27017')
    db = client["rawDB_prod"]
    return db

def deep_index(lst, w):
    return [key for key, value in lst.items() if w in value]

# def get_IATA_details(current_file, sheet_file, path, Today, Time):
def get_IATA_details(sheet_file, path, Today, Time, corporate_name, fdate, ftime, db):
    json_dict = dict()
    data_dump = list()
    
    wb = xlrd.open_workbook(path)
    wb_sheet = wb.sheet_by_name(sheet_file)

    # Assigning of Keywords to be check, give smaller case in the value field so
    # we can lower the scrape data and do comparision
    # column_names = dict()
    # column_names["IATA Code"] = ["iata code", "iata", "iatacode", "iata number", "iata code7-digit red = fix"]
    # column_names["Cat 25 Ready"] = ["cat 25 ready"]
    # column_names["Agent Name"] = ["agent name", "ticketing agent name", "agency", "agency name"]
    # column_names["POS City"] = ["pos city"]
    # column_names["Country"] = ["country", "POS Country "]
    # column_names["Location"] = ["location"]
    # # column_names["GDS/CRS"] = ["gds/crs", "crs / gds"]
    # column_names["Galileo"] = ["galileo"]
    # column_names["Sabre"] = ["sabre"]
    # column_names["Amadeus"] = ["amadeus"]
    # column_names["Abacus"] = ["abacus"]
    # column_names["Worldspan"] = ["worldspan", "world span"]
    # column_names["FLX"] = ["flx"]
    # column_names["Apollo"] = ["apollo"]
    # column_names["Farelogix"] = ["farelogix"]
    # column_names["Travelsky"] = ["travelsky"]
    # column_names["Filing Date"] = ["filing date"]
    # column_names["GDS"] = ["gds", "gds/crs", "crs / gds"]
    # column_names["PCC"] = ["pcc", "pseudo city code"]
    # column_names["MARS"] = ["mars", "mars(cugo)", "mars (cugo)"]
    # column_names["Corporate Code"] = ["corporate code"]
    # column_names["DATE ADDED"] = ["date added"]
    # column_names["DATE REMOVED"] = ["date removed"]
    # column_names["Filing Date"] = ["Filing_Date"]
    # column_names["Account Code"] = [" account code (if available)", "account code",
    #                                 "agent gds account code (if available)", "account code (if available)"]
    # column_names["Remarks"] = ["remarks", "remarks addition /deletion", "remarksaddition /deletion", "comments"]
    # column_names["Dedicated IATA"] = ["dedicated iata (yes/no)", "dedicated iata"]
    # print(column_names)
    column_names = db.corporate.patterns.find_one({"flag": "keyword_pattern"}, {"iata_sheet": 1})["iata_sheet"]
    # print(column_names)

    map_ = dict()
    col_no = list()
    assigned_header = False
    Okay_to_break = False
    found_header = 0
    empty_row_count = 0

    for row_idx in range(0, wb_sheet.nrows):
        print("row ", row_idx)
        Check_each_row = ""
        row_data_dict = dict()

        for col_idx in range(0, wb_sheet.ncols):
            try:

                # # assign header
                print(str(wb_sheet.cell(row_idx, col_idx).value).lower().strip().replace("\n",""))
                if assigned_header == False:
                    if deep_index(column_names,
                                  str(wb_sheet.cell(row_idx, col_idx).value).lower().strip().replace("\n",
                                                                                                     "")) != []:
                        column = deep_index(column_names,
                                            str(wb_sheet.cell(row_idx, col_idx).value).lower().strip().replace("\n",
                                                                                                               ""))[
                            0]
                        map_[column] = col_idx
                        found_header = found_header + 1
                        print("header ", column, " count : ", found_header)
                        col_no.append(col_idx)

                if assigned_header == True:
                    try:
                        
                        Check_each_row = Check_each_row + str(wb_sheet.cell(row_idx, col_idx).value).strip()
                        row_data_dict[list(map_.keys())[list(map_.values()).index(col_idx)]] = str(
                            wb_sheet.cell(row_idx, col_idx).value).strip()
                        if list(map_.keys())[list(map_.values()).index(col_idx)] == "Account Code" and \
                                isinstance(wb_sheet.cell(row_idx, col_idx).value, float):
                            if "GDS" in row_data_dict and "amadeus" in row_data_dict["GDS"].lower():
                                row_data_dict["Account Code"] = "{:06d}".format(int(wb_sheet.cell(row_idx, col_idx).value))
                            else:
                                row_data_dict["Account Code"] = str(int(wb_sheet.cell(row_idx, col_idx).value))

                        if list(map_.keys())[list(map_.values()).index(col_idx)] == "IATA Code" and \
                                isinstance(wb_sheet.cell(row_idx, col_idx).value, float):
                            row_data_dict["IATA Code"] = str(int(wb_sheet.cell(row_idx, col_idx).value))

                        if list(map_.keys())[list(map_.values()).index(col_idx)] == "PCC" and \
                                isinstance(wb_sheet.cell(row_idx, col_idx).value, float):
                            row_data_dict["PCC"] = str(int(wb_sheet.cell(row_idx, col_idx).value))    

                    except UnicodeEncodeError as error:
                        print(error)
                        print("UnicodeEncodeError error")
                        row_data_dict[list(map_.keys())[list(map_.values()).index(col_idx)]] = str(
                            (wb_sheet.cell(row_idx, col_idx).value).encode('ascii', 'ignore')).strip()

                    except ValueError as error:
                        print(error)

                    if max(col_no) == col_idx:
                        print('checking for break condition')
                        print(Check_each_row)
                        if Check_each_row == "":
                            if empty_row_count > 2:
                                Okay_to_break = True
                            else:
                                empty_row_count += 1
                        else:
                            print(row_data_dict)
                            data_dump.append(row_data_dict)

            except UnicodeEncodeError as error:
                # # Not able to read strike words so exception handling is required
                print(error)
            except IndexError as error:
                print(error)

        if found_header > 3 and ("IATA Code" in map_.keys() or "Agent Name" in map_.keys()):
            assigned_header = True
            print("header_assigned----------------------------------")

        # # Program should break once there is no required data
        if Okay_to_break == True:
            break
    
    pprint.pprint(data_dump)
    json_dict['vendor'] = corporate_name
    json_dict['system_date'] = Today
    json_dict['data'] = data_dump

    return json_dict["data"]

       
def get_record2_cat35(vendor, db):
    
    data_dict = dict()
    
    data_dict['Carrier Code'] = "EK"
    data_dict['Tariff Code'] = ""
    data_dict['Rule Number'] = ""
    data_dict['OW / RT'] = ""
    data_dict['Fare Type'] = ""
    data_dict['Fare Class'] = ""
    data_dict['LOC 1'] = ""
    data_dict['LOC 2'] = ""
    data_dict['Footnote'] = ""
    data_dict['DOW'] = ""
    data_dict['Effective Date'] = ""
    data_dict['Discontinue Date'] = ""
    data_dict['Not Applicable'] = ""
    data_dict['Routing Number'] = ""
       
    return data_dict

def get_record3_cat35(vendor, db, file_date, file_time):
    data_dict = dict()
    cur = db.corporate.coversheet.find_one({"corporate_name" : vendor, "file_date": file_date, "file_time": file_time})
    print(cur)
    data_dict['PSGR Type'] = "ADT"
    # try:
    #     print("checking for indicator in gross_net")
    #     data_dict['Net/Gross Indicator'] = cur['cover_sheet']['details_of_ticketing_instructions']['gross_nett'] #, // from excel
    #     if data_dict['Net/Gross Indicator'] is None:
    #         raise Exception("exception")
    # except Exception as error:
    #     try:
    #         print("checking for indicator in fare_amount_box")
    #         fare_amount_box = cur['cover_sheet']['details_of_ticketing_instructions']['fare_amount_box'] #, // from excel
    #         if isinstance(fare_amount_box, str):
    #             fare_amount_box = fare_amount_box.lower()
    #             if "net" in fare_amount_box:
    #                 data_dict['Net/Gross Indicator'] = "Net"
    #             elif "nett" in fare_amount_box:
    #                 data_dict['Net/Gross Indicator'] = "Net"
    #             elif "gross" in fare_amount_box:
    #                 data_dict['Net/Gross Indicator'] = "Gross"
    #             else:
    #                 raise Exception("exception")
    #         else:
    #             raise Exception("exception")
    #     except Exception as error:
    #         print("checking for indicator in fare_sheet")
    #         fare_cur = db.corporate.faresheet.find_one(
    #             {"corporate_name": vendor, "file_date": file_date, "file_time": file_time},
    #             {"sheets.sheet_desc": 1, "_id": 0}
    #             )
    #         indicator_net = False
    #         indicator_gross = False

    #         if fare_cur is not None:
    #             print("fare_cur")
    #             print(fare_cur)
    #             for desc in fare_cur["sheets"]:
    #                 if "sheet_desc" in desc:
    #                     sheet_desc = desc["sheet_desc"].lower()
    #                     if "net" in sheet_desc:
    #                         indicator_net = True
    #                     if "nett" in sheet_desc:
    #                         indicator_net = True
    #                     if "gross" in sheet_desc:
    #                         indicator_gross = True

    #         if indicator_net and indicator_gross:
    #             data_dict['Net/Gross Indicator'] = "Net, Gross"
    #         elif indicator_net:
    #             data_dict['Net/Gross Indicator'] = "Net"
    #         elif indicator_gross:
    #             data_dict['Net/Gross Indicator'] = "Gross"
    #         else:
    #             print("checking for fare indicator in instructions and rule sheet")
    #             fare_indicator_pattern = ["fare(s)?\s*is\s*net(t)*", "resultant(s)?\s*fares\s*are\s*net(t)?", "resultant(s)?\s*fares\s*are\s*gross"]
    #             instrtn_cur = db.corporate.rulesheet.find_one(
    #                 {"corporate_name": vendor, "file_date": file_date, "file_time": file_time},
    #                 {"_id": 0, "corporate_name": 0, "system_date": 0, "system_time": 0, "file_date": 0, "file_time": 0}
    #             )
    #             indicator_set = False
    #             if instrtn_cur is not None:
    #                 for instrtn_sheet in instrtn_cur:
    #                     # for instrtn_header in instrtn_cur[instrtn_sheet]:
    #                     for instrtn in instrtn_cur[instrtn_sheet]:
    #                         for pattern in fare_indicator_pattern:
    #                             if (re.search(pattern, instrtn["field_value"], flags=re.IGNORECASE) is not None):
    #                                 indicator_set = True
    #                                 if "net" in pattern:
    #                                     data_dict['Net/Gross Indicator'] = "Net"
    #                                     break
    #                                 elif "gross" in pattern:
    #                                     data_dict['Net/Gross Indicator'] = "Gross"
    #                                     break
    #                         if indicator_set:
    #                             break
    #                     if indicator_set:
    #                         break
    #                     # if indicator_set:
    #                     #     break
                
    #             if not indicator_set:
    #                 data_dict['Net/Gross Indicator'] = None

    # checking for commission percent
    fs_cur = db.corporate.faresheet.find_one({"corporate_name" : vendor, "file_date": file_date, "file_time": file_time})
    
    comission_str = ""
    comission_str_query = ""

    if cur is not None:
        try:
            # print cur['cover_sheet']
            try:
                print("check for market commission")
                market_commission = cur["cover_sheet"]["details_of_ticketing_instructions"]["market_comission"]
            except Exception as error:
                market_commission = ""

            try:
                print("checking for commission in fare sheet data")
                fares_data = fs_cur['sheets'][0]['sheet_desc'].replace('\n', ' ')
            except Exception as error:
                fares_data = ""

            print(fares_data)
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
        except Exception as error:
            print(error)
            comission_str = ""

    try:
        commission_int = int(comission_str)
        if commission_int == 0:
            data_dict["Net/Gross Indicator"] = "Net"
        else:
            data_dict["Net/Gross Indicator"] = "Gross"
    except:
        data_dict["Net/Gross Indicator"] = "Net"

    print("fare indicator : " + str(data_dict['Net/Gross Indicator']))
    data_dict['Percent'] = comission_str
    data_dict['Amount'] = ""
    data_dict['Currency'] = ""
    data_dict['No Charge'] = ""
    data_dict['Indicator'] = ""
    data_dict['Carrier 1'] = "EK"
    data_dict['PSGR/Auditor Coupon'] = ""
    data_dict['Type'] = ""
    data_dict['Tour CAR Code'] = cur['cover_sheet']['details_of_ticketing_instructions']['tour_code'] # // from excel
    data_dict['Ticket Designator'] = cur['cover_sheet']['details_of_ticketing_instructions']['ticket_designator'] #, // from excel
    data_dict['Fare Data'] = ""
    data_dict['OW/RT'] = ""
    data_dict['Global Indicator'] = ""
    data_dict['Rule Tariff'] = ""
    data_dict['Fare Box text'] = ""
    data_dict['Fare Box Amount'] = ""
    data_dict['Carrier 2'] = ""
    data_dict['Rule No'] = ""
    data_dict['PSGR/Auditor Coupon 1'] =""
    data_dict['Fare Class'] = ""
    data_dict['Fare Type'] = ""
    data_dict['Security Table'] = ""
    data_dict['Fare Creator Table'] = ""
    data_dict['methodType'] = ""
    data_dict["Commission_Percent"] = comission_str
    return data_dict

def repeat_iata_data_for_gds_header(iata_data, gds_headers):

    print("gds header present")
    print(gds_headers)
    repeated_iata_data = list()
    for rec in iata_data:
        gds_removed = copy.copy(rec)
        for header in gds_headers:
            del gds_removed[header]

        # print("gds removed ")
        # print(gds_removed)
        for key in rec:
            if key in gds_headers:
                new_rec = dict(gds_removed)
                new_rec.update({"GDS": key})

                print(rec[key])
                if "/" in rec[key]:
                    pcc_list = rec[key].split("/")
                    for pcc in pcc_list:
                        new_pcc_rec = dict(new_rec)
                        new_pcc_rec.update({"PCC": pcc})
                        repeated_iata_data.append(new_pcc_rec)
                elif "\n" in rec[key]:
                    pcc_list = rec[key].split("\n")
                    for pcc in pcc_list:
                        new_pcc_rec = dict(new_rec)
                        new_pcc_rec.update({"PCC": pcc})
                        repeated_iata_data.append(new_pcc_rec)
                elif "," in rec[key]:
                    pcc_list = rec[key].split(",")
                    for pcc in pcc_list:
                        new_pcc_rec = dict(new_rec)
                        new_pcc_rec.update({"PCC": pcc})
                        repeated_iata_data.append(new_pcc_rec)
                else:
                    new_rec.update({"PCC": rec[key]})
                    # print("new record")
                    # print(new_rec)
                    repeated_iata_data.append(new_rec)
                
    return repeated_iata_data

def repeat_iata_data_for_gds(iata_data):
    repeated_iata_data = list()

    for rec in iata_data:

        # print("gds removed ")
        # print(gds_removed)
        if "/" in rec["PCC"]:
            print("splitting by /")
            print(rec)
            pcc_list = rec["PCC"].split("/")
            for pcc in pcc_list:
                new_pcc_rec = copy.deepcopy(rec)
                new_pcc_rec["PCC"] = pcc.strip()
                repeated_iata_data.append(new_pcc_rec)
        elif "\n" in rec["PCC"]:
            print("splitting by \n")
            print(rec)
            pcc_list = rec["PCC"].split("\n")
            for pcc in pcc_list:
                new_pcc_rec = copy.deepcopy(rec)
                new_pcc_rec["PCC"] = pcc.strip()
                repeated_iata_data.append(new_pcc_rec)
        else:
            print(rec["PCC"])
            repeated_iata_data.append(rec)
                
    return repeated_iata_data

def repeat_iata_data_for_account_code(iata_data, db, corp_name, fdate, ftime):

    repeated_iata_data = list()
    for rec in iata_data:
        if "GDS" in rec and "mars" in rec["GDS"].lower():
            print("setting ticket desig as account code for mars")
            cur = db.corporate.coversheet.find_one({"corporate_name": corp_name, "file_date": fdate, "file_time": ftime})
            td = ""
            if cur is not None:
                td = cur["cover_sheet"]["details_of_ticketing_instructions"]["ticket_designator"]
            rec["Account Code"] = td
            print(rec["GDS"], rec["Account Code"])

        if "Account Code" not in rec:
            try:
                if "PCC" in rec and rec["PCC"] != "" and "Remarks" in rec:
                    if "account code" in rec["Remarks"].lower():
                        rec["Account Code"] = rec["Remarks"].split(":")[1].strip()
                    else:
                        rec["Account Code"] = ""
                else:
                    rec["Account Code"] = ""
            except Exception as error:
                rec["Account Code"] = ""

        # case -1
        if '/' in rec['Account Code']:
            arr_acc = str(rec['Account Code']).split('/')
            for each in arr_acc:
                doc = dict()
                doc = copy.deepcopy(rec)
                doc['Account Code'] = str(each).strip()
                if len(doc["Account Code"]) == 6 and "amadeus" in doc["GDS"].lower():
                    doc["Account Code"] = str("C0000000000000000000")[:-6] + doc["Account Code"]
                repeated_iata_data.append(doc)
        elif 'R,U' in rec['Account Code']:
            # print each_list['Account Code']
            if ' and ' in rec['Account Code']:
                li = str(rec['Account Code']).split(' and ')
                for each in li:
                    doc = dict()
                    doc = copy.deepcopy(rec)
                    # doc['Account Code'] = each
                    doc['RU_Replace'] = True
                    # doc['RU_Replace_TXT'] = each.replace('R,U', '')
                    lene = len(each.replace('R,U', ''))
                    doc['Account Code'] = str("C0000000000000000000")[:-lene] + each.replace('R,U', '').strip()
                    repeated_iata_data.append(doc)

                    # print doc['RU_Replace']
                    # print doc['RU_Replace_TXT']
            else:
                doc = dict()
                doc = copy.deepcopy(rec)
                # doc['Account Code'] = each_list['Account Code']
                doc['RU_Replace'] = True
                lene = len(rec["Account Code"].replace("R,U", ""))
                doc['Account Code'] = str("C0000000000000000000") [:-lene] + rec["Account Code"].replace("R,U", "").strip()
                # doc['Account Code'] = str(rec['Account Code']).replace('R,U', '').strip()
                repeated_iata_data.append(doc)
                # print doc['Account Code']
                # print doc['RU_Replace']
                # print doc['RU_Replace_TXT']
        else:
            if len(rec["Account Code"]) == 6 and "amadeus" in rec["GDS"].lower():
                rec["Account Code"] = str("C0000000000000000000")[:-6] + rec["Account Code"]
            repeated_iata_data.append(rec)

    return repeated_iata_data  

def get_security_tbl(vendor,IATA_data,  db, fdate, ftime):
    list_dict = list()
    account_dict = dict()
    account_list = list()
    excluded_list = list()

    print("iata_data")
    print(IATA_data)

    gds_list = ["amadeus", "galileo", "sabre", "worldspan", "apollo", "axcess", "abacus", \
                "travelsky", "topas", "infini", "ita", "electronic data system", "expedia" \
                "switchworks", "vayant", "flx", "mars", "farelogix"
               ]

    if IATA_data is not None and len(IATA_data) > 0:

        print("number of records in initial iata block : ", str(len(IATA_data)))
        iata_headers = IATA_data[0].keys()
        print("header in sheet")
        print(iata_headers)
        iata_headers = [(header.lower().strip(), header) for header in iata_headers]
        gds_headers = [header[1] for header in iata_headers if header[0] in gds_list]
        gds_header_flag = False
        if len(gds_headers) > 0:
            gds_header_flag = True
            print("multiple gds given at same row, repeating iata records for each gds")
            iata_data_gdsh_repeated = repeat_iata_data_for_gds_header(copy.deepcopy(IATA_data), gds_headers)
            IATA_data = iata_data_gdsh_repeated
        print("number of records after gds header breakdown : ", str(len(IATA_data)))

        iata_data_gds_repeated = repeat_iata_data_for_gds(copy.deepcopy(IATA_data))
        if len(iata_data_gds_repeated) > 0:
            IATA_data = iata_data_gds_repeated
        print("number of records after gds value breakdown : ", str(len(IATA_data)))

        iata_data_acc_repeated = repeat_iata_data_for_account_code(copy.deepcopy(IATA_data), db, vendor, fdate, ftime)
        if len(iata_data_acc_repeated) > 0:
            IATA_data = iata_data_acc_repeated
        print("number of records after account code breakdown : ", str(len(IATA_data)))

        for each_cur in IATA_data:
            trans_dict = dict()
            print(each_cur)
            if "PCC" in each_cur and "(" in each_cur["PCC"]:
                each_cur["PCC"] = each_cur["PCC"][:each_cur["PCC"].index("(")].strip()
            if "PCC" in each_cur:
                trans_dict["PCC"] = each_cur["PCC"]
            else:
                trans_dict["PCC"] = ""

            if "GDS" in each_cur:
                gds_splitter = None
                if "-" in each_cur["GDS"]:
                    gds_splitter = "-"
                elif "–" in each_cur["GDS"]:
                    gds_splitter = "–"
                
                if gds_splitter != None:
                    gds_split = each_cur["GDS"].split(gds_splitter)
                    if len(gds_split[0].strip()) > 2:
                        each_cur["GDS"] = gds_split[0].strip()
                    else:
                        each_cur["GDS"] = gds_split[1].strip()

                print(each_cur)
                trans_dict["GDS"] = each_cur["GDS"]
                if "amadeus" in trans_dict["GDS"].lower():
                    if " " in trans_dict["PCC"]:
                        li = trans_dict["PCC"].split(" ")
                        try:
                            first_block = li[0]
                            second_block = li[1]
                            pcc_string_builder = second_block[2] + second_block[0] + second_block[1] \
                                             + second_block[3] + second_block[4] + second_block[5]
                            trans_dict["LOC1"] = first_block.replace("*", "")
                            trans_dict["transformed_PCC"] = pcc_string_builder.replace("*", "")
                        except IndexError:
                            trans_dict["LOC1"] = ""
                            trans_dict["transformed_PCC"] = ""
                    else:
                        try:
                            first_block = trans_dict["PCC"][0:3]
                            second_block = trans_dict["PCC"][3:]
                            pcc_string_builder = second_block[2] + second_block[0] + second_block[1] \
                                             + second_block[3] + second_block[4] + second_block[5]
                            trans_dict["LOC1"] = first_block.replace("*", "")
                            trans_dict["transformed_PCC"] = pcc_string_builder.replace("*", "")
                        except IndexError as error:
                            trans_dict["LOC1"] = ""
                            trans_dict["transformed_PCC"] = ""
                    
                    if len(trans_dict["transformed_PCC"]) > 2:
                        if (trans_dict["transformed_PCC"][0] == "0" or trans_dict["transformed_PCC"][0] == "1") and trans_dict["transformed_PCC"][1].isalpha():
                            trans_dict["Locales Type"] = "V"
        

                elif "travelsky" in trans_dict["GDS"].lower():
                    trans_dict["LOC1"] = ""
                    if gds_header_flag:
                        trans_dict["transformed_PCC"] = trans_dict["PCC"]
                    else:
                        trans_dict["transformed_PCC"] = each_cur.get("IATA Code", "")
                    trans_dict["Locales Type"] = "I"
                else:
                    ## Need to get logic for other then amadeus of GDS/CSR
                    trans_dict["LOC1"] = ""
                    trans_dict["transformed_PCC"] = trans_dict["PCC"]

            else:
                trans_dict["GDS"] = ""
                trans_dict["LOC1"] = ""
                trans_dict["transformed_PCC"] = ""
            
            if isinstance(trans_dict["transformed_PCC"], str):
                trans_dict["transformed_PCC"] = trans_dict["transformed_PCC"].replace(" ", "")

            if "ACCOUNT CODE" not in each_cur:
                each_cur['ACCOUNT CODE'] = ""

            if "Account Code" in each_cur:
                ac_splitter = None
                if "-" in each_cur["Account Code"]:
                    ac_splitter = "-"

                if ac_splitter != None:
                    ac_split = each_cur["Account Code"].split(ac_splitter)
                    if ac_split[0].lower().strip() in gds_list:
                        each_cur["Account Code"] = ac_split[1].strip()
                    elif ac_split[1].lower().strip() in gds_list:
                        each_cur["Account Code"] = ac_split[0].strip()

            if "Account Code" not in each_cur:
                try:
                    if "Remarks" in each_cur:
                        if "account code" in each_cur["Remarks"].lower():
                            each_cur["Account Code"] = each_cur["Remarks"].split(":")[1].strip()
                        else:
                            each_cur["Account Code"] = ""
                    else:
                        each_cur["Account Code"] = ""
                except Exception as error:
                    each_cur["Account Code"] = ""

            data_dict = dict()
            data_dict['Application Tag'] = ""
            data_dict['Travel Agency'] = ""
            data_dict['CXR/CRS Code'] = trans_dict['GDS']  # ,//from excel - GDS Code
            data_dict['Duty/Function Code'] = ""
            data_dict['LOC 1'] = trans_dict.get('LOC1', None)  # , //from excel - for Amadeus
            data_dict['LOC 2'] = ""
            data_dict['Locales Type'] = trans_dict.get("Locales Type", "T")  # , //default
            data_dict['Locales Code'] = trans_dict['transformed_PCC'] if trans_dict['transformed_PCC'] != "" else trans_dict['PCC'] # , // from excel - PCC
            data_dict['Account Code'] = each_cur['Account Code']  # , //from excel
            data_dict['Filing Type'] = "L"  # , //unless specified
            data_dict['Changes Only'] = ""
            data_dict['Secondary Seller Control'] = ""

            if data_dict["Locales Code"] == "":
                excluded_list.append(data_dict)
            else:
                list_dict.append(data_dict)
            account_dict['Account Code'] = each_cur['Account Code']  # , //from excel
            account_list.append(account_dict)
    # print("account_list len : " + str(len(account_list)))
    print("security table : ")
    pprint.pprint(list_dict)
    return account_list, list_dict, excluded_list


def parse_iata(path, corporate_name, db, SYSTEM_DATE, SYSTEM_TIME, FILE_DATE, FILE_TIME, iata_sheet_name):
    print("parsing iata")
    Today = SYSTEM_DATE
    # hard coded time for now we can update this when we get excel name format
    Time = SYSTEM_TIME
    ftime = FILE_TIME
    fdate = FILE_DATE
    sheet_name = iata_sheet_name
    IATA_dict = dict()
    IATA_dict["IATA_data"] = get_IATA_details(sheet_name, path, Today, Time, corporate_name, fdate, ftime, db)
    IATA_dict["record2_cat35"] = get_record2_cat35(corporate_name.lower(), db)
    IATA_dict["record3_cat35"] = get_record3_cat35(corporate_name, db, FILE_DATE, FILE_TIME)
    account_list, security_tbl, excluded_list = get_security_tbl(corporate_name,IATA_dict["IATA_data"], db, FILE_DATE, FILE_TIME)
    IATA_dict["cat1"] = account_list
    IATA_dict["security_tbl"] = security_tbl
    IATA_dict["excluded_list"] = excluded_list
    insertion_details = db.corporate.iatasheet.insert_one({
            "corporate_name": corporate_name,
            "system_date": SYSTEM_DATE,
            "system_time": SYSTEM_TIME,
            "file_date": FILE_DATE,
            "file_time": FILE_TIME,
            "IATA_List": IATA_dict
        })
    print("inserted record id : " + str(insertion_details.inserted_id))
    return insertion_details.inserted_id


if __name__ == '__main__':

    # excel_path = 'D:/flynava/EK/cat_25/Cat_25_21 templates/Cat_25_21 templates/AE_Essilor_MEast_2020-21_ZZESL3ZZ_WP239928_New.xlsx'
    # excel_path = 'D:/flynava/EK/cat_25/Cat_25_21 templates/Cat_25_21 templates/Barclays_Global CORP_2019-2022-WP208394.xlsx'
    # excel_path = 'D:/flynava/EK/cat_25/Cat_25_21 templates/Cat_25_21 templates/Ericsson_Global Deal.xlsx'
    # excel_path = 'D:/flynava/EK/cat_25/Cat_25_21 templates/Cat_25_21 templates/GB_Ineos_NYZ3_LocCorp_WP237164.xlsx'
    # excel_path = 'D:/flynava/EK/cat_25/Cat_25_21 templates/Cat_25_21 templates/IN_Sandvik_Asia_Mar20-Feb21_NEW_WP240519.xlsx'
    # excel_path = 'D:/flynava/EK/cat_25/Cat_25_21 templates/Cat_25_21 templates/Occidental_2019_-_2021_WP239124.xlsx'
    # excel_path = 'D:/flynava/EK/cat_25/Cat_25_21 templates/Cat_25_21 templates/SA_CORP_SRACO_KSA_ZZSZZ3ZZ_WP238564_Renewal.xlsx'
    # excel_path = 'D:/flynava/EK/cat_25/Cat_25_21 templates/Cat_25_21 templates/SC_GLOBAL_SUPPLY_CENTRE_PTY_LTD_NEW_WP240952.xlsx'
    # excel_path = 'D:/flynava/EK/cat_25/Cat_25_21 templates/Cat_25_21 templates/US_Factset_Research_Systems__Inc._NEW_WP236744.xlsx'
    # excel_path = 'D:/flynava/EK/cat_25/Cat_25_21 templates/Cat_25_21 templates/DK__AtlasCopCo_Miniglobal_WP237689.xlsx'
    # excel_path = 'D:/flynava/EK/cat_25/Cat_25_21 templates/Cat_25_21 templates/ES_WP240238__RULES__AB_BIOTICS_WP240238.xlsx'
    # excel_path = 'D:/flynava/EK/cat_25/Cat_25_21 templates/Cat_25_21 templates/IT_Etro_Spa__Renewal_WP239809.xlsx'
    # excel_path = 'D:/flynava/EK/cat_25/Cat_25_21 templates/Cat_25_21 templates/MICE_World_Art_Dubai_Agent_WP237107.xlsx'
    
    # excel_path = 'D:/flynava/EK/cat_25/Cat_25_21 templates/Cat_25_21 templates/AU_Western_Australia_Government_-_QGW3_WP240393.xlsx'
    # excel_path = 'D:/flynava/EK/cat_25/Cat_25_21 templates/Cat_25_21 templates/HK_HSBC_Cugo_NEW_WP239965.xlsx'
    # excel_path = 'D:/flynava/EK/cat_25/Cat_25_21 templates/Cat_25_21 templates/CN_HCRS_Retail_Design_WP241121_New.xlsx'
    excel_path = 'D:/flynava/EK/cat_25/Cat_25_21 templates/Cat_25_21 templates/KR_Hyundai_Construction__NEW_WP240782.xlsx'
    # excel_path = 'D:/flynava/EK/cat_25/Cat_25_21 templates/Cat_25_21 templates/ZA_ZA20SC006-ExecuJet_Corporate_NEW_WP239471.xlsx'
    # excel_path = 'D:/flynava/EK/cat_25/Cat_25_21 templates/Cat_25_21 templates/Brückner_BKG3_DE CORP-WP228758.xlsx'

    db = get_raw_db()
    parse_iata(excel_path, "Hyundai E&C", db, "", "", "2020-11-13", "13:11:45", "Agents Details ")