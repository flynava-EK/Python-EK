import pymongo
from bson.objectid import ObjectId
import pandas as pd
import re
from itertools import permutations,combinations
import ast
import numpy as np
import time
import copy
pavan={}
def get_od_details(place,raw_db,atpco_db,pdss_db):
    od_list=[]
    global pavan
    # if any([re.search(i, place, flags=re.IGNORECASE) for i in['network','rest','world']]):
    if any([re.search(i, place, flags=re.IGNORECASE) for i in['network','rest','world']]): 
        od_list.extend([("1","1","","area"),("2","2","","area"),("3","3","","area"),("1","1","US","area")])
        # print(od_list)
    else:
        place=place.replace("*","")
        if "POS" in place or "POO" in place:
            # print("jjg")
            place=place.replace("POS","").replace("POO","")[0:4].strip()
        if re.search("s(.*)e(.*)asia",place,flags=re.IGNORECASE):
            x=place.split("/")
            x=["se_asia/japan/korea" for i in x if re.search("s(.*)e(.*)asia",place,flags=re.IGNORECASE) ]
            place="/".join(x)
        if "/" not in place:
            # print("@occidental iah,")
            if "(" in place:
                place=place.replace("\n","").split("(")[0]
            else:
                place=place.split("\n")[0]
        if re.search(":", place,flags=re.IGNORECASE):
            # print("lll")
            place=place.split(":")[1].replace("\n","") 
        if re.search(":", place,flags=re.IGNORECASE):
            # print("lll")
            place=place.split(":")[1].replace("\n","") 
        if "(" in place and "/" in place:
            # print("sutya")
            place=place.split("(")[0].strip()
            # pla=""
            # for pp in range(len(place)):
            #     place[pp]=place[pp].split("(")[0]
            #     pla=pla+place[pp]+"/"
            # place=pla[:-1]
        # print(place) 
        if re.search("america",place,flags=re.IGNORECASE):
            # print("jj")
            p=""
            if re.search("n",place,flags=re.IGNORECASE):
                p=p+"north america/"
            if re.search("s",place,flags=re.IGNORECASE):
                p=p+"south america/"
            if re.search("ce",place,flags=re.IGNORECASE):
                p=p+"central america/"
            place=p[:-1]
        # print(place)    
        place=re.split(r'[^\w]', place.strip().replace(" ","12"))
        for p in range(len(place)):
            place[p]=place[p].lower().replace("12"," ").strip()
            # print(place[p],"ll")
            try:
                x="^"+re.sub('[^A-Za-z0-9]+', "!s*(.*)", place[p].replace(" ","_")).replace("!","\\").lower()+"$"
                x={"$regex":x,"$options":"i"}
                q2 =pdss_db.location.master.find_one({"loc_code": x})
                loc = q2['loc_code']
                loc_type = q2['loc_typ']
            except TypeError as e:
                # print("kk")
                q2=pdss_db.location.master.find_one({"location": x})
                loc = q2['loc_code']
                loc_type = q2['loc_typ']
            # print(loc_type,loc)
            if loc_type=="city":
                q3 = atpco_db.JUP_DB_ATPCO_Zone_Master.find_one({"CITY_CODE": loc})
                loc_code=q3['CITY_AREA']
                loc_country=q3['CITY_CNTRY']
            elif loc_type=="country":
                loc_code=atpco_db.JUP_DB_ATPCO_Zone_Master.find_one({"CITY_CNTRY": loc})['CITY_AREA']
                loc_country=loc
            elif loc_type=="airport":
                loc_cod=pdss_db.JUP_DB_City_Airport_Mapping.find_one({"Airport_Code":loc})
                loc=loc_cod['City_Code']
                loc_country=loc_cod['Country_Code']
                loc_code=atpco_db.JUP_DB_ATPCO_Zone_Master.find_one({"CITY_CNTRY": loc_country})['CITY_AREA']
            elif loc_type=="region":
                try:
                    loc_country=""
                    loc_code=atpco_db.JUP_DB_ATPCO_Zone_Master.find_one({"CITY_ZONE": loc})['CITY_AREA']
                except TypeError as e:
                    loc="US"
                    loc_country="US"
                    loc_code="0"
                try:
                    print("hiiii")
                    if loc=="230":
                        list0=["231","232","233","230"]
                    elif loc=="210":
                        list0=["210","211","212"]
                    elif loc=="170":
                        list0=["171","172"]
                    else:
                        list0=[loc]
                    # print(list0,loc)
                    result=list(atpco_db.JUP_DB_ATPCO_Zone_Master.aggregate([{"$match":{"CITY_ZONE":{"$in":list0}}},
                                                {"$group":{"_id":None,
                                                "city_cntry":{"$addToSet":"$CITY_CNTRY"}}}]))[0]['city_cntry']
                    pavan[loc]=result
                    # print(pavan)
                except Exception as e:
                    print(e)
                    continue
            elif loc_type=="area":
                loc_country=""
                loc_code=loc
                loc=str(int(loc))
            od_list.append((loc,loc_code,loc_country,loc_type))
    return od_list

def trip(tt):
    if "OW" in tt and "RT" in tt:
        trip_type=""
    elif "OW" in tt:
        trip_type="1"
    elif "RT" in tt:
        trip_type="2"
    else: 
        trip_type=""
    return trip_type

def list_of_string(a):
    try:
        print(a)
        a = ast.literal_eval(a)
        # print(a)
        a = [n.strip() for n in a]
        # print(a)
    except ValueError as e:
        a=[]
    return a

def america(x):
        flag='f'
        if re.search("america",x,flags=re.IGNORECASE):
            flag='e'
        if x=="US":
            flag='e'
        return flag

def combine_column_list(ff_list,appl="",ow_rt="",fare_type="",season_type=""):
    c_list=[]
    ff_list=list_of_string(ff_list)
    print(ff_list)
    for x in range(len(ff_list)):
        print(ff_list,appl,ow_rt,season_type,fare_type)
        c_list.append(str(appl+ow_rt+fare_type+season_type+ff_list[x]))
    # print(c_list,"jj")
    return c_list

def tolist(b):
    print(b,type(b))
    if not isinstance(b,float):
        print([b])
        return [b]
    else:
        print([""])
        return [""]

def set_of_things(a,b):
    a=set(list(a+b))
    return tuple(sorted(a))

def citycode(a,b):
    level=["city","country","area"]
    return level.index(a.lower())+level.index(b.lower())

def loc2_zonetable(loc2,atpco_db):
    td={}
    for k in range(len(loc2)):
        print(loc2[k])
        x=list_of_string(loc2[k])
        if len(x)==1:
            td[loc2[k]]=["00000000"]
        if len(x)>1:
            query=list(atpco_db.JUP_DB_ATPCO_Record_3_Table_978.aggregate([
               {
                    '$group': {
                            '_id': {
                                'tbl_no': '$TBL_NO'
                                },
                            'loc': {'$addToSet': '$GEO_LOC'}
                        }
               },
               {
                    '$addFields': {'loc_size': {'$size': '$loc'}}
               },
               {
                    '$match': {
                            'loc_size': {'$eq': len(x)},
                            'loc': {'$all': x}
                        }
               },
               {
                    '$project': {
                            '_id': 0,
                            'tbl_no': '$_id.tbl_no',
                            'loc_size': '$loc_size'
                        }
               },
               {"$group":{"_id":None,
               "table":{"$push":"$tbl_no"}}},
                {"$project":{"_id":0}}
                ]))
            if len(query)>0:
                query[0]['table'].sort()
                td[loc2[k]]=query[0]['table']
            else:
                td[loc2[k]]=[" "]
    return td
    
def tocat(n,loc1,cc,cat_temp):
    cat_tag=np.NaN
    if not isinstance(n,float):
        list11=list(cat_temp)
        print(cat_temp)
        for ii in range(len(cc)):
            if cc[ii]['rbd']==n and cc[ii]['pos']==loc1:
                print(cc[ii]['rbd'],cc[ii]['cat'],)
                list11[cc[ii]['cat']-1]=cc[ii]['cat_tag']
        list11[29]=list11[15]
        list11[31]=list11[15]
        cat_tag="".join(list11)
        print(cat_tag,n)
    return cat_tag

def rec_3_989(cc_list,appl,ow_rt,season_type,fare_type,f_family):
    record_3_table_989=[]
    f_family=list_of_string(f_family)
    for j in range(len(cc_list)):
        record_3_table_989.append({"appl":appl,"ow_rt":ow_rt,"season_type":season_type,"fare_type":fare_type,"fare_class":f_family[j],
                "carrier":"EK","tariff":"000","psgr_type":"ADT","rule":"","combine_column_list":cc_list[j]})
    return record_3_table_989

def rec_3_25(main_table,cateogry_over_riding,percent,record_3_table_989,appl,ow_rt,season_type,fare_type,atpco_db):
    record_3_cat_25={"psgr_type":"ADT","main_table":[main_table],"cat_override_tag":cateogry_over_riding,"RI":"","DI":"",'fare_indicator':"C","filing_type":"L","pct":percent}
    stk=""
    try:
        for jk in range(len(record_3_table_989)):
            stk=stk+record_3_table_989[jk]['combine_column_list']
        record_3_cat_25['combine_col']=appl+ow_rt+season_type+fare_type+stk+"EK"+"000"+"ADT"+""
    except TypeError as e:
        record_3_cat_25['combine_col']=""
        record_3_cat_25['main_table']=[""]
        record_3_cat_25['cat_override_tag']=""
        record_3_cat_25['pct']=""
    return record_3_cat_25

def base_main_tables(cc_list,cat_tag,percent,atpco_db):
    base_tbl,main_tbl="",""
    print(cc_list,percent)
    query=list(atpco_db.cat_new_25.aggregate([{"$match":{"appl_combine_column":{"$all":cc_list},"fare_class_size":len(cc_list)}},
                {"$group":{"_id":None,
                "tbl":{"$push":"$tbl_no"}}},
                {"$project":{"_id":0,"tbl":"$tbl"}}]))
    print(query,"query")
    # print((query[0]['tbl'].sort()[-1]))
    if len(query)>0:
        base_tbl=query[0]['tbl']
        base_tbl.sort()
        print(base_tbl)
        query1=list(atpco_db.JUP_DB_ATPCO_Record_3_Cat_25.aggregate([{"$match":{"BASE_TBL_NO" : {"$in":base_tbl},
                "CAT_OVERRIDE_TAGS" :cat_tag,"FARE_CAL_PERCENT" : percent,"FARE_CAL_FIND" : "C", "RESUL_FARE_DISCAT" : "L","PSGR_TYPE" : "ADT","PSGRSTATUS_APPL" : "",
                "PSGRSTATUS_PSGR" : "","PSGRSTATUS_TYPE" : "","PSGRSTATUS_GEO_SPEC" : "","PSGRSTATUS_AREA" : "","PSGRSTATUS_ZONE" : "","PSGRSTATUS_COUNTRY" : "",
                "PSGRSTATUS_CITY" : "","PSGRSTATUS_STATE" : "","ID" : "","AGE_MIN" : "","AGE_MAX" : "","PSSGR_OCC_FIRST" : "000","PSSGR_OCC_LAST" : "000",
                "TRAVELWHOLL_TYPE" : "","TRAVELWHOLL_GEO_SPEC" : "","TRVL_WHOLLY_WITHIN_AREA" : "","TRVL_WHOLLY_WITHIN_ZONE" : "","TRVL_WHOLLY_WITHIN_COUNTRY" : "",
                "TRVL_WHOLLY_WITHIN_CITY" : "","TRVL_WHOLLY_WITHIN_STATE" : "","PASSGR_TRAVEL_ORIGIN" : "","PSSGR_TRAVEL_TSI" : "","PSSGR_TRAVEL_TYPE" : "",
                "PSSGR_SPLIT" : "","PSSGR_TRAVEL_AREA" : "","PSSGR_TRAVEL_ZONE" : "","PSSGR_TRAVEL_COUNTRY" : "","PSSGR_TRAVEL_CITY" : "","PSSGR_TRAVEL_STATE" : "",
                "PSSGR_RESERVED" : "","FLT_SEGS" : "","NODISC" : "","FARE_CAL_MIN" : "","FARE_CAL_MAX" : "","FARE_CAL_AMT" : "000000000","FARE_CAL_CUR" : "",
                "FARE_CAL_DEC" : "","FARE_CAL_AMOUNT" : "000000000","FARE_CAL_CURR" : "","FARE_CAL_DEC1" : "","FARE_COMPARISON" : "","FARE_CAL_MIN_FARE" : "000000000",
                "FARE_CAL_MAX_FARE" : "000000000","FARE_CAL_CUR1" : "","FARE_CAL_DEC2" : "","FARE_CAL_MINF" : "000000000","FARE_CAL_MAXF" : "000000000",
                "FARE_COMP_CUR" : "","FARE_COMP_DEC" : "","FARE_COMP_RULES" : "000","FARE_COMP_CARRIER" : "","FARE_COMP_FARECLASS" : "","FARE_COMP_FARETYPE" : "",
                "TARIFF" : "","RESERVED" : "","RESUL_FARE_OWRT" : "","RESUL_FARE_GLOBAL" : "","RESUL_FARE_ROUTINGTARIFF" : "000","RESUL_FARE_ROUTINGNUMBER" : "99999",
                "RESUL_FARE_RESERVED" : "","RESUL_FARE_FARECLASS" : "","RESUL_FARE_FARE" : "","RESUL_FARE_SEASON" : "","RESUL_FARE_DAYOFWEEK" : "","RESUL_FARE_PRCCAT" : "",
                "RESUL_FARE_PRIMERBD1" : "","RESUL_FARE_PRIMERBDTABLE999" : "00000000","RESUL_FARE_PRIMESECTOR" : "","RESERVED_FUTURE_USE" : "", "TICKETING_CODE" : "",
                "RESUL_FARE_TCM" : "","RESUL_FARE_TICKETDESIGNATOR" : "","RESUL_FARE_TDM" : "","HIGHEST" : "","DATE_TBL_NO" : "00000000","TEXT_TBL_NO" : "00000000"}}
                ,{"$group":{"_id":None,
                "tbl":{"$push":"$TBL_NO"}}},
                {"$project":{"_id":0,"tbl":"$tbl"}}]))
        base_tbl=base_tbl[-1]
        if len(query1)>0:
            query1[0]['tbl'].sort()
            main_tbl=query1[0]['tbl'][-1]
            print(main_tbl)
            base_tbl=atpco_db.JUP_DB_ATPCO_Record_3_Cat_25.find_one({"TBL_NO":main_tbl})["BASE_TBL_NO"]
    print(base_tbl,main_tbl)

    return base_tbl+"-"+main_tbl

def rec_2(tariff,loc2_zone_table,rule_id,loc1,loc_2,cat,atpco_db):
    loc2=list_of_string(loc_2)
    print(cat)
    no_appl=""
    if  isinstance(cat,float):
        no_appl="X"
    loc2_zone_table=list_of_string(loc2_zone_table)
    print(loc2_zone_table,loc2)
    if loc2_zone_table==[""]:
        print("kk,")
        loc2_zone_table=[" "]
    db_query={"TARIFF":tariff,"LOC_1":loc1,"RULE_NO":rule_id,"LOC_1_ZONE_TABLE":"00000000",
            "LOC_2_ZONE_TABLE":loc2_zone_table[0],"LOC_2":loc2[0],"CXR_CODE" : "EK","JT_CXR_TBL_NO" : "00000000","FARE_APPL" : "","RESERVED" : "","BATCH_CI" : "EK","RESERVED2" : "","NO_APPL":no_appl}
    record_2={"flag":"false","tariff":tariff,"loc1":[loc1],"rule_id":rule_id,"loc1_zone_table":"00000000",
                "loc2_zone_table":loc2_zone_table[0],"loc2":loc2,"seq_no":" ","no_appl":no_appl}
    if record_2['loc2_zone_table']=="00000000":
        query_ani=list(atpco_db.JUP_DB_ATPCO_Record_2_Cat_25.find(db_query))
        if len(query_ani)>0:
            print("jaa",record_2)
            record_2["flag"]="true"
            record_2["seq_no"]=query_ani[0]['SEQ_NO']
    elif record_2['loc2_zone_table']!=" ":
        print("hiii")
        loc2_zone_table.sort()
        record_2['loc2']=[""]
        db_query["LOC_2_ZONE_TABLE"]=   {"$in":loc2_zone_table}
        db_query["LOC_2"]=""
        query_ani=list(atpco_db.JUP_DB_ATPCO_Record_2_Cat_25.find(db_query))
        if len(query_ani)>0:
            print('sucess',record_2)
            record_2["flag"],record_2["loc2_zone_table"],record_2['seq_no']="true",query_ani[0]['LOC_2_ZONE_TABLE'],query_ani[0]['SEQ_NO']
        else:
            record_2["loc2_zone_table"]=loc2_zone_table[-1]
    print(loc2_zone_table[0],record_2)
    return record_2

def ll(p):
    if not isinstance(p[0],float):
        return p
    else:
        return []

def rec_2_vice_versa(record_2,tariff_no,loc2):
    record_2['flag']="false"
    record_2['tariff']=tariff_no
    record_2['loc2']=loc2
    record_2['seq_no']=" "
    return record_2

def transform_pos(raw_db,pdss_db,atpco_db,corporate_name,raw_record_id,file_date, file_time, system_date, system_time):
    # exception,appl,season_type="","",""
    t=time.time()
    q0=pd.DataFrame(list(raw_db.corporate.instruction.aggregate([
             {"$match":{"_id" : ObjectId(raw_record_id)}},
            {"$unwind":"$fare_sheet.sheets"},
            {"$unwind":"$fare_sheet.sheets.sheet_data"},
            {"$project":{"_id":0,"desc":"$fare_sheet.sheets.sheet_desc",
            "origin":"$fare_sheet.sheets.sheet_data.origin",
            "dest":"$fare_sheet.sheets.sheet_data.destination",
            "discount_details":"$fare_sheet.sheets.sheet_data.discount_details",
            "ow/rt":"$fare_sheet.sheets.sheet_data.ow/rt",
            "fare_family_details":"$fare_sheet.sheets.sheet_data.fare_family_details",
            "ticket_designator":"$cover_sheet.details_of_ticketing_instructions.ticket_designator"}}])))
    t_k_d=q0["ticket_designator"].unique()
    sfi_query=raw_db.corporate.sfi.find_one({"corporate_name":corporate_name.strip(),"file_date":file_date,"file_time":file_time})
    exc_query=sfi_query['exclusions']
    cat_list=sfi_query['cat_tag']
    cover_cat=sfi_query['cover_sheet_tag']
    cat_temp="XBBBBBBBBBBBBBBBBBXXXXB XXXX B B X              B"
    list11=list(cat_temp)
    for ii in range(len(cover_cat)):
        if cover_cat[ii]['cat']==50:
            list11[48]=cover_cat[ii]['tag']
        if cover_cat[ii]['cat']==14:
            list11[13]=cover_cat[ii]['tag']
        if cover_cat[ii]['cat']==15:
            list11[14]=cover_cat[ii]['tag']
        if cover_cat[ii]['cat']==18:
            list11[17]=cover_cat[ii]['tag']
        if cover_cat[ii]['cat']==19:
            list11[18]=cover_cat[ii]['tag']
    cat_temp="".join(list11)
    # print(cat_temp)
    # print(exc_query)
    # print(len(cat_list))
    exc_temp=[]
    if len(exc_query)>0:
        for e in range(len(exc_query)):
            # print(exc_query[e])
            exc_temp.append({"origin":exc_query[e]['pos'],"dest":exc_query[e]['exclusions'].replace("EXCLUDED DESTINATIONS:","").strip(),'discount_deatils':[""],
                "desc":"","ow/rt":"","fare_family_details":[""],"ticket_designator":t_k_d[0]})
        # print(exc_temp)
    q0=q0.to_dict("records")+exc_temp
    q0=pd.DataFrame(q0)
    #Iterating through all the ods inside the instruction sheet
    final_list=[]
    t=time.time()
    q1=raw_db.JUP_DB_Corporate_Info.find_one({"corporate_name":{"$regex":re.sub('[^A-Za-z0-9]+', "!s*(.*)", corporate_name.replace(" ","_")).replace("!","\\").lower()}})
    if q1!=None:
        rule_id=q1['rule']
    else:
        rule_id=[""]
    # print(q0)
    q0=q0.assign(ticket_designator=q0.ticket_designator.str.split(',')).explode('ticket_designator').reset_index(drop=True)
    t_r_df = pd.DataFrame([{'ticket_designator':t_k_d[0] , 'rule_id': rule_id}])
    t_r_df=t_r_df.assign(ticket_designator=t_r_df.ticket_designator.str.split(',')).explode('ticket_designator').reset_index(drop=True)
    t_r_df=t_r_df.to_dict("records")
    rule_id=t_r_df[0]['rule_id']
    r=rule_id[0]
    for i in range(len(t_r_df)):
        t_r_df[i]['rule_id']=rule_id[i]
    t_r_df=pd.DataFrame(t_r_df)
    q0=pd.merge(q0, t_r_df, on=['ticket_designator'])
    q0 = q0[~q0['dest'].isin(["NB: excluded FBC"])]
    q0['dest_temp']=q0['dest']
    od=['origin','dest']
    q0.loc[q0['dest'].str.contains('Rest'), 'dest'] = 'network'
    for i in range(len(od)):
        temp_dict={}
        p=q0[od[i]].unique()
        for k in range(len(p)):
            temp_dict[p[k]]=get_od_details(place=p[k],raw_db=raw_db,atpco_db=atpco_db,pdss_db=pdss_db)
        col_name=od[i]+"_list"
        q0[col_name] = q0[od[i]].map(temp_dict)
    vice_versa=q0.loc[q0['dest'] == 'network'] 
    q4=atpco_db.JUP_DB_ATPCO_Record_2_Cat_25.find_one({"RULE_NO":{"$in":rule_id},"CAT_NO":"025"})
    if q4!=None:
        q0['rule_db_flag']=True
    else:
        q0['rule_db_flag']=False
    # print(q0.columns)
    q0=q0.explode("origin_list")
    q1=q0
    # print(q1.shape)
    q0['loc1']=q0['origin_list'].str[0]
    q0['loc1_code']=q0['origin_list'].str[1]
    q0['loc1_country']=q0['origin_list'].str[2]
    q0['loc1_type']=q0['origin_list'].str[3]
    # q0.to_csv("records.csv")
    q0=q0.to_dict("records")
    for od in range(len(q0)):
        # print(q0[od]['dest_list'])
        disc_details=[]
        trip_type="-"
        if "desc" in q0[od].keys() or "ow_rt" in q0[od].keys():
            trip_type=trip(tt=q0[od]['desc'])
            # print(q0[od]['discount_details'])
        try:
            for details in range(len(q0[od]['discount_details'])):
                if re.search("n\s*(.*)a",str(q0[od]['discount_details'][details]['discount']),flags=re.IGNORECASE) is None:
                    appl,season_type,exception,fare_type="","","",""
                    level=q0[od]['discount_details'][details]['fare_class']
                    rbd=q0[od]['discount_details'][details]['rbd'].split("/")
                    # print(str(q0[od]['discount_details'][details]['discount']))
                    q0[od]['discount_details'][details]['discount']=str(q0[od]['discount_details'][details]['discount']).replace("%","")
                    if re.search("no\s*(.*)discount",q0[od]['discount_details'][details]['discount'],flags=re.IGNORECASE):
                        q0[od]['discount_details'][details]['discount']="0.0"
                    if re.search('[^A-Za-z0-9.]+',q0[od]['discount_details'][details]['discount'],flags=re.IGNORECASE):
                        # print("hi")
                        q0[od]['discount_details'][details]['discount']=q0[od]['discount_details'][details]['discount'].split("\n")[0].strip()
                    q0[od]['discount_details'][details]['discount']=re.sub('[a-zA-Z]', 'k', q0[od]['discount_details'][details]['discount'])
                    # print(q0[od]['discount_details'][details]['discount'],"k")
                    q0[od]['discount_details'][details]['discount']=re.sub('[^a-zA-Z0-9 \n\.]', '!',q0[od]['discount_details'][details]['discount'])
                    # print(q0[od]['discount_details'][details]['discount'],"!")
                    discount=str(q0[od]['discount_details'][details]['discount']).replace("k","!").split("!")
                    # print(discount)
                    if len(discount)==1:
                        discount=[]
                        for i in range(len(rbd)):
                            discount.append(q0[od]['discount_details'][details]['discount'])
                    for r in range(len(rbd)):
                        # print(q0[od]['origin_list'][2])
                        disc_details.append((rbd[r].strip()+"-1",rbd[r].strip(),level,str(int(discount[r].replace(".0",""))).strip(),trip_type,season_type,appl,fare_type,exception,
                            (str(100-int(str(discount[r]).replace(".0",""))).zfill(3)).ljust(7, '0')))  
        except TypeError as e: 
            continue
        q0[od]['discount_list']=tuple(disc_details)
    print(pavan)
    def tariff(df):
        code={"11":"894","12":"878","13":"885","21":"878","22":"903","23":"901","31":"885","32":"901","33":"912"}
        name={"11":"FBRA1P","12":"FBRA12P","13":"FBRA13P","21":"FBRA12P","22":"FBRA2P","23":"FBRA23P","31":"FBRA13P","32":"FBRA23P","33":"FBRA3P"}
        df['od_code']=df['loc1_code']+df['loc2_code']
        df['tariff_name'] = df['od_code'].map(name)
        df['tariff_no']=df['od_code'].map(code)
        try:
            df['loc2_america']=df['dest'].apply(lambda x: america(x))
        except Exception as e:
            df['loc2_america']="surya"
        df['loc2america']=df['loc2_country'].apply(lambda x: america(x))
        df['loc1_america']=df['loc1_country'].apply(lambda x:america(x))
        df['tariff_name'][df.loc1_america.str.contains('e$')] = "FBRINPV"
        df['tariff_no'][df.loc1_america.str.contains('e$')] = "864"
        df['tariff_name'][df.loc2_america.str.contains('e$')] = "FBRINPV"
        df['tariff_no'][df.loc2_america.str.contains('e$')] = "864"
        df['tariff_name'][df.loc2america.str.contains('e$')] = "FBRINPV"
        df['tariff_no'][df.loc2america.str.contains('e$')] = "864"
        return df
    df=pd.DataFrame(q0)
    df=df.explode("dest_list")
    df['loc2']=df['dest_list'].str[0]
    df['loc2_code']=df['dest_list'].str[1]
    df['loc2_country']=df['dest_list'].str[2]
    df['loc2_type']=df['dest_list'].str[3]
    df["di"]="3"
    df['C'] = np.arange(len(df))
    #for row scenarioes
    df1=df[df['dest']!="network"]
    df2=df[df['dest']=="network"]
    df2=df2[df2['loc2']=="2"]
    dfp=pd.DataFrame()
    # print(df.shape)
    if df2.shape[0]>1:
        dfq=pd.DataFrame()
        df2=df2[['loc1','loc1_type','discount_list','loc1_country',"rule_id",'loc1_code','ticket_designator','rule_db_flag']]
        # df2=df2[['loc1','loc1_type','discount_list','loc1_country','loc1_code']]
        # print(df2)
        x=df2['loc1'].tolist()
        perm1=list(combinations(x,2))
        perm1=[list(i) for i in perm1]
        for i in range(len(perm1)):
            perm1[i].append(["3","4"]) 
            dfx= df[(df['loc1']==perm1[i][0]) & (df['loc2']==perm1[i][1])]
            if dfx.shape[0]>0:
                dfq=dfq.append(dfx)
                perm1[i][2]=["3-","4"]
                df=df[~df['C'].isin(dfx['C'].tolist())]
                # print(df.shape,"hi")
            else:
                # print("hii")
                dfx=df[(df['loc1']==perm1[i][1]) & (df['loc2']==perm1[i][0])]
                if dfx.shape[0]>0:
                    dfq=dfq.append(dfx)
                    perm1[i][0],perm1[i][1],perm1[i][2]=perm1[i][1],perm1[i][0],["3-","4"]
                    df=df[~df['C'].isin(dfx['C'].tolist())]
                    # print(df.shape)
                else:
                    x=atpco_db.JUP_DB_ATPCO_Record_2_Cat_25.find_one({"LOC_1":perm1[i][0],"RULE_NO":r,"LOC_1_ZONE_TABLE":"00000000","LOC_2_ZONE_TABLE":"00000000","LOC_2":perm1[i][1],"CXR_CODE" : "EK","JT_CXR_TBL_NO" : "00000000","FARE_APPL" : "","RESERVED" : "","BATCH_CI" : "EK","RESERVED2" : "","NO_APPL":""}) 
                    if x==None:
                        perm1[i][0],perm1[i][1]=perm1[i][1],perm1[i][0]
        dfp=pd.DataFrame(perm1,columns=["loc1","loc2","di"])
        dfp=dfp.explode("di")
        dfp=pd.merge(dfp, df2, on=['loc1'])
        # print(dfp.columns)
        df2=df2.drop(["rule_id",'ticket_designator','rule_db_flag'],axis=1)
        df2.rename(columns={"loc1":"loc2","loc1_type":"loc2_type","loc1_country":"loc2_country",'loc1_code':'loc2_code'},inplace=True)
        dfp=pd.merge(dfp, df2, on=['loc2'])
        # print(dfp.columns)
        def disc(a,b,c,d,e):
            if a=="3":
                return b
            if a=="4":
                return c
            if a=="3-":
                c=dfq[dfq['loc1']==d]
                c=c[c['loc2']==e]
                return c.iloc[0]['discount_list']
                # print(c)
        dfp['discount_list']=dfp.apply(lambda x: disc(x['di'],x['discount_list_x'],x['discount_list_y'],x['loc1'],x['loc2']),axis=1)
        dfp['di'] = np.where(dfp['di'] =="3-", "3", dfp['di'])
        dfp=tariff(dfp)
        dfp=dfp[['loc1',"discount_list","tariff_name","tariff_no","loc1_type","rule_id",'di','ticket_designator','loc2','rule_db_flag','loc2_type']]
        dfp.to_csv("suy.csv")
        # print(df2.shape)
        # print(df1.shape)
    # #for other scenarioes
    df['C'] = np.arange(len(df))
    df1=df[df['dest']!="network"]
    df2=df[df['dest']=="network"]
    print(df1.shape)
    # df1=df1.dropna()
    df1=df1[df1['discount_list']!=np.NaN]
    print(df1.shape,"ll")
    # df2=df2.drop_duplicates(subset=['loc1'], keep='last')
    df2=df2[df2['loc2']=="2"]
    # print(df2.shape)
    df2=df1.append(df2)
    print(df2.columns)
    a=df2.to_dict("records")
    b=a.copy()
    vsl=[]
    # print(pavan)
    for i in range(len(a)):
        for j in range(len(b)):
            if a[i]['dest']=="network" and a[i]['loc1_country']==b[j]['loc2_country']:
                # print(b[j]['loc1'],b[j]['loc2'],int(b[j]['loc2_code'])==int(a[i]['loc1_code']))
                # print(b[j]['C'])
                df=df[~df['C'].isin([b[j]['C']])]
                w=copy.deepcopy(b[j])
                o=copy.deepcopy(a[i]['discount_list'])
                w['discount_list'],w['di']=o,"4"
                vsl.extend([b[j],w])
            if a[i]['dest']=="network" and int(a[i]['loc1_code'])==int(b[j]['loc2_code']) and b[j]['loc2_type']=="region" and a[i]['loc1_country'] in pavan[b[j]['loc2']]:
                print(b[j]['loc1'],b[j]['loc2'],a[i]['loc1'],a[i]['loc2'])
                v={"loc1":a[i]['loc1'],"loc1_code":a[i]['loc1_code'],"loc1_country":a[i]['loc1_country'],"loc1_type":a[i]['loc1_type'],
                "loc2":b[j]['loc1'],"loc2_code":b[j]['loc1_code'],"loc2_country":b[j]['loc1_country'],"loc2_type":b[j]['loc1_type'],
                "rule_id":a[i]['rule_id'],"ticket_designator":a[i]['ticket_designator'],"rule_db_flag":a[i]['rule_db_flag'],
                "di":"3","discount_list":a[i]['discount_list']}
                g=copy.deepcopy(v)
                g['di']="4"
                g['discount_list']=b[j]['discount_list']
                vsl.extend([v,g])
            if (a[i]['loc1_country']==b[j]['loc2_country']) and (a[i]['loc2_country']==b[j]['loc1_country']):
                if a[i]['loc2_type']=="city":
                    print(b[j]['loc1'],b[j]['loc2'],int(b[j]['loc2_code'])==int(a[i]['loc1_code']),a[i]['loc1'],a[i]['loc2'])
                    df=df[~df['C'].isin([a[i]['C']])]
                    w=copy.deepcopy(a[i])
                    o=copy.deepcopy(b[j]['discount_list'])
                    w['discount_list'],w['di']=o,"4"
                    vsl.extend([a[i],w])
    vsl=pd.DataFrame(vsl)
    if vsl.shape[0]>0:
        print("oo")
        vsl['C']=np.arange(len(vsl))
        vsl1=vsl.astype(str)
        vsl1=vsl1.drop_duplicates()
        li=vsl1['C'].tolist()
        li=[int(p) for p in li]
        vsl=vsl[vsl['C'].isin(li)]
        # print(vsl.shape)
        vsl.to_csv("sury.csv")
        # print(df.shape)
        vsl=tariff(vsl)
        vsl=vsl[['loc1',"discount_list","tariff_name","tariff_no","loc1_type","rule_id",'di','ticket_designator','loc2','rule_db_flag','loc2_type']]
        print(vsl.columns)
    # print(df.shape)
    print(vsl.shape)
    print(dfp.shape)
    vsl=vsl.append(dfp)
    if vsl.shape[0]>0:
        vsl['loc2']=vsl.apply(lambda x: tolist(x['loc2']),axis=1)
    print(df.shape)
    vsl.to_csv("sury.csv")
    df=tariff(df)
    df=df[['loc1',"discount_list","tariff_name","tariff_no","loc1_type","rule_id",'di','ticket_designator','loc2','rule_db_flag','loc2_type']]
    df.to_csv("req.csv")
    col=list(df.columns)
    col.remove("loc2_type")
    df=df.drop_duplicates(col,keep= 'first')
    print(df.shape)
    df.to_csv("s.csv")
    df = df.groupby(by=['loc1',"discount_list","tariff_name","tariff_no","loc1_type","rule_id",'ticket_designator','di','rule_db_flag'],dropna=False,as_index=False)['loc2','loc2_type'].agg(lambda x: list(x))
    print(df.columns)
    df=df.append(vsl)
    print(df.shape)
    # print(df.shape)
    def locnumber(a):
        f=["airport","city","country","region","area"]
        b=[]
        if  not isinstance(a,list):
            a=[a]
        for i in range(len(a)):
            b.append(f.index(a[i]))
        return max(b)
    df.to_csv("rec.csv")

    df['loc1_no']=df.apply(lambda x: locnumber(x['loc1_type']),axis=1)
    df['loc2_no']=df.apply(lambda x: locnumber(x['loc2_type']),axis=1)
    print(df['loc2_no'].tolist())
    print(df['loc2'].tolist())
    df=df.explode("discount_list")
    df['fare_family']=df['discount_list'].str[0]
    df['rbd']=df['discount_list'].str[1]
    df['exception']=df['discount_list'].str[8]
    df['percent']=df['discount_list'].str[9]
    df['percentage']=df['discount_list'].str[3]
    df['compartment']=df['discount_list'].str[2]
    df['appl']=df['discount_list'].str[6]
    df['trip_type']=df['discount_list'].str[4]
    df['ow_rt']=df['discount_list'].str[4]
    df['fare_type']=df['discount_list'].str[7]
    df['season_type']=df['discount_list'].str[5]
    timeee=time.time()
    print(cat_temp,type(cat_temp))    
    df['cateogry_over_riding']=df.apply(lambda x : tocat(x['rbd'],x['loc1'],cat_list,str(cat_temp)), axis = 1)
    print(df['cateogry_over_riding'].unique())
    print(time.time()-timeee)
    print(df.columns)
    df['loc_2']= df['loc2'].astype(str)
    df22=df.groupby(['tariff_name','tariff_no','loc1','loc1_no','loc2_no','loc_2','rule_id',"cateogry_over_riding",'loc1_type','trip_type','compartment','exception','appl'
        ,'percentage','ticket_designator','fare_type','season_type','percent','ow_rt',"rule_db_flag","di"],as_index=False,dropna=False)['fare_family','rbd'].agg(lambda x: list(x))
    print(df22.shape)
# #     # # df22.to_csv("records.csv")
    def class_number(a):
        f=["first_class","economy_class","business_class","all_cabin"]
        # try:
        lo=0
        if not isinstance(a,float):
            lo=f.index(a)
        # except Exception as e:
            # x=0
        return lo
    # df22.to_csv("rec.csv")
    df22['class_no']=df22.apply(lambda x: class_number(x['compartment']),axis=1)
    print(df22['class_no'])
    loc2=df22['loc_2'].unique()
    td=loc2_zonetable(loc2,atpco_db)
    df22['loc2_zone_table']=df22['loc_2'].map(td)
    print(df22["loc2_zone_table"])
    df22['loc2'] = df22.apply(lambda x:list_of_string(x['loc_2']),axis=1)
#   df22['loc2_zone_table'] = df22['loc_2'].map(td)
    df22['loc2_zone_table_']= df22['loc2_zone_table'].astype(str)
    df22['fare_family_']=df22['fare_family'].astype(str)
    # df22.to_csv('err2.csv')
    record2=df22[['loc2_zone_table_',"tariff_no","rule_id",'loc1','loc_2','cateogry_over_riding']]
    print(record2.shape)
    record2=record2.drop_duplicates()
    record2['record_2']=record2.apply(lambda row : rec_2(row['tariff_no'], row['loc2_zone_table_'],row['rule_id'],row['loc1'],row['loc_2'],row['cateogry_over_riding'],atpco_db), axis = 1)
    rec2=time.time()
    df22=pd.merge(df22, record2, on=['loc2_zone_table_',"tariff_no","rule_id",'loc1','loc_2','cateogry_over_riding'])
    print("record2",time.time()-rec2)
    rec3=time.time()
    record3=df22[["fare_family_"]]
    record3=df22[['percent',"fare_family_","appl",'fare_type','season_type','ow_rt',"cateogry_over_riding"]]
    record3=record3.drop_duplicates()   
    # record3.to_csv('err2.csv')
    record3['combine_column_list']=record3.apply(lambda x : combine_column_list(x['fare_family_'],x['appl'],x['ow_rt'],x['fare_type'],x['season_type']), axis = 1)
    # record3.to_csv('err2.csv')
    record3['record_3_table_989']=record3.apply(lambda x : rec_3_989(x['combine_column_list'],x['appl'],x['ow_rt'],x['season_type'],x['fare_type'],x['fare_family_']),axis=1)
    # record3.to_csv('err2.csv')
    record3['b_m']=record3.apply(lambda x : base_main_tables(x['combine_column_list'],x["cateogry_over_riding"],x['percent'],atpco_db),axis=1)
    new = record3['b_m'].str.split("-", n = 1, expand = True)
    record3['base_tbl'],record3['main_table']=new[0],new[1]
    record3['record_3_cat_25']=record3.apply(lambda x : rec_3_25(x['main_table'],x['cateogry_over_riding'],x['percent'],x['record_3_table_989'],x['appl'],x['ow_rt'],x['season_type'],x['fare_type'],atpco_db),axis=1)
    print("record2",time.time()-rec2)
    print(record3.shape)
    # record3.to_csv('err2.csv')
    df22=pd.merge(df22, record3, on=['percent',"fare_family_","appl",'fare_type','season_type','ow_rt',"cateogry_over_riding"])
    print(df22.columns)
    df22['fare_family']=df22['fare_family'].apply(lambda x:ll(x))
    df22['rbd']=df22['rbd'].apply(lambda x:ll(x))
    df22['loc1_zone_table']="00000000"
    df22['exception']=df22['exception'].apply(lambda x:tolist(x))
    df22['fare_type']=df22['fare_type'].apply(lambda x:tolist(x))
    df22["fare_type_size"]=df22['fare_type'].str.len()
    df22['season_type']=df22['season_type'].apply(lambda x:tolist(x))
    df22["season_type_size"]=df22['season_type'].str.len()
    df22['appl']=df22['appl'].apply(lambda x:tolist(x))
    df22["appl_size"]=df22['appl'].str.len()
    #df22['di']="3"
    df22['ri']=""
    df22['loc1_zone_table']=df22['loc1_zone_table'].apply(lambda x:tolist(x))
    df22['ow_rt']=df22['ow_rt'].apply(lambda x: tolist(x))
    df22["ow_rt_size"]=df22['ow_rt'].str.len()
    df22['fare_class']=df22['fare_family']
    df22['fare_class_size']=df22['fare_class'].str.len()
    df22['main_table']=df22['main_table'].apply(lambda x:tolist(x))
    df22['base_tbl']=df22['base_tbl'].apply(lambda x:tolist(x))
    #df22=df22.drop(['loc2_zone_table_', 'fare_family_','b_m','loc1_type'],1)
    df22=df22.fillna("")
    df22.to_csv('err.csv')

    df22["loc1"]=df22['loc1'].apply(lambda x: tolist(x))
    
    def rec_2_225(a,b):
        c=a.copy()
        print("hi")
        if c['cat_override_tag']!='':
            print("hiii")
            c['DI']=b
            print(c['DI'])
        print(c)
        return c
    df22['record_3_cat_25']=df22.apply(lambda x: rec_2_225(x['record_3_cat_25'],x['di']),axis=1)
    def applnu(a):
        if a["no_appl"]=="X":
            lp=0
        else:
            lp=1
        return lp
    df22['appl_nu']=df22.apply(lambda x:applnu(x['record_2']),axis=1)
    print(df22['appl_nu'])
    def dest_no(a):
        if a==["1","2","3"]:
            return 1
        else:
            return 0
    df22['dest_no']=df22.apply(lambda x:dest_no(x['loc2']),axis=1)
    df22.sort_values(by=['tariff_no',"appl_nu","loc1_no","loc2_no","dest_no","di","class_no"], inplace=True)
    df22.to_csv("fil.csv")
    df22=df22.to_dict("records")
    raw_db.corporate.sfi.update_one({"corporate_name":corporate_name,"file_date":file_date,"file_time":file_time},{"$set":{"record_3":df22}},upsert=True)
if  __name__=="__main__":
    print("hhi")
    import time
    from bson.objectid import ObjectId
    st=time.time()
    client = pymongo.MongoClient(
    '3.6.201.161:27022',
    username='data.EK',
    password='data@123',
    authSource='admin',
    authMechanism='SCRAM-SHA-1')
    raw_db=client['rawDB_prod']
    pdss_db=client['PDSS_EK']
    atpco_db=client['ATPCO_EK']
    corporate_name="Occidental"
    # raw_record_id="603691f86e2d4397cc7ed0e6"
    raw_record_id=  "60369b5f6e2d4397cc7ed0f8"
    file_date=""
    system_time=""
    system_date=""
    file_time=""
    transform_pos(raw_db,pdss_db,atpco_db,corporate_name,raw_record_id,file_date, file_time, system_date, system_time)
    print(time.time()-st)
    #     # print(origin_list)
    #     if not re.search("fbc",q0[od]['dest'],flags=re.IGNORECASE) is not None:
    #         a=q0[od]['dest']
    #         if re.search("on\s*(.*)line",q0[od]['dest'],flags=re.IGNORECASE):
    #             multi_dest=list(db1.location.master.aggregate([{"$match": {"location": "ek_online_points"}},
    #                                               {"$group": {"_id": None,
    #                                                           "loc": {"$addToSet": "$loc_code"}}},
    #                                               {"$project": {"_id": 0, "loc": "$loc"}}]))[0]['loc']
            
    #             print(multi_dest)
    #             a='/'.join(multi_dest)
    #             print(a)
    #     print(a)
    #     dest_list=get_od_details(place=a)
    #     print(dest_list)
