import time
import pandas as pd
import re
import pymongo,copy
import numpy as np
import pprint
from bson.objectid import ObjectId

def record_2_rule(cat,rule_id,que,atpco_db):
    rule_db_flag=False
    rule_flag_query=atpco_db.JUP_DB_ATPCO_Record_2_Cat_All.find_one({"RULE_NO":{"$in":rule_id},"CAT_NO":cat})
    if rule_flag_query:
        rule_db_flag=True
    col_name,query=que[cat]['col_name'],que[cat]['query']
    return rule_db_flag,col_name,query

def get_explode_df(dfdict,outer_flag=[]):
    outer_sfi_flag="3"
    outer_flag=list(set(outer_flag))
    if outer_flag==["1"]:
        outer_sfi_flag="1"
    if outer_flag==["2"]:
        outer_sfi_flag="2"
    if outer_flag==[]:
        outer_sfi_flag=""
    df=pd.DataFrame(dfdict)
    if outer_sfi_flag!="":
        df['outer_sfi_flag']=outer_sfi_flag
    if "rule_id" in df.columns: 
        df=df.explode("rule_id")
    if  'rbd' in df.columns:
        df=df.explode('rbd')
    if 'tariff' in df.columns:
        # if "c/a/b" in list(df.columns):
        df1=copy.deepcopy(df)
        df2=df.loc[df['sfi_flag'] == False]
        df_=df.loc[df['sfi_flag'] == True]
        df1=df_.astype(str)
        df1 = df1.groupby(by=['rbd'],dropna=False)['text'].apply(list).reset_index()
        df1=df1.astype(str)
        df1 = df1.groupby(by=['text'],dropna=False)['rbd'].apply(list).reset_index()
        df1['len']=df1['rbd'].str.len()
        df1=df1.sort_values("len",axis=0)
        if df1.shape[0]>0:
            z=df1.iloc[-1]['rbd']
            if len(z)>3:
                df_=df_[~df_['rbd'].isin(z[1:])]
                df_['rbd'] = np.where(df_['rbd'] ==z[0] , " ", df_['rbd'])
                df=df_.append(df2)
                print(df.shape)
        df=df.explode('tariff')
        print(df.shape)
        print(df.dtypes)
        df['tariff_code']=df['tariff'].str[0]   
        df['tariff_name']=df['tariff'].str[1]
        df=df.drop("tariff",1)
        df.sort_values(by=['tariff_code'], inplace=True)
    if  'rbd' in df.columns:
        #df=df.explode('rbd')
        df['fare_family']=(df['rbd']+"-"+df['loc1']+"1").str.strip('\n\t')
    if "loc1" in df.columns:
        df['loc1']=""
    df=df.fillna("")
    #df.to_csv("cat_16.csv")
    df=df.to_dict("records")
    # #print(df)
    return df

def split_data(notes,cat_list):
    data={}
    ind = notes.index(cat_list[0])
    for val in notes[ind:]:
        if val in cat_list:
            lst = []
            data[val] = lst
        else:
            lst.append(val)
    return data

def cat_67(raw_db,req,ll):

    tsi=[]
    query_995={"REC_TYPE" : "3","ACTION" : "2","GEO_SPEC_TSI" : "","GEO_SPEC_TYPE" : "","GEO_SPEC" : "","GEO_SPEC_AREA_LOC2" : "",
                    "GEO_SPEC_ZONE_LOC2" : "","GEO_SPEC_COUNTRY_LOC2" : "","GEO_SPEC_CITY_LOC2" : "","GEO_SPEC_STATE_LOC2" : "","GEO_SPEC_AIRPORT_LOC2" : ""}
    for defi in range(len(req)):
        if re.search(req[defi]["definition"],ll,flags=re.IGNORECASE) is not None:
            que=query_995.copy()
            que["GEO_SPEC_TSI"]=str(req[defi]['tsi']).zfill(2)
            q6=atpco_db.JUP_DB_ATPCO_Record_3_Table_995.find_one(que)
            tsi.append((q6['TBL_NO'],req[defi]["definition"].upper()))
    #print(ll)   
    client = pymongo.MongoClient(
        '3.6.201.161:27022',
        username='data.EK',
        password='data@123',
        authSource='admin',
        authMechanism='SCRAM-SHA-1')
    atpco_db=client['ATPCO_EK']
    #print(tsi)
    x=str([int(d) for d in re.findall(r'-?\d+', ll)][0]).zfill(3)
    patterns = ['days', 'months']
    #print(tsi)
    for pattern in patterns:
        if re.search(pattern,ll, flags=re.IGNORECASE):
            dm=pattern[0].upper()
            unit=dm 
    return unit,tsi,x

def record3_tbl(query,col):
    client = pymongo.MongoClient(
        '3.6.201.161:27022',
        username='data.EK',
        password='data@123',
        authSource='admin',
        authMechanism='SCRAM-SHA-1'
    )
    atpco_db=client['ATPCO_EK']
    tbl_no=[]
    rec3_query=list(atpco_db.get_collection(col).aggregate([{"$match":query},
                {"$group":{"_id":None,
                "tbl":{"$push":"$TBL_NO"}}}]))
    if len(rec3_query)>0:
        tbl_no=rec3_query[0]['tbl']
    return tbl_no

def rec_2(loc1,fare_family,tariff_code,rule_id,tbl_no,df):
    tbl_no.sort()
    tt=[tbl_no[-1]]
    df=df.loc[(df['LOC_1']==loc1) & (df['TARIFF']==tariff_code) & (df['RULE_NO']==rule_id)& (df['FARE_CLASS']==fare_family)] 
    df=df.to_dict("records")
    te=[]
    for i in range(len(df)):
        for j in range(len(tbl_no)):
            if tbl_no[j] in df[i]["DATA_TABLE"]:
                te.append(tbl_no[j])
    te.sort()
    if len(te)>0:
        tt=te[-1]
    return tt
def text(t):
    t=[t]
    if ")" in t:
        t=[t.split(")")[1].strip()]
    return t    

def cat18(raw_db,pdss_db, atpco_db, corporate_name,a,tariff,rule_id,pos,cat_tag,rule_db_flag,query,col,cat_18,outer_sfi_18):
    tkt_loc,rbds="6",[]
    for line in range(len(a)):
        if re.search("RBD", a[line], flags=re.IGNORECASE) is not None:
            rbds.append(a[line])
        if re.search("re\s*(.*)issued", a[line], flags=re.IGNORECASE) and re.search("original", a[line], flags=re.IGNORECASE) is None:
            tkt_loc="4"
        elif re.search("original", a[line], flags=re.IGNORECASE) and re.search("re\s*(.*)issued", a[line], flags=re.IGNORECASE) is None:
            tkt_loc="2"
    if len(rbds)>0:
        rbddata=split_data(a,rbds)
    if rbddata:
        tt=time.time()
        for r in rbddata:
            list_18=[]
            x=r.replace('RBD',"").replace("S","").split("/")
            x=[l.strip() for l in x if l != '']
            y=rbddata[r][0]
            outer_sfi_18.append("1")
            for rr in x:
                cat_tag.append({"rbd":rr,"cat_tag":"X","cat":18,"pos":pos})
            ind=re.search("WP",y).start()
            list_18.extend([y[:ind-1],y[ind:].split("/")[1],y[ind:ind+re.search("/",y[ind:]).start()]])
            # #print(list_18)
            for pat in range(len(list_18)):
                if "XXX" in list_18[pat]:
                    continue
                else:
                    query["TICKET_ENDORSEMENT_TEXT"]={"$regex":"^"+list_18[pat].replace(" ","\s*(.*)").replace("/","\s*(.*)").replace("-","\s*(.*)").lower().replace("wp","").replace("id","").upper()+'$'}
                    query['TKT_LOC']=tkt_loc
                    tbl_no=record3_tbl(query,col)
                    # #print(tbl_no)
                    cat_18.append({"loc1":pos,"loc2":"","rule_id":rule_id,"text":[list_18[pat]],"tbl_no":tbl_no,"rbd":x,"sfi_flag":True,
                        "undefined_flag":"defined","tkt_loc":tkt_loc,"tariff":tariff,"rule_db_flag":rule_db_flag,"no_appl":""})
    elif len(a)==1:
        if re.search("non",a[0], flags=re.IGNORECASE)  or  re.search("wp",a[0], flags=re.IGNORECASE) is  None :
            outer_sfi_18.append("2")
            cat_18.append({"pos":pos,"rule_id":rule_id,"text":[a[0]],"rbd":"","sfi_flag":False,"no_appl":""})
    return cat_18,cat_tag,outer_sfi_18
def cat_5(raw_db,pdss_db, atpco_db, corporate_name,a,tariff,rule_id,pos,cat_tag,rule_db_flag,query,col,cat_05,outer_sfi_05):
    cat_gg={"sfi_flag":False,"undefined_flag":"defined","text":a,"loc1":pos,"loc2":"","rbd":[" "],"tariff":tariff,"rule_id":rule_id,"note_text":"",
    "ow_rt":"","adv_period":"","adv_unit":"","sector":"X","table":"","table_996":"","rule_db_flag":rule_db_flag,"pos":pos}
    rbds=[]
    for line in range(len(a)):
        # #print(line)
        x=a[line].replace(":","")
        print(x)
        rbd=[i for i in x.upper().split() if len(i)==1 and not any(ele in i for ele in ["-","/","&"]) ]
        if len(rbd)==0:
            rbd=[" "]
        print(rbd)
        if (re.search("published",a[line], flags=re.IGNORECASE) or re.search("base",a[line], flags=re.IGNORECASE)) and len(a)<4:
            for i in range(len(rbd)):
                cat_tag.append({"rbd":rbd[i],"cat_tag":"B","cat":5,"pos":pos})
            cat_gg['rbd']=rbd
            cat_gg['text']=[a[line]]
            cat_gg['note_text']=[a[line]]
            del cat_gg['tariff']
            cat_05.append(cat_gg)
            outer_sfi_05.append("2")
        elif re.search('NO ADVANCE PURCHASE RESTRICTIONS', a[line], flags=re.IGNORECASE) and len(a)<4:
            cat_gg['sfi_flag']=True
            cat_gg['note_text']=[a[line]]
            outer_sfi_05.append("1")
            if rbd==[" "]:
                cat_tag.append({"rbd":"all","cat_tag":"X","cat":5,"pos":pos})
            else:
                for i in range(len(rbd)):
                    cat_tag.append({"rbd":rbd[i],"cat_tag":"X","cat":5,"pos":pos})
            note_text_query=atpco_db.JUP_DB_ATPCO_Record_3_Table_996.find_one({"TEXT":notetext})
            if note_text_query!=None:
                cat_gg['table_996']=note_text_query['TBL']
                text_tbl_no=note_text_query['TBL']
                query['TEXT_TBL_NO']=text_tbl_no
                tbl_no=record3_tbl(query,col)
                cat_gg['table']=tbl_no
        elif re.search("RBD", a[line], flags=re.IGNORECASE) is not None:
            rbds.append(a[line])
    # #print(rbds)
    print(cat_05)
    if len(rbds) > 0:
        rbddata=split_data(a,rbds)
    # #print(rbddata)
        if rbddata:
            for r in rbddata:
                cat_5=cat_gg.copy()
                cat_5['text']=rbddata[r]
                rbd=[i for i in r.upper().split() if len(i)==1 and not any(ele in i for ele in ["-","/","&"]) ]
                if len(rbd)==0:
                    rbd=[" "]
                notetext=rbddata[r].copy()
                sfi_flag,text_tbl_no,tbl_no=True,"",""
                for li in range(len(rbddata[r])):
                    if 'NO ADVANCE PURCHASE REQUIREMENT' in rbddata[r][li].strip():
                        del notetext[li]
                        for rr in rbd:
                            cat_tag.append({"rbd":rr,"cat_tag":"X","cat":5,"pos":pos})
                    if 'PUBLISHED' in rbddata[r][li].strip():
                        outer_sfi_05.append("2")
                        sfi_flag=False
                        del cat_5['tariff']
                # #print(notetext)
                if sfi_flag==True:
                    outer_sfi_05.append("1")
                    # #print(rbd)
                    note_text_query=atpco_db.JUP_DB_ATPCO_Record_3_Table_996.find_one({"TEXT":notetext})
                    if note_text_query!=None:
                        text_tbl_no=note_text_query['TBL']
                    query['TEXT_TBL_NO']=text_tbl_no
                    tbl_no=record3_tbl(query,col)
                cat_5['note_text']=notetext
                cat_5['sfi_flag']=sfi_flag
                cat_5['text_tbl_no']=text_tbl_no
                cat_5['table']=tbl_no
                cat_5['rbd']=rbd
                cat_05.append(cat_5)
    
    return cat_05,outer_sfi_05,cat_tag
def cat6_cat7(req,raw_db, pdss_db, atpco_db, corporate_name,a,tariff,rule_id,pos,cat_tag,rule_db_flag,query,col,cat_no,cat_tt,outer_sfi):
    cat_gg={"sfi_flag":False,"undefined_flag":"undefined","text":[a],"pos":pos,"rule_id":rule_id,"loc1":pos,"loc2":"","no_appl":"","rule_db_flag":rule_db_flag,"rbd":[" "],"unit":"","duration":"","tbl_995":[],"tbl_rec_3":"","tsi1":"","tsi2":""}
    tsi=[]
    rbd_query=list(raw_db.rbd.mapping.aggregate([
    {"$unwind":"$RBD"},
    {
    "$group":{"_id":{"brand_type":"$brand_type"},
    "rbds":{"$addToSet":"$RBD"}}},
    {"$project":{"type":"$_id.brand_type",
        "rbd":"$rbds",
        "_id":0}}]))
    rb={}
    for i in rbd_query:
        rb[i['type']]=" ".join(i['rbd'])
    for line in range(len(a)):
        x,cat_dict=a[line].lower().replace("/"," "),cat_gg.copy()
        if re.search("flex\s*", x, flags=re.IGNORECASE) and "+" in x:
            x=x.replace("flex +",rb['flex_plus'])
        if re.search("flex", x, flags=re.IGNORECASE):
            x=x.replace("flex",rb['flex^'])
        if re.search("saver",x ,flags=re.IGNORECASE):
            x=x.replace("saver",rb['saver'])
        rbd=[i for i in x.upper().split() if len(i)==1 and i!="&"  and i!="-" and i!=" "]
        if len(rbd)==0:
            rbd=[" "]
        if re.search("no\s*restrictions\s*on", x, flags=re.IGNORECASE) : 
            cat_dict['rbd']=rbd
            cat_dict['text']=[a[line]]
            for r in range(len(rbd)):
                cat_tag.append({"rbd":rbd[r],"cat_tag":"X","cat":cat_no,"pos":pos})
            cat_dict['no_appl']="X"
            cat_dict['sfi_flag']=True
            cat_dict["tariff"]=tariff
            cat_dict['undefined_flag']="defined"
            outer_sfi.append("1")
        if  re.search('base ', x, flags=re.IGNORECASE) or re.search('published',x, flags=re.IGNORECASE):
            cat_dict['rbd']=rbd
            cat_dict['text']=[a[line]]
            cat_dict['undefined_flag']="defined"
            outer_sfi.append("2")
        if re.search("^no\s*restrictions$",a[line], flags=re.IGNORECASE) or  re.search("^not\s*applicable",a[line], flags=re.IGNORECASE) is not None :
            #print("suri")
            cat_tag.append({"rbd":"all","cat_tag":"X","cat":cat_no,"pos":pos})
            cat_dict['no_appl']="X"
            cat_dict['sfi_flag']=True
            cat_dict["tariff"]=tariff
            cat_dict['undefined_flag']="defined"
            outer_sfi.append("1")
        if re.search('travel', a[line], flags=re.IGNORECASE):
            #print("all")
            cat_tag.append({"rbd":"all","cat_tag":"X","cat":cat_no,"pos":pos})
            outer_sfi.append("1")
            cat_dict["tariff"]=tariff   
            cat_dict['undefined_flag']="defined"
            ll=a[line]
            unit,tsi,dur=cat_67(raw_db,req,ll)
            if cat_no==6:
                query['MIN_STAY']=dur
            if cat_no==7:
                query['MAX_STAY']=dur
            tsi.sort()
            query['UNIT']=unit
            cat_dict["text"]=[a[line]]
            cat_dict['duration']=dur
            cat_dict['unit']=unit
            if len(tsi)==2:
                cat_dict["tbl_995"]=[tsi[0][0],tsi[1][0]]
                query["FROM_GEO_TBL_NO"],cat_dict['tsi1'],query["TO_GEO_TBL_NO"],cat_dict['tsi2']=tsi[0][0],tsi[0][1],tsi[1][0],tsi[1][1]
                tbl_no=record3_tbl(query,col)
                cat_dict["sfi_flag"]=True
                cat_dict["tbl_rec_3"]=tbl_no
            else:
                cat_dict["sfi_flag"]=False
        cat_tt.append(cat_dict)
    return cat_tt,cat_tag,outer_sfi
def cat19(raw_db,corporate_name,tariff,rule_id,pos,pos_notes, cat_19,raw_query,cat_list,cat_data,rule_db_flag):
    cat_g={"sfi_flag":True,"undefined_flag":"defined","text":"","no_appl":"","loc1":pos,"loc2":"","rbd":[" "],"tariff":tariff,"pos":pos,"rule_id":rule_id,"pos":pos,"rule_db_flag":rule_db_flag}
    flag=True
    for key in cat_data:
        if re.search("19",key,flags=re.IGNORECASE)is not None:
            flag=False
            cat_list.append("19")
            cat_g['text']=cat_data[key]
            cat_g['no_appl']="X"
            cat_19.append(cat_g)
    if flag:
        if raw_query['cover_sheet']['details_of_ticketing_instructions']['child_infant_discounts'] is not None:
            cat_list.append("19")
            cat_g['text']=[raw_query['cover_sheet']['details_of_ticketing_instructions']['child_infant_discounts'].strip()]
            cat_g['no_appl']="X"
            cat_19.append(cat_g)
        else:
            for i in range(len(raw_query['rules_notes']['other_notes'])):
                if re.search("child\s*infant",raw_query['rules_notes']['other_notes'][i]['field_value'], flags=re.IGNORECASE) is not None:
                    flag=False
                    cat_list.append("19")
                    cat_g['text']=text(t=raw_query['rules_notes']['other_notes'][i]['field_value'])
                    cat_g['no_appl']="X"
                    cat_19.append(cat_g)
            if flag:
                for cat in range(len(pos_notes)): 
                    if re.search("child\s*infant",pos_notes[cat], flags=re.IGNORECASE) is not None:
                        cat_list.append("19")
                        cat_g['text']=text(t=pos_notes[cat])
                        cat_g['no_appl']="X"
                        cat_19.append(cat_g)
    return cat_19,cat_list
def cat_10_26(raw_db,corporate_name,pos,pos_notes,raw_query,cat_list,cat_data,cat_10,cat_26):
    cat_g={"sfi_flag":False,"undefined_flag":"defined","text":"","pos":pos,"rbd":[" "],"loc1":pos,"loc2":""}
    if raw_query['cover_sheet']['details_required_for_private_filing']['combinations']:
        xx=cat_g.copy()
        cat_list.append("10")
        xx['text']=[raw_query['cover_sheet']['details_required_for_private_filing']['combinations'].strip()]
        cat_10.append(xx)
    for i in range(len(raw_query['rules_notes']['other_notes'])):
        if re.search("combinations\s*permitted",raw_query['rules_notes']['other_notes'][i]['field_value'], flags=re.IGNORECASE) is not None:
            xx=cat_g.copy()
            cat_list.append("10")
            xx['text']=text(t=raw_query['rules_notes']['other_notes'][i]['field_value'])
            cat_10.append(xx)
        if re.search("group",raw_query['rules_notes']['other_notes'][i]['field_value'], flags=re.IGNORECASE) is not None:
            xx=cat_g.copy()
            cat_list.append("26")
            xx['text']=text(t=raw_query['rules_notes']['other_notes'][i]['field_value'])
            cat_26.append(xx)  
    for cat in range(len(pos_notes)): 
        if re.search("combinations\s*permitted",pos_notes[cat], flags=re.IGNORECASE) is not None:
            xx=cat_g.copy()
            cat_list.append("10")
            xx['text']=text(t=pos_notes[cat])
            cat_10.append(xx)
        if re.search("group",pos_notes[cat], flags=re.IGNORECASE) is not None:
            xx=cat_g.copy()
            cat_list.append("26")
            xx['text']=text(t=pos_notes[cat])  
            cat_26.append(xx)
    for key in cat_data:
        if re.search("10",key,flags=re.IGNORECASE)is not None:
            xx=cat_g.copy()
            cat_list.append("10")
            xx['text']=cat_data[key]
            cat_10.append(xx)
        if re.search("26",key,flags=re.IGNORECASE)is not None:
            xx=cat_g.copy()
            cat_list.append("26")
            xx['text']=cat_data[key]
            cat_26.append(xx)
    return cat_10,cat_26,cat_list
def cat16(raw_db,pdss_db, atpco_db, corporate_name,a,tariff,rule_id,pos,cat_tag,rule_db_flag,query,col,cat_l,outer_sfi_16):
    cat_1={"sfi_flag":False,"undefined_flag":"defined","text":a,"loc1":pos,"loc2":"","rbd":[" "],"tariff":tariff,"rule_id":rule_id,"note_text":"",
    "tbl_16":"","apl_vol":"","apl_canx":"","canx":"","tkt":"","appl":"","amt_1":"","cur_1":"","amt_2":"","cur_2":"","no_charge":"",
    "percent":"","higher_lower":"","cancel":"","noshow":"","refund":"","resissue":"","revalidation":"","lost_tkt":"","untktd_pta":"",
    "psgr":"","psgr_death":"","schedcharge":"","tktup":"","dec_1":"","text_tbl_no":"","rule_db_flag":rule_db_flag,
    "anytime_before_after":"ANYTIME","nonref_norescharge":"","vol_change":"","incol_changes":"","cancel_ref":""}
    changes_list=[]
    cancel_list=[]
    rbds=[]
    rbds_=[]
    cancel_list_before={}
    cancel_list_after={}
    cancel_list={}
    for line in range(len(a)):
        if  a[line].strip()=="CHANGES":
            change_index=line
        if  a[line].strip()=="CANCELLATIONS":
            cancel_index=line
    changes_list = a[change_index+1:cancel_index]
    # if len(changes_list)>3:
    #     changes_list = a[change_index+1:cancel_index]
    for lik in range(len(changes_list)):
        if re.search("RBD$", changes_list[lik], flags=re.IGNORECASE) is not None:
            rbds_.append(changes_list[lik])
    changes_list=split_data(changes_list,rbds_)
    cancel_list = a[cancel_index+1:]
    for li in range(len(cancel_list)):
        if re.search("RBD$", cancel_list[li], flags=re.IGNORECASE) is not None:
            rbds.append(cancel_list[li])
    rbddata=split_data(cancel_list,rbds)
    # #print(rbddata)
    for key in rbddata:
        for line in range(len(rbddata[key])):
            if  rbddata[key][line].strip()=="AFTER DEPARTURE":
                kk_index=line
        before_list = rbddata[key][1:kk_index]
        after_list = rbddata[key][kk_index+1:]
        if after_list[-1] not in before_list:
            before_list.append(after_list[-1])
        cancel_list_before[key],cancel_list_after[key]=before_list,after_list
    #print(changes_list)
    cat_kk={"changes":changes_list,"cancel_after":cancel_list_after,"cancel_before":cancel_list_before}
    for key in cat_kk:
        for rb in cat_kk[key]:
            x=rb.upper().split()
            rbd=[i for i in x if len(i)==1 and not any(ele in i for ele in ["-","/","&"," "])]
            if len(rbd)==0:
                rbd=[" "]
            #print(rbd)
            que1=query.copy()
            cat_gg=cat_1.copy()
            cat_gg['c/a/b']=rb
            cat_gg['rbd']=rbd
            sfi_flag,tbl_16=True,""
            if "cancel" in key:
                que1['APL_CANX'],que1['CANX'],que1['TKT']="X","X","X"
                cat_gg['apl_canx'],cat_gg['canx'],cat_gg['tkt']="X","X","X"
            if key=="cancel_before":
                que1['APPL']="2"
                cat_gg['appl']="2"
                cat_gg["anytime_before_after"]="BEFORE"
            if key=="cancel_after":
                que1['APPL']="3"
                cat_gg['appl']="3"
                cat_gg["anytime_before_after"]="AFTER"
            notetext=cat_kk[key][rb].copy()
            amount,currency,decimal="0000000","USD","2"
            for b in range(len(cat_kk[key][rb])):
                if "published" in cat_kk[key][rb][b].lower():
                    sfi_flag=False
                    outer_sfi_16.append("2")
                    del cat_gg['tariff']
                if re.search('^any\s*(.*)time',cat_kk[key][rb][b],flags=re.IGNORECASE) and re.search('no\s*(.*)show',cat_kk[key][rb][b],flags=re.IGNORECASE) is None:
                    outer_sfi_16.append("1")
                    que1['APPL']="1"
                    cat_gg['appl']="1"  
                    del notetext[b]
                if re.search('reissue\s*(.*)revalidation',cat_kk[key][rb][b],flags=re.IGNORECASE) and not "published" in cat_kk[key][rb][b].lower():
                    #print(rbd)
                    for r in range(len(rbd)):
                        cat_tag.append({"rbd":rbd[r],"cat_tag":"X","cat":16,"pos":pos})
                    que1['APL_VOL']="X"
                    cat_gg['apl_vol']="X"
                    # #print(len(notetext))
                    # #print(notetext)
                    outer_sfi_16.append("1")
                    for i in range(len(notetext)):
                        if re.search('reissue\s*(.*)revalidation',notetext[i],flags=re.IGNORECASE):
                            del notetext[i]
                            break
                elif re.search("no\s*(.*)show", cat_kk[key][rb][b],flags=re.IGNORECASE) is None :
                    if re.search('free',cat_kk[key][rb][b],flags=re.IGNORECASE):
                        for i in range(len(notetext)):
                            if re.search('free',notetext[i],flags=re.IGNORECASE) and re.search("no\s*(.*)show", notetext[i],flags=re.IGNORECASE) is None:
                                del notetext[i]
                                break
                    elif re.findall('\d*\.?\d+',cat_kk[key][rb][b])!=[]:
                        amount=re.findall('\d*\.?\d+',cat_kk[key][rb][b])
                        if len(amount)>0:
                            notetext.remove(cat_kk[key][rb][b])
                            amount=amount[0]
                            currency=cat_kk[key][rb][b][cat_kk[key][rb][b].index(amount[0])-4:cat_kk[key][rb][b].index(amount[0])].strip()
                            decimal=int(pdss_db.JUP_DB_Currency_Decimal_Master.find_one({"currency":str(currency)})['decimal'])                            
                            amount=str(int(amount.split(".")[0]))
                            amount=amount.ljust(decimal+len(amount), '0').zfill(7)
                            decimal=str(decimal) 
            # #print(amount,currency,decimal,rb,key)
            # #print(que1)
            cat_gg['sfi_flag']=sfi_flag
            notetext=" ".join(notetext)
            notetext = notetext.upper().split()
            x=[idx for idx, s in enumerate(notetext) if "PERMITTED" in s]
            if len(x)>0:
                del notetext[x[0]]
                del notetext[x[0]-1]
            notetext=" ".join(notetext)
            if sfi_flag:
                #print("nh")
                que1["AMT_1"]=amount
                cat_gg["amt_1"]=amount
                que1['CUR_1']=currency
                cat_gg['cur_1']=currency
                que1['DEC_1']=decimal
                cat_gg['dec_1']=decimal
                q1=atpco_db.Summary_996.find_one({"TEXT":notetext})
                if q1:
                    que1['TEXT_TBL_NO']=q1['TABLE']
                    cat_gg['text_tbl_no']=q1['TABLE']
                    q2=atpco_db.JUP_ATPCO_Record_3_Cat_16.find_one(que1)
                    if q2!=None:
                        cat_gg['tbl_16']=q2['TBL_NO']
            cat_gg['text']=cat_kk[key][rb]
            cat_gg['note_text']=notetext
            cat_l.append(cat_gg)
            #print(cat_tag)
    return cat_l,cat_tag,outer_sfi_16
def transform_rules(raw_db, pdss_db, atpco_db, corporate_name,raw_record_id, file_date, file_time, system_date, system_time):
    #getting the result from instruction
    raw_query = raw_db.corporate.instruction.find_one({'_id': raw_record_id})
    tar = list(raw_db.corporate.instruction.aggregate([{"$match":{"_id":raw_record_id}},
    {"$unwind":"$fare_sheet.sheets"},
    {"$unwind":"$fare_sheet.sheets.sheet_data"},
    {"$group":{"_id":None,
        "destination":{"$addToSet":"$fare_sheet.sheets.sheet_data.destination"}}}
    ]))[0]["destination"]
    
    # #print(q0)
    cat_list=raw_db.corporate.sfi.find_one({"corporate_name":corporate_name,"file_date":file_date,"file_time":file_time})['cover_sheet_cat']
    print(cat_list)
    cat_18,cat_06,cat_07,cat_19,cat_10,cat_26,cat_05,cat_16,rule_id,cat_tag=[],[],[],[],[],[],[],[],[""],[]
    outer_sfi_18,outer_sfi_06,outer_sfi_07,outer_sfi_05,outer_sfi_16=[],[],[],[],[]
    exclusions=[]
    code_name_query=raw_db.private.tariff.master.find_one({"_id":ObjectId("5fcf676f38b333dcab4fccc2")})
    code=code_name_query['code']
    name=code_name_query['name']
    #for cat_6_7
    req = list(raw_db.cat_6_7.find({}))
    re18=list(raw_db.cat_18_temp.find({}))
    re18=pd.DataFrame(re18)
    # #print(req)
    #getting the rule id 
    surya=0
    rule_query=raw_db.JUP_DB_Corporate_Info.find_one({"corporate_name":{"$regex":re.sub('[^A-Za-z0-9]+', "\\\s*(.*)",corporate_name).lower()}})
    print(rule_query)
    if rule_query:
        rule_id=rule_query['rule']
    # getting the distinct pos
    distinct_pos = set()
    for pos in range(len(raw_query['rules_notes']['pos_sheet_rules'])):
        distinct_pos.add(raw_query['rules_notes']['pos_sheet_rules'][pos]['field_key'])
    distinct_pos = list(distinct_pos)
    #for every each and every distinct pos iterating through the rules
    for each in range(len(distinct_pos)):
        pos_notes,cats_list=[],[]
    #getting loc1 value
        pos=distinct_pos[each].lower()[:re.search(r"specific",distinct_pos[each] ,flags=re.IGNORECASE).start()].replace("pos","").replace("poo","").strip().upper().replace("_","")
    #getting tariffs for the given pos
        if pos=="US":
            tariff=[("864","FBRINPV")]
        else:    
            pos_code=atpco_db.JUP_DB_ATPCO_Zone_Master.find_one({"CITY_CNTRY":pos})['CITY_AREA']
            # tariff=[(code[pos_code+i],name[pos_code+i]) for i in ["1","2","3"]]
            tariff=[(code[pos_code+i],name[pos_code+i]) for i in ["1","2","3"]]
            if any("Rest of World" in a for a in tar):
                tariff = tariff+[("864","FBRINPV")]

        for line in range(len(raw_query['rules_notes']['pos_sheet_rules'])):
            if raw_query['rules_notes']['pos_sheet_rules'][line]['field_key'] == distinct_pos[each]:
                pos_notes.append(raw_query['rules_notes']['pos_sheet_rules'][line]['field_value'])       
        for cat in range(len(pos_notes)): 
            if pos_notes[cat].startswith('Cat') or pos_notes[cat].startswith('CAT'):
                cats_list.append(pos_notes[cat])
            if re.search("^EXCLUDED\s*DESTINATIONS:",pos_notes[cat],flags=re.IGNORECASE):
                exclusions.append({"pos":pos,"exclusions":pos_notes[cat].replace("^EXCLUDED DESTINATIONS:","")})
        cat_data=split_data(pos_notes,cats_list)
    #iterating through each and every category of rules
        atpco_querys=raw_db.corporate.patterns.find_one({"_id":ObjectId("5fcf4f3638b333dcab4f6bde")})
        for key in cat_data:
            # print(cat_data[key])

            if re.search("18",key,flags=re.IGNORECASE) is not None:
                rule_db_flag,col,query=record_2_rule("018",rule_id,atpco_querys,atpco_db)
                cat_list.append("18")
                a=time.time()
                cat_18,cat_tag,outer_sfi_18=cat18(raw_db,pdss_db,atpco_db,corporate_name,cat_data[key],tariff,rule_id,pos,cat_tag,rule_db_flag,query,col,cat_18,outer_sfi_18)
            if re.search("5",key,flags=re.IGNORECASE) is not None:
                surya="5"
                rule_db_flag,col,query=record_2_rule("005",rule_id,atpco_querys,atpco_db)
                cat_list.append("05")
                cat_05,outer_sfi_05,cat_tag=cat_5(raw_db,pdss_db, atpco_db, corporate_name,cat_data[key],tariff,rule_id,pos,cat_tag,rule_db_flag,query,col,cat_05,outer_sfi_05)

            if re.search("06",key,flags=re.IGNORECASE) is not None:
                rule_db_flag,col,query=record_2_rule("006",rule_id,atpco_querys,atpco_db)
                cat_list.append("06")
                cat_06,cat_tag,outer_sfi_06=cat6_cat7(req,raw_db,pdss_db,atpco_db,corporate_name,cat_data[key],tariff,rule_id,pos,cat_tag,rule_db_flag,query,col,6,cat_06,outer_sfi_06)

            if re.search("07",key,flags=re.IGNORECASE) is not None:
                rule_db_flag,col,query=record_2_rule("007",rule_id,atpco_querys,atpco_db)
                cat_list.append("07")
                cat_07,cat_tag,outer_sfi_07=cat6_cat7(req,raw_db,pdss_db,atpco_db,corporate_name,cat_data[key],tariff,rule_id,pos,cat_tag,rule_db_flag,query,col,7,cat_07,outer_sfi_07)

            if re.search("16",key,flags=re.IGNORECASE) is not None:
                rule_db_flag,col,query=record_2_rule("016",rule_id,atpco_querys,atpco_db)
                cat_list.append("16")
                cat_16,cat_tag,outer_sfi_16=cat16(raw_db,pdss_db, atpco_db, corporate_name,cat_data[key],tariff,rule_id,pos,cat_tag,rule_db_flag,query,col,cat_16,outer_sfi_16)
        rule_db_flag,col,query=record_2_rule("019",rule_id,atpco_querys,atpco_db)
        cat_19,cat_list=cat19(raw_db,corporate_name,tariff,rule_id,pos,pos_notes,cat_19,raw_query,cat_list,cat_data,rule_db_flag)
        cat_10,cat_26,cat_list=cat_10_26(raw_db,corporate_name,pos,pos_notes,raw_query,cat_list,cat_data,cat_10,cat_26)
    cat_18=get_explode_df(cat_18,outer_sfi_18)
    if len(cat_18)!=0:
        cat_18=pd.DataFrame(cat_18)
        cat_18['tbl_no']=cat_18.apply(lambda x : rec_2(x['loc1'],x['fare_family'],x['tariff_code'],x['rule_id'],x['tbl_no'],re18),axis=1)
        cat_18=cat_18.to_dict("records")
    # print(time.time()-stt)
    cat_06=get_explode_df(cat_06,outer_sfi_06)
    print(cat_06)
    print(cat_18,cat_06)
    print(len(cat_06))
    cat_07=get_explode_df(cat_07,outer_sfi_07)
    print(cat_07)
    cat_05=get_explode_df(cat_05,outer_sfi_05)
    cat_16=get_explode_df(cat_16,outer_sfi_16)
    cat_19=get_explode_df(cat_19)
    cat_10=get_explode_df(cat_10)
    cat_26=get_explode_df(cat_26)
    print(cat_tag,cat_list)
    cat_list=list(set(cat_list+["01"]))
    raw_db.corporate.sfi.update_one({"corporate_name":corporate_name, "file_date": file_date, "file_time": file_time},{"$set":{"cat_18_trans":cat_18,"cat_10":cat_10,"cat_06":cat_06,"cat_07":cat_07,
        "cat_19":cat_19,"cat_26":cat_26,"cat_05":cat_05,"cat_16":cat_16,"cat_tag":cat_tag,"cover_sheet_cat":cat_list,"exclusions":exclusions}})
if  __name__=="__main__":
    from bson.objectid import ObjectId
    st=time.time()
    client = pymongo.MongoClient(
        '3.6.201.161:27022',
        username='data.EK',
        password='data@123',
        authSource='admin',
        authMechanism='SCRAM-SHA-1'
    )
    raw_db=client['rawDB_prod']
    pdss_db=client['PDSS_EK']
    atpco_db=client['ATPCO_EK']
    corporate_name="PT Wira Cipta Perkasa"
    system_date=""
    system_time=""
    file_date=""
    file_time=""
    raw_record_id=ObjectId("602e0956bda5bb14e00cfe89")
    transform_rules(raw_db, pdss_db, atpco_db, corporate_name,raw_record_id, file_date, file_time, system_date, system_time)
    print("time",time.time()-st)
    # ObjectId("5fc601d8c980639aae96fefe")


