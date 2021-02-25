from Coversheet import parse_coversheet
from Notes import notes2, rules
from CorporateFareSheetParser import parse_fares
from IATAParser import parse_iata
from CoversheetTransformer import transform_cs
from IATA_Transformer import trans_iata
from FareRuleSheetTransformer import transform_pos
from RulesTransformer import transform_rules 
from IdentifySheets import identify_sheets
from CoverSheetRulesParser import parse_coversheet_rules
import Utils
from bson.objectid import ObjectId
import datetime
import traceback
import time
import pymongo
from pymongo import MongoClient

def get_db_client():

    client = MongoClient(
            '3.6.201.161:27022',
            username='data.EK',
            password='data@123',
            authSource='admin',
            #authMechanism='SCRAM-SHA-256'
        )

    return client
client = get_db_client()
raw_db = client['rawDB_prod']
atpco_db=client["ATPCO_EK"]
pdss_db = client["PDSS_EK"]

def parse(excel_file):

    try:
        SYSTEM_DATE = datetime.date.today().strftime("%Y-%m-%d")
        SYSTEM_TIME = datetime.datetime.now().strftime("%H:%M:%S")
        # SYSTEM_TIME = now.strftime("%H:%M:%S")
        FILE_DATE = datetime.date.today().strftime("%Y-%m-%d")
        FILE_TIME = datetime.datetime.now().strftime("%H:%M:%S")
        logger = Utils.get_logger("CorporateTemplateProcessor")
        print("parsing excel ", excel_file)
        logger.info("parsing excel " + excel_file)
        p_start = time.time()

        # identify sheet names
        logger.info("sheet identification script started")
        f_start = time.time()
        name_details = identify_sheets(excel_file, raw_db)
        logger.info("sheet identification script ended, took : " + str(time.time() - f_start))

        # parse raw coversheet
        if "cover_sheet" in name_details and len(name_details["cover_sheet"]) > 0:
            logger.info("started parsing cover sheet")
            f_start = time.time()
            corporate_name, coversheet_id = parse_coversheet(raw_db, excel_file, SYSTEM_DATE, SYSTEM_TIME, FILE_DATE, FILE_TIME, name_details["cover_sheet"][0])
            print('corporate_name', corporate_name)
            logger.info("cover sheet parsing ended, took : " + str(time.time() - f_start))
            logger.debug("coporate name : %s", corporate_name)
        else:
            logger.error("cover sheet not found in the template")

        # temprorary workaround for barclays GB sheet
        if corporate_name == "Barclays":
            name_details["fare_sheet"].remove("GB")

        if "cover_sheet" in name_details and len(name_details["cover_sheet"]) > 0:
            logger.info("started parsing cover sheet rules")
            f_start = time.time()
            coversheet_rule_id = parse_coversheet_rules(raw_db, excel_file, corporate_name, SYSTEM_DATE, SYSTEM_TIME, FILE_DATE, FILE_TIME, name_details["cover_sheet"][0])
            logger.info("cover sheet rule parsing ended, took : " + str(time.time() - f_start))

        pos_sheet_rules_exist = False
        if "fare_sheet" in name_details and len(name_details["fare_sheet"]) > 0:
            logger.info("started parsing fare sheet rules")
            f_start = time.time()
            pos_sheet_rules_exist, notes2__id = notes2(excel_file, corporate_name, raw_db, SYSTEM_DATE, SYSTEM_TIME, FILE_DATE, FILE_TIME, name_details["fare_sheet"])
            logger.info("fare sheet rules parsing ended, took : " + str(time.time() - f_start))

        logger.info("started paring notes sheet")
        f_start = time.time()
        rules_id = rules(excel_file, corporate_name, raw_db, SYSTEM_DATE, SYSTEM_TIME, FILE_DATE, FILE_TIME, pos_sheet_rules_exist)
        logger.info("notes sheet parsing ended, took : " + str(time.time() - f_start))

        # parse raw faresheet
        if "fare_sheet" in name_details and len(name_details["fare_sheet"]) > 0:
            logger.info("started parsing fare sheet")
            f_start = time.time()
            fares_id = parse_fares(excel_file, corporate_name, raw_db, SYSTEM_DATE, SYSTEM_TIME, FILE_DATE, FILE_TIME, name_details["fare_sheet"])
            logger.info("fare sheet parsing ended, took : " + str(time.time() - f_start))

        # parse raw iatasheet
        if "iata_sheet" in name_details and len(name_details["iata_sheet"]) > 0:
            logger.info("started parsing iata sheet")
            f_start = time.time()
            iata_id = parse_iata(excel_file, corporate_name, raw_db, SYSTEM_DATE, SYSTEM_TIME, FILE_DATE, FILE_TIME, name_details["iata_sheet"][0])
            logger.info("iata sheet parsing ended, took : " + str(time.time() - f_start))

        coversheet_data = raw_db['corporate.coversheet'].find_one(
            {
                '_id' :  ObjectId(coversheet_id)
            },
            {'cover_sheet': 1, '_id': 0}
        )

        rulesheet_data = raw_db['corporate.rulesheet'].find_one(
            {
                'dummy_id' : rules_id
            },
            {'_id': 0, 'system_date': 0, 'system_time': 0, 'file_date': 0, 'file_time': 0, 'corporate_name': 0}
        )

        faresheet_data = raw_db['corporate.faresheet'].find_one(
            {
                '_id': ObjectId(fares_id)
            },
            {'sheets': 1, '_id': 0}
        )

        iatasheet_data = raw_db['corporate.iatasheet'].find_one(
            {
            '_id': ObjectId(iata_id)
            },
            {'IATA_List': 1, '_id': 0}
        )

        raw_record_id = raw_db['corporate.instruction'].insert_one(
            {
                'cover_sheet': coversheet_data.get('cover_sheet', {}),
                'fare_sheet': faresheet_data,
                'IATA_List': iatasheet_data.get('IATA_List', {}),
                'rules_notes': rulesheet_data,
                'system_date': SYSTEM_DATE,
                'system_time': SYSTEM_TIME,
                'file_time':FILE_TIME,
                'file_date': FILE_DATE,
                'corporate_name': corporate_name,
                #"cover_sheet_cat":raw_db['temp'].find_one({"corporate_name":corporate_name})['cover_sheet_cat']
            }
        )
        print(raw_record_id.inserted_id)

        return {
            "corporate_name": corporate_name,
            "file_date": FILE_DATE,
            "file_time": FILE_TIME,
            "raw_record_id": str(raw_record_id.inserted_id)
        }
    except Exception as e:

        print(e)
        print(traceback.print_exc())
        print("error occured: deleting inserted records in raw dbs")
        raw_db['corporate.coversheet'].remove({
            'corporate_name': corporate_name,
            'file_date': FILE_DATE,
            'file_time': FILE_TIME
        })

        raw_db['corporate.rulesheet'].remove({
            'corporate_name': corporate_name,
            'file_date': FILE_DATE,
            'file_time': FILE_TIME
        })

        raw_db['corporate.faresheet'].remove({
            'corporate_name': corporate_name,
            'file_date': FILE_DATE,
            'file_time': FILE_TIME
        })

        raw_db['corporate.iatasheet'].remove({
            'corporate_name': corporate_name,
            'file_date': FILE_DATE,
            'file_time': FILE_TIME
        })

        raw_db['corporate.instruction'].remove({
            'corporate_name': corporate_name,
            'file_date': FILE_DATE,
            'file_time': FILE_TIME
        })

        return {
        "msg": "parsing of template failed",
        "error": str(e)
        }

def transform(corporate_name, raw_record_id, file_date, file_time, system_date, system_time):

    try:
        print('transformation of cover sheet started')
        transform_cs(raw_db, atpco_db, pdss_db, corporate_name, raw_record_id, file_date, file_time, system_date, system_time)
        print('transformation of cover sheet ended')

        print("transformation of rules started")
        transform_rules(raw_db, pdss_db, atpco_db, corporate_name, raw_record_id, file_date, file_time, system_date, system_time)
        print("transformation of rules ended")

        print('transformation of fare and rule sheet started')
        transform_pos(raw_db, pdss_db, atpco_db, corporate_name, raw_record_id, file_date, file_time, system_date, system_time)
        print('transformation of fare and rule sheet ended')

        print('transformation of iata sheet started')
        trans_iata(raw_db, atpco_db, pdss_db, corporate_name, raw_record_id, file_date, file_time, system_date, system_time)
        print('transformation of iata sheet ended')


        # print("transformation of category started")
        # cat_trans(raw_db, corporate_name, raw_record_id, file_date, file_time, system_date, system_time)
        print(corporate_name)

        raw_db.corporate.sfi.update(
            {"corporate_name": corporate_name, "file_date": file_date, "file_time": file_time},
            {
                '$set': {
                    "raw_record_id": raw_record_id
                }
            }
        )
        transformed_record_id = raw_db.corporate.sfi.find_one({"corporate_name": corporate_name, "file_date": file_date, "file_time": file_time},{"_id": 1})["_id"]

        return {
            "msg": "transformation done",
            "transformed_record_id": str(transformed_record_id)
        }

    except Exception as e:
        print("error occured")
        print(e)
        print(traceback.print_exc())

        raw_db.corporate.sfi.remove({
            "corporate_name": corporate_name,
            "file_date": file_date,
            "file_time": file_time
            })

        return {
            "msg": "tranfromation failed",
            "error": str(e)
        }
