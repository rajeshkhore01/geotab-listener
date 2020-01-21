import pymysql
import os
import json
import datetime
import sys
import geotab_calls
import constant as constant

sessionDetails = {}

#Get Geotab session details from the DB of the reseller
def get_session_details(conn, resellerId):
    with conn.cursor() as cur:
        query = """SELECT session_id, api_key FROM geotab_session WHERE reseller_id = %s"""
        query_tuple = resellerId
        result  = cur.execute(query, query_tuple)

        for row in cur:
            sessionDetails["sessionId"] = row[0]
            sessionDetails["apiKey"] = row[1]
            cur.close()
            
    return sessionDetails

#Get the device details from DB    
def get_device(deviceId, conn):
        print("... getting device info from database")
        with conn.cursor() as cur:
                #select d.geotab_serial_no, d.current_state, d.is_terminated, d.is_deleted,  c.company_status, c.is_deleted from device d left join companies c on c.id = d.fleet_id  where d.id = '007'
                query = """select d.geotab_serial_no, d.current_state, d.is_terminated, d.is_deleted, c.company_status, c.is_deleted, d.reseller_id from device d left join company c on c.id = d.fleet_id  where d.id = %s"""
                query_tuple = (deviceId)
                result  = cur.execute(query, query_tuple)
                cur.close()
                if(result is 0):
                        print("... device not found in database")
                        # insertUnregisteredDevice(deviceId, conn)
                        return None
                        
                for row in cur:
                        d = Device(row[0],row[1],row[2],row[3],row[4],row[5],row[6])
                        return d

#If the incoming device is not registered, the device will be added with erroneous state
def insertUnregisteredDevice(deviceId, conn):
        try:
                with conn.cursor() as cur:
                        insert_query = """INSERT INTO device(id, current_state,error_code,model,created_at) values (%s, %s, %s, %s, %s)"""
                        insert_tuple = (deviceId, constant.ERROR_STATE, constant.UNREGISTERED_ERROR_CODE, constant.DEFAULT_DEVICE_MODEL, datetime.datetime.utcnow())
                        result  = cur.execute(insert_query, insert_tuple)
                        
                        conn.commit()
                        cur.close()
        except Exception as err:
                print('error in inserting unregistered device ' + str(err))

# Function to change the state of the device in DB
def updateDeviceStatus(deviceId, deviceStatus, errorCode, conn):
        try:
                if(errorCode is 105):
                        newDeviceId = str(deviceId) + "-old"
                        update_query = """UPDATE device SET current_state = %s, error_code = %s, id = %s, updated_at = %s WHERE id = %s"""
                        query_tuple = (deviceStatus, errorCode, newDeviceId, datetime.datetime.utcnow(), deviceId)
                else:
                        update_query = """UPDATE device SET current_state = %s, error_code = %s, updated_at = %s WHERE id = %s"""
                        query_tuple = (deviceStatus, errorCode, datetime.datetime.utcnow(), deviceId)
                with conn.cursor() as curs:
                        result  = curs.execute(update_query, query_tuple)
                        conn.commit()
                        curs.close()
        except Exception as err:
                print('... error updating device status' + str(err))
                
#Function to insert Communication Logs into DB 
def insertIntoCommunicationLog(conn, device_id, communication_at, request_payload, response_status, error_msg, comm_status, lambda_id, platform_response, platformResponseAt, geotab_payload):
        with conn.cursor() as cur:
                insert_query = """INSERT INTO communication_log(device_id, communication_at, request_payload, geotab_status, error_message, communication_status, lambda_id, geotab_response, geotab_response_at, geotab_payload) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
                #insert_tuple = (event.get("deviceID"), communication_at, str(event), 0, str(err.value[1]), 1, str(context.aws_request_id), str(err.value[0]), datetime.datetime.utcnow())
                insert_tuple = (device_id, communication_at, request_payload, response_status, error_msg, comm_status, lambda_id, platform_response, platformResponseAt, geotab_payload)
                result  = cur.execute(insert_query, insert_tuple)
                conn.commit()
                cur.close()
                
#Gets info about the reseller required for Geotab authentication
def getResellerCredentials(resellerId, conn):

    with conn.cursor() as cur:
        select_query = "SELECT user_id, password FROM reseller WHERE company_id = %s"
        query_tuple = (resellerId)
        result  = cur.execute(select_query, query_tuple)
        cur.close()

        if(result is 0):
                print("... reseller not found in database")
                return None
                        
        else:
                for row in cur:
                        return row[0], row[1] 

class Device(object):
        def __init__(self, platformSerialNum="", deviceState="",isTerminated ="", isDeleted="", isCompanyActive="",isCompanyDeleted="", resellerId=""):
                self.platformSerialNum = platformSerialNum
                self.state = deviceState
                self.isTerminated = isTerminated
                self.isDeleted = isDeleted
                self.isCompanyActive = isCompanyActive
                self.isCompanyDeleted = isCompanyDeleted
                self.resellerId = resellerId
