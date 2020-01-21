import json
import os
import datetime
from urllib.parse import urlencode
from botocore.vendored import requests
import db_statements
import main
import util as util
import constant as constant
import pytz
import pymysql

# The value maps for incoming data values and outgoind data values
gpsRecordKeyValueMap = {"speedMPH": "speed", "latitude": "latitude", "longitude": "longitude",
                        "gpsStatus": "IsGpsValid", "deviceStatus": "IsIgnitionOn"}
statusRecordCodes = {"backupPowerV": constant.BACKUPPOWERV, "mainPowerV": constant.MAINPOWERV, "RSSI": constant.RSSI,
                     "internalTemperatureC": constant.INTERNALTEMPERATUREC, "eventType": constant.TOW_DETECTION}
accelerationKeyValueMap = {"accelerometerXmm_s2": "X", "accelerometerYmm_s2": "Y", "accelerometerZmm_s2": "Z"}

sessionDetails = {}


class GeotabError(Exception):

    def __init__(self, *value):
        self.value = [a for a in value]

    def __str__(self):
        return (repr(self.value))


class GeotabSessionExpiredError(Exception):

    def __init__(self, *value):
        self.value = [a for a in value]

    def __str__(self):
        return (repr(self.value))


class GeotabDeviceDeletedError(Exception):

    def __init__(self, *value):
        self.value = [a for a in value]

    def __str__(self):
        return (repr(self.value))


def send(serialNo, event, conn, resellerId, communication_at, aws_request_id):
    try:
        sessionDetails = get_session_details(conn, resellerId)
        headers, payload = build_payload(serialNo, event, conn, sessionDetails)

        print("... payload: " + str(payload))

        response, payload = send_to_geotab(headers, payload)
        db_statements.insertIntoCommunicationLog(conn, event.get("deviceID"), communication_at, str(event), 1, None, 1,
                                                 str(aws_request_id), str(response), datetime.datetime.utcnow(),
                                                 str(payload))

    except GeotabSessionExpiredError as err:

        # get a new sessionId
        apiKey, sessionID = authenticate(conn, resellerId)

        # update the sessionId element to the new sessionID
        payload["params"]["sessionId"] = sessionID

        # retry with new
        try:
            response, payload = send_to_geotab(headers, payload)
            db_statements.insertIntoCommunicationLog(conn, event.get("deviceID"), communication_at, str(event), 1, None,
                                                     1, str(aws_request_id), str(response), datetime.datetime.utcnow(),
                                                     str(payload))

        except GeotabDeviceDeletedError as err:
            raise
        except GeotabError as err:
            raise
        except Exception as err:
            raise

    except GeotabDeviceDeletedError as err:
        print(err)
        db_statements.updateDeviceStatus(event.get("deviceID"), constant.ERROR_STATE,
                                         constant.GEOTAB_DELETED_ERROR_CODE, conn)
        db_statements.insertIntoCommunicationLog(conn, event.get("deviceID"), communication_at, str(event), 0, str(err),
                                                 0, str(aws_request_id), str(response), datetime.datetime.utcnow(),
                                                 str(payload))

        raise Exception(err)

    except GeotabError as err:
        db_statements.insertIntoCommunicationLog(conn, event.get("deviceID"), communication_at, str(event), 0, str(err),
                                                 0, str(aws_request_id), str(response), datetime.datetime.utcnow(),
                                                 str(payload))
        raise Exception(err)

    except Exception as err:
        print(err)
        db_statements.insertIntoCommunicationLog(conn, event.get("deviceID"), communication_at, str(event), 0, str(err),
                                                 0, str(aws_request_id), str(response), datetime.datetime.utcnow(),
                                                 str(payload))
        raise Exception(err)


def get_session_details(conn, resellerId):
    # Get the session details for Device
    sessionDetails = db_statements.get_session_details(conn, resellerId)

    if (sessionDetails is None or len(sessionDetails) == 0):
        return json.dumps(gr.__dict__)

    return sessionDetails


def authenticate(conn, resellerId):
    print("in authenticate")
    username, password = db_statements.getResellerCredentials(resellerId, conn)

    try:
        authenticationDataObject = {"method": "Authenticate",
                                    "params": {"username": username,
                                               "password": password}
                                    }

        payload = {"JSON-RPC": json.dumps(authenticationDataObject, separators=(',', ':'))}
        payload = urlencode(payload)

        headers = {
            "Cache-Control": "no-cache",
            "Content-Type": "application/x-www-form-urlencoded"
        }

        response = requests.post(url=os.environ['request_url'], data=payload, headers=headers)
        if response.status_code == 200 and "error" not in response.json():
            apiKey = response.json().get('result').get('userId')
            sessionID = response.json().get('result').get('sessionId')

            save_session_details_to_db(conn, apiKey, sessionID, 1, resellerId)
            return apiKey, sessionID

        else:
            print("Geotab Authentication Failed")
            raise ValueError(response.json().get("error").get("message"))
            return {
                'statusCode': 200,
                'body': json.dumps('success')
            }
    except Exception as err:
        print("in Exception in authenticate")


# Saved the received session details to DB in the table "geotab_session"        
def save_session_details_to_db(conn, apiKey, sessionID, isSuccess, resellerId):
    created_at = datetime.datetime.utcnow()

    try:
        with conn.cursor() as cur:
            update_query = """UPDATE geotab_session SET session_id = %s, created_at = %s, is_success = %s where api_key = %s and reseller_id = %s"""
            update_tuple = (sessionID, created_at, isSuccess, apiKey, resellerId)
            result = cur.execute(update_query, update_tuple)
            conn.commit()
            cur.close()
    except Exception as err:
        print(result)
        print(err)


def build_payload(serialNo, event, conn, sessionDetails):
    print("... entering build_payload")

    pst = pytz.timezone('America/Los_Angeles')
    utc = pytz.UTC

    try:
        dateTimePST = datetime.datetime.strptime(event['dateRTCPST'], '%m/%d/%Y %H:%M:%S')
        dt_pst = pst.localize(dateTimePST)
        dt_utc = dt_pst.astimezone(utc)
        dateTimeRTC = str(dt_utc)

        print("... event[dateRTCPST]: " + event['dateRTCPST'])
        print("... converting to UTC: " + str(dateTimeRTC))

        dateTimePST = datetime.datetime.strptime(event['dateGPSPST'], '%m/%d/%Y %H:%M:%S')
        dt_pst = pst.localize(dateTimePST)
        dt_utc = dt_pst.astimezone(utc)
        dateTimeGPS = str(dt_utc)

        print("... event[dateGPSPST]: " + event['dateGPSPST'])
        print("... converting to UTC: " + str(dateTimeGPS))

    except Exception as e:
        print('... error retrieving date :' + str(e))
        raise

    # payload for GPS Record
    thirdPartyGpsRecord = {
        "dateTime": dateTimeGPS,
        "serialNo": serialNo,
        "type": "ThirdPartyGpsRecord"
    }

    # Payload for Status Record
    thirdPartyStatusPayload = {
        "dateTime": dateTimeRTC,
        "serialNo": serialNo,
        "type": "ThirdPartyStatusRecord"
    }

    # Payload for acceleration record
    thirdPartyAccelerationRecord = {
        "dateTime": dateTimeRTC,
        "serialNo": serialNo,
        "type": "ThirdPartyAccelerationRecord"
    }

    payloadList = []
    gpsRecordFlag = False
    accelerationFlag = False

    for key, value in event.items():

        if (gpsRecordKeyValueMap.get(key)):

            # don't include gps data if lat/lng is 0; this is a CH special condition
            if (event.get("latitude") != 0 and event.get("longitude") != 0):

                gpsRecordFlag = True

                # map the CH variable to the Geotab
                attr = gpsRecordKeyValueMap.get(key)

                if (key == "gpsStatus"):
                    if (value == "Locked"):
                        thirdPartyGpsRecord[attr] = True
                elif (key == "deviceStatus"):
                    thirdPartyGpsRecord[attr] = (event.get("speedMPH") > 0)
                else:
                    thirdPartyGpsRecord[attr] = value

        # Preparing values for Status record
        elif (statusRecordCodes.get(key)):
            attr = statusRecordCodes.get(key)
            if (attr):
                newPayload = thirdPartyStatusPayload.copy()
                newPayload["code"] = attr
                if (attr is 5):
                    backupPower = event.get(key) * 1000
                    if (backupPower <= 3800):
                        newPayload["code"] = 1
                    newPayload["value"] = int(backupPower)
                elif (attr is 8):
                    newPayload["value"] = int(event.get(key) * 1000)
                elif (attr is 6):
                    newPayload["value"] = int(event.get(key) * 100)
                elif (attr is 2):
                    if ("Tow Alert" in event.get(key)):
                        newPayload["value"] = 1
                    else:
                        continue
                else:
                    newPayload["value"] = event.get(key)

                payloadList.append(newPayload)

        # Preparing values for Acceleration record
        else:
            attr = accelerationKeyValueMap.get(key)
            if (attr):
                accelerationFlag = True
                thirdPartyAccelerationRecord[attr] = value

    if (accelerationFlag):
        payloadList.append(thirdPartyAccelerationRecord)

    if (gpsRecordFlag):
        payloadList.append(thirdPartyGpsRecord)

    # Final Add Data Object
    addDataObject = {
        "method": "AddData",
        "params": {
            "sessionId": sessionDetails.get("sessionId"),
            "apiKey": sessionDetails.get("apiKey"),
            "recordsToAdd": payloadList
        }
    }

    headers = {
        "Cache-Control": "no-cache",
        "Content-Type": "application/x-www-form-urlencoded"
    }

    return headers, addDataObject


def send_to_geotab(headers, pre_payload):
    # Call to Geotab for Adding data
    print("... in send_to_geotab()")

    try:
        payload = {"JSON-RPC": json.dumps(pre_payload, separators=(',', ':'))}
        encodedPayload = urlencode(payload)
        response = requests.post(url=os.environ['request_url'], data=encodedPayload, headers=headers)
    except Exception as err:
        print("... " + str(err))
        raise

    print("... status code: " + str(response.status_code))
    print("... response: " + str(response.content))

    if ('error' in response.json() or 'error' in response.json().get('result')):

        error = response.json().get("error").get("errors")[0].get("name")
        message = response.json().get("error").get("errors")[0].get("message")

        print("error : " + error)
        print("message : " + message)

        try:
            if (error == "SessionExpiredException"):
                print("Session Expired Error")
                raise GeotabSessionExpiredError(response.content, message, payload)
            elif (error == "NotSupportedException"):
                print("Not supported exception")
                raise GeotabError(response.content, message, payload)
            elif (error == "Exception"):
                raise Exception
        except GeotabSessionExpiredError:
            raise
        except GeotabError:
            raise
        except Exception:
            raise

    print("... call to Geotab a success")
    return response.content, payload
