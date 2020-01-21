import json
import os
import pymysql
import datetime
from urllib.parse import urlencode
#from botocore.vendored import requests
#import boto3
import db_statements
import geotab_calls
import util as util
import constant as constant


#Initiating SQL connection


def isEmpty(value):
        return not IsNotNull(value)

def IsNotNull(value):
    return value is not None and len(value) > 0

#Main method of Lambda
def lambda_handler(in_event, context):

        try:
                gr = GatewayResponse()

                rds_host = os.getenv('rds_host','172.20.4.139')
                db = os.getenv('db_name','dev_jdbc')
                user = os.getenv('db_user','dev_geotab_gateway')
                passwd = os.getenv('db_password','Pct#2019D3v!')
                productId = os.getenv('productId','10048')
                request_url = os.getenv('request_url','https://myadminapitest.geotab.com/v2/MyAdminApi.ashx')

                conn = pymysql.connect(rds_host, user, passwd, db)

        #        conn = pymysql.connect(os.environ['rds_host'], user=os.environ['db_user'], passwd=os.environ['db_password'],
        #                               db=os.environ['db_name'], connect_timeout=10)

                communication_at = datetime.datetime.utcnow()
                print("***incoming event***")
                print("... in_event: " + str(in_event))

                event = in_event["body"]
                print("... event: " + str(event))

                lambda_id = str(context.aws_request_id)

                device_id = event.get(constant.DEVICE_ID)
                gr.deviceId = device_id

                if(isEmpty(device_id)):
                        gr.setResponse(False,constant.INVALID_DEVICE_ID)
                        return json.dumps(gr.__dict__)
                        
                # get the device details from DB
                device = db_statements.get_device(device_id, conn)
                if(device is None):
                        print("... device not registered")
                        print("... inserting unregistered device")

                        db_statements.insertUnregisteredDevice(device_id, conn)
                        db_statements.insertIntoCommunicationLog(conn, device_id, communication_at, str(event), 0, constant.DEVICE_ID_NOT_REGISTERED, 0, lambda_id, None, None, None)
                        gr.setResponse(False,constant.DEVICE_ID_NOT_REGISTERED)
                        return json.dumps(gr.__dict__)
                        
                if(device.platformSerialNum is None):
                        gr.setResponse(False,constant.DEVICE_ID_NOT_REGISTERED)
                        return json.dumps(gr.__dict__)
                
                print('... device details : ' + json.dumps(device.__dict__))
                
                if(IsNotNull(device.state) and device.state == constant.DEVICE_STATUS_ERRORNEOUS):
                        gr.setResponse(False,"Device is in errorneous state, unable to process.")
                        return json.dumps(gr.__dict__)
                
                if(device.isDeleted is not None and device.isDeleted == 1):
                        gr.setResponse(False,"Device is in deleted state, unable to process.")
                        return json.dumps(gr.__dict__)
                
                if((device.isCompanyActive is not None and device.isCompanyActive == 0)
                        and (device.isCompanyDeleted is not None and device.isCompanyDeleted == 0)):
                        gr.setResponse(False,"Fleet to which device  associated is inactive or deleted, unable to process.")
                        return json.dumps(gr.__dict__)
                        
                if(IsNotNull(device.platformSerialNum) and IsNotNull(device.state)):
                        if(device.state ==constant.DEVICE_STATUS_PENDDING):
                                print("... device needs to be activated")
                                db_statements.updateDeviceStatus(device_id, constant.DEVICE_STATUS_ACTIVE, None, conn)
                                
                if(device.resellerId is None):
                        gr.setResponse(False, "No Reseller found for the device, unable to process.")
                        return json.dumps(gr.__dict__)
                
                # sending device data to geotab       
#                geotab_calls.send(device.platformSerialNum, event, conn, device.resellerId, communication_at, context.aws_request_id)

#        except GeotabError as err:
#                print("... in ch_processing exception" + str(err))
#                gr.setResponse(False,"Error recieved from geotab while sending message")
#                return json.dumps(gr.__dict__)
        except Exception as err:
                print("... " + str(err))
                gr.setResponse(False,"Error ")
                return json.dumps(gr.__dict__)
        else:
                gr.setResponse(True,"Success")
                return json.dumps(gr.__dict__)


class GatewayResponse(object):
        def __init__(self, deviceId="", success=False, message="", errorCode=""):
                self.deviceId = deviceId
                self.success = success
                self.message = message
                self.errorCode = errorCode
                
        def setResponse(self, success = False,message=""):
                self.success = success
                self.message = message
