import json
import datetime

def IsNotNull(value):
    return value is not None and len(value) > 0

def isEmpty(value):
        return not IsNotNull(value)