from userregistration import app
from flask import redirect, request,make_response
from datetime import datetime
from flask import jsonify

import jwt
import requests
import json
      
def validatetoken(request):
    print('inside validatetoken',request.headers)
    if 'Authorization' in request.headers:
        print('inside aut')
        natjwtfrhead=request.headers.get('Authorization')
        if natjwtfrhead.startswith("Bearer "):
            natjwtfrheadf =  natjwtfrhead[7:]
        natjwtdecoded = jwt.decode(natjwtfrheadf, verify=False)        
        userid=natjwtdecoded['uid']
        entityid='IN'
        print('getting value')
        print(userid,entityid)
        #entityid=natjwtdecoded['entityid']
        if  (not userid) or (userid ==""):
            dbqerr['natstatus'] == "error"
            dbqerr['statusdetails']="No user id in request"
            resp = make_response(jsonify(dbqerr), 400)
            return(resp)
        return userid,entityid