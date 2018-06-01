from allprocessing.allprocessingmain import app
from flask import redirect, request,make_response
from datetime import datetime
from flask import jsonify

import jwt
import requests
import json
      
def validatetoken(request):
    
    if 'Authorization' in request.headers:

        natjwtfrhead=request.headers.get('Authorization')
        if natjwtfrhead.startswith("Bearer "):
            natjwtfrheadf =  natjwtfrhead[7:]
        natjwtdecoded = jwt.decode(natjwtfrheadf, verify=False)        
        userid=natjwtdecoded['uid']
        entityid='IN'
        #entityid=natjwtdecoded['entityid']
        if  (not userid) or (userid ==""):
            dbqerr={}
            dbqerr['natstatus'] == "error"
            dbqerr['statusdetails']="No user id in request"
            resp = make_response(jsonify(dbqerr), 400)
            return(resp)
        print(userid,entityid)
        return userid,entityid
