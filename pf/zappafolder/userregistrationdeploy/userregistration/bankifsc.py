from userregistration.userregistrationmain import app
from flask import redirect, request,make_response
from datetime import datetime
from flask import jsonify

from userregistration import dbfunc as db
import psycopg2
import jwt
import requests
import json


      
@app.route('/bankdet',methods=['GET','POST','OPTIONS'])
def bankdets():
#This is called by setjws service
    if request.method=='OPTIONS':
        print("inside bankdets options")
        return 'ok'

    elif request.method=='POST':
        print("inside bankdetails POST")
        payload= request.get_json()
        print(payload)
        reqdataifsc=payload['ifsc']
        print(reqdataifsc)
        print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        
        con,cur=db.mydbopncon()

        command = cur.mogrify("select * from bankifscmaster where ifsc = %s;",(reqdataifsc,) )
        cur, dbqerr = db.mydbfunc(con,cur,command)
        print(cur)
        print(dbqerr)
        print(type(dbqerr))
        print(dbqerr['natstatus'])

        if cur.closed == True:
            if(dbqerr['natstatus'] == "error" or dbqerr['natstatus'] == "warning"):
                dbqerr['statusdetails']="IFSC Fetch failed"
            resp = make_response(jsonify(dbqerr), 400)
            return(resp)
        else:
            pass
        
        records=[]
        for record in cur:  
            print('inside for')
            print(record)             
            records.append(record)

        print(len(records))

        #if len(records) == 0:
        if cur.rowcount == 0:
            bank, ifsc, micr, branch, address, contact, city, district, state, entity = ['']*10
            failed=True
            errormsg='Not a valid IFSC'
        else:
            bank, ifsc, micr, branch, address, contact, city, district, state, entity = records[0]
            failed=False
            errormsg=''
        
        bankdetailresp = bank+' '+address+'    '+city+' '+state
        print(bankdetailresp)
        
        return (json.dumps({'bank':bank,'ifsc':ifsc,'micr':micr,'branch':branch,'address':address,'contact':contact, 'city':city, 'district':district, 'state':state,'failed':failed,'errormsg':errormsg}))
    
