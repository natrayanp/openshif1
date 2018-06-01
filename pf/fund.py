from app import app
#from .hello_world import app
from flask import request, make_response, jsonify, Response, redirect
from datetime import datetime
import dbfunc as db
import jwtdecodenoverify as jwtnoverify
from dateutil import tz
from datetime import datetime
from datetime import date
from multiprocessing import Process

import psycopg2
import json
import jwt
import time

@app.route('/funddatafetch',methods=['GET','POST','OPTIONS'])
def funddatafetch():
#This is called by fund data fetch service
    if request.method=='OPTIONS':
        print("inside FUNDDATAFETCH options")
        return make_response(jsonify('inside FUNDDATAFETCH options'), 200)  

    elif request.method=='POST':
        print("inside PFDATAFETCH GET")
        
        print((request))        
        print(request.headers)
        userid,entityid=jwtnoverify.validatetoken(request)
        print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        print(userid,entityid)
        payload= request.get_json()
        print(payload)
        print('after')
        con,cur=db.mydbopncon()

        print(con)
        print(cur)
        teee='%'+payload+'%'
        #cur.execute("select row_to_json(art) from (select a.*, (select json_agg(b) from (select * from pfstklist where pfportfolioid = a.pfportfolioid ) as b) as pfstklist, (select json_agg(c) from (select * from pfmflist where pfportfolioid = a.pfportfolioid ) as c) as pfmflist from pfmaindetail as a where pfuserid =%s ) art",(userid,))
        #command = cur.mogrify("select row_to_json(art) from (select a.*,(select json_agg(c) from (select * from webapp.fundsipdt where sipfndnatcode = a.fndnatcode ) as c) as fnsipdt from webapp.fundmaster as a where fnddisplayname like %s) art",(teee,))
        command = cur.mogrify("select row_to_json(art) from (select a.fndnatcode,a.fndschcdfrmbse,a.fnddisplayname,a.fndminpuramt,a.fndaddpuramt,a.fndmaxpuramt,a.fndpuramtinmulti,a.fndpurcutoff,a.fndamcnatcode, (select json_agg(c) from (select sipfreq,sipfreqdates,sipminamt,sipmaxamt,sipmulamt,sipmininstal,sipmaxinstal,sipmingap from webapp.fundsipdt where sipfndnatcode = a.fndnatcode ) as c) as fnsipdt from webapp.fundmaster as a where UPPER(fnddisplayname) like (%s)) art",(teee,))
        cur, dbqerr = db.mydbfunc(con,cur,command)
        print(command)
        print(cur)
        print(dbqerr)
        print(type(dbqerr))
        print(dbqerr['natstatus'])

        if cur.closed == True:
            if(dbqerr['natstatus'] == "error" or dbqerr['natstatus'] == "warning"):
                dbqerr['statusdetails']="pf Fetch failed"
            resp = make_response(jsonify(dbqerr), 400)
            return(resp)


        #Model to follow in all fetch
        records=[]
        for record in cur:  
            records.append(record[0])        
        print(records)
        print("Fund details returned for user: "+userid)

        for record in records:
            print("--------------")
            print(record)
            print("--------------")
            if record.get('fnsipdt') != None:
                for sipdt in record.get('fnsipdt'):
                    print(sipdt)
                    mydate=int(datetime.now().strftime('%d'))+10
                    mymnt=int(datetime.now().strftime('%m'))
                    mynxtmnt=int(datetime.now().strftime('%m'))+1
                    myyr=datetime.now().strftime('%Y')
                    sipdates=sipdt.get('sipfreqdates').split(',')
                    print(sipdt['sipfreqdates'])
                    sipdt['sipfreqdates']=[]
                    for sipdate in sipdates:
                        print(type(sipdate))
                        print(type(mydate))
                        if(int(sipdate)>mydate):
                            now = (datetime.strptime((str(sipdate)+'/'+str(mymnt)+'/'+str(myyr)), '%d/%m/%Y').date()).strftime('%d-%b-%Y')         
                                                
                            print(now)
                            print(type(now))
                            
                            #sipdt['sipfreqdates'].append(str(sipdate)+'/'+str(mymnt)+'/'+str(myyr))
                            sipdt['sipfreqdates'].append(now)
                            
                        else:
                            #sipdt['sipfreqdates'].append(str(sipdate)+'/'+str(mynxtmnt)+'/'+str(myyr))
                            #now = datetime.strptime((str(sipdate)+'/'+str(mynxtmnt)+'/'+str(myyr)), '%d/%m/%Y').date()
                            now1=(datetime.strptime((str(sipdate)+'/'+str(mynxtmnt)+'/'+str(myyr)), '%d/%m/%Y').date()).strftime('%d-%b-%Y')
                            print(now1)                         
                            print(type(now1))
                            sipdt['sipfreqdates'].append(now1)


                    print(sipdt['sipfreqdates'])
                
                


        
        resp = json.dumps(records)
    
    return resp
