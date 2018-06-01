from pf import app
from pf import dbfunc as db
from pf import jwtdecodenoverify as jwtnoverify
#from order import dbfunc as db
#from order import jwtdecodenoverify as jwtnoverify


#from order import app
from flask import request, make_response, jsonify, Response, redirect
from datetime import datetime
from order import dbfunc as db
from order import jwtdecodenoverify as jwtnoverify
from dateutil import tz
from datetime import datetime, timedelta
from datetime import date
from multiprocessing import Process
from multiprocessing import Pool
from pf import mforderapi
from pf import mforder
import requests

import psycopg2
import json
import jwt
import time


#@app.route('/mfsiporder',methods=['POST','GET','OPTIONS'])
#def sip_order_processing():
def sip_order_processing(sip_data_for_processing):
#This is called for sip processing
    '''
    if request.method=='OPTIONS':
        print("inside sip_order_processing options")
        return make_response(jsonify('inside sip_order_processing options'), 200)  

    elif request.method=='POST':
        print("inside sip_order_processing GET")
        print((request))        
        print(request.headers)
        payload_org= request.get_json()
        sip_pay 
        print(payload_org)
        userid,entityid=jwtnoverify.validatetoken(request)
        print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

    '''
    payload_org = sip_data_for_processing
    #sip_mandate_details = payload_org['sip_mandate_details']
    userid = payload_org['userid']
    entityid = payload_org['entityid']

    print(userid,entityid)
    
    con,cur=db.mydbopncon()

    command = cur.mogrify("select json_agg(b) from (SELECT * FROM webapp.mforderdetails WHERE mfor_ordertype = 'SIP' AND mfor_orderstatus='PNS' AND mfor_pfuserid = %s AND mfor_entityid =%s) as b;",(userid,entityid,))
    print(command)
    cur, dbqerr = db.mydbfunc(con,cur,command)
                            
    if cur.closed == True:
        if(dbqerr['natstatus'] == "error" or dbqerr['natstatus'] == "warning"):
            dbqerr['statusdetails']="Data for order multiprocess fetch failed"
        resp = make_response(jsonify(dbqerr), 400)
        return(resp)
    print("cur") 
    print(cur)
    
    #Model to follow in all fetch
    sip_records=[]
    sip_records_orderids=[]
    
    if cur:
        for record in cur:           
            sip_records = record[0]
    
    if sip_records is None:
        sip_records = []
    print(sip_records)
    print("len(sip_records")
    print(len(sip_records))
    if len(sip_records) > 0:    
        print("i am insider sip records")
        for record in sip_records:    
            sip_records_orderids.append(record['mfor_uniquereferencenumber'])

        sipordersets = sip_records
        siporderids = sip_records_orderids
        

        pool = Pool(processes=10)
        result = pool.map_async(sip_prepare_order, sipordersets)   
        result.wait()  
        print(result.get())        
        pool.close()
        pool.join()
        str2=tuple(siporderids)
        todt  = datetime.now().strftime('%d-%b-%Y')
        frmdt = (datetime.now() + timedelta(days=-1)).strftime('%d-%b-%Y')    

        ###  this should be a API call in lambda  #####
        all_recs = mforder.fetchsucfai_recs(con, cur, str2, 'SIP', userid, entityid,frmdt,todt,'VAS')
        ###  this should be a API call in lambda  #####
        resp_recs = all_recs['sip']
        resp_suc_recs = resp_recs['success_recs']
        
        ###  this should be a API call in lambda  #####
        sip_submit_rec = {
            'sip_pay':'',
            'one_time_pay' : '',
            'succrecs' : resp_suc_recs,
            'userid': userid,
            'entityid': entityid
        }
 
        
        recs = mfordersubmit_cpy(sip_submit_rec)
        ###  this should be a API call in lambda  #####
    
    else:
        recs = {
            'status' : 'notrantoprocess',
            'sip_orderids': []
        }


    return recs    
    #below code is copy fro mforder.py.mfordersubmit, which is to be removed END



def sip_prepare_order(orderrecord):
#function that calls the mforder.py method over HTTP In lambda
    ord=orderrecord

    ###  this should be a API call in lambda  #####    
    resp = mforder.prepare_order(ord)
    ###  this should be a API call in lambda  #####

    return resp



def mfordersubmit_cpy(payload_org):
################ COPY FROM mfordersubmit #########################
    
    con,cur=db.mydbopncon()
    
    one_time_pay_details = payload_org['one_time_pay']
    sip_pay_details = payload_org['sip_pay']  #Not required for one time
    payload = payload_org['succrecs']
    userid = payload_org['userid']
    entityid = payload_org['entityid']


    ord_ids=[]
    for payld in payload:
        ord_ids.append(payld['mfor_uniquereferencenumber'])

    str=tuple(ord_ids)
    print(str)

    if userid is None or userid == '':
        userid,entityid=jwtnoverify.validatetoken(request)
    
    if entityid is None or userid == '':
        userid,entityid=jwtnoverify.validatetoken(request)

    con,cur=db.mydbopncon()

    command = cur.mogrify("""
                SELECT mfor_msgjson FROM webapp.mforderdetails WHERE mfor_uniquereferencenumber IN %s AND mfor_pfuserid = %s AND mfor_entityid = %s and mfor_orderstatus = 'VAS';
                """,(str,userid,entityid,))
    print(command)
    cur, dbqerr = db.mydbfunc(con,cur,command)
    if cur.closed == True:
        if(dbqerr['natstatus'] == "error" or dbqerr['natstatus'] == "warning"):
            dbqerr['statusdetails']="selecting order to submit to BSE failed"
        resp = make_response(jsonify(dbqerr), 400)
        return(resp)
    
    #Model to follow in all fetch
    orders=[]
    for record in cur:
        print(record[0])
        orders.append(record[0])

    print(orders)
    order_records = json.dumps(orders)
    
    ###  this should be a API call in lambda  #####
    orderresp = mforderapi.place_order_bse(order_records)
    ###  this should be a API call in lambda  #####

    print(orderresp)
    #Add code to update the order id

    ot_orderids=[]
    sip_orderids=[]

    for orderres in orderresp:
        
        if orderres['success_flag'] == '0':
        
            if orderres['order_type'] == 'OneTime':
                ot_orderids.append(orderres['trans_no'])
                command = cur.mogrify("""
                    UPDATE webapp.mforderdetails SET mfor_orderstatus = 'PPY', mfor_orderid = %s, mfor_bseremarks = %s WHERE mfor_uniquereferencenumber = %s AND mfor_pfuserid = %s AND mfor_entityid = %s;
                """,(orderres['order_id'],orderres['bse_remarks'],orderres['trans_no'],userid,entityid,))
            
            elif orderres['order_type'] == 'SIP':
                sip_orderids.append(orderres['trans_no'])
                command = cur.mogrify("""
                    UPDATE webapp.mforderdetails SET mfor_orderstatus = 'INP', mfor_orderid = %s, mfor_bseremarks = %s WHERE mfor_uniquereferencenumber = %s AND mfor_pfuserid = %s AND mfor_entityid = %s;
                """,(orderres['order_id'],orderres['bse_remarks'],orderres['trans_no'],userid,entityid,))
        
        else:
            if orderres['order_type'] == 'OneTime':
                ot_orderids.append(orderres['trans_no'])
                command = cur.mogrify("""
                    UPDATE webapp.mforderdetails SET mfor_orderstatus = 'FAI', mfor_valierrors = %s WHERE mfor_uniquereferencenumber = %s AND mfor_pfuserid = %s AND mfor_entityid = %s;
                """,(orderres['bse_remarks'],orderres['trans_no'],userid,entityid,))

            elif orderres['order_type'] == 'SIP':
                sip_orderids.append(orderres['trans_no'])
                command = cur.mogrify("""
                    UPDATE webapp.mforderdetails SET mfor_orderstatus = 'FAI', mfor_valierrors = %s WHERE mfor_uniquereferencenumber = %s AND mfor_pfuserid = %s AND mfor_entityid = %s;
                """,(orderres['bse_remarks'],orderres['trans_no'],userid,entityid,))

        print(command)

        cur, dbqerr = db.mydbfunc(con,cur,command)
        con.commit()

    if orderres['order_type'] == 'OneTime':
        str2 = tuple(ot_orderids)           
        
        frmdt = (datetime.now() + timedelta(days=-1)).strftime('%d-%b-%Y')
        todt = datetime.now().strftime('%d-%b-%Y')

        all_recs = mforder.fetchsucfai_recs(con, cur, str2, 'One Time', userid, entityid, frmdt, todt, 'BFP')
        print('*******************ord_type')
        print(orderres['order_type'])
        print(str2)
        print(all_recs)
        print('*******************ord_type')

        resp_recs = all_recs
        '''
        resp_recs={
            'success_recs': all_records['suc_records,
            'failure_recs': fai_records
        }
        '''
        print(json.dumps(resp_recs))
        
    elif orderres['order_type'] == 'SIP':
        resp_recs = {
            'status' : 'completed',
            'sip_orderids': sip_orderids
            }
    con.commit()
    cur.close()
    con.close()  
    return json.dumps(resp_recs)

################ COPY FROM mfordersubmit #########################