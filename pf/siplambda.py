from app import app
import dbfunc as db
import jwtdecodenoverify as jwtnoverify
#from order import dbfunc as db
#from order import jwtdecodenoverify as jwtnoverify


#from order import app
from flask import request, make_response, jsonify, Response, redirect
from datetime import datetime
import dbfunc as db
import jwtdecodenoverify as jwtnoverify
from dateutil import tz
from datetime import datetime
from datetime import date
from multiprocessing import Process
from multiprocessing import Pool

import psycopg2
import json
import jwt
import time



#@app.route('/mfsiporderplace',methods=['GET','POST','OPTIONS'])
#example for model code http://www.postgresqltutorial.com/postgresql-python/transaction/
#def mfsiporderplace():
def mfsiporderplace(payload):
    '''
    if request.method=='OPTIONS':
        print ("inside mfsiporderplace options")
        return jsonify({'body':'success'})

    elif request.method=='POST':   
        print ("inside mfsiporderplace post")

        print(request.headers)
        payload= request.get_json()
        #payload = request.stream.read().decode('utf8')            
        print(payload)
    '''

    userid = payload.userid
    entityid = payload.entityid
        
    con,cur=db.mydbopncon()



# Main function to prepare and submit Transaction to BSE
def prepare_order(orderrecord):
    ord=orderrecord
    print("processing order" + ord['mfor_uniquereferencenumber'])
    #time.sleep(0.5)
    #return json.dumps({'mfor_uniquereferencenumber': ord['mfor_uniquereferencenumber'],'order_id': '','amount':ord['mfor_amount']})

    
    if(ord['mfor_ordertype'] == 'One Time'):
        has_error, order_json = prepare_onetime_ord(ord)
        if has_error:
            pass
        else:
            #CALL ORDER PLACEMENT API to place order
            #order_id = <value returned from BSE>
            orderstat = 'PPY'
    
    elif(ord['mfor_ordertype'] == 'SIP'):
        has_error, order_json = prepare_isip_ord(ord)
        if has_error:
            pass
        else:
            #CALL ORDER PLACEMENT API to place order
            #order_id = <value returned from BSE>
            orderstat = 'INP'

    con,cur=db.mydbopncon()

    if has_error:
        orderstat = 'FAI'
        command = cur.mogrify("""
            UPDATE webapp.mforderdetails SET mfor_valierrors = %s, mfor_orderstatus = %s WHERE mfor_uniquereferencenumber = %s AND mfor_entityid = %s;
        """,(order_json,orderstat,ord['mfor_uniquereferencenumber'],ord['mfor_pfuserid'],ord['mfor_entityid'],))

        print(command)

        cur, dbqerr = db.mydbfunc(con,cur,command)
        con.commit()                                
        cur.close()
        con.close()   

        print("order id :" + ord['mfor_uniquereferencenumber'] + ", has errors in processing")
        
        return json.dumps({'mfor_uniquereferencenumber': ord['mfor_uniquereferencenumber'],'order_id': '','amount':ord['mfor_amount']})
    
    else:
        #update the order id in the table
        command = cur.mogrify("""
            UPDATE webapp.mforderdetails SET mfor_orderid = %s, mfor_orderstatus = %s WHERE mfor_uniquereferencenumber = %s AND mfor_entityid = %s;
        """,(order_id,orderstat,ord['mfor_uniquereferencenumber'],ord['mfor_pfuserid'],ord['mfor_entityid'],))

        print(command)

        cur, dbqerr = db.mydbfunc(con,cur,command)
        con.commit()                                
        cur.close()
        con.close()  
        print("order id :" + ord['mfor_uniquereferencenumber'] + ", successfully processed")

        return json.dumps({'mfor_uniquereferencenumber': ord['mfor_uniquereferencenumber'],'order_id': order_id,'amount':ord['mfor_amount']})
    

# function to VALIDATE and CREATE data for ISIP order
def prepare_isip_ord(ord):
    #Prepares ISIP order

    haserror = False
    errormsg = ''
    # Fill all fields for a ISIP creation
    # Change fields if its a redeem or addl purchase
    data_dict = {
        'trans_code': ord['mfor_transactioncode'],
        'trans_no': ord['mfor_uniquereferencenumber'],        
        'scheme_cd': ord['mfor_schemecd'],
        'client_code': ord['mfor_uniquereferencenumber'],
        'trans_mode' : ord['mfor_transmode'],   #D/P
        'dptxn_mode' : ord['mfor_dptxn'],       #C/N/P (CDSL/NSDL/PHYSICAL)
        'freq_allowed' : 1,
        'Remarks' : '',
        'first_ord_flg' : 'N',
        'borkerage' : '',
        'xsip_mandate_id' : '',
        'dpc_flg' : 'N',
        'xsip_reg_id' : '',        
        'Param3' : ''        
    }

    if(ord['mfor_internalrefnum']):
        data_dict['internal_transaction'] = ord['mfor_internalrefnum']
    else:
        data_dict['internal_transaction'] = '' 


    strdt = dateformat1(ord['mfor_sipstartdate'])
    if(strdt):
        data_dict['start_date'] = strdt
    else:
        haserror = True
        errormsg = errormsg + "Missing SIP start date: "        
    
    if(ord['mfor_freqencytype']):
        data_dict['freq_type'] = ord['mfor_freqencytype']
    else:
        haserror = True
        errormsg = errormsg + "Missing SIP frequency: " 

    if(ord['mfor_amount']):
        data_dict['order_amt'] = ord['mfor_amount']
    else:
        haserror = True
        errormsg = errormsg + "Missing SIP amount: " 

    if(ord['mfor_numofinstallment']):
        data_dict['num_of_instalment'] = ord['mfor_numofinstallment']
    else:
        haserror = True
        errormsg = errormsg + "Missing instalment numbers: " 


    if(ord['mfor_foliono']):
        data_dict['folio_no'] = ord['mfor_foliono']
    else:
        data_dict['folio_no'] = ''

    if(ord['mfor_sipmandateid']):
        data_dict['isip_mandate_id'] = ord['mfor_sipmandateid']
    else:
        haserror = True
        errormsg = errormsg + "Missing ISIP Mandate id: " 

    if(ord['mfor_subbrcode']):
        data_dict['subbr_code'] = ord['mfor_subbrcode']
    else:
        data_dict['subbr_code'] = '' 

    if(ord['mfor_subbrokerarn']):
        data_dict['subbr_arn'] = ord['mfor_subbrokerarn']
    else:
        data_dict['subbr_arn'] = '' 

    if(ord['mfor_euin']):
        data_dict['euin'] = ord['mfor_euin']
    else:
        data_dict['euin'] = ''    

    if(ord['mfor_euinflag']):
        data_dict['euin_flg'] = ord['mfor_euinflag']
    else:
        data_dict['euin_flg'] = ''    

    if(ord['mfor_ipadd']):
        data_dict['ipadd'] = ord['mfor_ipadd']
    else:
        data_dict['ipadd'] = ''

    if haserror:
        return (True , errormsg)
    else:
        return( False, json.dumps(data_dict))



