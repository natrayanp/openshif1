from app import app
import dbfunc as db
import jwtdecodenoverify as jwtnoverify

from flask import request, make_response, jsonify, Response, redirect
from dateutil import tz
from datetime import datetime, date
import settings
from multiprocessing import Process
from multiprocessing import Pool

import requests
import json
import zeep

#@app.route('/orderapi',methods=['GET','POST','OPTIONS'])
#def place_order_bse():
def place_order_bse(jsondata):
    '''
    if request.method=='OPTIONS':
        print ("inside orderapi options")
        return 'inside orderapi options'

    elif request.method=='POST':
        print("inside orderapi POST")

        print((request))        
        #userid,entityid=jwtnoverify.validatetoken(request)
        print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        payload= request.get_json()
        #payload=json.loads(payload)
        print(payload)
        bse_order = json.loads(payload)
        '''
    bse_orders=json.loads(jsondata)
    print("insde bse order")
    print(bse_orders)

    global client
    global pass_dict
    global results_updated
    results_updated = []
    client = zeep.Client(wsdl=settings.WSDL_ORDER_URL[settings.LIVE])
    set_soap_logging()
    ## get the password 
    pass_dict = soap_get_password_order(client)

    for bse_order in bse_orders:
        bse_order['password'] = pass_dict['password']
        bse_order['pass_key'] = pass_dict['passkey']
        
    print(bse_orders)
    
    print("multiprocessing starts")
    pool = Pool(processes=10)
    result = pool.map_async(send_one_order, bse_orders)
    print(result)
    

    result.wait()        
    #print(result.get())
    '''
    order_resp=[]
    for bse_order in bse_orders:
        result=send_one_order(bse_order)
        order_resp.append(result)
    
    print("end with ontime")
    '''
    print(result.get())
    order_resp = result.get()
    #print(results_updated)
    pool.close()
    pool.join()    
    print('all done')
    return order_resp

def update_res(val):
    print(val)
    results_updated.append(val)

def send_one_order(bse_order):
    print('*&*&^%^%%^^&&&&&&&%%$*()_)*%#$#')
    print(bse_order)
    print('*&*&^%^%%^^&&&&&&&%%$*()_)*%#$#')
    bse_order['user_id'] = settings.USERID[settings.LIVE]
    bse_order['member_id'] = settings.MEMBERID[settings.LIVE]
    '''
    bse_order['password'] = pass_dict['password']
    bse_order['pass_key'] = pass_dict['passkey']
    '''
    client = zeep.Client(wsdl=settings.WSDL_ORDER_URL[settings.LIVE])


    ## prepare order, post it to BSE and save response
    ## for lumpsum transaction 
    if (bse_order['mfor_ordertype'] == 'OneTime'):
        ## prepare the transaction record
        #bse_order = prepare_order(transaction, pass_dict)
        ## post the transaction
        print('****************************')
        print("before delete")
        print(bse_order)

        del bse_order['mfor_ordertype']
        
        print("after delete")
        print(bse_order)
        
        order_resp = soap_post_onetime_order(client, bse_order)
        
        print("order id")
        print(order_resp)
        print('****************************')
    

    elif(bse_order['mfor_ordertype'] == 'SIP'):
        ## since xsip orders cannot be placed in off-market hours, 
        ## placing a lumpsum order instead 
        #bse_order = prepare_xsip_order(transaction, pass_dict)
        ## post the transaction
        mandate_type = bse_order['mfor_sipmandatetype']
        del bse_order['mfor_ordertype']
        del bse_order['mfor_sipmandatetype']

        if mandate_type == 'I':
            order_resp = soap_post_isip_order(client, bse_order)
            print(order_resp)
        elif mandate_type == 'X':
            #order_resp = soap_post_isip_order(client, bse_order)
            #print(order_resp)
            pass
        elif mandate_type == 'E':
            #order_resp = soap_post_isip_order(client, bse_order)
            #print(order_resp)
            pass
    else:
        '''
        raise Exception(
            "Internal error 630: Invalid order_type in transaction"
        )
        '''
        order_resp  = {
            'trans_code' : bse_order['trans_code'],
            'trans_no' : bse_order['trans_no'],
            'order_id' : bse_order['order_id'],
            'user_id' : bse_order['user_id'],
            'member_id' : bse_order['member_id'],
            'client_code' : bse_order['client_code'],
            'bse_remarks' : 'order not sent to BSE due to internal errors',
            'success_flag' :100,
            'order_type' : '',
        }
    
    return order_resp

def soap_get_password_order(client):
    method_url = settings.METHOD_ORDER_URL[settings.LIVE] + 'getPassword'
    svc_url = settings.SVC_ORDER_URL[settings.LIVE]
    print(method_url)
    print(svc_url)
    header_value = soap_set_wsa_headers(method_url, svc_url)
    print("reached here")
    response = client.service.getPassword(
        _soapheaders=[header_value],
        UserId=settings.USERID[settings.LIVE], 
        Password=settings.PASSWORD[settings.LIVE], 
        PassKey=settings.PASSKEY[settings.LIVE] 
    )
    
    response = response.split('|')
    status = response[0]
    if (status == '100'):
        pass_dict = {'password': response[1], 'passkey': settings.PASSKEY[settings.LIVE]}
        return pass_dict
    else:
        raise Exception(
            "BSE error 640: Login unsuccessful for Order API endpoint"
        )


# set logging such that its easy to debug soap queries
def set_soap_logging():
	import logging.config
	logging.config.dictConfig({
		'version': 1,
	    'formatters': {
	        'verbose': {
	            'format': '%(name)s: %(message)s'
	        }
	    },
	    'handlers': {
	        'console': {
	            'level': 'DEBUG',
	            'class': 'logging.StreamHandler',
	            'formatter': 'verbose',
	        },
	    },
	    'loggers': {
	        'zeep.transports': {
	            'level': 'DEBUG',
	            'propagate': True,
	            'handlers': ['console'],
	        },
	    }
	})


## fire SOAP query to post the order 
def soap_post_onetime_order(client, bse_order):
    method_url = settings.METHOD_ORDER_URL[settings.LIVE] + 'orderEntryParam'
    header_value = soap_set_wsa_headers(method_url, settings.SVC_ORDER_URL[settings.LIVE])
    response = client.service.orderEntryParam(
        bse_order['trans_code'],
        bse_order['trans_no'],
        bse_order['order_id'],
        bse_order['user_id'],
        bse_order['member_id'],
        bse_order['client_code'],
        bse_order['scheme_cd'],
        bse_order['buy_sell'],
        bse_order['buy_sell_type'],
        bse_order['dptxn_mode'],
        bse_order['order_amt'],
        bse_order['order_qty'],
        bse_order['all_redeem'],
        bse_order['folio_no'],
        bse_order['remarks'],
        bse_order['kyc_status'],
        bse_order['internal_transaction'],
        bse_order['subbr_code'],
        bse_order['euin'],
        bse_order['euin_flg'],
        bse_order['min_redeem'],
        bse_order['dpc_flg'],
        bse_order['ipadd'],
        bse_order['password'],
        bse_order['pass_key'],
        bse_order['subbr_arn'],
        bse_order['param2'],
        bse_order['param3'],
        _soapheaders=[header_value]
    )


    response = response.split('|')
    print(response)
    ## store the order response in a table
    order_resp= store_order_response(response, 'OneTime')
    
    status = order_resp['success_flag']
    if (status == '0'):
        # order successful
        print("ontime order successful")
    else:
        print("order failure")
        
        '''
        raise Exception(
            "BSE error 641: %s" % response[6]
        )
        '''
    return order_resp


## fire SOAP query to post the XSIP order 
def soap_post_isip_order(client, bse_order):
    method_url = settings.METHOD_ORDER_URL[settings.LIVE] + 'xsipOrderEntryParam'
    header_value = soap_set_wsa_headers(method_url, settings.SVC_ORDER_URL[settings.LIVE])
    response = client.service.xsipOrderEntryParam(
        bse_order['trans_code'],
        bse_order['trans_no'],
        bse_order['scheme_cd'],
        bse_order['member_id'],
        bse_order['client_code'],
        bse_order['user_id'],
        bse_order['internal_transaction'],
        bse_order['trans_mode'],
        bse_order['dptxn_mode'],
        bse_order['start_date'],
        bse_order['freq_type'],
        bse_order['freq_allowed'],
        bse_order['order_amt'],
        bse_order['num_of_instalment'],
        bse_order['Remarks'],
        bse_order['folio_no'],
        bse_order['first_ord_flg'],
        bse_order['borkerage'],  
        bse_order['xsip_mandate_id'],      
        # '',
        bse_order['subbr_code'],
        bse_order['euin'],
        bse_order['euin_flg'],
        bse_order['dpc_flg'],
        bse_order['xsip_reg_id'],
        bse_order['ipadd'],
        bse_order['password'],
        bse_order['pass_key'],
        bse_order['subbr_arn'],
        # bse_order.mandate_id,
        bse_order['isip_mandate_id'],
        bse_order['Param3'],
        _soapheaders=[header_value]
    )

    ## this is a good place to put in a slack alert

    response = response.split('|')
    ## store the order response in a table
    order_resp = store_order_response(response, 'SIP')
    status = response[7]
    if (status == '0'):
        # order successful
        #return order_id
        print("order successful")
    else:
        print("order failure")
        '''
        raise Exception(
            "BSE error 642: %s" % response[6]
        )
        '''
    return order_resp


def get_payment_link_direct(payload):
    '''
    Gets the payment link corresponding to a client
    Called immediately after creating transaction 
    '''
    print('inside payment')
    print(payload)
    ## get the payment link and store it
    client = zeep.Client(wsdl=settings.WSDL_PAYLNK_URL[settings.LIVE])
    set_soap_logging()
    print('before passdict')
    pass_dict = soap_get_password_paylnk(client)
    print('after passdict')
    payment_url = soap_create_payment(client, payload, pass_dict)
    #str(payload['client_code']), payload['transaction_ids'], payload['total_amt']
    #soap_create_payment(client, client_code, transaction_id, pass_dict,total_amt):
    print('paym,ent url')
    print(payment_url)
    return payment_url


# store response to order entry from bse 
def store_order_response(response, order_type):
## lumpsum order 
    if (order_type == 'OneTime'):
        trans_response = {
            'trans_code' : response[0],
            'trans_no' : response[1],
            'order_id' : response[2],
            'user_id' : response[3],
            'member_id' : response[4],
            'client_code' : response[5],
            'bse_remarks' : response[6],
            'success_flag' : response[7],
            'order_type' : 'OneTime',
        }
    ## SIP order  
    elif (order_type == 'SIP'):
        trans_response = {
            'trans_code' : response[0],
            'trans_no' : response[1],
            'member_id' : response[2],
            'client_code' : response[3],
            'user_id' : response[4],
            'order_id' : response[5],
            'bse_remarks' : response[6],
            'success_flag' : response[7],
            'order_type' : 'SIP',
        }
    #trans_response.save()
    return trans_response




def soap_get_password_paylnk(client):
    method_url = settings.METHOD_PAYLNK_URL[settings.LIVE] + 'GetPassword'
    svc_url = settings.SVC_PAYLNK_URL[settings.LIVE]
    header_value = soap_set_wsa_headers(method_url, svc_url)      
    '''
    response = client.service.GetPassword(
        UserId=settings.USERID[settings.LIVE],
        MemberId=settings.MEMBERID[settings.LIVE], 
        Password=settings.PASSWORD[settings.LIVE], 
        PassKey=settings.PASSKEY[settings.LIVE], 
        _soapheaders=[header_value]
    )
    '''
    response = client.service.GetPassword ({
        'MemberId': settings.MEMBERID[settings.LIVE],  
        'UserId': settings.USERID[settings.LIVE],
        'Password': settings.PASSWORD[settings.LIVE], 
        'PassKey': settings.PASSKEY[settings.LIVE]
        }, 
        _soapheaders=[header_value]
        )
    
    print('after response')
    print(response)

    #response = response.split('|')
    #status = response[0]
    if (response.Status == '100'):
        # login successful
        pass_dict = {'password': response.ResponseString, 'passkey': settings.PASSKEY[settings.LIVE]}
        #pass_dict = {'password': response[1], 'passkey': settings.PASSKEY[settings.LIVE]}
        return pass_dict
    else:
        raise Exception(
            "BSE error 640: Login unsuccessful for upload API endpoint"
        )


## fire SOAP query to get the payment url 
def soap_create_payment(client, payload, pass_dict):

    client_code = str(payload['client_code'])
    transaction_id = payload['transaction_ids']
    #total_amt = payload['total_amt']

    print(client_code)
    print(transaction_id)

    emptyArrayPlaceholder = client.get_type('ns3:ArrayOfstring')
    options = emptyArrayPlaceholder()
    print(options)
    for tran_id in transaction_id:
        options['string'].append(tran_id)

    print(options)

    method_url = settings.METHOD_PAYLNK_URL[settings.LIVE] + 'PaymentGatewayAPI'
    header_value = soap_set_wsa_headers(method_url, settings.SVC_PAYLNK_URL[settings.LIVE])
    #logout_url = 'http://localhost:8000/orpost'
    response = client.service.PaymentGatewayAPI({
        'AccNo': payload['acc_num'],
        'BankID': payload['bank_id'],
        'ClientCode': client_code,
        'EncryptedPassword': pass_dict['password'],
        'IFSC':payload['ifsc'],
        'LogOutURL':payload['logout_url'],
        'MemberCode':settings.MEMBERID[settings.LIVE],
        'Mode':payload['mode'],
        #'Orders':{"string": transaction_id},
        'Orders':options,
        'TotalAmount':payload['total_amt']
        },
        #settings.MEMBERID[settings.LIVE]+'|'+client_code+'|'+logout_url,
        _soapheaders=[header_value]
    )
    print(response)
    #response = response.split('|')
    #status = response[0]
    payment_url = response.ResponseString.replace('\n', ' ').replace('\r', '')
    if (response.Status == '100'):
        # getting payment url successful
        #payment_url = response.ResponseString


        response_string = { 
            'payment_url':payment_url,
            'status': 'success',
            'url_type' : 'dirpayhtml'
            }
        print(response_string)
        return response_string
    else:
        response_string = { 
            'payment_url':'',
            'status': 'failed',
            'url_type' : 'dirpayhtml'
            }

        return response_string
    '''
    else:
        raise Exception(
            "BSE error 646: Payment link creation unsuccessful: %s" % response.ResponseString
        )
    '''

# every soap query to bse must have wsa headers set 
def soap_set_wsa_headers(method_url, svc_url):    

    header = zeep.xsd.Element('{http://test.python-zeep.org}auth', zeep.xsd.ComplexType([
        zeep.xsd.Element('{http://www.w3.org/2005/08/addressing}Action', zeep.xsd.String()),
        zeep.xsd.Element('{http://www.w3.org/2005/08/addressing}To', zeep.xsd.String())
        ])
    )
    header_value = header(Action=method_url, To=svc_url)
    
    
    return header_value














@app.route('/custcreationapi',methods=['GET','POST','OPTIONS'])
def create_user_bse():
	if request.method=='OPTIONS':
		print ("inside custcreation options")
		return 'inside custcreation options'

	elif request.method=='POST':
		print("inside custcreationapi POST")

		print((request))        
		#userid,entityid=jwtnoverify.validatetoken(request)
		print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
		payload= request.get_json()
		#payload=json.loads(payload)
		print(payload)
		
		#clientdata,fatcadata=custdatavalidation(payload)
		#reqdataifsc=payload[''ifsc']
		## initialise the zeep client for order wsdl
		client = zeep.Client(wsdl=settings.WSDL_UPLOAD_URL[settings.LIVE])
		set_soap_logging()

		## get the password 
		pass_dict = soap_get_password_upload(client)
		## prepare the user record 
		#client_code='1234test'
		#payload=request.get_json()

		bse_user = prepare_user_param(payload)
		## post the user creation request
		user_response = soap_create_user(client, bse_user, pass_dict)
		print('user_response: ',user_response)
		## TODO: Log the soap request and response post the user creation request
		if user_response['bsesttuscode'] == '100':
			#pass_dict = soap_get_password_upload(client)			
			bse_fatca = prepare_fatca_param(payload)
			fatca_response = soap_create_fatca(client, bse_fatca, pass_dict)
			print('fatca_response: ',fatca_response)
			## TODO: Log the soap request and response post the fatca creation request
			if fatca_response['bsesttuscode'] == '100':
				return make_response(jsonify({'statuscode': fatca_response['bsesttuscode'], 'statusmessage': "User and Fatca record created successfully"}),200)
			else:
				return make_response(jsonify({'statuscode': fatca_response['bsesttuscode'], 'statusmessage': fatca_response['bsesttusmsg']}),400)
		else:
			return make_response(jsonify({'statuscode': user_response['bsesttuscode'], 'statusmessage': user_response['bsesttusmsg']}),400)


# prepare the string that will be sent as param for user creation in bse
def prepare_user_param(payload):
	# make the list that will be used to create param
	print('inside prepare_user_param')
	d=payload
	print(type(d))
	print( d['clientcode'])
	
	param_list = [
		('CODE', d['clientcode']),
		('HOLDING', d['clientholding']),
		('TAXSTATUS', d['clienttaxstatus']),
		('OCCUPATIONCODE', d['clientoccupationcode']),
		('APPNAME1', d['clientappname1']),
		('APPNAME2', d['clientappname2']),
		('APPNAME3', d['clientappname3']),
		('DOB', d['clientdob']),  							#to change the format
		('GENDER', d['clientgender']),						
		('FATHER/HUSBAND/gurdian', d['clientguardian']),
		('PAN', d['clientpan']),
		('NOMINEE', d['clientnominee']),
		('NOMINEE_RELATION', d['clientnomineerelation']),
		('GUARDIANPAN', d['clientguardianpan']),
		('TYPE', d['clienttype']),
		('DEFAULTDP', d['clientdefaultdp']),
		('CDSLDPID', d['clientcdsldpid']),
		('CDSLCLTID', d['clientcdslcltid']),
		('NSDLDPID', d['clientnsdldpid']),
		('NSDLCLTID', d['clientnsdlcltid']),
		('ACCTYPE_1', d['clientacctype1']),
		('ACCNO_1', d['clientaccno1']),
		('MICRNO_1', d['clientmicrno1']),
		('NEFT/IFSCCODE_1', d['clientifsccode1']),
		('default_bank_flag_1', d['defaultbankflag1']),
		('ACCTYPE_2', d['clientacctype2']),
		('ACCNO_2', d['clientaccno2']),
		('MICRNO_2', d['clientmicrno2']),
		('NEFT/IFSCCODE_2', d['clientifsccode2']),
		('default_bank_flag_2', d['defaultbankflag2']),
		('ACCTYPE_3', d['clientacctype3']),
		('ACCNO_3', d['clientaccno3']),
		('MICRNO_3', d['clientmicrno3']),
		('NEFT/IFSCCODE_3', d['clientifsccode3']),
		('default_bank_flag_3', d['defaultbankflag3']),
		('ACCTYPE_4', d['clientacctype4']),
		('ACCNO_4', d['clientaccno4']),
		('MICRNO_4', d['clientmicrno4']),
		('NEFT/IFSCCODE_4', d['clientifsccode4']),
		('default_bank_flag_4', d['defaultbankflag4']),
		('ACCTYPE_5', d['clientacctype5']),
		('ACCNO_5', d['clientaccno5']),
		('MICRNO_5', d['clientmicrno5']),
		('NEFT/IFSCCODE_5', d['clientifsccode5']),
		('default_bank_flag_5', d['defaultbankflag5']),
		('CHEQUENAME', d['clientchequename5']),
		('ADD1', d['clientadd1'] ),
		('ADD2', d['clientadd2']),
		('ADD3', d['clientadd3']),
		('CITY', d['clientcity']),
		('STATE', d['clientstate']),
		('PINCODE', d['clientpincode']),
		('COUNTRY', d['clientcountry']),
		('RESIPHONE', d['clientresiphone']),
		('RESIFAX', d['clientresifax']),
		('OFFICEPHONE', d['clientofficephone']),
		('OFFICEFAX', d['clientofficefax']),
		('EMAIL', d['clientemail']),
		('COMMMODE',d['clientcommmode']),
		('DIVPAYMODE', d['clientdivpaymode']),
		('PAN2', d['clientpan2']),
		('PAN3', d['clientpan3']),
		('MAPINNO', d['mapinno']),
		('CM_FORADD1', d['cm_foradd1']),
		('CM_FORADD2', d['cm_foradd2']),
		('CM_FORADD3', d['cm_foradd3']),
		('CM_FORCITY', d['cm_forcity']),
		('CM_FORPINCODE', d['cm_forpincode']),
		('CM_FORSTATE', d['cm_forstate']),
		('CM_FORCOUNTRY', d['cm_forcountry']),
		('CM_FORRESIPHONE', d['cm_forresiphone']),
		('CM_FORRESIFAX', d['cm_forresifax']),
		('CM_FOROFFPHONE', d['cm_foroffphone']),
		('CM_FOROFFFAX', d['cm_forofffax']),
		('CM_MOBILE', d['cm_mobile'])
	]

	# prepare the param field to be returned
	user_param = ''
	for param in param_list:
		user_param = user_param + '|' + str(param[1])
	# print user_param
	return user_param[1:]


# prepare the string that will be sent as param for fatca creation in bse
def prepare_fatca_param(payload):
	# make the list that will be used to create param
	print('inside prepare_fatca_param')
	d=payload

	param_list = [
		('PAN_RP', d['pan_rp']),
		('PEKRN', d['pekrn']),
		('INV_NAME', d['inv_name']),
		('DOB', d['dob']),
		('FR_NAME', d['fr_name']),
		('SP_NAME', d['sp_name']),
		('TAX_STATUS', d['tax_status']),
		('DATA_SRC', d['data_src']),
		('ADDR_TYPE', d['addr_type']),
		('PO_BIR_INC', d['po_bir_inc']),
		('CO_BIR_INC', d['co_bir_inc']),
		('TAX_RES1', d['tax_res1']),
		('TPIN1', d['tpin1']),
		('ID1_TYPE', d['id1_type']),
		('TAX_RES2', d['tax_res2']),
		('TPIN2', d['tpin2']),
		('ID2_TYPE', d['id2_type']),
		('TAX_RES3', d['tax_res3']),
		('TPIN3', d['tpin3']),
		('ID3_TYPE', d['id3_type']),
		('TAX_RES4', d['tax_res4']),
		('TPIN4', d['tpin4']),
		('ID4_TYPE', d['id4_type']),
		('SRCE_WEALT', d['srce_wealt']),
		('CORP_SERVS', d['corp_servs']),
		('INC_SLAB', d['inc_slab']),
		('NET_WORTH', d['net_worth']),
		('NW_DATE', d['nw_date']),
		('PEP_FLAG', d['pep_flag']),
		('OCC_CODE', d['occ_code']),
		('OCC_TYPE', d['occ_type']),
		('EXEMP_CODE', d['exemp_code']),
		('FFI_DRNFE', d['ffi_drnfe']),
		('GIIN_NO', d['giin_no']),
		('SPR_ENTITY', d['spr_entity']),
		('GIIN_NA', d['giin_na']),
		('GIIN_EXEMC', d['giin_exemc']),
		('NFFE_CATG', d['nffe_catg']),
		('ACT_NFE_SC', d['act_nfe_sc']),
		('NATURE_BUS', d['nature_bus']),
		('REL_LISTED', d['rel_listed']),
		('EXCH_NAME', d['exch_name']),
		('UBO_APPL', d['ubo_appl']),
		('UBO_COUNT', d['ubo_count']),
		('UBO_NAME', d['ubo_name']),
		('UBO_PAN', d['ubo_pan']),
		('UBO_NATION', d['ubo_nation']),
		('UBO_ADD1', d['ubo_add1']),
		('UBO_ADD2', d['ubo_add2']),
		('UBO_ADD3', d['ubo_add3']),
		('UBO_CITY', d['ubo_city']),
		('UBO_PIN', d['ubo_pin']),
		('UBO_STATE', d['ubo_state']),
		('UBO_CNTRY', d['ubo_cntry']),
		('UBO_ADD_TY', d['ubo_add_ty']),
		('UBO_CTR', d['ubo_ctr']),
		('UBO_TIN', d['ubo_tin']),
		('UBO_ID_TY', d['ubo_id_ty']),
		('UBO_COB', d['ubo_cob']),
		('UBO_DOB', d['ubo_dob']),
		('UBO_GENDER', d['ubo_gender']),
		('UBO_FR_NAM', d['ubo_fr_nam']),
		('UBO_OCC', d['ubo_occ']),
		('UBO_OCC_TY', d['ubo_occ_ty']),
		('UBO_TEL', d['ubo_tel']),
		('UBO_MOBILE', d['ubo_mobile']),
		('UBO_CODE', d['ubo_code']),
		('UBO_HOL_PC', d['ubo_hol_pc']),
		('SDF_FLAG', d['sdf_flag']),
		('UBO_DF', d['ubo_df']),
		('AADHAAR_RP', d['aadhaar_rp']),
		('NEW_CHANGE', d['new_change']),
		('LOG_NAMe',d['log_name']),
		('DOC1', d['filler1']),
		('DOC2', d['filler2'])
	]

	# prepare the param field to be returned
	fatca_param = ''
	for param in param_list:
		fatca_param = fatca_param + '|' + str(param[1])
	# print fatca_param
	return fatca_param[1:]

## fire SOAP query to get password for Upload API endpoint
## used by all functions except create_transaction_bse() and cancel_transaction_bse()

def soap_get_password_upload(client):
	print('inside soap_get_password_upload')
	method_url = settings.METHOD_UPLOAD_URL[settings.LIVE] + 'getPassword'
	svc_url = settings.SVC_UPLOAD_URL[settings.LIVE]
	header_value = soap_set_wsa_headers(method_url, svc_url)
	response = client.service.getPassword(
		MemberId=settings.MEMBERID[settings.LIVE], 
		UserId=settings.USERID[settings.LIVE],
		Password=settings.PASSWORD[settings.LIVE], 
		PassKey=settings.PASSKEY[settings.LIVE], 
		_soapheaders=[header_value]
	)
	print('after response')
	response = response.split('|')
	print('bse response: ',response)
	status = response[0]
	if (status == '100'):
		# login successful
		pass_dict = {'password': response[1], 'passkey': settings.PASSKEY[settings.LIVE]}
		print('#################')
		print(pass_dict)
		print('#################')
		return pass_dict
	else:
		
		raise Exception(
			"BSE error 640: Login unsuccessful for upload API endpoint"
		)
		
		#return make_response(jsonify({'statuscode': status, 'statusmessage': response[1]}),400)

## fire SOAP query to create a new user on bsestar
def soap_create_user(client, user_param, pass_dict):
	method_url = settings.METHOD_UPLOAD_URL[settings.LIVE] + 'MFAPI'
	header_value = soap_set_wsa_headers(method_url, settings.SVC_UPLOAD_URL[settings.LIVE])
	response = client.service.MFAPI(
		'02',
		settings.USERID[settings.LIVE],
		pass_dict['password'],
		user_param,
		_soapheaders=[header_value]
	)
	
	## this is a good place to put in a slack alert

	response = response.split('|')
	status = response[0]
	if (status == '100'):
		# User creation successful
		return {'bsesttuscode': response[0], 'bsesttusmsg': response[1],'stcdtoreturn':200}
		#return make_response(jsonify({'statuscode': response[0], 'statusmessage': response[1]}),200)
		
	else:		
		'''
		raise Exception(
			"BSE error 644: User creation unsuccessful: %s" % response[1]
		)
		'''
		return {'bsesttuscode': response[0], 'bsesttusmsg': response[1],'stcdtoreturn':400}
		#return make_response(jsonify({'statuscode': response[0], 'statusmessage': response[1]}),400)


## fire SOAP query to create fatca record of user on bsestar
def soap_create_fatca(client, fatca_param, pass_dict):
	method_url = settings.METHOD_UPLOAD_URL[settings.LIVE] + 'MFAPI'
	header_value = soap_set_wsa_headers(method_url, settings.SVC_UPLOAD_URL[settings.LIVE])
	response = client.service.MFAPI(
		'01',
		settings.USERID[settings.LIVE],
		pass_dict['password'],
		fatca_param,
		_soapheaders=[header_value]
	)
	
	## this is a good place to put in a slack alert

	response = response.split('|')
	status = response[0]
	if (status == '100'):
		# Fatca creation successful
		return {'bsesttuscode': response[0], 'bsesttusmsg': response[1],'stcdtoreturn':200}
		#return make_response(jsonify({'statuscode': response[0], 'statusmessage': response[1]}),200)
		
	else:
		'''
		raise Exception(
			"BSE error 645: Fatca creation unsuccessful: %s" % response[1]
		)
		'''
		return {'bsesttuscode': response[0], 'bsesttusmsg': response[1],'stcdtoreturn':400}
		#return make_response(jsonify({'statuscode': response[0], 'statusmessage': response[1]}),400)

	

# set logging such that its easy to debug soap queries
def set_soap_logging():
	import logging.config
	logging.config.dictConfig({
		'version': 1,
	    'formatters': {
	        'verbose': {
	            'format': '%(name)s: %(message)s'
	        }
	    },
	    'handlers': {
	        'console': {
	            'level': 'DEBUG',
	            'class': 'logging.StreamHandler',
	            'formatter': 'verbose',
	        },
	    },
	    'loggers': {
	        'zeep.transports': {
	            'level': 'DEBUG',
	            'propagate': True,
	            'handlers': ['console'],
	        },
	    }
	})

	################ HELPER SOAP FUNCTIONS ################

# every soap query to bse must have wsa headers set 
def soap_set_wsa_headers(method_url, svc_url):
	print(method_url)
	print(svc_url)
	header = zeep.xsd.Element("None", zeep.xsd.ComplexType([
        zeep.xsd.Element('{http://www.w3.org/2005/08/addressing}Action', zeep.xsd.String()),
        zeep.xsd.Element('{http://www.w3.org/2005/08/addressing}To', zeep.xsd.String())
        ])
    )
	header_value = header(Action=method_url, To=svc_url)
	return header_value




def get_payment_link_bse(payload):
    '''
    Gets the payment link corresponding to a client
    Called immediately after creating transaction 
    '''
    print('inside payment')
    print(payload)
    #client_code = payload['client_code']
    ## get the payment link and store it
    client = zeep.Client(wsdl=settings.WSDL_UPLOAD_URL[settings.LIVE])
    set_soap_logging()
    pass_dict = soap_get_password_upload(client)
    payment_url = soap_create_bse_payment(client, payload, pass_dict)

    return payment_url


def soap_create_bse_payment(client, payload,  pass_dict):
    client_code = payload['client_code']
    method_url = settings.METHOD_UPLOAD_URL[settings.LIVE] + 'MFAPI'
    header_value = soap_set_wsa_headers(method_url, settings.SVC_UPLOAD_URL[settings.LIVE])
    #logout_url = 'http://localhost:8000/orpost'
    response = client.service.MFAPI(
        '03',
        settings.USERID[settings.LIVE],
        pass_dict['password'],
        settings.MEMBERID[settings.LIVE]+'|'+client_code+'|'+payload['logout_url'],
        _soapheaders=[header_value]
    )
    print
    response = response.split('|')
    status = response[0]

    if (status == '100'):
        # getting payment url successful
        payment_url = response[1]
        
        response_string = { 
            'payment_url':payment_url,
            'status': 'success',
            'url_type' : 'bsepayurl'
            }
        print(response_string)
        return response_string
    else:
        response_string = { 
            'payment_url':'',
            'status': 'failed',
            'url_type' : 'bsepayurl'
            }
        return response_string
        '''
        raise Exception(
            "BSE error 646: Payment link creation unsuccessful: %s" % response[1]
        )
        '''

#@app.route('/paystatusapi',methods=['GET','POST','OPTIONS'])
#def paystatusapi():
def paystatusapi(jsondata):
    '''
    if request.method=='OPTIONS':
        print ("inside orderapi options")
        return 'inside orderapi options'

    elif request.method=='POST':
        print("inside orderapi POST")

        print((request))        
        #userid,entityid=jwtnoverify.validatetoken(request)
        print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        payload= request.get_json()
        #payload=json.loads(payload)
        print(payload)
        order_recs = json.loads(payload)

        #Gets whether user has paid for a transaction created on BSEStar
        '''
    order_recs = jsondata
    ## initialise the zeep client for wsdl
    client = zeep.Client(wsdl=settings.WSDL_UPLOAD_URL[settings.LIVE])
    set_soap_logging()

    ## get the password
    pass_dict = soap_get_password_upload(client)

    #payment_status = soap_get_payment_status(order_recs)

    
    for order_rec in order_recs:
        order_rec['password'] = pass_dict['password']
        order_rec['pass_key'] = pass_dict['passkey']
        
    print(order_recs)

    ## get payment status
    print("payment status multiprocessing starts")
    pool = Pool(processes=10)
    result = pool.map_async(soap_get_payment_status, order_recs)
    print(result)
    
    result.wait()
    print(result.get())
    payment_status = result.get()
    print("end with payment status multiprocessing")
    pool.close()
    pool.join()        

    return payment_status


## fire SOAP query to create a new mandate on bsestar
def soap_get_payment_status(order_recs):
    # find order_id for transaction
    #transaction = Transaction.objects.get(id=transaction_id)
    client = zeep.Client(wsdl=settings.WSDL_UPLOAD_URL[settings.LIVE])
    method_url = settings.METHOD_UPLOAD_URL[settings.LIVE] + 'MFAPI'
    header_value = soap_set_wsa_headers(method_url, settings.SVC_UPLOAD_URL[settings.LIVE])
    response = client.service.MFAPI(
        '11',
        settings.USERID[settings.LIVE],
        order_recs['password'],
        str(order_recs['client_code'])+'|'+str(order_recs['order_id'])+'|'+str(order_recs['segment']),
        _soapheaders=[header_value]
    )

    ## this is a good place to put in a slack alert
    print(response)
    response = response.split('|')    
    status = response[0]
    reponsemsg = response[1]
    '''
    if (status == '100'):
        if response[1] == 'PAYMENT NOT INITIATED FOR GIVEN ORDER' in response[1]:
            # payment not initiated for this order


            else:
                # no change
                return '0'
        else:
            # payment successful- update in db
            transaction.status = '5'
            transaction.save()
            return '5'
    else:		
        raise Exception(
            "BSE error 644: Get payment status unsuccessful: %s" % response[1]
        )
    '''

    pay_status = {
        'client_code': order_recs['client_code'],
        'order_id':  order_recs['order_id'],
        'segment' : order_recs['segment'],
        'bse_status_code' : status,
        'bse_status_msg' : reponsemsg
    }

    return pay_status
