from customercreationapi.customercreationapimain import app
#from .hello_world import app
from flask import request, make_response, jsonify, Response, redirect
from customercreationapi import settings
from datetime import datetime

import requests
import json
import zeep	
import os


@app.route('/fileuploadapi',methods=['GET','POST','OPTIONS'])
def create_fileupload_bse():
	if request.method=='OPTIONS':
		print ("inside FILEUPLOADAPI options")
		return 'inside FILEUPLOADAPI options'

	elif request.method=='POST':
		print("inside FILEUPLOADAPI POST")

		print((request))        
		#userid,entityid=jwtnoverify.validatetoken(request)
		print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
		
		print('starting file read')
		file=request.files['myfiles']
		f=file.read()
		print('--------------------')
		print(type(file))
		print(type(f))
		fls=bytearray(f)
		print('--------------------')
		print('file is done')
		data = request.form
		print(data)
		print(data['ClientCode'])
		print('data is done')
		print(data)

		## initialise the zeep client for order wsdl
		client = zeep.Client(wsdl=settings.WSDL_FILEUP_URL[settings.LIVE])
		set_soap_logging()

		## get the password 
		pass_dict = soap_get_password_file_upload(client)
		print(pass_dict)
		print(pass_dict['bsesttuscode'])

		if(pass_dict['bsesttuscode']=='100'):
			encpasswd = pass_dict['bsesttusmsg']
		else:
			## Issue in getting password? return bsestatus details to client
			print('inside else')
			return make_response(jsonify({'statuscode': pass_dict['bsesttuscode'], 'statusmessage': pass_dict['bsesttusmsg']}),400)
		
		#this is the data i get from the post request
		#{'ClientCode':clientcode,'filetype':'AOFANDCHQ','pFileBytes':fls,'publickey':'to be implemented'}
		#clientcode='A000000001'
		clientcode=data['ClientCode']
		filename=''
		membercode=settings.MEMBERID[settings.LIVE]
		bseuserid=settings.USERID[settings.LIVE]
		if data['filetype'] == 'AOFandCancelledCheque':
			dates=datetime.now().strftime('%d%m%Y')
			filename=membercode+clientcode+dates+'.tiff'

		flag='UCC'
		'''
		membercode= '17123'		
		
		'''
		print(filename)
		print(type(fls))
		print('starting upload')
		method_url = settings.METHOD_FILEUP_URL[settings.LIVE] + 'UploadFile'
		svc_url = settings.SVC_FILEUP_URL[settings.LIVE]
		header_value = soap_set_wsa_headers(method_url, svc_url)		
		fileup_resp = client.service.UploadFile({'Flag':flag,'UserId':bseuserid,'EncryptedPassword':encpasswd,'MemberCode':membercode,'ClientCode':clientcode,'FileName':filename,'DocumentType':'','pFileBytes':fls,'Filler1':'Null','Filler2':'Null','_soapheaders':[header_value]})
		print('starting response reading')
		input_dict = zeep.helpers.serialize_object(fileup_resp)
		print(input_dict)
		print(type(input_dict))
		print(fileup_resp)

		if(input_dict['Status']=='100'):
			return make_response(jsonify(input_dict),200)
		else:
			## Issue in getting password? return bsestatus details to client
			print('inside else')
			return make_response(jsonify(input_dict),400)


		return ("success")
		
## fire SOAP query to get password for Upload API endpoint
## used by all functions except create_transaction_bse() and cancel_transaction_bse()
def soap_get_password_file_upload(client):
	method_url = settings.METHOD_FILEUP_URL[settings.LIVE] + 'GetPassword'
	svc_url = settings.SVC_FILEUP_URL[settings.LIVE]
	header_value = soap_set_wsa_headers(method_url, svc_url)
	response = client.service.GetPassword({'UserId':settings.USERID[settings.LIVE],'MemberId':settings.MEMBERID[settings.LIVE],'Password':settings.PASSWORD[settings.LIVE],'_soapheaders':[header_value]})
	'''	
		UserId=settings.USERID[settings.LIVE],
		MemberId=settings.MEMBERID[settings.LIVE], 
		Password=settings.PASSWORD[settings.LIVE], 
		_soapheaders=[header_value]
	)	
	'''
	#response = response.split('|')
	print(type(response))
	input_dict = zeep.helpers.serialize_object(response)
	print(input_dict)
	print(type(input_dict))
	status = input_dict['Status']
	respstring=input_dict['ResponseString']	
	print(respstring)
	print(status)
	if (status == '100'):
		# login successful
		encpasswd = respstring
		return ({'bsesttuscode': status, 'bsesttusmsg': encpasswd,'stcdtoreturn':200})
	else:
		print('inside else')
		return ({'bsesttuscode': status, 'bsesttusmsg': respstring,'stcdtoreturn':400})
		
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

	################ HELPER SOAP FUNCTIONS

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