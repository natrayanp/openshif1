from userregistration.userregistrationmain import app
from flask import redirect, request,make_response
from datetime import datetime
from flask import jsonify

from userregistration import dbfunc as db
from userregistration import jwtdecodenoverify as jwtnoverify
from flask import Flask, request, redirect, url_for
from werkzeug.utils import secure_filename
from userregistration import settings

import psycopg2, psycopg2.extras
import jwt
import requests
import json
import boto3


@app.route('/uploadfile', methods=['GET', 'POST','OPTIONS'])
def upload_file():
#Upload file to s3
    print(request)
    
    if request.method == 'POST':
        con,cur=db.mydbopncon()
        userid,entityid=jwtnoverify.validatetoken(request)
        # check if the post request has the file part
        if 'selectFile' not in request.files:
            print('No file part')
            return 'failed'
        file = request.files['selectFile']
        
        # if user does not select file, browser also
        # submit a empty part without filename
        if file.filename == '':
            print('No selected file')
            return 'no file selected'
        if file:
            filename = secure_filename(file.filename)            
            print(type(file))
            filecontenttype = file.content_type            
            print("filecontenttype :",filecontenttype)                 
            print('userid :',userid)
            print('entityid :',entityid)
            #take values for insert START
            filetype = filename[:-5]
            print('filetype',filetype)
            filename = userid+entityid+filename
            files3bucket='zappa-44lyjdddx'
            files3key=filename
            #filesubmitstaus = 'I'

            print('filename',filename)
            #take values for insert END

            print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
            data=file
            print('starting upload to s3')
            
            s3 = boto3.client('s3')
            
            try:
                s3.upload_fileobj(data, files3bucket, files3key)
            except Exception as e:
                print(e)
                dbqerr={}
                db.mydbcloseall(con,cur)
                dbqerr['natstatus'] = 'error'
                dbqerr['statusdetails'] = 'File upload failed'
                resp = make_response(jsonify(dbqerr), 400)
                return(resp)
            else:
                print('success')
            
            #INSERT UPLOADED DOC DETAILS START
            
            command = cur.mogrify("INSERT INTO fileuploadmaster (fupllguserid,fuplfilecat,fuplfiletype,fuplfilename,fuplfiles3bucket,fuplfiles3key,fuplfilesubmitstaus,fuploctime,fupllmtime,fuplentityid) VALUES (%s,'E',%s,%s,%s,%s,'I',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,%s);",(userid,filetype,filename,files3bucket,files3key,entityid,))
            cur, dbqerr = db.mydbfunc(con,cur,command)
            print(dbqerr['natstatus'])
            if cur.closed == True:
                if(dbqerr['natstatus'] == "error" or dbqerr['natstatus'] == "warning"):
                    dbqerr['statusdetails']="SIGNUP update failed"
                resp = make_response(jsonify(dbqerr), 400)
                return(resp)
            con.commit()                  
            print(cur)
            print('consider insert or update is successful')

            #INSERT UPLOADED DOC DETAILS END
                       
             
    elif request.method == 'OPTIONS':
        return "ok"
    else:
        print("inside else")
    
    respjsonstr=fetchfilelist(userid,entityid,con,cur,s3)

    db.mydbcloseall(con,cur)
    return make_response(jsonify(respjsonstr), 200)



@app.route('/uploadedfilelist', methods=['GET','OPTIONS'])
def uploaded_file_list():
#returns the files for the user in s3
    if request.method == 'OPTIONS':
        return 'ok'
    elif request.method == 'GET':
        con,cur=db.mydbopncon()
        #userid,entityid=jwtnoverify.validatetoken(request)
        userid = 'BfulXOzj3ibSPSBDVgzMEAF1gax1'
        entityid='IN'
        s3 = boto3.client('s3')
        respjsonstr=fetchfilelist(userid,entityid,con,cur,s3)
        
        print('sending values back to frontend')
        db.mydbcloseall(con,cur)
        print((respjsonstr))
        return make_response(jsonify(respjsonstr), 200)
    else:
        print('invalid option for this function')
        return 'ok'
    


@app.route('/uploadedfiledelete', methods=['POST','OPTIONS'])
def uploaded_file_delete():
#handles user action for deleting file
    if request.method == 'OPTIONS':
        return 'ok'
    elif request.method == 'POST':
        print('uploadfiledelete inside POST')
        payload= request.get_json()
        print(payload)
        objbucket=payload['files3bucket']
        objkey=payload['files3key']
        category=payload['filecat']
        userid,entityid=jwtnoverify.validatetoken(request)
        print('starting with s3')
        
        s3 = boto3.client('s3')
        try:
            s3.delete_object(Bucket=objbucket, Key=objkey)
        except Exception as e:
            print('file deletion failed')
            dbqerr={}
            dbqerr['natstatus'] = 'error'
            dbqerr['statusdetails'] = 'File deletion failed'
            resp = make_response(jsonify(dbqerr), 400)
            return(resp)
        else:
            print('file deletion successful')
        
        print('file deletion successful and starting with db connection')
        con,cur=db.mydbopncon()
        
        #Delete the entry of the file from db START
        command = cur.mogrify("DELETE FROM fileuploadmaster WHERE fuplfiles3bucket = %s AND fuplfiles3key = %s AND fuplfilecat = %s AND fupllguserid = %s AND fuplentityid = %s",(objbucket,objkey,category,userid,entityid,))
        print(command)
        cur, dbqerr = db.mydbfunc(con,cur,command)
        print(cur)
        print(dbqerr)
        print(type(dbqerr))
        print(dbqerr['natstatus'])

        if cur.closed == True:
            if(dbqerr['natstatus'] == "error" or dbqerr['natstatus'] == "warning"):
                dbqerr['statusdetails']="Fileupload master delete failed"
                resp = make_response(jsonify(dbqerr), 400)
                return(resp)
        con.commit()                  
        print(cur)
        print('consider delete is successful')

        #Delete the entry of the file from db END

        respjsonstr=fetchfilelist(userid,entityid,con,cur,s3)

        db.mydbcloseall(con,cur)
    return make_response(jsonify(respjsonstr), 200)


@app.route('/uploadedfilesubmit', methods=['OPTIONS','GET'])
def uploaded_file_submit():
#handles user action for final submit to BSE
    if request.method == 'OPTIONS':
        print('options')
        return 'ok'
    elif request.method == 'GET':
        print('iam inside get of uploadedfilesubmit')
        userid,entityid=jwtnoverify.validatetoken(request)
        con,cur=db.mydbopncon()
        
        cmdqry = "SELECT lgclientcode FROM userlogin WHERE lguserid = %s AND lgentityid = %s"        
        command = cur.mogrify(cmdqry,(userid,entityid,))
        print(command)
        cur, dbqerr = db.mydbfunc(con,cur,command)
        rowcount = cur.rowcount

        if cur.closed == True:
            if(dbqerr['natstatus'] == "error" or dbqerr['natstatus'] == "warning"):
                dbqerr['statusdetails']="Fileuploadsubmit data fetch failed"
                resp = make_response(jsonify(dbqerr), 400)
                return(resp)

        records=[]
        if rowcount != 0:                
            for record in cur:  
                print('inside for')
                print(record)             
                records.append(record)
        print(records)
        clientcode = records[0]
        
        #clientcode='A000000001'

        cmdqry = "SELECT fuplfiles3bucket,fuplfiles3key,fuplfiletype FROM fileuploadmaster WHERE fuplfilesubmitstaus != 'S' and fuplfilecat = 'E' AND fupllguserid = %s AND fuplentityid = %s"          
        command = cur.mogrify(cmdqry,(userid,entityid,))
        print(command)
        cur, dbqerr = db.mydbfunc(con,cur,command)
        rowcount = cur.rowcount

        if cur.closed == True:
            if(dbqerr['natstatus'] == "error" or dbqerr['natstatus'] == "warning"):
                dbqerr['statusdetails']="Fileuploadsubmit data fetch failed"
                resp = make_response(jsonify(dbqerr), 400)
                return(resp)

        records=[]
        if rowcount != 0:                
            for record in cur:  
                print('inside for')
                print(record)             
                records.append(record)

        print(records)
        #======delete
        #records=['nat','kumar']
        #======
        reqjsons_payload={}
        for record in records:
            #======Uncomment
            bucket,key,filetype = record
            #======Uncomment
            s3 = boto3.resource('s3')
            obj = s3.Object(bucket, key)
            fls1=obj.get()['Body'].read()            
            fls=bytearray(fls1)

            files_payload = {'myfiles': fls}
            reqjsons_payload={'ClientCode':clientcode,'filetype':filetype,'publickey':'to be implemented'}     
            
            print('starting upload')
            #url='http://127.0.0.1:8000/fileuploadapi'
            url=settings.BSESTAR_AOFUPLOAD_URL[settings.LIVE]
            print(url)
            r = requests.post(url, files=files_payload, data=reqjsons_payload)
            rj= json.loads(r.text)
            print (r.content)
            if r.status_code != 200:	
                resp = make_response(jsonify({'natstatus':'error','statusdetails':rj['ResponseString']}), 400)
            else:
                print('calling bse is successful')
                #for successful cases response is made at the end
                #resp=make_response(jsonify({'natstatus':'error','statusdetails':r.content}), 400)

    else:
        print('No records to upload')
        return 'No records to upload'


    #All successful so we are updating the user status
    #Only time we come here is for GET request

    #Update userstatus to P to indicate completion of document upload Completed
    command = cur.mogrify("UPDATE userlogin SET lguserstatus = 'P', lglmtime = CURRENT_TIMESTAMP WHERE lguserid = %s AND lgentityid = %s;",(userid,entityid,))        
    print(command)            
    cur, dbqerr = db.mydbfunc(con,cur,command)
    print(dbqerr['natstatus'])
    if cur.closed == True:
        if(dbqerr['natstatus'] == "error" or dbqerr['natstatus'] == "warning"):
            dbqerr['statusdetails']="user status update after doc upload failed"
            resp = make_response(jsonify(dbqerr), 400)
            return(resp)
    #con.commit()                  
    #print(cur)
    print('consider insert or update is successful')

    #INSERT NOTIFICATION ENTRY FOR COMPLETION OF REGISTRAION AND DOC UPLOAD START


    command = cur.mogrify("SELECT count(*) FROM notifimaster WHERE nfname='reistrationcomplet' AND nfmuserid = %s and nfmentityid = %s;",(userid,entityid,))
    print(command)
    cur, dbqerr = db.mydbfunc(con,cur,command)
    rowcount = cur.rowcount

    if cur.closed == True:
        if(dbqerr['natstatus'] == "error" or dbqerr['natstatus'] == "warning"):
            dbqerr['statusdetails']="Fileuploadsubmit data fetch failed"
            resp = make_response(jsonify(dbqerr), 400)
            return(resp)

    records=[]
    if rowcount != 0:                
        for record in cur:  
            print('inside for')
            print(record)             
            records.append(record)
    print(records)
    countis = records[0]

    if countis == 0:
        nfmid=datetime.now().strftime('%Y%m%d%H%M%S%f')
        print('nfmid :',nfmid)
        command = cur.mogrify("INSERT INTO notifimaster (nfmid,nfname,nfmuserid,nfmscreenid,nfmessage,nfmsgtype,nfmprocessscope,nfmnxtact,nfmnxtactmsg,nfmnxtactnavtyp,nfmnxtactnavdest,nfmstartdt,nfmoctime,nfmlmtime,nfmentityid) VALUES (%s,'reistrationcomplet',%s,'dashboard','Registration process completed, you can start buying','notifaction','P','Y','','NONE','NONE',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,%s) WHERE NOT EXISTS (SELECT 1 FROM notifimaster WHERE nfname='reistrationcomplet');",(nfmid,userid,entityid,))
        print(command)
        cur, dbqerr = db.mydbfunc(con,cur,command)
        print(dbqerr['natstatus'])
        if cur.closed == True:
            if(dbqerr['natstatus'] == "error" or dbqerr['natstatus'] == "warning"):
                dbqerr['statusdetails']="docupload complete notification update failed"
                resp = make_response(jsonify(dbqerr), 400)
                return(resp)
        con.commit()                  
        print(cur)
        print('consider insert or update is successful')

    #INSERT NOTIFICATION ENTRY FOR COMPLETION OF REGISTRAION AND DOC UPLOAD END   
    #Now send resonse back to client confirming registration success.  Navigate to home screen
    resp = make_response(jsonify({'natstatus':'success','statusdetails':'File upload successful'}), 200)
    #resp = make_response('File upload successful', 200)
    
    #resp = 'success da nattu'
    return resp


def fetchfilelist(userid,entityid,con,cur,s3):
#Function to fetch the list of files avaialbel in S3 for the user as per the data inthe table
    
    cmdqry = "SELECT fuplfilecat,fuplfiletype,fuplfilename,fuplfiles3bucket,fuplfiles3key FROM fileuploadmaster WHERE fuplfilesubmitstaus != 'S' AND fupllguserid = %s AND fuplentityid = %s"
          
    command = cur.mogrify(cmdqry,(userid,entityid,))
    print(command)
    cur, dbqerr = db.mydbfunc(con,cur,command)
    rowcount = cur.rowcount

    if cur.closed == True:
        if(dbqerr['natstatus'] == "error" or dbqerr['natstatus'] == "warning"):
            dbqerr['statusdetails']="Fileupload data fetch failed"
            resp = make_response(jsonify(dbqerr), 400)
            return(resp)
    
    records=[]
    if rowcount != 0:                
        for record in cur:  
            print('inside for')
            print(record)             
            records.append(record)
    print(records)
    
    respjsonstr=[]
    if len(records) !=0:
        for record in records:
            filecat,filetype,filename,files3bucket,files3key=record
            url ='htt://testurlfor'+filename
            
            url = s3.generate_presigned_url(
            ClientMethod='get_object',
            Params={
                'Bucket': files3bucket,
                'Key': files3key
            }
            )
            
            #print(url)

            respjsonstr.append({'filecat':filecat,'filetype':filetype,'filename':filename,'files3bucket':files3bucket,'files3key':files3key,'files3link':url})
    else:
        print('No upload record')

    return respjsonstr