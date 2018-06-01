from flask import redirect, request,make_response
from datetime import datetime, timedelta
from flask import jsonify
from allprocessing import dbfunc as db
import multiprocessing


import psycopg2
import jwt
import requests
import json


      
#@app.route('/notiprocess',methods=['GET','POST','OPTIONS'])
def notiprocess(module,lazyloadid,userid,entityid):
    #pending logics to be written    
    #This is called by notification service
    print("inside notiprocessing")

    lazyloadid=lazyloadid          
    screenid=module
    userid=userid
    entityid= entityid
    print('value of lazyload',lazyloadid)
    print('value of screenid',screenid)
    print('value of userid',userid)
    print('value of entityid',entityid)
            
    print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    

    #This is to be moved to a configurable place
    #conn_string = "host='localhost' dbname='postgres' user='postgres' password='password123'"
    #This is to be moved to a configurable place
    #con=psycopg2.connect(conn_string)
    #cur = con.cursor()

    con,cur=db.mydbopncon()
    print(lazyloadid)
    isnotiusrup2dt=cknotiusrup2dt(screenid,userid,entityid,con,cur)

    if screenid == 'signin':
        pass

        '''            
        #processes day and session items here
        print('inside sigin')

        #start the processing multiprocess
        
        #QUERY THAT FETCHES ALL TO BE PROCESSED.  PASS nfuuid,nfuuserid,nfuentityid TO THE SPAWNED PROCESS TO PROCESS THEM
        command = cur.mogrify("SELECT nfuuid,nfuuserid,nfuentityid FROM notifiuser WHERE nfustatus = 'P' AND nfprocessscope IN ('D','S') AND nfuuserid = %s AND nfuentityid = %s;" ,(userid,entityid,))
        cur, dbqerr = db.mydbfunc(con,cur,command)
        print(cur)
        print(dbqerr)
        print(type(dbqerr))
        print(dbqerr['natstatus'])
        if cur.closed == True:
            if(dbqerr['natstatus'] == "error" or dbqerr['natstatus'] == "warning"):
                dbqerr['statusdetails']="loginuser Fetch failed"
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

        if len(records) == 0:
            #NO RECORDS TO PROCESS
            pass
        else:
            #HAVE RECORDS TO PROCESS   
            pass        


        #start the processing multiprocess      
            
            
        #return 'ok'
        '''

    elif screenid == 'dashboard':
        #process the everypage load items + Newly added day and session(flag to find this is nfuprocesbypgldsvc = 'Y')
        print('inside dashboard')
        
        command = cur.mogrify("UPDATE notifiuser SET nfulazyldid = %s, nfulazyldidstatus = 'N' WHERE nfuuserid = %s AND nfuentityid = %s AND nfuscreenid = %s;",(lazyloadid,userid,entityid,screenid,))
        #command1 = cur.mogrify("select json_agg(c) from (SELECT nfuuid,nfumessage,nfumsgtype FROM notifiuser WHERE nfuuserid = %s AND nfuentityid = %s AND nfuscreenid='dashboard' AND nfustatus = 'C' and nfulazyldid = %s) as c;",(userid,entityid,lazyloadid) )
        print(command)
        print('after lazid update')               
        cur, dbqerr = db.mydbfunc(con,cur,command)
        print(dbqerr['natstatus'])
        if cur.closed == True:
            if(dbqerr['natstatus'] == "error" or dbqerr['natstatus'] == "warning"):
                dbqerr['statusdetails']="pf Fetch failed"
            resp = make_response(jsonify(dbqerr), 400)
            return(resp)
        con.commit()                  
        print(cur)
        print('consider insert or update is successful')
        
        # Send the response back and continue processing

        #start the processing multiprocess START
        #QUERY THAT FETCHES ALL TO BE PROCESSED.  PASS nfuuid,nfuuserid,nfuentityid TO THE SPAWNED PROCESS TO PROCESS THEM
        
        cmdqry = "SELECT nfumid,nfuname,nfuuserid,nfuentityid FROM notifiuser WHERE nfustatus = 'P' AND nfprocessscope NOT IN ('D','S') AND nfuuserid = %s AND nfuentityid = %s"
        cmdqry = cmdqry + " UNION "
        cmdqry = cmdqry + "SELECT nfumid,nfuname,nfuuserid,nfuentityid FROM notifiuser WHERE nfustatus = 'P' AND nfprocessscope IN ('D','S') AND nfuprocesbypgldsvc = 'Y' AND nfuuserid = %s AND nfuentityid = %s;"
                    
        command = cur.mogrify(cmdqry,(userid,entityid,userid,entityid,))
        print(command)
        cur, dbqerr = db.mydbfunc(con,cur,command)
        rowcount = cur.rowcount

        records=[]
        if rowcount != 0:                
            for record in cur:  
                print('inside for')
                print(record)             
                records.append(record)
        print(records)
        if len(records) !=0:
            jobs = []
            for i in records:
                uid,name,userid,entityid = i
                print('uid :',uid, 'name :',name, ' userid : ',userid,' entityid : ',entityid)
                #prosta=notiprocessingfunctions(uid,name,userid,entityid)
                p = multiprocessing.Process(name = name, target=notiprocessingfunctions, args=(uid,name,userid,entityid,))
                jobs.append(p)
                p.start()
                p.join()
        #start the processing multiprocess END

    elif lazyloadid != 'dashboard' or lazyloadid != 'signin':
        pass
    print('returning ok while i still process')
    db.mydbcloseall(con,cur)
    return True


def cknotiusrup2dt(screenid,userid,entityid,con,cur):
    print('inside cknotiusrup2dt')
    command = cur.mogrify("select distinct nfuoctime from notifiuser where nfuuserid = %s and nfuentityid = %s;",(userid,entityid,) )
    cur, dbqerr = db.mydbfunc(con,cur,command)
    print(cur)
    print(dbqerr)
    print(type(dbqerr))
    print(dbqerr['natstatus'])
    rowcount = cur.rowcount

    if rowcount != 0:
        records=[]
        for record in cur:  
            print('inside for')
            print(record)             
            records.append(record)
        datetimenf, = records[0]
                 
    elif rowcount == 0:
        #just to make sure the if condition fails
        datetimenf = datetime.now()- timedelta(1)
    else:
        pass

    print('current time : ',datetime.utcnow().date())
    print('datetimemnf',datetimenf.date())   
    if datetimenf.date() == datetime.utcnow().date():
        print('notification user table is done today')

        #delete entries in notiuser that are changed in master after the previous run of this code today
        #command = cur.mogrify("DELETE FROM notifiuser WHERE nfuuserid = %s AND nfuentityid = %s ",(userid,entityid,))
        query = "DELETE FROM notifiuser WHERE nfuuserid = %s AND nfuentityid = %s "
        query = query + "AND nfuname IN (SELECT DISTINCT(nfname) FROM notifimaster WHERE nfmuserid IN (%s,'ALL') AND nfmentityid = %s "
        query = query + "AND nfmlmtime > (SELECT nfuoctime FROM notifiuser WHERE nfuuserid IN (%s,'ALL') AND nfuentityid = %s ORDER BY nfuoctime DESC LIMIT 1) )"
        command = cur.mogrify(query,(userid,entityid,userid,entityid,userid,entityid,))

        print(command)
        cur, dbqerr = db.mydbfunc(con,cur,command)
        print(cur)
        print(dbqerr)
        print(type(dbqerr))
        print(dbqerr['natstatus'])

        if cur.closed == True:
            if(dbqerr['natstatus'] == "error" or dbqerr['natstatus'] == "warning"):
                dbqerr['statusdetails']="usernotify delete failed"
            resp = make_response(jsonify(dbqerr), 400)
            return(resp)
        con.commit()                  
        print(cur)
        print('consider delete is successful')

        
        #notification user table is done today so no action required
        query = "INSERT INTO notifiuser (nfumid,nfuname,nfuuserid,nfuscreenid,nfumessage,nfumsgtype,nfprocessscope,nfuhvnxtact,nfunxtactmsg,nfunxtactnavtyp,nfunxtactnavdest,nfulazyldidstatus,nfustatus,nfuprocesbypgldsvc,nfutodayshowncount,nfuoctime,nfulmtime,nfuentityid) "
        query = query+ "(SELECT nfmid,nfname,nfmuserid,nfmscreenid,nfmessage,nfmsgtype,nfmprocessscope,nfmnxtact,nfmnxtactmsg,nfmnxtactnavtyp,nfmnxtactnavdest,'N','P','Y',0,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,%s"
        query = query + "FROM notifimaster WHERE nfmuserid IN (%s,'ALL') AND nfmentityid = %s "
        query = query + "AND nfmlmtime > (SELECT nfuoctime FROM notifiuser WHERE nfuuserid IN (%s,'ALL') AND nfuentityid = %s ORDER BY nfuoctime DESC LIMIT 1) "
        query = query + "AND nfname NOT IN (SELECT DISTINCT(nfuname) FROM notifiuser WHERE nfuuserid IN (%s,'ALL') AND nfuentityid = %s) )"
        command = cur.mogrify(query,(entityid,userid,entityid,userid,entityid,userid,entityid,))

    else:
        print('inside else')
        #notification user table is old, create from master now
        command = cur.mogrify("DELETE FROM notifiuser WHERE nfuuserid = %s AND nfuentityid = %s",(userid,entityid,))
        print(command)
        cur, dbqerr = db.mydbfunc(con,cur,command)
        print(cur)
        print(dbqerr)
        print(type(dbqerr))
        print(dbqerr['natstatus'])

        if cur.closed == True:
            if(dbqerr['natstatus'] == "error" or dbqerr['natstatus'] == "warning"):
                dbqerr['statusdetails']="usernotify delete failed"
            resp = make_response(jsonify(dbqerr), 400)
            return(resp)
        con.commit()                  
        print(cur)
        print('consider delete is successful')

        query = "INSERT INTO notifiuser (nfumid,nfuname,nfuuserid,nfuscreenid,nfumessage,nfumsgtype,nfprocessscope,nfuhvnxtact,nfunxtactmsg,nfunxtactnavtyp,nfunxtactnavdest,nfulazyldidstatus,nfustatus,nfutodayshowncount,nfuoctime,nfulmtime,nfuentityid) "
        query = query+"(SELECT nfmid,nfname,nfmuserid,nfmscreenid,nfmessage,nfmsgtype,nfmprocessscope,nfmnxtact,nfmnxtactmsg,nfmnxtactnavtyp,nfmnxtactnavdest,'N','P',0,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,%s "
        query = query + "FROM notifimaster WHERE nfmuserid IN (%s,'ALL') AND nfmentityid = %s)"
        command = cur.mogrify(query,(entityid,userid,entityid,))

    print(command)
    cur, dbqerr = db.mydbfunc(con,cur,command)
    print(cur)
    print(dbqerr)
    print(type(dbqerr))
    print(dbqerr['natstatus'])

    if cur.closed == True:
        if(dbqerr['natstatus'] == "error" or dbqerr['natstatus'] == "warning"):
            dbqerr['statusdetails']="pf Fetch failed"
        resp = make_response(jsonify(dbqerr), 400)
        return(resp)
    
    con.commit()                  
    print(cur)
    print('consider insert or update is successful')
    
    #Update nfprocessscope = P records as topick for processing and not sent to ui as part of lazyid yet START
    query = "UPDATE notifiuser SET nfustatus = 'P' WHERE nfuuserid = %s AND nfuentityid = %s AND nfuscreenid = %s AND nfprocessscope IN ('P');"
    command = cur.mogrify(query,(userid,entityid,screenid))

    print(command)
    cur, dbqerr = db.mydbfunc(con,cur,command)
    print(cur)
    print(dbqerr)
    print(type(dbqerr))
    print(dbqerr['natstatus'])

    if cur.closed == True:
        if(dbqerr['natstatus'] == "error" or dbqerr['natstatus'] == "warning"):
            dbqerr['statusdetails']="pf Fetch failed"
        resp = make_response(jsonify(dbqerr), 400)
        return(resp)
    
    con.commit()                  
    print(cur)
    print('consider insert or update is successful')
    #Update nfprocessscope = P records as topick for processing and not sent to ui as part of lazyid yet END

    print('cknotiusrup2dt completed')
    return True

#function where you have put all notification related processings START
#This is called in the multiprocesser section for each entry in notifuser table START

def notiprocessingfunctions(uid,name,userid,entityid):
#Function which holds the logic for all notification processes
    print("inside notification processing function")
    print('name= ',name)
    if name == 'pendingregistration':
        print('inside if of pendingregistration')
        userstatus = getuserstatus(userid,entityid)
        print(userstatus)
        if userstatus != 'R':
            print('inside pendingregistration !=R')
            deletenotification(userid,entityid,name,uid,'both')
        elif userstatus == 'R':
            print('inside pendingregistration ==R')
            updatenotificationstatus(userid,entityid,uid)
        else:
            print('inside else')
            pass
    else:
        print('inside else of pendingregistration')

    if name == 'pendingregisupload':
        userstatus = getuserstatus(userid,entityid)
        print(userstatus)
        if userstatus != 'U':
            print('inside pendingregisupload !=U')
            deletenotification(userid,entityid,name,uid,'both')
        elif userstatus == 'U':
            print('inside pendingregisupload ==U')
            updatenotificationstatus(userid,entityid,uid)
        else:
            pass
    else:
        print('inside else of pendingregisupload')
    '''
    if name =='somethingsomething'
    do something something
    '''


#function where you have put all notification related processings END
#This is called in the multiprocesser section for each entry in notifuser table END


def deletenotification(userid,entityid,name,uid,recordtodelete):
    con,cur=db.mydbopncon()

    if recordtodelete == 'both' or recordtodelete == 'master':
        command = cur.mogrify("DELETE FROM notifimaster WHERE nfmuserid = %s AND nfmentityid = %s AND nfname= %s AND nfmid = %s;" ,(userid,entityid,name,uid,))
        cur, dbqerr = db.mydbfunc(con,cur,command)
        print(dbqerr['natstatus'])
        if cur.closed == True:
            if(dbqerr['natstatus'] == "error" or dbqerr['natstatus'] == "warning"):
                dbqerr['statusdetails']="loginuser Fetch failed"
            db.mydbcloseall(con,cur)
            resp = make_response(jsonify(dbqerr), 400)
            return(resp)
        else:
            pass
	
    if recordtodelete == 'both' or recordtodelete == 'notiuser':
        command = cur.mogrify("DELETE FROM notifiuser WHERE nfuuserid = %s AND nfuentityid = %s AND nfuname= %s AND nfumid = %s;" ,(userid,entityid,name,uid,))
        cur, dbqerr = db.mydbfunc(con,cur,command)
        print(dbqerr['natstatus'])
        if cur.closed == True:
            if(dbqerr['natstatus'] == "error" or dbqerr['natstatus'] == "warning"):
                dbqerr['statusdetails']="loginuser Fetch failed"
            db.mydbcloseall(con,cur)
            resp = make_response(jsonify(dbqerr), 400)
            return(resp)
        else:
            pass

    con.commit()    
    db.mydbcloseall(con,cur)

def updatenotificationstatus(userid,entityid,uid):
    con,cur=db.mydbopncon()

    command = cur.mogrify("UPDATE notifiuser SET nfustatus = 'C', nfulmtime = CURRENT_TIMESTAMP WHERE nfuuserid = %s AND nfuentityid = %s AND nfumid = %s;",(userid,entityid,uid,))                        
    cur, dbqerr = db.mydbfunc(con,cur,command)
    print(dbqerr['natstatus'])
    if cur.closed == True:
        if(dbqerr['natstatus'] == "error" or dbqerr['natstatus'] == "warning"):
            dbqerr['statusdetails']="pf Fetch failed"
            resp = make_response(jsonify(dbqerr), 400)
            return(resp)
    con.commit()                  
    print(cur)
    print('consider insert or update is successful')
    db.mydbcloseall(con,cur)


def getuserstatus(userid,entityid):
    #This is to be moved to a configurable place
    #conn_string = "host='localhost' dbname='postgres' user='postgres' password='password123'"
    #This is to be moved to a configurable place
    #con=psycopg2.connect(conn_string)
    #cur = con.cursor()
    print('inside getuserstatus')
    con,cur=db.mydbopncon()
    print('inside getuserstatus')
    command = cur.mogrify("SELECT lguserstatus FROM userlogin WHERE lguserid = %s AND lgentityid = %s;" ,(userid,entityid,))
    print(command)
    cur, dbqerr = db.mydbfunc(con,cur,command)
    print(cur)
    print(dbqerr)
    print(type(dbqerr))
    print(dbqerr['natstatus'])
    if cur.closed == True:
        if(dbqerr['natstatus'] == "error" or dbqerr['natstatus'] == "warning"):
            dbqerr['statusdetails']="Userstatus Fetch failed"
        resp = make_response(jsonify(dbqerr), 400)
        return(resp)
    else:
        pass

    records=[]
    for record in cur:  
        print('inside for')
        print(record)             
        records.append(record)
    
    if len(records) ==1:
        db.mydbcloseall(con,cur)
        print('returning record :',records[0][0])
        return records[0][0]
    else:
        dbqerr['statusdetails']="Userstatus Fetch returned no records"
        dbqerr['natstatus'] == "error"
        db.mydbcloseall(con,cur)
        resp = make_response(jsonify(dbqerr), 400)
        return(resp)


