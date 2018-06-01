from portfolio.portfoliomain import app
from flask import request, make_response, jsonify, Response, redirect
from datetime import datetime
from portfolio import dbfunc as db
from portfolio import jwtdecodenoverify as jwtnoverify
from dateutil import tz
from datetime import datetime

import psycopg2
import json
import jwt


@app.route('/pfdatafetch',methods=['GET','POST','OPTIONS'])
def funddatafetc1h():
#This is called by fund data fetch service
    if request.method=='OPTIONS':
        print("inside FUNDDATAFETCH options")
        return make_response(jsonify('inside FUNDDATAFETCH options'), 200)  

    elif request.method=='GET':
        print("inside PFDATAFETCH GET")
        

        print((request))        
        print(request.headers)
        userid,entityid=jwtnoverify.validatetoken(request)
        print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        print(userid,entityid)
        print('after')
        
        con,cur=db.mydbopncon()
        
        print(con)
        print(cur)
        
        #cur.execute("select row_to_json(art) from (select a.*, (select json_agg(b) from (select * from pfstklist where pfportfolioid = a.pfportfolioid ) as b) as pfstklist, (select json_agg(c) from (select * from pfmflist where pfportfolioid = a.pfportfolioid ) as c) as pfmflist from pfmaindetail as a where pfuserid =%s ) art",(userid,))
        command = cur.mogrify("select row_to_json(art) from (select a.*, (select json_agg(b) from (select * from webapp.pfstklist where pfportfolioid = a.pfportfolioid ) as b) as pfstklist, (select json_agg(c) from (select c.*,(select json_agg(d) from (select * from webapp.pfmforlist where orormflistid = c.ormflistid) as d) as ormffundorderlists from webapp.pfmflist c where pfportfolioid = a.pfportfolioid ) as c) as pfmflist from webapp.pfmaindetail as a where pfuserid =%s ) art",(userid,))
        cur, dbqerr = db.mydbfunc(con,cur,command)
        print("#########################################3")
        print(command)
        print("#########################################3")
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
        
        print("portfolio details returned for user: "+userid)
        
        resp = json.dumps(records)
    
    return resp


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
        command = cur.mogrify("select row_to_json(art) from (select a.fndnatcode,a.fnddisplayname,a.fndminpuramt,a.fndaddpuramt,a.fndmaxpuramt,a.fndpuramtinmulti,a.fndpurcutoff, (select json_agg(c) from (select sipfreq,sipfreqdates,sipminamt,sipmaxamt,sipmulamt,sipmininstal,sipmaxinstal,sipmingap from webapp.fundsipdt where sipfndnatcode = a.fndnatcode ) as c) as fnsipdt from webapp.fundmaster as a where UPPER(fnddisplayname) like (%s)) art",(teee,))
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
        
        resp = json.dumps(records)
    
    return resp


@app.route('/pfdatasave',methods=['GET','POST','OPTIONS'])
#example for model code http://www.postgresqltutorial.com/postgresql-python/transaction/
def pfdatasavee():
    
    if request.method=='OPTIONS':
        print ("inside PFDATASAVE options")
        return jsonify({'body':'success'})

    elif request.method=='POST':   
        print ("inside PFDATASAVE post")

        print(request.headers)
        payload= request.get_json()
        #payload = request.stream.read().decode('utf8')    
        
        pfdata = payload
        print("------------------")
        print(pfdata)
        print("------------------")
        print("pfdata['pfstartdt']")
        pfdata['pfstartdt']=dateformat(pfdata['pfstartdt'])
        print(pfdata['pfstartdt'])

        print("pfdata['pftargetdt']")
        pfdata['pftargetdt']=dateformat(pfdata['pftargetdt'])
        print(pfdata['pftargetdt'])


        #print(type(pfdata['pfstartdt']))
        print("------------------")
        userid,entityid=jwtnoverify.validatetoken(request)
        con,cur=db.mydbopncon()

        print(con)
        print(cur)

        print("pfdata before removing")
        print(pfdata)
        savetype = ""
        if 'pfportfolioid' in pfdata:
            if pfdata.get('pfportfolioid') == "New":
                savetype = "New"
            else:
                savetype = "Old"
        else:
            #if 'pfportfolioid' itself not in the data it is error we shouuld exit
            print('pfportfolioid is not in the messages')
            return jsonify({'natstatus':'error','statusdetails':'Data error (Portfolio id missing)'})

        if 'pfstklist' in pfdata:
            pfstlsdata = pfdata.pop("pfstklist")            
            print("pfstlsdata")
        else:
            pfstlsdata=None
            print("key pfstklist not in the submitted record")
            #return jsonify({'natstatus':'error','statusdetails':'Data error (stocklist missing)'})
        
        if 'pfmflist' in pfdata:
            pfmflsdata = pfdata.pop("pfmflist")
            print("pfmflist")
            print(pfmflsdata)
        else:
            pfmflsdata=None
            print("key pfmflist not in the submitted record")
            #return jsonify({'natstatus':'error','statusdetails':'Data error (mflist missing)'})


        print("after removing")
        print("pfdata")
        print(pfdata)
        
        ''' Prepre DB connection START 
        conn_string = "host='localhost' dbname='postgres' user='postgres' password='password123'"
        con=psycopg2.connect(conn_string)
        cur = con.cursor()
        ### Prepre DB connection END ###
        '''
        command = cur.mogrify("BEGIN;")
        cur, dbqerr = db.mydbfunc(con,cur,command)
        if cur.closed == True:
            if(dbqerr['natstatus'] == "error" or dbqerr['natstatus'] == "warning"):
                dbqerr['statusdetails']="DB query failed, BEING failed"
            resp = make_response(jsonify(dbqerr), 400)
            return(resp)

        savetimestamp = datetime.now()
        pfsavedate=savetimestamp.strftime('%Y%m%d') 
        pfsavetimestamp=savetimestamp.strftime('%Y/%m/%d %H:%M:%S')
        
        print("pfsavetimestamp1")
        print("pfsavetimestamp1")
        #useridstr=pfdata.get('pfuserid')
        useridstr=userid
        pfdata['pfuserid']=userid

        if savetype == "New": 
            print('inside New')
            pfdata['pfoctime']= pfsavetimestamp
            pfdata['pflmtime']= pfsavetimestamp
            print('MAX query')
            command = cur.mogrify("SELECT MAX(pfpfidusrrunno) FROM webapp.pfmaindetail where pfuserid = %s",(useridstr,))
            cur, dbqerr = db.mydbfunc(con,cur,command)
            print(cur)
            print(dbqerr)
            print(type(dbqerr))
            print(dbqerr['natstatus'])

            if cur.closed == True:
                if(dbqerr['natstatus'] == "error" or dbqerr['natstatus'] == "warning"):
                    dbqerr['statusdetails']="Max Number identification Failed"
                resp = make_response(jsonify(dbqerr), 400)
                return(resp)
            
            for record in cur:
                print(record)

            if record[0]== None:
                pfmainnextmaxval = 1
            else:
                pfmainnextmaxval = record[0] +1

            pfdata['pfpfidusrrunno']=pfmainnextmaxval
            pfdata['pfportfolioid']=useridstr+str(pfmainnextmaxval)
            
            if pfdata.get('pfbeneusers') == None:
                pfdata['pfbeneusers']=useridstr
            
            pfdata['entityid']=entityid

            pfdatajsondict = json.dumps(pfdata)

            command = cur.mogrify("INSERT INTO webapp.pfmaindetail select * from json_populate_record(NULL::webapp.pfmaindetail,%s);",(str(pfdatajsondict),))

            cur, dbqerr = db.mydbfunc(con,cur,command)
            if cur.closed == True:
                if(dbqerr['natstatus'] == "error" or dbqerr['natstatus'] == "warning"):
                    dbqerr['statusdetails']="Couldn't save Porfolio (main insert  Failed).  Contact support"
                resp = make_response(jsonify(dbqerr), 400)
                return(resp)
    
            pfstlsseqnum=1
            if pfstlsdata!=None:
                for d in pfstlsdata:                
                    d['pfstoctime']= pfsavetimestamp
                    d['pfstlmtime']= pfsavetimestamp
                    d['pfstklistid']='st'+pfdata.get('pfportfolioid')+str(pfstlsseqnum)                
                    d['pfportfolioid']=pfdata.get('pfportfolioid')                    
                    pfstlsseqnum=pfstlsseqnum+1
                    
                    pfstlsdatajsondict = json.dumps(d)
                    command = cur.mogrify("INSERT INTO webapp.pfstklist select * from json_populate_record(NULL::webapp.pfstklist,%s);",(str(pfstlsdatajsondict),))
                    print(command)

                    cur, dbqerr = db.mydbfunc(con,cur,command)
                    if cur.closed == True:
                        if(dbqerr['natstatus'] == "error" or dbqerr['natstatus'] == "warning"):
                            dbqerr['statusdetails']="stocklist insert  Failed"
                        resp = make_response(jsonify(dbqerr), 400)
                        return(resp)


            else:
                print("done nothing as pfstlsdata is none")

            print("pfmflsdata")
            print(pfmflsdata)
            pfmflsseqnum=1
            if pfmflsdata!=None:
                for d in pfmflsdata:   
                    print("d is printed")
                    print(d)
                    d['ormflistid']= "mf"+pfdata.get('pfportfolioid')+str(pfmflsseqnum)              
                    d['ormfoctime']= pfsavetimestamp
                    d['ormflmtime']= pfsavetimestamp
                    d['orportfolioid']= pfdata.get('pfportfolioid')
                    d['entityid']=entityid
                    d['ormfseqnum']=pfmflsseqnum
                    #d['pfportfolioid']=pfdata.get('pfportfolioid')
                    if 'ormffundorderlists' in d:
                        pfmflsordata = d.pop("ormffundorderlists")
                        print("ormffundorderlists")
                        print(pfmflsordata)
                    else:
                        pfmflsordata=None
                        print("key ormffundorderlists not in the submitted record")

                    pfmflsseqnum=pfmflsseqnum+1
                    pfmflsdatajsondict = json.dumps(d)
                    command = cur.mogrify("INSERT INTO webapp.pfmflist select * from json_populate_record(NULL::webapp.pfmflist,%s);",(str(pfmflsdatajsondict),))
                    
                    cur, dbqerr = db.mydbfunc(con,cur,command)
                    if cur.closed == True:
                        if(dbqerr['natstatus'] == "error" or dbqerr['natstatus'] == "warning"):
                            dbqerr['statusdetails']="mflist insert Failed"
                        resp = make_response(jsonify(dbqerr), 400)
                        return(resp)   

                    pfmforlsseqnum=1
                    if pfmflsordata != None:
                        for e in pfmflsordata: 
                            print("PRINTING e new")
                            print(e)
                            e['orormflistid']= d.get('ormflistid')                 
                            e['ormfoctime']= pfsavetimestamp
                            e['ormflmtime']= pfsavetimestamp
                            e['orormfpflistid']= "or"+d.get('ormflistid')+str(pfmforlsseqnum)
                            e['entityid']=entityid
                            e['orormfseqnum']=pfmforlsseqnum                            
                            pfmforlsseqnum = pfmforlsseqnum+1
                            pfmflsordatajsondict = json.dumps(e)

                            command = cur.mogrify("INSERT INTO webapp.pfmforlist select * from json_populate_record(NULL::webapp.pfmforlist,%s);",(str(pfmflsordatajsondict),))
                    
                            cur, dbqerr = db.mydbfunc(con,cur,command)
                            if cur.closed == True:
                                if(dbqerr['natstatus'] == "error" or dbqerr['natstatus'] == "warning"):
                                    dbqerr['statusdetails']="mflist details insert Failed"
                                resp = make_response(jsonify(dbqerr), 400)
                                return(resp)  
                    else:
                        print("done nothing as the pfmflsordata is none")


            else:
                print("done nothing as pfmflsdata is none")

            con.commit()    
            cur.close()
            con.close()
            return jsonify({'natstatus':'success','statusdetails':'New portfolio ' + pfdata.get('pfportfolioname') +' created'})

            
            '''
            [{'pfmfnav': '75', 'pfmfpercent': '70', 'pfmfamt': 0, 'pfmffundname': 'MNC', 'pfmfallotedamt': 4121.599999999999}, {'pfmfnav': '66', 'pfmfpercent': '10', 'pfmfamt': 0, 'pfmffundname': 'OII', 'pfmfallotedamt': 588.8000000000001}]

            [{'pfstexchange': 'NSE', 'pfstallotedamt': 588.8000000000001, 'pfstpercent': '10', 'pfstTotUnit': '0', 'pfsttradingsymbl': 'ITC', 'pfstamt': 0, 'pfsttotunit': 0, 'pfstltp': '788'}, {'pfstexchange': 'NSE', 'pfstallotedamt': 588.8000000000001, 'pfstpercent': '10', 'pfstTotUnit': '6', 'pfsttradingsymbl': 'SBIN', 'pfstamt': 0, 'pfsttotunit': 0, 'pfstltp': '89'}]

            {'pfstkamtsplittype': '%', 'pftargetintrate': None, 'pfportfolioname': 'ASDFASDF', 'pfmfamtsplittype': '%', 'pfsummed': 5888, 'pfpurpose': 'ASDFASDF', 'pftargetdt': None, 'pfplannedinvamt': 5888, 'pfstartdt': None, 'pfportfolioid': 'New', 'pfbeneUsers': None, 'pfuserid': None, 'pfinvamtfeq': None}
            '''
        elif savetype == "Old" :
            print('inside old')
            pfdata['pflmtime']= pfsavetimestamp
            pfdata.get('pfuserid')            
 
            #To update these fields
            # pfportfolioname,pfpurpose,pfbeneusers,pfstartdt,pftargetdt,pftargetintrate,pfplannedinvamt,pfinvamtfeq,pfstkamtsplittype,pfmfamtsplittype,pflmtime

            command = cur.mogrify("SELECT pfportfolioname,pfportfolioid FROM webapp.pfmaindetail where pfuserid = %s",(useridstr,))
            cur, dbqerr = db.mydbfunc(con,cur,command)

            if cur.closed == True:
                if(dbqerr['natstatus'] == "error" or dbqerr['natstatus'] == "warning"):
                    dbqerr['statusdetails']="Portfolioname fetch Failed"
                resp = make_response(jsonify(dbqerr), 400)
                return(resp)
            

            #Check if the pf name already exits START
            if cur.rowcount == 0:
                pass
                '''
                dbqerr['statusdetails']="No Portfolioname exists"
                resp = make_response(jsonify(dbqerr), 400)
                return(resp)
                '''
            else:
                print("inside else")
                for record in cur:
                    print(record[1])
                    if (pfdata.get('pfportfolioname')==record[0]):
                        if(pfdata.get('pfportfolioid') != record[1]):
                            dbqerr['statusdetails']="Portfolioname already exists"
                            resp = make_response(jsonify(dbqerr), 400)
                            return(resp)   
            #Check if the pf name already exits START

            #PF details update START

            pfdatajsondict = json.dumps(pfdata)
            command = cur.mogrify("""
                                  update webapp.pfmaindetail set(pfportfolioname,pfpurpose,pfbeneusers,pfstartdt,pftargetdt,pftargetintrate,pfplannedinvamt,pfstkamtsplittype,pfmfamtsplittype,pflmtime) = 
                                  (select pfportfolioname,pfpurpose,pfbeneusers,pfstartdt,pftargetdt,pftargetintrate,pfplannedinvamt,pfstkamtsplittype,pfmfamtsplittype,pflmtime from json_to_record (%s)
                                  AS (pfportfolioname varchar(50),pfpurpose varchar(600),pfbeneusers varchar(40),pfstartdt date,pftargetdt date,pftargetintrate numeric(5,2),pfplannedinvamt numeric(16,5),pfstkamtsplittype varchar(10),pfmfamtsplittype varchar(10),pflmtime timestamp))
                                  where pfportfolioid = %s and pfuserid = %s;
                                  """,(str(pfdatajsondict),pfdata.get('pfportfolioid'),useridstr,))
            print(command)
            cur, dbqerr = db.mydbfunc(con,cur,command)

            if cur.closed == True:
                if(dbqerr['natstatus'] == "error" or dbqerr['natstatus'] == "warning"):
                    dbqerr['statusdetails']="main update  Failed"
                resp = make_response(jsonify(dbqerr), 400)
                return(resp)

            #PF details update END

            ###PF stock details update START###

            ''' feel we should not remove intead update existing and insert new so commenting it
            #Remove all the existing records START
            command = cur.mogrify("DELETE FROM webapp.pfstklist WHERE pfportfolioid = %s;",(pfdata.get('pfportfolioid'),))
            print(command)
            cur, dbqerr = db.mydbfunc(con,cur,command)
            if cur.closed == True:
                if(dbqerr['natstatus'] == "error" or dbqerr['natstatus'] == "warning"):
                    dbqerr['statusdetails']="stocklist deletion  Failed"
                resp = make_response(jsonify(dbqerr), 400)
                return(resp)
            #Remove all the existing records END

            #Insertion of stocklist records START
            pfstlsseqnum=1
            if pfstlsdata!=None:
                for d in pfstlsdata:                
                    print("pfstlsdata else inside for")
                    print(d)
                    d['pfstoctime']= pfsavetimestamp
                    d['pfstlmtime']= pfsavetimestamp
                    d['pfstklistid']='st'+pfdata.get('pfportfolioid')+str(pfstlsseqnum)
                    d['pfportfolioid']=pfdata.get('pfportfolioid')
                    d['entityid']=entityid
                    pfstlsseqnum=pfstlsseqnum+1
                    pfstlsdatajsondict = json.dumps(d)
                    command = cur.mogrify("INSERT INTO webapp.pfstklist select * from json_populate_record(NULL::webapp.pfstklist,%s);",(str(pfstlsdatajsondict),))
                    print(command)
                    cur, dbqerr = db.mydbfunc(con,cur,command)
                    if cur.closed == True:
                        if(dbqerr['natstatus'] == "error" or dbqerr['natstatus'] == "warning"):
                            dbqerr['statusdetails']="stocklist insert  Failed"
                        resp = make_response(jsonify(dbqerr), 400)
                        return(resp)
            else:
                print("done nothing as pfstlsdata is none")
            #Insertion of stocklist records END
            '''
            ###PF stock details update END###

            ###PF MF details update START###

            #Remove all the existing MF list records START
            '''
            command = cur.mogrify("DELETE FROM webapp.pfmflist WHERE pfportfolioid = %s;",(pfdata.get('pfportfolioid'),))
            print(command)
            cur, dbqerr = db.mydbfunc(con,cur,command)
            if cur.closed == True:
                if(dbqerr['natstatus'] == "error" or dbqerr['natstatus'] == "warning"):
                    dbqerr['statusdetails']="mflist deletion  Failed"
                resp = make_response(jsonify(dbqerr), 400)
                return(resp)
            print('pfmflist removed')
            '''
            #Remove all the existing MF list records END


            
            if pfmflsdata!=None:
                for d in pfmflsdata:  
                    print("pfmflsdata inside for")
                    print(d)
                    d['ormfoctime']= pfsavetimestamp
                    d['ormflmtime']= pfsavetimestamp

                    if(d['ormflistid']==""):
                        #New fund getting added to the PF
                        command = cur.mogrify("SELECT MAX(ormfseqnum) FROM webapp.pfmflist where orportfolioid = %s and entityid =%s;",(pfdata.get('pfportfolioid'),entityid,))
                        print(command)
                        cur, dbqerr = db.mydbfunc(con,cur,command)
                                               
                        if cur.closed == True:
                            if(dbqerr['natstatus'] == "error" or dbqerr['natstatus'] == "warning"):
                                dbqerr['statusdetails']="Fund MAX sequence failed"
                            resp = make_response(jsonify(dbqerr), 400)
                            return(resp)

                        #Model to follow in all fetch
                        records=[]
                        for record in cur:  
                            records.append(record[0])
                        print("iam printing records to see")
                        print(records)
                        
                        if(records[0] == None):
                            pfmflsseqnum=1
                        else:
                            pfmflsseqnum=records[0]+1

                        d['ormflistid']='mf'+pfdata.get('pfportfolioid')+str(pfmflsseqnum)
                        d['orportfolioid']=pfdata.get('pfportfolioid')
                        d['entityid']=entityid
                        d['ormfseqnum'] = pfmflsseqnum
                        
                        if 'ormffundorderlists' in d:
                            pfmflsordata = d.pop("ormffundorderlists")
                            print("ormffundorderlists old")
                            print(pfmflsordata)
                        else:
                            pfmflsordata=None
                            print("key ormffundorderlists not in the submitted record")

                        pfmflsdatajsondict = json.dumps(d)
                        command = cur.mogrify("INSERT INTO webapp.pfmflist select * from json_populate_record(NULL::webapp.pfmflist,%s) where entityid = %s;",(str(pfmflsdatajsondict),entityid,))
                        print(command)                
                        cur, dbqerr = db.mydbfunc(con,cur,command)
                        if cur.closed == True:
                            if(dbqerr['natstatus'] == "error" or dbqerr['natstatus'] == "warning"):
                                dbqerr['statusdetails']="mflist insert Failed"
                            resp = make_response(jsonify(dbqerr), 400)
                            return(resp)

                        pfmforlsseqnum=1
                        if pfmflsordata != None:
                            for e in pfmflsordata: 
                                print("PRINTING e")
                                print(e)                                           
                                e['ormfoctime']= pfsavetimestamp
                                e['ormflmtime']= pfsavetimestamp
                                e['entityid']=entityid
                                #For new SIP or onetime record for the fund
                                if(e['orormflistid'] ==""):
                                    e['orormfpflistid']= "or"+d.get('ormflistid')+str(pfmforlsseqnum)
                                    e['orormflistid']= d.get('ormflistid')                              
                                    e['orormfseqnum'] = pfmforlsseqnum
                                    pfmforlsseqnum = pfmforlsseqnum+1
                                    pfmflsordatajsondict = json.dumps(e)

                                    command = cur.mogrify("INSERT INTO webapp.pfmforlist select * from json_populate_record(NULL::webapp.pfmforlist,%s) where entityid = %s;",(str(pfmflsordatajsondict),entityid,))
                        
                                    cur, dbqerr = db.mydbfunc(con,cur,command)
                                    if cur.closed == True:
                                        if(dbqerr['natstatus'] == "error" or dbqerr['natstatus'] == "warning"):
                                            dbqerr['statusdetails']="mflist details insert Failed"
                                        resp = make_response(jsonify(dbqerr), 400)
                                        return(resp)
                                else:
                                    #For existing SIP or onetime record for the fund
                                    pass
                                    #'''
                                    #This condition doesn't come for new fund insert itself so commenting.
                                    #command = cur.mogrify("UPDATE webapp.pfmforlist select * from json_populate_record(NULL::webapp.pfmforlist,%s) where orormfpflistid = %s and entityid = %s",(str(pfmflsordatajsondict),e.get('orormfpflistid'),entityid,))
                                    
                                    #cur, dbqerr = db.mydbfunc(con,cur,command)
                                    #if cur.closed == True:
                                    #    if(dbqerr['natstatus'] == "error" or dbqerr['natstatus'] == "warning"):
                                    #        dbqerr['statusdetails']="mflist details insert Failed"
                                    #    resp = make_response(jsonify(dbqerr), 400)
                                    #    return(resp)
                                    #'''
                        else:
                            pass
                    else:
                        #fund is existing so we have to update
                        #d['ormflistid']='mf'+pfdata.get('pfportfolioid')+str(pfmflsseqnum)
                        #d['orportfolioid']=pfdata.get('pfportfolioid')
                        d['entityid']=entityid
                        
                        if 'ormffundorderlists' in d:
                            pfmflsordata = d.pop("ormffundorderlists")
                            print("ormffundorderlists old")
                            print(pfmflsordata)
                        else:
                            pfmflsordata=None
                            print("key ormffundorderlists not in the submitted record")

                        pfmflsdatajsondict = json.dumps(d)
                        #command = cur.mogrify("UPDATE webapp.pfmflist select * from json_populate_record(NULL::webapp.pfmflist,%s) WHERE ormflistid =%s AND entityid = %s;",(str(pfmflsdatajsondict),d.get('ormflistid'),entityid,))
                        
                        command = cur.mogrify("""
                                    UPDATE webapp.pfmflist set(ormffundname,ormffndnameedit,ormfdathold,ormflmtime) = 
                                    (select ormffundname,ormffndnameedit,ormfdathold,ormflmtime from json_to_record (%s)
                                    AS (ormffundname varchar(100),ormffndnameedit varchar(100),ormfdathold text,ormflmtime timestamp))
                                    WHERE ormflistid =%s AND entityid = %s;
                                """,(str(pfmflsdatajsondict),d.get('ormflistid'),entityid,))                       
                        print(command)                
                        cur, dbqerr = db.mydbfunc(con,cur,command)
                        if cur.closed == True:
                            if(dbqerr['natstatus'] == "error" or dbqerr['natstatus'] == "warning"):
                                dbqerr['statusdetails']="mflist insert Failed"
                            resp = make_response(jsonify(dbqerr), 400)
                            return(resp)

                        #pfmforlsseqnum=1
                        if pfmflsordata != None:
                            for e in pfmflsordata: 
                                print("PRINTING e")
                                print(e)                                           
                                e['ormfoctime']= pfsavetimestamp
                                e['ormflmtime']= pfsavetimestamp
                                e['entityid']=entityid
                                #For new SIP or onetime record for the fund
                                if(e['orormfpflistid'] ==""):                                    
                                    command = cur.mogrify("SELECT MAX(orormfseqnum) FROM webapp.pfmforlist where orormflistid = %s and entityid =%s;",(d.get('ormflistid'),entityid,))
                                    print(command)
                                    cur, dbqerr = db.mydbfunc(con,cur,command)
                                                        
                                    if cur.closed == True:
                                        if(dbqerr['natstatus'] == "error" or dbqerr['natstatus'] == "warning"):
                                            dbqerr['statusdetails']="Fund MAX sequence failed"
                                        resp = make_response(jsonify(dbqerr), 400)
                                        return(resp)

                                    #Model to follow in all fetch
                                    records=[]
                                    for record in cur:  
                                        records.append(record[0])
                                    print("iam printing records to see")
                                    print(type(records[0]))

                                    if(records[0] == None):
                                        pfmforlsseqnum=1
                                    else:
                                        if(type(records[0])=="Decimal"):
                                            pfmforlsseqnum = int(Decimal(records[0]))+1
                                            
                                        else:
                                            pfmforlsseqnum=records[0]+1
                                    print(pfmforlsseqnum)
                                    print(type(pfmforlsseqnum))
                                    e['orormfpflistid']= "or"+d.get('ormflistid')+str(pfmforlsseqnum)
                                    e['orormflistid']= d.get('ormflistid')
                                    e['orormfseqnum'] = str(pfmforlsseqnum)
                                    
                                    pfmforlsseqnum = pfmforlsseqnum+1
                                    print(e)
                                    pfmflsordatajsondict = json.dumps(e)

                                    command = cur.mogrify("INSERT INTO webapp.pfmforlist select * from json_populate_record(NULL::webapp.pfmforlist,%s) where entityid = %s;",(str(pfmflsordatajsondict),entityid,))
                        
                                    cur, dbqerr = db.mydbfunc(con,cur,command)
                                    if cur.closed == True:
                                        if(dbqerr['natstatus'] == "error" or dbqerr['natstatus'] == "warning"):
                                            dbqerr['statusdetails']="mflist details insert Failed"
                                        resp = make_response(jsonify(dbqerr), 400)
                                        return(resp)
                                else:
                                    #For existing SIP or onetime record for the fund
                                    pfmflsordatajsondict = json.dumps(e)
                                    command = cur.mogrify("UPDATE webapp.pfmforlist select * from json_populate_record(NULL::webapp.pfmforlist,%s) where orormfpflistid = %s and entityid = %s",(str(pfmflsordatajsondict),e.get('orormfpflistid'),entityid,))
                                    
                                    command = cur.mogrify("""
                                                UPDATE webapp.pfmforlist set(ormffundordelsfreq,ormffundordelsstdt,ormffundordelsamt,ormfsipinstal,ormfsipendt,ormfsipdthold,ormfselctedsip,ormffndstatus,ormflmtime) = 
                                                (select ormffundordelsfreq,ormffundordelsstdt,ormffundordelsamt,ormfsipinstal,ormfsipendt,ormfsipdthold,ormfselctedsip,ormffndstatus,ormflmtime from json_to_record (%s)
                                                AS (ormffundordelsfreq varchar(20),ormffundordelsstdt numeric(2),ormffundordelsamt numeric(16,5),ormfsipinstal numeric(3),ormfsipendt date,ormfsipdthold text,ormfselctedsip text,ormffndstatus varchar(10),ormflmtime timestamp))
                                                WHERE orormfpflistid = %s AND entityid = %s;
                                            """,(str(pfmflsordatajsondict),e.get('orormfpflistid'),entityid,))



                                    cur, dbqerr = db.mydbfunc(con,cur,command)
                                    if cur.closed == True:
                                        if(dbqerr['natstatus'] == "error" or dbqerr['natstatus'] == "warning"):
                                            dbqerr['statusdetails']="mflist details insert Failed"
                                        resp = make_response(jsonify(dbqerr), 400)
                                        return(resp)
                        else:
                            pass

            else:
                print("done nothing as pfmflsdata is none") 
            ###PF MF details update END###
            con.commit()
            cur.close()
            con.close()
            print('save successful')
            return jsonify({'natstatus':'success','statusdetails':'Portfolio ' + pfdata.get('pfportfolioname') +' Updated'})


@app.route('/onlypf',methods=['GET','POST','OPTIONS'])
def onlypff():
    records=[]
    
    if 'Authorization' in request.headers:
        natjwtfrhead=request.headers.get('Authorization')
        if natjwtfrhead.startswith("Bearer "):
            natjwtfrheadf =  natjwtfrhead[8:-1]
        natjwtdecoded = jwt.decode(natjwtfrheadf, verify=False)
        userid=natjwtdecoded['userid']
        if  (not userid) or (userid ==""):
            dbqerr['natstatus'] == "error"
            dbqerr['statusdetails']="No user id in request"
            resp = make_response(jsonify(dbqerr), 400)
            return(resp)
        conn_string = "host='localhost' dbname='postgres' user='postgres' password='password123'"
        con=psycopg2.connect(conn_string)
        cur = con.cursor()
        #cur.execute("select row_to_json(art) from (select a.*, (select json_agg(b) from (select * from pfstklist where pfportfolioid = a.pfportfolioid ) as b) as pfstklist, (select json_agg(c) from (select * from pfmflist where pfportfolioid = a.pfportfolioid ) as c) as pfmflist from pfmaindetail as a where pfuserid =%s ) art",(userid,))
        command = cur.mogrify("select pfportfolioname from webapp.pfmaindetail;")
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

    
        #Model to follow in all fetch
        for record in cur:  
            records.append(record[0])        
        print("portfolio name only returned for user: "+userid)
    
    return json.dumps(records)


def dateformat(datestr):
#code to convert the date from UTC to IST
    if (datestr is not None):    
        if(datestr[-1:] == 'Z'):
            print(datetime.strptime(datestr, "%Y-%m-%dT%H:%M:%S.%fZ"))
            utc = datetime.strptime(datestr, "%Y-%m-%dT%H:%M:%S.%fZ")
            from_zone = tz.tzutc()
            to_zone = tz.tzlocal()
            utc = utc.replace(tzinfo=from_zone)
            # Convert time zone
            central = utc.astimezone(to_zone)
            centralstr=central.strftime('%Y-%m-%d')
            print(centralstr)
            return centralstr
        else:
            return datestr

        
