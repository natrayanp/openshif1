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



@app.route('/pfdatafetch',methods=['GET','POST','OPTIONS'])
def pfdatafetch():
#This is called by fund data fetch service
    if request.method=='OPTIONS':
        print("inside PFDATAFETCH options")
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
        command = cur.mogrify("select row_to_json(art) from (select a.*,(select json_agg(b) from (select * from webapp.pfstklist where pfportfolioid = a.pfportfolioid ) as b) as pfstklist, (select json_agg(c) from (select c.*,(select json_agg(d) from (select * from webapp.pfmforlist where orormflistid = c.ormflistid ) as d) as ormffundorderlists from webapp.pfmflist c where orportfolioid = a.pfportfolioid ) as c) as pfmflist from webapp.pfmaindetail as a where pfuserid =%s  ORDER BY pfportfolioid) art",(userid,))
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
        print("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
        print(pfdata)
        print("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
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
            if pfdata.get('pfportfolioid') == "NEW":
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

        if'pfscreen' in pfdata:
            screenid= pfdata.get('pfscreen')
            if screenid == "pfs":
                filterstr="NEW"
            elif screenid == "ord":
                filterstr="INCART"
        else:
           screenid=None
           print("key pfmflist not in the submitted record")       


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
            
            #Model to follow in all fetch
            records=[]
            for record in cur:  
                records.append(record[0])
            print("iam printing records to see")
            print(records)

            if record[0]== None:
                pfmainnextmaxval = 1
            else:
                if(type(record[0])=="Decimal"):
                    pfmainnextmaxval = int(Decimal(records[0]))+1                                
                else:
                    pfmainnextmaxval=records[0]+1

            pfdata['pfpfidusrrunno']=str(pfmainnextmaxval)
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
                    d['orpfuserid']=pfdata.get('pfuserid')

                    
                    
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
                            e['ororportfolioid']=d.get('orportfolioid')
                            e['orpfportfolioname']=pfdata.get('pfportfolioname')
                            e['ororpfuserid']=d.get('orpfuserid')
                            e['orormffundname']=d.get('ormffundname')
                            e['orormffndcode']=d.get('ormffndcode')
                            e['ororfndamcnatcode']=d.get('ormfnatamccode')
                            e['entityid']=entityid
                            e['orormfseqnum']=pfmforlsseqnum                                
                            pfmforlsseqnum = pfmforlsseqnum+1
                            pfmflsordatajsondict = json.dumps(e)
                            print(e['ormffundordelsstdt'])

                            if(e.get('ormffundordelsstdt')==0):
                                if (e['ormffundordelstrtyp']=='One Time'):
                                    print("inside onetime no action")
                                else:
                                    dbqerr={}
                                    if(dbqerr['natstatus'] == "error" or dbqerr['natstatus'] == "warning"):
                                        dbqerr['statusdetails']="SIP START DATE is Mandatory"
                                    resp = make_response(jsonify(dbqerr), 400)
                                    return(resp)
                                '''
                                e['ormffundordelsstdt']=None
                                print("inside if")
                                else:
                                print("inside if")
                                pass
                                '''


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
            '''
            con.commit()    
            cur.close()
            con.close()
            return jsonify({'natstatus':'success','statusdetails':'New portfolio ' + pfdata.get('pfportfolioname') +' created'})
            '''
            
            '''
            [{'pfmfnav': '75', 'pfmfpercent': '70', 'pfmfamt': 0, 'pfmffundname': 'MNC', 'pfmfallotedamt': 4121.599999999999}, {'pfmfnav': '66', 'pfmfpercent': '10', 'pfmfamt': 0, 'pfmffundname': 'OII', 'pfmfallotedamt': 588.8000000000001}]

            [{'pfstexchange': 'NSE', 'pfstallotedamt': 588.8000000000001, 'pfstpercent': '10', 'pfstTotUnit': '0', 'pfsttradingsymbl': 'ITC', 'pfstamt': 0, 'pfsttotunit': 0, 'pfstltp': '788'}, {'pfstexchange': 'NSE', 'pfstallotedamt': 588.8000000000001, 'pfstpercent': '10', 'pfstTotUnit': '6', 'pfsttradingsymbl': 'SBIN', 'pfstamt': 0, 'pfsttotunit': 0, 'pfstltp': '89'}]

            {'pfstkamtsplittype': '%', 'pftargetintrate': None, 'pfportfolioname': 'ASDFASDF', 'pfmfamtsplittype': '%', 'pfsummed': 5888, 'pfpurpose': 'ASDFASDF', 'pftargetdt': None, 'pfplannedinvamt': 5888, 'pfstartdt': None, 'pfportfolioid': 'New', 'pfbeneUsers': None, 'pfuserid': None, 'pfinvamtfeq': None}
            '''
        elif savetype == "Old" :
            print('inside old')
            pfdata['pflmtime']= pfsavetimestamp
            pfdata.get('pfuserid')            

            #If request is from pfscreen then we update pf details, if it is from order screen skip this.
            if screenid == "pfs":
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
            else:
                pass
            ###PF stock details update START###
            ###PF stock details update END ####

            ###PF MF details update START###

            pfmflsdatalist=[]
            pfmforlsdatalist=[]
            if pfmflsdata!=None:
                for d in pfmflsdata:  
                    print("pfmflsdata inside for")
                    print(d)
                    d['ormfoctime']= pfsavetimestamp
                    d['ormflmtime']= pfsavetimestamp      

                    command = cur.mogrify("SELECT ormflistid FROM webapp.pfmflist WHERE ormffndcode = %s AND orpfuserid = %s AND orportfolioid = %s AND entityid =%s;",(d.get('ormffndcode'),userid,pfdata.get('pfportfolioid'),entityid,))
                    print(command)
                    cur, dbqerr = db.mydbfunc(con,cur,command)
                                        
                    if cur.closed == True:
                        if(dbqerr['natstatus'] == "error" or dbqerr['natstatus'] == "warning"):
                            dbqerr['statusdetails']="Fund MAX sequence failed"
                        resp = make_response(jsonify(dbqerr), 400)
                        return(resp)

                    #Model to follow in all fetch
                    if cur.rowcount == 1:
                        for record in cur:  
                            d['ormflistid'] = record[0]                                

                    elif cur.rowcount == 0:
                        print("Fund doesn't exist in this users portfolio")
                        d['ormflistid'] = ""
                    else:
                        dbqerr = {'natstatus': 'error', 'statusdetails': 'Same fund exists multiple times in the portfolio'}
                        return(make_response(jsonify(dbqerr), 400))
                    
                    print("is the fund already exists:")
                    print(d['ormflistid'])              

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
                            if(type(records[0])=="Decimal"):
                                pfmflsseqnum = int(Decimal(records[0]))+1                                
                            else:
                                pfmflsseqnum=records[0]+1

                        d['ormflistid']='mf'+pfdata.get('pfportfolioid')+str(pfmflsseqnum)
                        d['orportfolioid']=pfdata.get('pfportfolioid')
                        d['entityid']=entityid
                        d['ormfseqnum'] = str(pfmflsseqnum)
                        d['orpfuserid']=pfdata.get('pfuserid')
                        pfmflsdatalist.append(d['ormflistid'])
                        

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
                                #e['orormfpflistid']= "or"+d.get('ormflistid')+str(pfmforlsseqnum)
                                e['ororportfolioid']=d.get('orportfolioid')
                                e['orpfportfolioname']=pfdata.get('pfportfolioname')
                                e['ororpfuserid']=d.get('orpfuserid')
                                e['orormffundname']=d.get('ormffundname')
                                e['orormffndcode']=d.get('ormffndcode')
                                e['orormflistid']= d.get('ormflistid') 
                                
                                if(e.get('ormffundordelsstdt')==0):
                                    if (e['ormffundordelstrtyp']=='One Time'):
                                        print("inside if")
                                    else:
                                        dbqerr={}
                                        if(dbqerr['natstatus'] == "error" or dbqerr['natstatus'] == "warning"):
                                            dbqerr['statusdetails']="SIP START DATE is Mandatory"
                                        resp = make_response(jsonify(dbqerr), 400)
                                        return(resp)
                                    
                                
                                #For new SIP or onetime record for the fund
                                if(e['orormfpflistid'] ==""):
                                    e['orormfpflistid']= "or"+d.get('ormflistid')+str(pfmforlsseqnum)                                                                 
                                    e['orormfseqnum'] = pfmforlsseqnum
                                    pfmforlsdatalist.append(e['orormfpflistid'])
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

                        d['entityid']=entityid
                        d['orpfuserid']=pfdata.get('pfuserid')
                        pfmflsdatalist.append(d['ormflistid'])

                        if 'ormffundorderlists' in d:
                            pfmflsordata = d.pop("ormffundorderlists")
                            print("ormffundorderlists old")
                            print(pfmflsordata)
                        else:
                            pfmflsordata=None
                            print("key ormffundorderlists not in the submitted record")

                        pfmflsdatajsondict = json.dumps(d)
                        #command = cur.mogrify("UPDATE webapp.pfmflist select * from json_populate_record(NULL::webapp.pfmflist,%s) WHERE ormflistid =%s AND entityid = %s;",(str(pfmflsdatajsondict),d.get('ormflistid'),entityid,))
                        
                        #donot update if the fund is fixed : START
                        if(d['ormffndnameedit'] == 'fixed'):
                            command = cur.mogrify("""
                                        UPDATE webapp.pfmflist set(ormffundname,ormffndcode,ormffndnameedit,ormfdathold,ormflmtime) = 
                                        (select ormffundname,ormffndcode,ormffndnameedit,ormfdathold,ormflmtime from json_to_record (%s)
                                        AS (ormffundname varchar(100),ormffndcode varchar(100),ormffndnameedit varchar(100),ormfdathold text,ormflmtime timestamp))
                                        WHERE ormflistid =%s AND entityid = %s;
                                    """,(str(pfmflsdatajsondict),d.get('ormflistid'),entityid,))                       
                            print(command)                
                            cur, dbqerr = db.mydbfunc(con,cur,command)
                            if cur.closed == True:
                                if(dbqerr['natstatus'] == "error" or dbqerr['natstatus'] == "warning"):
                                    dbqerr['statusdetails']="mflist insert Failed"
                                resp = make_response(jsonify(dbqerr), 400)
                                return(resp)
                        #donot update if the fund is fixed : END

                        #pfmforlsseqnum=1
                        if pfmflsordata != None:
                            for e in pfmflsordata: 
                                print("PRINTING e")
                                print(e)                                           
                                e['ormfoctime']= pfsavetimestamp
                                e['ormflmtime']= pfsavetimestamp
                                #e['orormfpflistid']= "or"+d.get('ormflistid')+str(pfmforlsseqnum)
                                e['orormffundname']=d.get('ormffundname')
                                e['orormffndcode']=d.get('ormffndcode')
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
                                    e['ororportfolioid']=d.get('orportfolioid')
                                    e['orpfportfolioname']=pfdata.get('pfportfolioname')
                                    e['ororpfuserid']=d.get('orpfuserid')
                                    pfmforlsdatalist.append(e['orormfpflistid'])
                                    
                                    pfmforlsseqnum = pfmforlsseqnum+1
                                    print(e)
                                    pfmflsordatajsondict = json.dumps(e)

                                    command = cur.mogrify("INSERT INTO webapp.pfmforlist select * from json_populate_record(NULL::webapp.pfmforlist,%s) where entityid = %s;",(str(pfmflsordatajsondict),entityid,))
                                    print(command)
                                    cur, dbqerr = db.mydbfunc(con,cur,command)
                                    if cur.closed == True:
                                        if(dbqerr['natstatus'] == "error" or dbqerr['natstatus'] == "warning"):
                                            dbqerr['statusdetails']="mflist details insert Failed"
                                        resp = make_response(jsonify(dbqerr), 400)
                                        return(resp)
                                else:
                                    #For existing SIP or onetime record for the fund

                                    pfmforlsdatalist.append(e['orormfpflistid'])
                                    pfmflsordatajsondict = json.dumps(e)                                    
                                    
                                    #Record which are only editable to be updated.
                                    if(e['ormffndstatus']=='New'):   
                                        command = cur.mogrify("""
                                                    UPDATE webapp.pfmforlist set(orormffundname,orormffndcode,ormffundordelsfreq,ormffundordelsstdt,ormffundordelsamt,ormfsipinstal,ormfsipendt,ormfsipdthold,ormfselctedsip,ormffndstatus,ormflmtime) = 
                                                    (select orormffundname,orormffndcode,ormffundordelsfreq,ormffundordelsstdt,ormffundordelsamt,ormfsipinstal,ormfsipendt,ormfsipdthold,ormfselctedsip,ormffndstatus,ormflmtime from json_to_record (%s)
                                                    AS (orormffundname varchar(100),orormffndcode varchar(100),ormffundordelsfreq varchar(20),ormffundordelsstdt varchar(11),ormffundordelsamt numeric(16,5),ormfsipinstal numeric(3),ormfsipendt date,ormfsipdthold text,ormfselctedsip text,ormffndstatus varchar(10),ormflmtime timestamp))
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
                            pass
                
            else:
                print("done nothing as pfmflsdata is none") 

            #do the clean up of fund sip or oneitme records removed: START
            str2=tuple(pfmflsdatalist)
            print(pfmflsdatalist)
            print(str2)   

            #str3 = "','".join(pfmforlsdatalist)
            #str4 = "'" + str3 + "'"
            str4=tuple(pfmforlsdatalist)
            print(pfmforlsdatalist)
            print(str4)

            if pfmforlsdatalist:
                #Delete the records (SIP or One time records) that are deleted from a fund
                print("inside if")
                command = cur.mogrify("DELETE FROM webapp.pfmforlist where orormfpflistid NOT IN %s AND entityid =%s AND ororpfuserid = %s AND ororportfolioid = %s AND ormffndstatus = %s;",(str4,entityid,userid,pfdata.get('pfportfolioid'),filterstr,))
                print(command)
            else:
                #Delete all the records as all records (SIP or One time records) are deleted for a fund.  
                # But exclude ( this condition ormffndstatus = 'New') already executed order records.
                print("inside else")
                command = cur.mogrify("DELETE FROM webapp.pfmforlist where entityid =%s AND ororpfuserid = %s AND ororportfolioid = %s  AND ormffndstatus = %s;",(entityid,userid,pfdata.get('pfportfolioid'),filterstr,))
                print(command)            
            cur, dbqerr = db.mydbfunc(con,cur,command)
                                
            if cur.closed == True:
                if(dbqerr['natstatus'] == "error" or dbqerr['natstatus'] == "warning"):
                    dbqerr['statusdetails']="Fund MAX sequence failed"
                resp = make_response(jsonify(dbqerr), 400)
                return(resp)
            '''
            records=[]
            for record in cur:  
                print(record)  
                records.append(record[0])
            print("iam printing records to see")
            print((records[0]))
            '''
            #do the clean up of fund sip or oneitme records removed: END
        
            #do the clean up of funds removed: START            

            if pfmflsdatalist:
                command = cur.mogrify("DELETE FROM webapp.pfmflist where ormflistid NOT IN %s AND entityid =%s AND orpfuserid=%s AND orportfolioid= %s;",(str2,entityid,userid,pfdata.get('pfportfolioid'),))
                print(command)
            else:
                command = cur.mogrify("DELETE FROM webapp.pfmflist where entityid =%s AND orpfuserid=%s AND orportfolioid= %s;",(entityid,userid,pfdata.get('pfportfolioid'),))
                print(command)

            cur, dbqerr = db.mydbfunc(con,cur,command)
                                
            if cur.closed == True:
                if(dbqerr['natstatus'] == "error" or dbqerr['natstatus'] == "warning"):
                    dbqerr['statusdetails']="Fund MAX sequence failed"
                resp = make_response(jsonify(dbqerr), 400)
                return(resp)
            
            #do the clean up of funds removed: END 
            ###PF MF details update END###

        print("All done and starting cleanups")


        # POST UPDATES FOR PF SCREEN : START
        if screenid == "pfs":
            # Execute button to show or not : START
            command = cur.mogrify("UPDATE webapp.pfmaindetail SET pfshowadcrtbtn = 'Y' WHERE pfportfolioid in (SELECT ororportfolioid FROM webapp.pfmforlist WHERE UPPER(ormffndstatus) = 'NEW' and ororpfuserid = %s AND entityid = %s);",(userid,entityid,))
            print(command)

            cur, dbqerr = db.mydbfunc(con,cur,command)
                                
            if cur.closed == True:
                if(dbqerr['natstatus'] == "error" or dbqerr['natstatus'] == "warning"):
                    dbqerr['statusdetails']="Fund MAX sequence failed"
                resp = make_response(jsonify(dbqerr), 400)
                return(resp)

            command = cur.mogrify("UPDATE webapp.pfmaindetail SET pfshowadcrtbtn = '' WHERE 0 = (SELECT count(*) FROM webapp.pfmforlist WHERE UPPER(ormffndstatus) = 'NEW' and ororpfuserid = %s AND entityid = %s);",(userid,entityid,))
            print(command)

            cur, dbqerr = db.mydbfunc(con,cur,command)
                                
            if cur.closed == True:
                if(dbqerr['natstatus'] == "error" or dbqerr['natstatus'] == "warning"):
                    dbqerr['statusdetails']="Fund MAX sequence failed"
                resp = make_response(jsonify(dbqerr), 400)
                return(resp)

            # Execute button to show or not : END
        else:
            pass
        # POST UPDATES FOR PF SCREEN : END

        # POST UPDATES FOR ORDER SCREEN : START
        if screenid == "ord":
            pass
        else:
            pass
        # POST UPDATES FOR ORDER SCREEN : END


        #POST UPDATES COMMON:START 
        # (source: mforder.py -> mfordersave; referredin: portfolio.py ->executepf,pfdatasave )
        # Fund edit/delete allowed : START
        #If atleast one of the order is not new, we should not allow the fund to be removed and edited
        #in this case we mark ormffndnameedit as fixed    
        
        command = cur.mogrify("UPDATE webapp.pfmflist SET ormffndnameedit = 'fixed' WHERE ormflistid in (SELECT distinct orormflistid FROM webapp.pfmforlist WHERE UPPER(ormffndstatus) IN ('INCART') and ororpfuserid = %s AND entityid = %s);",(userid,entityid,))
        print(command)

        cur, dbqerr = db.mydbfunc(con,cur,command)
                            
        if cur.closed == True:
            if(dbqerr['natstatus'] == "error" or dbqerr['natstatus'] == "warning"):
                dbqerr['statusdetails']="Fund MAX sequence failed"
            resp = make_response(jsonify(dbqerr), 400)
            return(resp)

        #command = cur.mogrify("UPDATE webapp.pfmflist SET ormffndnameedit = 'noedit' WHERE ormflistid in (SELECT distinct orormflistid FROM webapp.pfmforlist WHERE UPPER(ormffndstatus) = 'NEW' and ororpfuserid = %s AND entityid = %s) AND 0 = (SELECT COUNT( distinct orormflistid) FROM webapp.pfmforlist WHERE UPPER(ormffndstatus) != 'NEW' and ororpfuserid = %s AND entityid = %s);",(userid,entityid,userid,entityid,))
        command = cur.mogrify("UPDATE webapp.pfmflist SET ormffndnameedit = 'noedit' WHERE ormflistid NOT IN (SELECT distinct orormflistid FROM webapp.pfmforlist WHERE UPPER(ormffndstatus) IN ('INCART') and ororpfuserid = %s AND entityid = %s);",(userid,entityid,))
        print(command)

        cur, dbqerr = db.mydbfunc(con,cur,command)
                            
        if cur.closed == True:
            if(dbqerr['natstatus'] == "error" or dbqerr['natstatus'] == "warning"):
                dbqerr['statusdetails']="Fund MAX sequence failed"
            resp = make_response(jsonify(dbqerr), 400)
            return(resp)
        # Fund edit/delete allowed : END

        #POST UPDATES COMMON:END

        con.commit()
        cur.close()
        con.close()
        #clearuppostsave(pfmflsdatalist,pfmforlsdatalist,entityid,userid,pfdata)
        print('save successful')
        return jsonify({'natstatus':'success','statusdetails':'Portfolio ' + pfdata.get('pfportfolioname') +' Saved/Updated'})




@app.route('/onlypf',methods=['GET','POST','OPTIONS'])
def onlypff():
    if request.method=='OPTIONS':
        print("inside onlypf options")
        return make_response(jsonify('inside onlypf options'), 200)  

    elif request.method=='POST':
        print("inside ONLYPF post")
        records=[]    
        print((request))        
        print(request.headers)
        userid,entityid=jwtnoverify.validatetoken(request)
        print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        print(userid,entityid)
        payload= request.get_json()
        print(payload)

        x=[]
        for d in payload:
            x.append(d['pfportfolioid'])
        
        if len(x) > 0:
            pftoexclude=tuple(x)
        else:
            pftoexclude='%'

        #pftoexclude='%'
        print(pftoexclude)
        
        con,cur=db.mydbopncon()
        '''
        conn_string = "host='localhost' dbname='postgres' user='postgres' password='password123'"
        con=psycopg2.connect(conn_string)
        cur = con.cursor()
        '''
        #cur.execute("select row_to_json(art) from (select a.*, (select json_agg(b) from (select * from pfstklist where pfportfolioid = a.pfportfolioid ) as b) as pfstklist, (select json_agg(c) from (select * from pfmflist where pfportfolioid = a.pfportfolioid ) as c) as pfmflist from pfmaindetail as a where pfuserid =%s ) art",(userid,))
        if pftoexclude == '%':
            command = cur.mogrify("select json_agg(art) from (select a.*, (select json_agg(b) from (select * from webapp.pfstklist where 1=2) as b) as pfstklist, (select json_agg(c) from (select * from webapp.pfmflist where 1=2 ) as c) as pfmflist from webapp.pfmaindetail as a where pfuserid =%s and pfportfolioid like %s and entityid = %s ) art",(userid,pftoexclude,entityid,))
        else:
            command = cur.mogrify("select json_agg(art) from (select a.*, (select json_agg(b) from (select * from webapp.pfstklist where 1=2) as b) as pfstklist, (select json_agg(c) from (select * from webapp.pfmflist where 1=2 ) as c) as pfmflist from webapp.pfmaindetail as a where pfuserid =%s and pfportfolioid NOT IN %s and entityid = %s ) art",(userid,pftoexclude,entityid,))
        
        
        
        #command = cur.mogrify("select pfportfolioname from webapp.pfmaindetail;")
        print(command)
        
        cur, dbqerr = db.mydbfunc(con,cur,command)
        print(cur)
        print(dbqerr)
        print(type(dbqerr))
        print(dbqerr['natstatus'])

        if cur.closed == True:
            if(dbqerr['natstatus'] == "error" or dbqerr['natstatus'] == "warning"):
                dbqerr['statusdetails']="pf main Fetch failed"
            resp = make_response(jsonify(dbqerr), 400)
            return(resp)


        #Model to follow in all fetch
        for record in cur:  
            records.append(record[0])        
        print("portfolio details only returned for user: "+userid)
        print(records)
    if records[0]==None:
        print("inside if")
        records = [[]]
    else:
        pass

    print(records)
    return json.dumps(records)


'''
@app.route('/pforderdatafetch',methods=['GET','OPTIONS'])
def pforderdatafetch():
#This is called by fund data fetch service
    if request.method=='OPTIONS':
        print("inside pforderdatafetch options")
        return make_response(jsonify('inside FUNDDATAFETCH options'), 200)  

    elif request.method=='GET':
        print("inside pforderdatafetch GET")
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
        #command = cur.mogrify("select row_to_json(art) from (select a.*,(select json_agg(b) from (select * from webapp.pfstklist where pfportfolioid = a.pfportfolioid ) as b) as pfstklist, (select json_agg(c) from (select c.*,(select json_agg(d) from (select * from webapp.pfmforlist where orormflistid = c.ormflistid AND ormffndstatus='INCART' AND entityid=%s) as d) as ormffundorderlists from webapp.pfmflist c where orportfolioid = a.pfportfolioid ) as c) as pfmflist from webapp.pfmaindetail as a where pfuserid =%s AND entityid=%s) art",(entityid,userid,entityid,))
        command = cur.mogrify(
            """
        	WITH portport as (select ororportfolioid,orormflistid,orormfpflistid from webapp.pfmforlist where ormffndstatus = 'INCART' AND ororpfuserid = %s AND entityid = %s) 
            select row_to_json(art) from (
            select a.*,
            (select json_agg(b) from (select * from webapp.pfstklist where pfportfolioid = a.pfportfolioid ) as b) as pfstklist, 
            (select json_agg(c) from (select c.*,(select json_agg(d) from (select * from webapp.pfmforlist where orormflistid in (SELECT distinct orormflistid FROM PORTPORT) AND ororportfolioid =a.pfportfolioid AND orormflistid=c.ormflistid AND ormffndstatus = 'INCART' AND entityid = %s) as d) as ormffundorderlists 
            from webapp.pfmflist c where ormflistid in (SELECT distinct orormflistid FROM portport) AND  entityid = %s AND orportfolioid =a .pfportfolioid ) as c) as pfmflist 
	        from webapp.pfmaindetail as a where pfuserid = %s AND entityid = %s AND pfportfolioid IN (SELECT distinct ororportfolioid FROM portport) ORDER BY pfportfolioid  ) art
            """,(userid,entityid,entityid,entityid,userid,entityid,))

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
'''

@app.route('/executepf',methods=['POST','OPTIONS'])
def executepf():
    print("try")
    if request.method=='OPTIONS':
        print ("inside executepf options")
        return jsonify({'body':'success'})

    elif request.method=='POST':   
        print ("inside executepf post")
        
        print((request))        
        print(request.headers)
        userid,entityid=jwtnoverify.validatetoken(request)
        print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        print(userid,entityid)
        payload= request.get_json()
        print(payload)
        
        
        con,cur=db.mydbopncon()

        #update the pf values to INPROGRESS.  Only for NEW items
        command = cur.mogrify("""update webapp.pfmforlist set ormffndstatus = 'INCART'
                                WHERE orormflistid IN (select ormflistid FROM webapp.pfmflist 
                                WHERE orportfolioid = (SELECT DISTINCT pfPortfolioid FROM webapp.pfmaindetail where pfPortfolioid = %s AND pfuserid = %s AND entityid = %s ) AND entityid = %s)
                                AND UPPER(ormffndstatus) = 'NEW' AND entityid = %s;
                                """,(payload,userid,entityid,entityid,entityid,))
        print(command)
        cur, dbqerr = db.mydbfunc(con,cur,command)

        if cur.closed == True:
            if(dbqerr['natstatus'] == "error" or dbqerr['natstatus'] == "warning"):
                dbqerr['statusdetails']="main update  Failed"
            resp = make_response(jsonify(dbqerr), 400)
            return(resp)
        
        con.commit()
        
        #UPDATE THE SHOW CART BUTTON TO BLANK so the button doesn't come up: START
        con,cur=db.mydbopncon()
        command = cur.mogrify("UPDATE webapp.pfmaindetail SET pfshowadcrtbtn = '' WHERE pfportfolioid = %s and pfuserid = %s AND entityid = %s;",(payload,userid,entityid,))
        print(command)

        cur, dbqerr = db.mydbfunc(con,cur,command)
        
        if cur.closed == True:
            if(dbqerr['natstatus'] == "error" or dbqerr['natstatus'] == "warning"):
                dbqerr['statusdetails']="Fund MAX sequence failed"
            resp = make_response(jsonify(dbqerr), 400)
            return(resp)

        con.commit()        
        #UPDATE THE SHOW CART BUTTON TO BLANK so the button doesn't come up: END

        # Fund edit/delete allowed : START
        #If atleast one of the order is not new, we should not allow the fund to be removed and edited
        #in this case we mark ormffndnameedit as fixed    
    
        #POST UPDATES COMMON:START 
        # (source: mforder.py -> mfordersave; referredin: portfolio.py ->executepf,pfdatasave )
        # Fund edit/delete allowed : START
        #If atleast one of the order is not new, we should not allow the fund to be removed and edited
        #in this case we mark ormffndnameedit as fixed    
        
        command = cur.mogrify("UPDATE webapp.pfmflist SET ormffndnameedit = 'fixed' WHERE ormflistid in (SELECT distinct orormflistid FROM webapp.pfmforlist WHERE UPPER(ormffndstatus) IN ('INCART') and ororpfuserid = %s AND entityid = %s);",(userid,entityid,))
        print(command)

        cur, dbqerr = db.mydbfunc(con,cur,command)
                            
        if cur.closed == True:
            if(dbqerr['natstatus'] == "error" or dbqerr['natstatus'] == "warning"):
                dbqerr['statusdetails']="Fund MAX sequence failed"
            resp = make_response(jsonify(dbqerr), 400)
            return(resp)

        #command = cur.mogrify("UPDATE webapp.pfmflist SET ormffndnameedit = 'noedit' WHERE ormflistid in (SELECT distinct orormflistid FROM webapp.pfmforlist WHERE UPPER(ormffndstatus) = 'NEW' and ororpfuserid = %s AND entityid = %s) AND 0 = (SELECT COUNT( distinct orormflistid) FROM webapp.pfmforlist WHERE UPPER(ormffndstatus) != 'NEW' and ororpfuserid = %s AND entityid = %s);",(userid,entityid,userid,entityid,))
        command = cur.mogrify("UPDATE webapp.pfmflist SET ormffndnameedit = 'noedit' WHERE ormflistid NOT IN (SELECT distinct orormflistid FROM webapp.pfmforlist WHERE UPPER(ormffndstatus) IN ('INCART') and ororpfuserid = %s AND entityid = %s);",(userid,entityid,))
        print(command)

        cur, dbqerr = db.mydbfunc(con,cur,command)
                            
        if cur.closed == True:
            if(dbqerr['natstatus'] == "error" or dbqerr['natstatus'] == "warning"):
                dbqerr['statusdetails']="Fund MAX sequence failed"
            resp = make_response(jsonify(dbqerr), 400)
            return(resp)
        # Fund edit/delete allowed : END

        #POST UPDATES COMMON:END

        # Fund edit/delete allowed : END

        con.commit()        

        cur, dbqerr = db.mydbfunc(con,cur,command)    

        command = cur.mogrify("""select row_to_json(art) from (SELECT orormfpflistid,orormflistid,ororportfolioid,ororpfuserid,entityid FROM webapp.pfmforlist WHERE orormflistid IN (select ormflistid FROM webapp.pfmflist WHERE orportfolioid = (SELECT DISTINCT pfPortfolioid FROM webapp.pfmaindetail where pfPortfolioid = 'BfulXOzj3ibSPSBDVgzMEAF1gax11' AND pfuserid = 'BfulXOzj3ibSPSBDVgzMEAF1gax1' AND entityid = 'IN' ) AND entityid = 'IN') AND UPPER(ormffndstatus) = 'INPROGRESS' AND entityid = 'IN') as art;
                        """,(payload,userid,entityid,entityid,entityid,))
        print(command)
        cur, dbqerr = db.mydbfunc(con,cur,command)
        records=[]
        for record in cur:  
            #record[0]['pfuserid']=userid
            #record[0]['pfPortfolioid']=payload
            records.append(record[0])

        eligblerecords=json.dumps(records)
        print(records)
        print("eligblerecords")
        print(eligblerecords)

        
        # call lamdba function without expecting response to start background processing: START
        '''
        client = boto3.client(‘lambda’)
        d = {'calID': '92dqiss5bg87etcqeeamlmob2g@group.calendar.google.com', 'datada': '2017-12-22T16:40:00+01:00', 'dataa': '2017-12-22T17:55:00+01:00', 'email': 'example@hotmail.com'}
        responselam = client.invoke(
            FunctionName='arn:aws:lambda:eu-west-1:13737373737:function:test',
            LogType='None',
            #Payload=json.dumps(d)
            Payload=eligblerecords
        )
        if(responselam['StatusCode']==202):
            print(success)
            send success response with message processing in progress
        else:
            print(failure)
            update the pf values back to NEW
            send error response      
        '''
        # call lamdba function without expecting response to start background processing: END        

        #THE CODE BELOW IS TO BE PUT IN SEPARATE LAMBDA TO RUN IN BACKGROUND: START
        #con,cur=db.mydbopncon()
        #payload= request.get_json()
        #print(payload)

        payload=json.loads(eligblerecords)

        for rectopros in payload:
            print(rectopros['orormfpflistid'])

            command = cur.mogrify("""select row_to_json(art) from (SELECT orormfpflistid,entityid FROM webapp.pfmforlist WHERE orormflistid IN (select ormflistid FROM webapp.pfmflist WHERE orportfolioid = (SELECT DISTINCT pfPortfolioid FROM webapp.pfmaindetail where pfPortfolioid = 'BfulXOzj3ibSPSBDVgzMEAF1gax11' AND pfuserid = 'BfulXOzj3ibSPSBDVgzMEAF1gax1' AND entityid = 'IN' ) AND entityid = 'IN') AND UPPER(ormffndstatus) = 'INPROGRESS' AND entityid = 'IN') as art;
                            """,(payload,userid,entityid,entityid,entityid,))
            print(command)
            cur, dbqerr = db.mydbfunc(con,cur,command)
            records=[]
            for record in cur:  
                #record[0]['pfuserid']=userid
                #record[0]['pfPortfolioid']=payload
                records.append(record[0])

            eligblerecords=json.dumps(records)
            print(records)
            print(eligblerecords)        
        

        #THE CODE BELOW IS TO BE PUT IN SEPARATE LAMBDA TO RUN IN BACKGROUND: END
        




        #cur.commit()
        cur.close()
        con.close()
        return jsonify({'body':'success fss'})




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
        elif(isinstance(datestr, str)):
            print(datestr)
            datefrm = datetime.strptime(datestr, '%d-%m-%Y')
            print(datefrm)
        else:
            return datestr






        
#THIS CODE IS IN THE API : START
#THIS CODE IS IN THE API : START
def transactions(tranrecord):

    #tranrecord= payload
    client = zeep.Client(wsdl=WSDL_ORDER_URL[settings.LIVE])
    set_soap_logging()

    ## get the password for posting order
    pass_dict = soap_get_password_order(client)

    ## prepare order, post it to BSE and save response
    ## for lumpsum transaction 
    if (transaction.order_type == '1'):
        ## prepare the transaction record
        bse_order = prepare_order(transaction, pass_dict)
        ## post the transaction
        order_id = soap_post_order(client, bse_order)

    ## for SIP transaction 
    elif (transaction.order_type == '2'):
        ## for basanti-2: since xsip orders cannot be placed in off-market hours, 
        ## placing a lumpsum order instead 
        bse_order = prepare_xsip_order(transaction, pass_dict)
        ## post the transaction
        order_id = soap_post_xsip_order(client, bse_order)

    else:
        raise Exception(
            "Internal error 630: Invalid order_type in transaction table"
        )

    ## update internal's transaction table to have a foreign key to TransactionBSE or TransactionXsipBSE table
    transaction.bse_trans_no = bse_order.trans_no
    transaction.save()

    ## TODO: MANUALLY update folio number & status assigned to a transaction after the mf is allotted to user
    ## have added it here for purpose of testing only
    transaction.status = '2'
    transaction.save()
    if (transaction.transaction_type == 'R'):
        ## TODO: make changes to purchase transactions corresponding to the redeem transaction
        pass

    return order_id



def soap_get_password_order(client):
    method_url = METHOD_ORDER_URL[settings.LIVE] + 'getPassword'
    svc_url = SVC_ORDER_URL[settings.LIVE]
    header_value = soap_set_wsa_headers(method_url, svc_url)
    response = client.service.getPassword(
        UserId=settings.USERID[settings.LIVE], 
        Password=settings.PASSWORD[settings.LIVE], 
        PassKey=settings.PASSKEY[settings.LIVE], 
        _soapheaders=[header_value]
    )
    print
    response = response.split('|')
    status = response[0]
    if (status == '100'):
        # login successful
        pass_dict = {'password': response[1], 'passkey': settings.PASSKEY[settings.LIVE]}
        return pass_dict
    else:
        raise Exception(
            "BSE error 640: Login unsuccessful for Order API endpoint"
        )


# prepare the TransactionBSE record
def prepare_order(transaction, pass_dict):

    trans_no = prepare_trans_no(transaction.user_id, transaction.order_type)

    # Fill all fields for a FRESH PURCHASE
    # Change fields if its a redeem or addl purchase
    data_dict = {
        'trans_code': 'NEW',
        'trans_no': trans_no,
        'user_id': settings.USERID[settings.LIVE],
        'member_id': settings.MEMBERID[settings.LIVE],
        'client_code': transaction.user_id,
        'scheme_cd': transaction.scheme_plan.bse_code,
        'buy_sell': 'P',
        'buy_sell_type': 'FRESH',
        'all_redeem': 'N',
        'min_redeem': 'N',
        'password': pass_dict['password'],
        'pass_key': pass_dict['passkey'],
        'internal_transaction': transaction.id, 
    }

    if (transaction.transaction_type == 'P'):
        # FRESH PURCHASE transaction
        data_dict['order_val'] = int(transaction.amount)

    else:
        # ADDITIONAL PURCHASE OR REDEEM transaction
        ## set folio_no by looking at previous transactions
        trans_l = get_previous_trans(transaction)

        ### assumption: pick the first relevant transaction's folio number if there are multiple transactions 
        # data_dict['folio_no'] = '123456789012345'
        data_dict['folio_no'] = trans_l[0].folio_number

        if (transaction.transaction_type == 'A'):
            # ADDITIONAL PURCHASE order
            data_dict['buy_sell_type'] = 'ADDITIONAL'
            data_dict['order_val'] = int(transaction.amount)
        
        elif (transaction.transaction_type == 'R'):
            # REDEEM order
            data_dict['buy_sell'] = 'R'

            ## set all_redeem flag in case entire investment is being redeemed, else 
            if (transaction.all_redeem):
                data_dict['all_redeem'] = 'Y'
                data_dict['order_val'] = ''
            elif (transaction.all_redeem == None):
                raise Exception(
                    "Internal error 634: all_redeem field of internal transaction table is not set for a redeem transaction"
                )

    form = NewOrderForm(data_dict)
    # form.is_valid() calls form.clean() as well as model.full_clean()
    # so validators on model are also applied
    if form.is_valid():
        bse_transaction = form.save()
        return bse_transaction
    else:
        raise Exception(
            'BSE error 648: %s' % form.errors
        )


# prepare the TransactionXsipBSE record
def prepare_xsip_order(transaction, pass_dict):

    if (transaction.order_type != '2'):
        raise Exception(
            "Internal error 630: XSIP Order entry cannot be prepared because Transaction argument passed is not SIP"
        )

    trans_no = prepare_trans_no(transaction.user_id, transaction.order_type)

    # find mandate id; if not found then create one
    from users.models import Mandate
    mandates = Mandate.objects.filter(
        user = transaction.user,
        status__in = (2,3,4,5), 
    )
    create_new = True
    # check if any of the mandates is valid
    for mandate in mandates:
        tr_list = Transaction.objects.filter(
            user = transaction.user,
            order_type = '2',
            status__in = ('2','5','6'),
            mandate = mandate.id,
        )
        amount_exhausted = 0
        for tr in tr_list:
            amount_exhausted += tr.amount
        if (mandate.amount >= amount_exhausted + transaction.amount):
            # use this mandate
            transaction.mandate_id = mandate.id
            create_new = False
            break
    if create_new:
        # decide amount of mandate
        mandate_amount = 100000
        if transaction.amount > mandate_amount:
            mandate_amount = transaction.amount
        # query bse for new mandate
        transaction.mandate_id = create_mandate_bse(transaction.user_id, mandate_amount)
    # in either case save the mandate_id in transaction entry
    transaction.save()

    # Internally, SIP transactions will necessarly have first instalment on day of placing xsip order itself and xsip start date (2nd instalment) will be atleast 30 days away
    import datetime
    now = datetime.date.today()
    num_days = (transaction.sip_start_date - now).days
    if (num_days < 30 and num_days >= 0):
        raise Exception(
            "BSE error 649: XSIP start date must be atleast 30 days from today"
        )
    elif (num_days > 60):
        raise Exception(
            "BSE error 649: XSIP start date must be at most 60 days from today"
        )

    data_dict = {
        'trans_code': 'NEW',
        'trans_no': trans_no,
        'user_id': settings.USERID[settings.LIVE],
        'member_id': settings.MEMBERID[settings.LIVE],
        'client_code': transaction.user_id,
        'scheme_cd': transaction.scheme_plan.bse_code,
        'start_date': transaction.sip_start_date.strftime('%d/%m/%Y'),
        'inst_amt': int(transaction.amount),
        'num_inst': int(transaction.sip_num_inst),
        'first_order_flag': 'Y',
        # 'first_order_flag': 'N',
        'mandate_id': transaction.mandate_id,
        'password': pass_dict['password'],
        'pass_key': pass_dict['passkey'],
        'internal_transaction': transaction.id, 
    }

    # ADDITIONAL PURCHASE order
    if (transaction.transaction_type == 'A'):
        ## set folio_no by looking at previous transactions
        trans_l = get_previous_trans(transaction)
        ## assumption: pick the first relevant transaction's folio number if there are multiple transactions 
        data_dict['folio_no'] = trans_l[0].folio_number
        transaction.folio_number = trans_l[0].folio_number
        transaction.save()

    form = NewXsipOrderForm(data_dict)
    # form.is_valid() calls form.clean() as well as model.full_clean()
    # so validators on model are also applied
    if form.is_valid():
        bse_transaction = form.save()
        # print bse_transaction
        return bse_transaction
    else:
        raise Exception(
            'BSE error 649: %s' % form.errors
        )


# prepare the TransactionBSE record
def prepare_order_cxl(transaction, order_id, pass_dict):

    trans_no = prepare_trans_no(transaction.user_id, transaction.order_type)

    data_dict = {
        'trans_code': 'CXL',
        'trans_no': trans_no,
        'user_id': settings.USERID[settings.LIVE],	
        'password': pass_dict['password'],
        'pass_key': pass_dict['passkey'],
        'internal_transaction': transaction.id, 
        ## not reqd by BSE. added for easier tracking internally
        'client_code': transaction.user_id,
        'member_id': settings.MEMBERID[settings.LIVE], 
    }
    ## depening on whether lumpsum order or xsip order, change data_dict and apply different form 
    if (transaction.order_type == '2'):
        data_dict['xsip_reg_id'] = order_id
        form = CxlXsipOrderForm(data_dict)
    elif (transaction.order_type == '1'):
        data_dict['order_id'] = order_id
        form = CxlOrderForm(data_dict)
    else:
        raise Exception(
            "Internal error 630: Invalid order_type in transaction table: "
        )

    # form.is_valid() calls form.clean() as well as model.full_clean()
    # so validators on model are also applied
    if form.is_valid():
        bse_transaction = form.save()
        # print bse_transaction
        return bse_transaction
    else:
        raise Exception(
            'BSE error 650: %s' % form.errors
        )


# store response to order entry from bse 
def store_order_response(response, order_type):
## lumpsum order 
    if (order_type == '1'):
        trans_response = TransResponseBSE(
            trans_code = response[0],
            trans_no = response[1],
            order_id = response[2],
            user_id = response[3],
            member_id = response[4],
            client_code = response[5],
            bse_remarks = response[6],
            success_flag = response[7],
            order_type = '1',
        )
    ## SIP order  
    elif (order_type == '2'):
        trans_response = TransResponseBSE(
            trans_code = response[0],
            trans_no = response[1],
            member_id = response[2],
            client_code = response[3],
            user_id = response[4],
            order_id = response[5],
            bse_remarks = response[6],
            success_flag = response[7],
            order_type = '2',
        )
    trans_response.save()
    return trans_response.order_id

## fire SOAP query to post the order 
def soap_post_order(client, bse_order):
    method_url = METHOD_ORDER_URL[settings.LIVE] + 'orderEntryParam'
    header_value = soap_set_wsa_headers(method_url, SVC_ORDER_URL[settings.LIVE])
    response = client.service.orderEntryParam(
        bse_order.trans_code,
        bse_order.trans_no,
        bse_order.order_id,
        bse_order.user_id,
        bse_order.member_id,
        bse_order.client_code,
        bse_order.scheme_cd,
        bse_order.buy_sell,
        bse_order.buy_sell_type,
        bse_order.dp_txn,
        bse_order.order_val,
        bse_order.qty,
        bse_order.all_redeem,
        bse_order.folio_no,
        bse_order.remarks,
        bse_order.kyc_status,
        bse_order.ref_no,
        bse_order.sub_br_code,
        bse_order.euin,
        bse_order.euin_val,
        bse_order.min_redeem,
        bse_order.dpc,
        bse_order.ip_add,
        bse_order.password,
        bse_order.pass_key,
        bse_order.param1,
        bse_order.param2,
        bse_order.param3,
        _soapheaders=[header_value]
    )

    ## this is a good place to put in a slack alert

    response = response.split('|')
    ## store the order response in a table
    order_id = store_order_response(response, '1')
    status = response[7]
    if (status == '0'):
        # order successful
        return order_id
    else:
        raise Exception(
            "BSE error 641: %s" % response[6]
        )


## fire SOAP query to post the XSIP order 
def soap_post_xsip_order(client, bse_order):
    method_url = METHOD_ORDER_URL[settings.LIVE] + 'xsipOrderEntryParam'
    header_value = soap_set_wsa_headers(method_url, SVC_ORDER_URL[settings.LIVE])
    response = client.service.xsipOrderEntryParam(
        bse_order.trans_code,
        bse_order.trans_no,
        bse_order.scheme_cd,
        bse_order.member_id,
        bse_order.client_code,
        bse_order.user_id,
        bse_order.int_ref_no,
        bse_order.trans_mode,
        bse_order.dp_txn,
        bse_order.start_date,
        bse_order.freq_type,
        bse_order.freq_allowed,
        bse_order.inst_amt,
        bse_order.num_inst,
        bse_order.remarks,
        bse_order.folio_no,
        bse_order.first_order_flag,
        bse_order.brokerage,
        bse_order.mandate_id,
        # '',
        bse_order.sub_br_code,
        bse_order.euin,
        bse_order.euin_val,
        bse_order.dpc,
        bse_order.xsip_reg_id,
        bse_order.ip_add,
        bse_order.password,
        bse_order.pass_key,
        bse_order.param1,
        # bse_order.mandate_id,
        bse_order.param2,
        bse_order.param3,
        _soapheaders=[header_value]
    )

    ## this is a good place to put in a slack alert

    response = response.split('|')
    ## store the order response in a table
    order_id = store_order_response(response, '2')
    status = response[7]
    if (status == '0'):
        # order successful
        return order_id
    else:
        raise Exception(
            "BSE error 642: %s" % response[6]
        )

