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
import os


      
@app.route('/registdetfetch',methods=['GET','POST','OPTIONS'])
def registdetfetch():
#This is called by setjws service
    if request.method=='OPTIONS':
        print("inside REGISTRATIONDETAILSFETCH options")
        return make_response(jsonify('inside REGISTRATIONDETAILSFETCH options'), 200)  

    elif request.method=='GET':
        print("inside REGISTRATIONDETAILSFETCH GET")
        
        print((request))        
        userid,entityid=jwtnoverify.validatetoken(request)
        print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        
        '''
        try:
            con
        except NameError:
            print("con not defined so assigning as null")
            conn_string = "host='localhost' dbname='postgres' user='postgres' password='password123'"
            con=psycopg2.connect(conn_string)
            cur = con.cursor(cursor_factory=psycopg2.extras.DictCursor)
        else:            
            if con.closed:
                conn_string = "host='localhost' dbname='postgres' user='postgres' password='password123'"
                con=psycopg2.connect(conn_string)
                cur = con.cursor(cursor_factory=psycopg2.extras.DictCursor)
        '''



        #need to think on a way to get entity id for this

        #Select registration details for the userid and entity id derived from JWT START

        con,cur=db.mydbopncon()

        print(con)
        print(cur)
        
        command = cur.mogrify("SELECT clientcode, clientholding, clienttaxstatus, clientoccupationcode,clientappname1,(CASE WHEN clientdob < '01-01-1801' THEN NULL ELSE clientdob END) AS clientdob, clientgender , clientpan, clientnominee, clientnomineerelation, (CASE WHEN clientnomineedob < '01-01-1801' THEN NULL ELSE clientnomineedob END) AS clientnomineedob, clientnomineeaddress, clienttype, clientacctype1, clientaccno1, clientmicrno1, clientifsccode1, defaultbankflag1, clientadd1, clientadd2, clientadd3, clientcity, clientstate, clientpincode, clientcountry, clientemail, clientcommmode, clientdivpaymode, cm_foradd1, cm_foradd2, cm_foradd3, cm_forcity, cm_forpincode , cm_forstate, cm_forcountry, cm_mobile FROM uccclientmaster WHERE ucclguserid = %s AND uccentityid = %s;",(userid,entityid,))
        print(command)
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
    
        if (cur.rowcount) != 1:
            errorresp= {'natstatus':'error','statusdetails':'ZERO or MORE THAN ONE registration record for user'}
            resps = make_response(jsonify(errorresp), 400)
            print(resps)
            return(resps)
        else:
            rec = cur.fetchone()
            #createdetailfrm
            clientname = rec['clientappname1']
            clientpan = rec['clientpan']
            clientcode = rec['clientcode']
            clientgender = rec['clientgender']
            clientdob = rec['clientdob']
            clientemail = rec['clientemail']
            clientmobile = rec['cm_mobile']
            clientcommode = rec['clientcommmode']
            clientholding=rec['clientholding']
            #clientpepflg='' this is set in fatca

            #CLIENT TAX STATUS: 1-individual, 21-NRE, 24-NRO
            if rec['clienttaxstatus']=='21' or rec['clienttaxstatus']=='24':
                clientisnri = True
                clienttaxstatusres = False 
                clienttaxstatusnri = rec['clienttaxstatus']
            else:
                clientisnri = False
                clienttaxstatusres = True 
                clienttaxstatusnri = ''

            clientocupation = rec['clientoccupationcode']

            if clientocupation in ['01','43']:
                clientocutyp = 'B'
            elif clientocupation in ['02','03','04','09','41','42','44']:
                clientocutyp = 'S'
            elif clientocupation in ['05','06','07','08','99']:
                clientocutyp = 'O'
            else:
                clientocutyp=''

            clientnomineename = rec['clientnominee']
            if clientnomineename:
                clienthasnominee = True
                clientnomineerel = rec['clientnomineerelation']
                clientnomineedob = rec['clientnomineedob']
                clientnomineeaddres = rec['clientnomineeaddress']
            else:
                clienthasnominee = False
                clientnomineerel = ''
                clientnomineedob = ''
                clientnomineeaddres = ''
            
            clientfndhldtype = rec['clienttype'] #Defaulted to PHYS
            #createclientaddfrm
            clientaddress1 =  rec['clientadd1']
            clientaddress2 =  rec['clientadd2']
            clientaddress3 =  rec['clientadd3']
            clientcity =  rec['clientcity']

            clientstate = getcountryorstate(rec['clientstate'],'uccstname')
            
            
            
            clientpincode =  rec['clientpincode']
            print('pincode to send :',rec['clientpincode'])            
            clientcountry = rec['clientcountry']
            #clientcountry = getcountryorstate(rec['clientcountry'],'ucccnname')
            
          

            if clientisnri:
                clientforinadd1 =  rec['cm_foradd1']
                clientforinadd2 =  rec['cm_foradd2']
                clientforinadd3 =  rec['cm_foradd3']
                clientforcity =  rec['cm_forcity']
                clientforstate =  rec['cm_forstate']
                #clientforcountry =  rec['cm_forcountry']
                

                clientforcountry = getcountryorstate(rec['cm_forcountry'],'ucccnname')
                #forcntry[ucccncode.index(rec['cm_forcountry'])]
                clientforpin =  rec['cm_forpincode']
            else:
                clientforinadd1 =  ''
                clientforinadd2 =  ''
                clientforinadd3 =  ''
                clientforcity =  ''
                clientforstate = ''
                clientforcountry =  ''
                clientforpin =  ''

            #createclientbankfrm
            clientactype = rec['clientacctype1']
            clientacnumb = rec['clientaccno1']
            clientmicrno = rec['clientmicrno1']
            clientifsc = rec['clientifsccode1']


        command = cur.mogrify("SELECT pan_rp,inv_name,tax_status,po_bir_inc,co_bir_inc,tax_res1,tpin1,id1_type,tax_res2,tpin2,id2_type,tax_res3,tpin3,id3_type,tax_res4,tpin4,id4_type,srce_wealt,inc_slab,pep_flag,occ_code,occ_type FROM fatcamaster WHERE fatcalguserid = %s AND fatcaentityid = %s;",(userid,entityid,))
        print(command)
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
        
        if (cur.rowcount) != 1:
            errorresp= {'natstatus':'error','statusdetails':'ZERO or MORE THAN ONE Fatca record for user'}
            resps = make_response(jsonify(errorresp), 400)
            print(resps)
            return(resps)
        else:
            rec = cur.fetchone()
            #createclientfatcafrm
            clientsrcwealth = rec['srce_wealt']
            clientincslb = rec['inc_slab']
            clientpobir = rec['po_bir_inc']
            clientcobir = rec['co_bir_inc']
            #clienttaxrescntry1 = rec['tax_res1']
            clienttaxrescntry1 = getcountryorstate(rec['tax_res1'],'fatcnname')
            #forcntry[ucccncode.index(rec['tax_res1'])]
            clienttaxid1 = rec['tpin1']
            clienttaxidtype1 = rec['id1_type']
            #clienttaxrescntry2 = rec['tax_res2']
            clienttaxrescntry2 = getcountryorstate(rec['tax_res2'],'fatcnname')
            #forcntry[ucccncode.index(rec['tax_res2'])]
            clienttaxid2 = rec['tpin2']
            clienttaxidtype2 = rec['id2_type']
            #clienttaxrescntry3 = rec['tax_res3']
            clienttaxrescntry3 = getcountryorstate(rec['tax_res3'],'fatcnname')
            #forcntry[ucccncode.index(rec['tax_res3'])]
            clienttaxid3 = rec['tpin3']
            clienttaxidtype3 = rec['id3_type']
            #clienttaxrescntry4 = rec['tax_res4']
            clienttaxrescntry4 = getcountryorstate(rec['tax_res4'],'fatcnname')
            #forcntry[ucccncode.index(rec['tax_res4'])]
            clienttaxid4 = rec['tpin4']
            clienttaxidtype4 = rec['id4_type']
            clientpepflg = rec['pep_flag']
        
    
        #Select registration details for the userid and entity id derived from JWT END
        db.mydbcloseall(con,cur)
        return (json.dumps({'clientname':clientname,'clientpan':clientpan,'clientcode':clientcode,'clientgender':clientgender,'clientdob':str(clientdob),'clientemail':clientemail,'clientmobile':clientmobile,'clientcommode':clientcommode,'clientholding':clientholding,'clientpepflg':clientpepflg,'clientisnri':clientisnri,'clienttaxstatusres':clienttaxstatusres,'clienttaxstatusnri':clienttaxstatusnri,'clientocupation':clientocupation,'clientocutyp':clientocutyp,'clientnomineename':clientnomineename,'clienthasnominee':clienthasnominee,'clientnomineerel':clientnomineerel,'clientnomineedob':str(clientnomineedob),'clientnomineeaddres':clientnomineeaddres,'clientfndhldtype':clientfndhldtype,'clientaddress1':clientaddress1,'clientaddress2':clientaddress2,'clientaddress3':clientaddress3,'clientcity':clientcity,'clientstate':clientstate,'clientcountry':clientcountry,'clientpincode':clientpincode,'clientforinadd1':clientforinadd1,'clientforinadd2':clientforinadd2,'clientforinadd3':clientforinadd3,'clientforcity':clientforcity,'clientforstate':clientforstate,'clientforcountry':clientforcountry,'clientforpin':clientforpin,'clientactype':clientactype,'clientacnumb':clientacnumb,'clientmicrno':clientmicrno,'clientifsc':clientifsc,'clientsrcwealth':clientsrcwealth,'clientincslb':clientincslb,'clientpobir':clientpobir,'clientcobir':clientcobir,'clienttaxrescntry1':clienttaxrescntry1,'clienttaxid1':clienttaxid1,'clienttaxidtype1':clienttaxidtype1,'clienttaxrescntry2':clienttaxrescntry2,'clienttaxid2':clienttaxid2,'clienttaxidtype2':clienttaxidtype2,'clienttaxrescntry3':clienttaxrescntry3,'clienttaxid3':clienttaxid3,'clienttaxidtype3':clienttaxidtype3,'clienttaxrescntry4':clienttaxrescntry4,'clienttaxid4':clienttaxid4,'clienttaxidtype4':clienttaxidtype4,'clientpepflg':clientpepflg}))

@app.route('/dtlfrmsave',methods=['GET','POST','OPTIONS'])
def dtlfrmsave():
#This is called when save for later button is clicked
    if request.method=='OPTIONS':
        print("inside DETAIL FORM SAVE options")
        return make_response(jsonify('inside DETAIL FORM SAVE options'), 200)  

    elif request.method=='POST':
        print("inside DETAIL FORM SAVE post")
        
        regisdbdata={}
        fatcadbdata={}

        print((request))        
        userid,entityid=jwtnoverify.validatetoken(request)
        print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        data = request.get_json()
        print(data)

        regisdbdata['clientappname1'] = data['clientname']
        regisdbdata['clientpan'] = data['clientpan']
        regisdbdata['clientcode'] = data['clientcode']
        regisdbdata['clientgender'] = data['clientgender']

        print('date',data['clientdob'])

        #regisdbdata['clientdob'] = data['clientdob']
        #data['clientdob'] = None


        if data['clientdob'] == 'None':
            regisdbdata['clientdob'] = '01/01/1800'
            fatcadbdata['dob'] = '01/01/1800'
            print('correctly inside elif  none string')
        elif data['clientdob'] == None:
            regisdbdata['clientdob'] = '01/01/1800'
            fatcadbdata['dob'] = '01/01/1800'
            print('correctly inside elif  none')
        elif data['clientdob']:
            regisdbdata['clientdob'] = data['clientdob']
            fatcadbdata['dob'] = data['clientdob']
        else:
            regisdbdata['clientdob'] = '01/01/1800'
            fatcadbdata['dob'] = '01/01/1800'
            print('correctly inside else')

        #regisdbdata['clientdob'] = (item or NULL if data['clientdob'])
        regisdbdata['clientemail'] = data['clientemail']
        regisdbdata['cm_mobile'] = data['clientmobile']
        regisdbdata['clientcommmode'] = 'M'     #M- Mail
        regisdbdata['clientholding'] = 'SI'     #SI - Single, JO - Joint, AS - Anyone or Survivor

        if data['clientpepflg']:
            fatcadbdata['pep_flag'] = 'Y'
        else:
            fatcadbdata['pep_flag'] = 'N'

        regisdbdata['clienttype'] = 'P'     #P- Physical, D-Demat

        regisdbdata['clientadd1'] = data['clientaddress1']
        regisdbdata['clientadd2'] = data['clientaddress2']
        regisdbdata['clientadd3'] = data['clientaddress3']
        regisdbdata['clientcity'] = data['clientcity']
        #regisdbdata['clientstate'] = data['clientstate']

        
        regisdbdata['clientstate'] = getcountryorstate(data['clientstate'],'uccstcode')
        #fatstcode[statesname.index(data['clientstate'])]

        regisdbdata['clientpincode'] = data['clientpincode']

        regisdbdata['clientcountry'] = data['clientcountry']
        #regisdbdata['clientcountry'] = getcountryorstate(data['clientcountry'],'ucccncode')
        #ucccncode[forcntry.index(data['clientcountry'])]
        
        regisdbdata['cm_foradd1'] = data['clientforinadd1']
        regisdbdata['cm_foradd2'] = data['clientforinadd2']
        regisdbdata['cm_foradd3'] = data['clientforinadd3']
        regisdbdata['cm_forcity'] = data['clientforcity']
        regisdbdata['cm_forstate'] = data['clientforstate']


        #regisdbdata['cm_forcountry'] = data['clientforcountry']
        regisdbdata['cm_forcountry'] = getcountryorstate(data['clientforcountry'],'ucccncode')
        #ucccncode[forcntry.index(data['clientforcountry'])]

        regisdbdata['cm_forpincode'] = data['clientforpin']
        regisdbdata['clientacctype1'] = data['clientactype']
        regisdbdata['clientaccno1'] = data['clientacnumb']
        regisdbdata['clientmicrno1'] = data['clientmicrno']
        regisdbdata['clientifsccode1'] = data['clientifsc']
        regisdbdata['defaultbankflag1'] = 'Y'
        

        if data['clientisnri']:
            regisdbdata['clienttaxstatus'] = data['clienttaxstatusnri']
            fatcadbdata['tax_status'] = data['clienttaxstatusnri']
        else:
            regisdbdata['clienttaxstatus']='01'
            fatcadbdata['tax_status']='01'


        regisdbdata['clientoccupationcode'] = data['clientocupation']
        fatcadbdata['occ_code'] = data['clientocupation']

        if data['clientocupation'] in ['01','43']:
            fatcadbdata['occ_type'] = 'B'
        elif data['clientocupation'] in ['02','03','04','09','41','42','44']:
            fatcadbdata['occ_type'] = 'S'
        elif data['clientocupation'] in ['05','06','07','08','99']:
            fatcadbdata['occ_type'] = 'O'
        else:
            fatcadbdata['occ_type']=''

        if data['clienthasnominee']:
            regisdbdata['clientnominee'] = data['clientnomineename']
            regisdbdata['clientnomineerelation'] = data['clientnomineerel']            
            regisdbdata['clientnomineeaddress'] = data['clientnomineeaddres']
            
        else:
            regisdbdata['clientnominee'] = ''
            regisdbdata['clientnomineerelation'] = ''
            regisdbdata['clientnomineeaddress'] = ''
        
        if data['clientnomineedob']:
            regisdbdata['clientnomineedob'] = data['clientnomineedob']
        else:
            regisdbdata['clientnomineedob'] = '01-01-1800'


        fatcadbdata['srce_wealt'] = data['clientsrcwealth']
        fatcadbdata['inc_slab'] = data['clientincslb']
        print('########################')
        print(data['clientpobir'])
        print(data['clientcobir'])
        print('########################')
        fatcadbdata['po_bir_inc'] = data['clientpobir']
        fatcadbdata['co_bir_inc'] = data['clientcobir']
        #fatcadbdata['tax_res1'] = data['clienttaxrescntry1']
        fatcadbdata['tax_res1'] = getcountryorstate(data['clienttaxrescntry1'],'fatcncode')
        #ucccncode[forcntry.index(data['clienttaxrescntry1'])]
        fatcadbdata['tpin1'] = data['clienttaxid1']
        fatcadbdata['id1_type'] = data['clienttaxidtype1']
        #fatcadbdata['tax_res2'] = data['clienttaxrescntry2']
        fatcadbdata['tax_res2'] = getcountryorstate(data['clienttaxrescntry2'],'fatcncode')
        #ucccncode[forcntry.index(data['clienttaxrescntry2'])]
        fatcadbdata['tpin2'] = data['clienttaxid2']
        fatcadbdata['id2_type'] = data['clienttaxidtype2']
        #fatcadbdata['tax_res3'] =  data['clienttaxrescntry3']
        fatcadbdata['tax_res3'] =  getcountryorstate(data['clienttaxrescntry3'],'fatcncode')
        #ucccncode[forcntry.index(data['clienttaxrescntry3'])]
        fatcadbdata['tpin3'] = data['clienttaxid3']
        fatcadbdata['id3_type'] = data['clienttaxidtype3']
        #fatcadbdata['tax_res4'] =  data['clienttaxrescntry4']
        fatcadbdata['tax_res4'] =  getcountryorstate(data['clienttaxrescntry4'],'fatcncode')
        #ucccncode[forcntry.index(data['clienttaxrescntry4'])]
        fatcadbdata['tpin4'] = data['clienttaxid4']
        fatcadbdata['id4_type'] = data['clienttaxidtype4']


        regisdbdata['clientdivpaymode'] = '02'     #01-Cheque, 02-Direct Credit, 03-ECS, 04-NEFT, 05-RTGS
        regisdbdata['ucclmtime'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        fatcadbdata['addr_type'] ='1'              #1 - Residential or Business;2 - Residential; 3 - Business;4 - Registered Office; 5 -Unspecified
        fatcadbdata['data_src'] = 'E'              #E - Electronic   P - Physical
        fatcadbdata['ubo_appl'] = 'N'
        fatcadbdata['sdf_flag'] = 'N'
        fatcadbdata['ubo_df'] = 'N'
        fatcadbdata['exch_name'] = 'O'              #B - BSE, N - NSE, O - Others
        fatcadbdata['new_change'] = 'N'             #N-New
        
        #Logname to be set from the lamda
        fatcadbdata['log_name'] = 'logname.log'
        #Logname to be set from the lamda

        fatcadbdata['fatcalmtime']=datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        regiskeys = ",".join(regisdbdata.keys())
        regisvals = "','".join(item or '' for item in regisdbdata.values())
        regisvals = "'"+regisvals+"'"
        fatcakeys = ",".join(fatcadbdata.keys())
        fatcavals = "','".join(item or '' for item in fatcadbdata.values())
        fatcavals = "'"+fatcavals+"'"
        
        con,cur=db.mydbopncon()

        #update UCC CLIENT MASTER table START
        comqry = 'UPDATE webapp.uccclientmaster SET ('+ regiskeys + ') = (' + regisvals + ') WHERE ucclguserid = %s AND uccentityid = %s;'
        command = cur.mogrify(comqry,(userid,entityid,))
        print('command :',command)                
 
        cur, dbqerr = db.mydbfunc(con,cur,command)

        if cur.closed == True:
            if(dbqerr['natstatus'] == "error" or dbqerr['natstatus'] == "warning"):
                dbqerr['statusdetails']="User details insert Failed"
            resp = make_response(jsonify(dbqerr), 400)
            return(resp)
        #update UCC CLIENT MASTER table END

        #update FATCA CLIENT MASTER table START
        comqry = 'UPDATE fatcamaster SET ('+ fatcakeys + ') = (' + fatcavals + ') WHERE fatcalguserid = %s AND fatcaentityid = %s;'
        command = cur.mogrify(comqry,(userid,entityid,))
        print('command :',command)                
 
        cur, dbqerr = db.mydbfunc(con,cur,command)

        if cur.closed == True:
            if(dbqerr['natstatus'] == "error" or dbqerr['natstatus'] == "warning"):
                dbqerr['statusdetails']="FATCA details insert Failed"
            resp = make_response(jsonify(dbqerr), 400)
            return(resp)
        #update FATCA CLIENT MASTER table END

        con.commit()
        db.mydbcloseall(con,cur)
        print('commit successful')
        return make_response(jsonify({'natstatus':'success','statusdetails':'Data saved'}), 200) 
        

@app.route('/regisandfatcsubmit',methods=['GET','POST','OPTIONS'])
def regisandfatcsubmit():
    #Endpoint to submit both registration and fatca details
    if request.method=='OPTIONS':
        print("inside regisandfatcsubmit options")
        return make_response(jsonify('inside regisandfatcsubmit options'), 200)  

    elif request.method=='POST':
        #return make_response(jsonify('inside regisandfatcsubmit options'), 200) 
        print("inside regisandfatcsubmit POST")
        
        print((request))        
        userid,entityid=jwtnoverify.validatetoken(request)
        print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        
        con,cur=db.mydbopncon()

        print(con)
        print(cur)
        
        command = cur.mogrify("SELECT row_to_json(t) from(SELECT * FROM uccclientmaster A FULL OUTER JOIN fatcamaster B ON A.ucclguserid = B.fatcalguserid) as t where t.fatcalguserid = %s and t.fatcaentityid = %s AND t.ucclguserid = %s AND t.uccentityid = %s;",(userid,entityid,userid,entityid,))
        print(command)
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

        #if len(records) == 0:
        if (cur.rowcount) == 0:
            errorresp= {'natstatus':'error','statusdetails':'User not registered/activated'}
            resps = make_response(jsonify(errorresp), 400)
            print(resps)    
            return(resps)
        else:
            custrecord = records[0][0]

        print(custrecord)
        print(type(custrecord))

        #make all NONE to '' so JOSN serialisation will not fail
        for key, value in custrecord.items():
            custrecord[key] = '' if value is None else str(value)

        
        #Next step is to post this data to customer creation and wait for its make_response
        #Based on the response send response to front end
        del custrecord['fatcalguserid']
        del custrecord['fatcaoctime']
        del custrecord['fatcalmtime']   
        del custrecord['fatcaentityid']
        del custrecord['ucclguserid']
        del custrecord['uccoctime']   
        del custrecord['ucclmtime']   
        del custrecord['uccentityid']
        del custrecord['clientnomineedob']
        del custrecord['clientnomineeaddress']

        processadd = custrecord['clientadd1']+custrecord['clientadd2']+custrecord['clientadd3']

        custrecord['clientadd1'] = processadd[:30]
        if (len(processadd) > 30):
            custrecord['clientadd2'] = processadd[30:60]
            if (len(processadd) > 60):
                custrecord['clientadd3'] = processadd[60:90]
            else:
                custrecord['clientadd3'] = ''
        else:
            custrecord['clientadd2'] = custrecord['clientadd3'] = ''
        
        appname1 = custrecord['clientappname1'] 							#firstname

        '''
        if (custrecord['clientappname2'] != ''):							#middlename
            appname1 = appname1 + ' ' + custrecord['clientappname2']		
        if (custrecord[clientappname3] != ''):
            appname1 = appname1 + ' ' + custrecord['clientappname3']		#lastname
        '''
        custrecord['clientappname1'] = appname1[:70]
        
        
        custrecord['clientdob'] = (datetime.strptime(custrecord['clientdob'], '%Y-%m-%d').date()).strftime('%d/%m/%Y')
        custrecord['dob'] = (datetime.strptime(custrecord['clientdob'], '%d/%m/%Y').date()).strftime('%m/%d/%Y')
        #(datetime.strptime(custrecord['clientdob'], '%Y-%m-%d').date()).strftime('%d/%m/%Y')
        

        #All formating should be done above
        for key, value in custrecord.items():
            custrecord[key] = '' if value is None else str(value)

        #url='http://192.168.1.27:8000/custcreation'
        url=settings.BSESTAR_USERCREATION_URL[settings.LIVE];
        print(url)
        print(custrecord)
        
        r = requests.post(url, json=custrecord)
        print(r.text)
        rj= json.loads(r.text)   
        print('rj :',rj)     
        if r.status_code != 200:	
            resp = make_response(jsonify({'natstatus':'error','statusdetails':rj['statusmessage']}), 400)
        else:
            #Update userstatus to U to indicate completion of registration and pending document upload
            command = cur.mogrify("UPDATE userlogin SET lguserstatus = 'U', lglmtime = CURRENT_TIMESTAMP WHERE lguserid = %s AND lgentityid = %s;",(userid,entityid,))        
            print(command)            
            cur, dbqerr = db.mydbfunc(con,cur,command)
            print(dbqerr['natstatus'])
            if cur.closed == True:
                if(dbqerr['natstatus'] == "error" or dbqerr['natstatus'] == "warning"):
                    dbqerr['statusdetails']="pf Fetch failed"
                resp = make_response(jsonify(dbqerr), 400)
                return(resp)
            con.commit()                  
            #print(cur)
            print('consider insert or update is successful')
                        
            #INSERT NOTIFICATION ENTRY FOR PENDING REGISTRAION DOC UPLOAD START
            nfmid=datetime.now().strftime('%Y%m%d%H%M%S%f')
            print('nfmid :',nfmid)
            command = cur.mogrify("INSERT INTO notifimaster (nfmid,nfname,nfmuserid,nfmscreenid,nfmessage,nfmsgtype,nfmprocessscope,nfmnxtact,nfmnxtactmsg,nfmnxtactnavtyp,nfmnxtactnavdest,nfmstartdt,nfmoctime,nfmlmtime,nfmentityid) VALUES (%s,'pendingregisupload',%s,'dashboard','<div fxLayout=#column# fxLayoutWrap><div><p> Account Open Form (AOF) is sent to your email. Please upload the form to complete registration </p><p>Go to Setting > Docupload </div><div>','notifaction','P','Y','','NONE','NONE',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,%s) ON CONFLICT DO NOTHING;",(nfmid,userid,entityid,))
            print(command)
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

            #INSERT NOTIFICATION ENTRY FOR PENDING REGISTRAION DOC UPLOAD END            
            
            #Now send resonse back to client confirming registration success.  Nav to registration success screen
            resp = make_response(jsonify({'natstatus':'success','statusdetails':'client registration successful'}), 200)
        
        db.mydbcloseall(con,cur)
        #return the resp as set above
        return resp


def getcountryorstate(nameorcode,wantcodeorname):
    forcntry=['Afghanistan','Aland Islands','Albania','Algeria','American Samoa','Angola','Anguilla','Antarctica','Antigua And Barbuda','Argentina','Armenia','Aruba','Australia','Austria','Azerbaijan','Bahamas','Bahrain','Bangladesh','Barbados','Belarus','Belgium','Belize','Benin','Bermuda','Bhutan','Bolivia','Bosnia And Herzegovina','Botswana','Bouvet Island','Brazil','British Indian Ocean Territory','Brunei Darussalam','Bulgaria','Burkina Faso','Burundi','Cambodia','Cameroon','Canada','Cape Verde','Cayman Islands','Central African Republic','Chad','Chile','China','Christmas Island','Cocos (Keeling) Islands','Colombia','Comoros','Congo','Congo The Democratic Republic Of The','Cook Islands','Costa Rica','Cote DIvoire','Croatia','Cuba','Cyprus','Czech Republic','Denmark','Djibouti','Dominica','Dominican Republic','Ecuador','Egypt','El Salvador','Equatorial Guinea','Eritrea','Estonia','Ethiopia','Falkland Islands (Malvinas)','Faroe Islands','Fiji','Finland','France','French Guiana','French Polynesia','French Southern Territories','Gabon','Gambia','Georgia','Germany','Ghana','Gibraltar','Greece','Greenland','Grenada','Guadeloupe','Guam','Guatemala','Guernsey','Guinea','Guinea-Bissau','Guyana','Haiti','Heard Island And Mcdonald Islands','Honduras','Hong Kong','Hungary','Iceland','India','Indonesia','Iran Islamic Republic Of','Iraq','Ireland','Isle Of Man','Israel','Italy','Jamaica','Japan','Jersey','Jordan','Kazakhstan','Kenya','Kiribati','Korea Democratic Peoples Republic Of','Korea Republic Of','Kuwait','Kyrgyzstan','Lao Peoples Democratic Republic','Latvia','Lebanon','Lesotho','Liberia','Libyan Arab Jamahiriya','Liechtenstein','Lithuania','Luxembourg','Macao','Macedonia The Former Yugoslav Republic of','Madagascar','Malawi','Malaysia','Maldives','Mali','Malta','Marshall Islands','Martinique','Mauritania','Mauritius','Mayotte','Mexico','Micronesia Federated States Of','Moldova Republic Of','Monaco','Mongolia','Montserrat','Morocco','Mozambique','Myanmar','Namibia','Nauru','Nepal','Netherlands','Netherlands Antilles','New Caledonia','New Zealand','Nicaragua','Niger','Nigeria','Niue','Norfolk Island','Northern Mariana Islands','Norway','Oman','Pakistan','Palau','Palestinian Territory Occupied','Panama','Papua New Guinea','Paraguay','Peru','Philippines','Pitcairn','Poland','Portugal','Puerto Rico','Qatar','Reunion','Romania','Russian Federation','Rwanda','Saint Helena','Saint Kitts And Nevis','Saint Lucia','Saint Pierre And Miquelon','Saint Vincent And The Grenadines','Samoa','San Marino','Sao Tome And Principe','Saudi Arabia','Senegal','Serbia And Montenegro','Seychelles','Sierra Leone','Singapore','Slovakia','Slovenia','Solomon Islands','Somalia','South Africa','South Georgia And The South Sandwich Islands','Spain','Sri Lanka','Sudan','Suriname','Svalbard And Jan Mayen','Swaziland','Sweden','Switzerland','Syrian Arab Republic','Taiwan, Province Of China','Tajikistan','Tanzania, United Republic Of','Thailand','Timor-Leste','Togo','Tokelau','Tonga','Trinidad And Tobago','Tunisia','Turkey','Turkmenistan','Turks And Caicos Islands','Tuvalu','Uganda','Ukraine','United Arab Emirates','United Kingdom','United States of America','United States Minor Outlying Islands','Uruguay','Uzbekistan','Vanuatu','Venezuela','Viet Nam','Virgin Islands, British','Virgin Islands, U.S.','Wallis And Futuna','Western Sahara','Yemen','Zambia','Zimbabwe']
    
    ucccncode=['001','002','003','004','005','007','008','009','010','011','012','013','014','015','016','017','018','019','020','021','022','023','024','025','026','027','028','029','030','031','032','033','034','035','036','037','038','039','040','041','042','043','044','045','046','047','048','049','050','051','052','053','054','055','056','057','058','059','060','061','062','063','064','065','066','067','068','069','070','071','072','073','074','075','076','077','078','079','080','081','082','083','084','085','086','087','088','089','090','091','092','093','094','095','097','098','099','100','101','102','103','104','105','106','107','108','109','110','111','112','113','114','115','116','117','118','119','120','121','122','123','124','125','126','127','128','129','130','131','132','133','134','135','136','137','138','139','140','141','142','143','144','145','146','147','148','149','150','151','152','153','154','155','156','157','158','159','160','161','162','163','164','165','166','167','168','169','170','171','172','173','174','175','176','177','178','179','180','181','182','183','184','185','186','187','188','189','190','191','192','193','194','195','196','197','198','199','200','201','202','203','204','205','206','207','208','209','210','211','212','213','214','215','216','217','218','219','220','221','222','223','224','225','226','227','228','229','230','231','232','233','234','235','236','237','238','239','240','241','242','243']
    fatcncode=['AF','AX','AL','DZ','AS','AO','AI','AQ','AG','AR','AM','AW','AU','AT','AZ','BS','BH','BD','BB','BY','BE','BZ','BJ','BM','BT','BO','BA','BW','BV','BR','IO','BN','BG','BF','BI','KH','CM','CA','CV','KY','CF','TD','CL','CN','CX','CC','CO','KM','CG','CD','CK','CR','CI','HR','CU','CY','CZ','DK','DJ','DM','DO','EC','EG','SV','GQ','ER','EE','ET','FK','FO','FJ','FI','FR','GF','PF','TF','GA','GM','GE','DE','GH','GI','GR','GL','GD','GP','GU','GT','GG','GN','GW','GY','HT','HM','HN','HK','HU','IS','IN','ID','IR','IQ','IE','IM','IL','IT','JM','JP','JE','JO','KZ','KE','KI','KP','KR','KW','KG','LA','LV','LB','LS','LR','LY','LI','LT','LU','MO','MK','MG','MW','MY','MV','ML','MT','MH','MQ','MR','MU','YT','MX','FM','MD','MC','MN','MS','MA','MZ','MM','NA','NR','NP','NL','AN','NC','NZ','NI','NE','NG','NU','NF','MP','NO','OM','PK','PW','PS','PA','PG','PY','PE','PH','PN','PL','PT','PR','QA','RE','RO','RU','RW','SH','KN','LC','PM','VC','WS','SM','ST','SA','SN','RS','SC','SL','SG','SK','SI','SB','SO','ZA','GS','ES','LK','SD','SR','SJ','SZ','SE','CH','SY','TW','TJ','TZ','TH','TL','TG','TK','TO','TT','TN','TR','TM','TC','TV','UG','UA','AE','GB','US','UM','UY','UZ','VU','VE','VN','VG','VI','WF','EH','YE','ZM','ZW']

    statesname=['Andaman & Nicobar','Andhra Pradesh','Arunachal Pradesh','Assam','Bihar','Chandigarh','Chhattisgarh','Dadra and Nagar Haveli','Daman and Diu','GOA','Gujarat','Haryana','Himachal Pradesh','Jammu & Kashmir','Jharkhand','Karnataka','Kerala','Lakshadweep','Madhya Pradesh','Maharashtra','Manipur','Meghalaya','Mizoram','Nagaland','New Delhi','Orissa','Others','Pondicherry','Punjab','Rajasthan','Sikkim','Tamil Nadu','Telengana','Tripura','Uttar Pradesh','Uttaranchal','West Bengal']
    uccstcode=['AN','AP','AR','AS','BH','CH','CG','DN','DD','GO','GU','HA','HP','JM','JK','KA','KE','LD','MP','MA','MN','ME','MI','NA','ND','OR','OH','PO','PU','RA','SI','TN','TG','TR','UP','UC','WB']
    fatstcode=['AN','AP','AR','AS','BR','CH','CG','DN','DD','GA','GJ','HR','HP','JK','JH','KA','KL','LD','MP','MH','MN','ML','MZ','NL','DL','OR','XX','PY','PB','RJ','SK','TN','XX','TR','UP','UA','WB']

    if nameorcode == None:
        item = ''
    elif nameorcode =='':
        item = ''
    else:
        if wantcodeorname == 'ucccncode':
            try:
                item=ucccncode[forcntry.index(nameorcode)]
            except (ValueError, IndexError) as error:
                item=''
        elif wantcodeorname == 'fatcncode':  
            try:  
                item = fatcncode[forcntry.index(nameorcode)]
            except (ValueError, IndexError) as error:
                item=''
        elif wantcodeorname == 'ucccnname':        
            try:
                item= forcntry[ucccncode.index(nameorcode)]    
            except (ValueError, IndexError) as error:
                item=''
        elif wantcodeorname == 'fatcnname':        
            try:
                item = forcntry[fatcncode.index(nameorcode)]    
            except (ValueError, IndexError) as error:
                item=''
        elif wantcodeorname == 'uccstcode':    
            try:
                item = uccstcode[statesname.index(nameorcode)]
            except (ValueError, IndexError) as error:
                item=''
        elif wantcodeorname == 'fatstcode':    
            try:
                item = fatstcode[statesname.index(nameorcode)]
            except (ValueError, IndexError) as error:
                item=''
        elif wantcodeorname == 'uccstname':
            try:
                item = statesname[uccstcode.index(nameorcode)]
            except (ValueError, IndexError) as error:
                item=''
        elif wantcodeorname == 'fatstname':
            try:      
                item = statesname[fatstcode.index(nameorcode)]    
            except (ValueError, IndexError) as error:
                item=''
        else:
            item = ''

    return item


