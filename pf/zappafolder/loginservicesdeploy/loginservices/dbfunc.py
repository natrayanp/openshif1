from .loginservicesmain import app
from flask import jsonify

import psycopg2
import psycopg2.extras


def mydbfunc(con,cur,command):

    try:
        cur.execute(command)        
        myerror={'natstatus':'success','statusdetails':''}
    except psycopg2.Error as e:
        print(e)
        myerror= {'natstatus':'error','statusdetails':''}
    except psycopg2.Warning as e:
        print(e)
        myerror={'natstatus':'warning','statusdetails':''}
        #myerror = {'natstatus':'warning','statusdetails':e}
    finally:
        if myerror['natstatus'] != "success":    
            con.rollback()
            cur.close()
            con.close()
            
    return cur,myerror

def mydbopncon():

    try:
        con
    except NameError:
        print("con not defined so assigning as null")
        #conn_string = "host='localhost' dbname='postgres' user='postgres' password='password123'"
        conn_string = "host='mysb.cygdhnb2k7oe.ap-southeast-1.rds.amazonaws.com' dbname='mysbdb' user='natrayan' password='Nirudhi1'"
        
        con=psycopg2.connect(conn_string)
        cur = con.cursor(cursor_factory=psycopg2.extras.DictCursor)
        print('con and cur assigned')
    else:            
        if con.closed:
            #conn_string = "host='localhost' dbname='postgres' user='postgres' password='password123'"
            conn_string = "host='mysb.cygdhnb2k7oe.ap-southeast-1.rds.amazonaws.com' dbname='mysbdb' user='natrayan' password='Nirudhi1'"
            
            con=psycopg2.connect(conn_string)
            cur = con.cursor(cursor_factory=psycopg2.extras.DictCursor)
    
    return con,cur

def mydbcloseall(con,cur):
#close cursor and connection before exit
    con.commit()
    cur.close()
    con.close()
    print('connection closed')