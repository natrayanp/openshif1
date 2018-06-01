from pf import app
from pf import dbfunc as db
from pf import jwtdecodenoverify as jwtnoverify

from flask import request, make_response, jsonify, Response, redirect
from pf import settings


from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support.select import Select
from selenium.common.exceptions import TimeoutException, NoSuchElementException, StaleElementReferenceException, ErrorInResponseException, ElementNotVisibleException, UnexpectedAlertPresentException, NoAlertPresentException
from httplib import BadStatusLine

# for datetime processing
from pytz import timezone
from datetime import datetime, timedelta, date
from time import strptime, sleep
from dateutil.relativedelta import relativedelta
from dateutil import tz

@app.route('/mforderstatuspg_web',methods=['GET','POST','OPTIONS'])
#example for model code http://www.postgresqltutorial.com/postgresql-python/transaction/
#should be called from mforderapi.
def mforderstatuspg_web():
    
    if request.method=='OPTIONS':
        print ("inside mforderstatus_web options")
        return jsonify({'body':'success'})

    elif request.method=='POST':   
        print ("inside mforderstatus_web post")
        print(request.headers)
        payload= request.get_json()
        #payload = request.stream.read().decode('utf8')    
        
        print("line 42:",payload)

        #check if client code is available in payload
        frmc = payload.get("from_client_code")
        toc = payload.get("to_client_code")
        
        if frmc == None or frmc == '':
            resp = make_response({'natstatus': 'error', 'statusdetails': 'No client code provided in request'}, 400)
            return resp
        elif toc == None or toc == '':
            payload["to_client_code"] = frmc


        driver = init_driver("chrome",False)
        #driver = init_driver("firfox",False)

        try:
            driver = login(driver)
            order_status_recs, driver = get_transaction_status(driver,payload)
        finally:
            quit_driver(driver)

        return order_status_recs

def login(driver):
    '''
    Logs into the BSEStar web portal using login credentials defined in settings
    '''
    try:
        url = settings.BSESTAR_LOGIN_PG[settings.LIVE]
        driver.get(url)
        print("Opened login page")
        
        # enter credentials
        userid = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, "txtUserId")))
        memberid = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, "txtMemberId")))
        password = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, "txtPassword")))
        userid.clear()
        userid.send_keys(settings.USERID[settings.LIVE])
        memberid.clear()
        memberid.send_keys(settings.MEMBERID[settings.LIVE])
        password.clear()
        password.send_keys(settings.PASSWORD[settings.LIVE])
        
        if settings.LIVE == 0: #in test environment wait untill captche is keyedin
            myvalue = True
            while myvalue:
                #present = wait.until(EC.text_to_be_present_in_element((By.NAME, "txtCaptcha"), "valueyouwanttomatch"))
                elem = driver.find_element_by_name("txtCaptcha").get_attribute('value')
                print (elem)
                if (len(elem) == 5):
                    myvalue = False
            print("outside while")

        submit = driver.find_element_by_id("btnLogin")
        submit.click()

        assert "WELCOME : NATRAYAN" in driver.page_source

        print("Logged in")
        return driver

    except (TimeoutException, NoSuchElementException, StaleElementReferenceException, 
        ErrorInResponseException, ElementNotVisibleException):
        print("Retrying in login")
        return login(driver)

    except (BadStatusLine):
        print("Retrying for BadStatusLine in login")
        driver = init_driver()
        return login(driver)


def get_transaction_status(driver, payload):
    '''
    get status of transactions
    '''
    try:
        order_status_recs, driver = find_order_status(driver, payload.get("dt"), payload.get("tran_type"), payload.get("from_client_code"), payload.get("to_client_code"))
        
        # update status of all orders incl sip instalment orders
        #update_order_status(driver)
        return order_status_recs, driver

    except (TimeoutException, StaleElementReferenceException, ErrorInResponseException, ElementNotVisibleException):
        print("Retrying")
        return get_transaction_status(driver,payload)

    except (BadStatusLine):
        print("Retrying for BadStatusLine in login")
        driver = init_driver()     
        driver = login(driver)
        return get_transaction_status(driver,payload)


def find_order_status(driver, dt=None, tran_type='P',frmclntcd = None,toclntcd = None):
#P - PURHCASE, R - REDEMPTION
    if dt:
        dt = dt
    else:
        dt = datetime.now().strftime('%d-%m-%Y'))


    url = settings.BSESTAR_ORDER_STATUS_PG[settings.LIVE]
    driver.get(url)
    print("Navigated to order status page")

    date = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, 'txtToDate')))
    date.clear()
    date.send_keys(date_dict['date'].strftime("%d-%b-%Y"))
    sleep(2)    # needed as page refreshes after setting date
    # make_ready(driver)

    if frmclntcd == None or frmclntcd == '' or toclntcd == None or toclntcd == '':
        pass
    else:
        frmcltcd = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, 'txtFromCltCode')))
        date.clear()
        date.send_keys(frmclntcd)

        tocltcd = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, 'txtToCltCode')))
        date.clear()
        date.send_keys(toclntcd)

    element = driver.find_element_by_xpath('//*[@id="ddlBuySell"]')
    all_options = element.find_elements_by_tag_name("option")
    for option in all_options:
        print("Value is: %s" % option.get_attribute("value"))
        if option.get_attribute("value") == tran_type:
            #selected option purcharse or redemption as per tran_type
            option.click()

    submit = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, "btnSubmit")))
    submit.click()
    sleep(2)
    
    ## parse the table- find orders for this date
    table = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, "//table[@class='glbTableD']/tbody")))
    print ('html loading done')
    rows = table.find_elements(By.XPATH, "tr[@class='tblERow'] | tr[@class='tblORow']")

    order_status_recs = []
    for row in rows:
        fields = row.find_elements(By.XPATH, "td")
        recs = {
            'order_id' : fields[3].text,
            'status' : fields[18].text,
            'client_code' : fields[5].text,
            'scheme_code' : fields[7].text,
            'buy_sell' : fields[10].text
        }
        order_status_recs.append(recs)
    
    print("line 196:", order_status_recs)

    return order_status_recs, driver


def init_driver(browsertyp = "chrome", headles = False):
# instantiate a browser with options object so you can set the size and headless preference
# intialises and returns the driver
    options = Options()
    if headles:
        options.add_argument("--headless")
        options.add_argument("--window-size=1920x1080")

    if browsertyp == "chrome":
        driver = webdriver.Chrome(chrome_options=options)
    elif browsertyp == "firefox":
        driver = webdriver.Firefox(firefox_options=options)

    return driver    


def quit_driver(driver):
#Quit the driver
    driver.quit()

#ddtopmenubar > ul > li:nth-child(6) > a
#export PATH=$PATH:/home/natrayan/project/AwsProject/puppeteer/geko
