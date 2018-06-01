from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support.select import Select


# for datetime processing
from pytz import timezone
from datetime import datetime, timedelta, date
from time import strptime, sleep
from dateutil.relativedelta import relativedelta
from dateutil import tz


options = Options()
#options.add_argument("--headless")
#chrome_options.add_argument("--window-size=1920x1080")


#driver = webdriver.Firefox(firefox_options=options)
driver = webdriver.Chrome(chrome_options=options)

'''
driver.get("https://camskra.com/")


#driver.get("file:///C:/Users/natrayanpalani.REG1/Desktop/new23.html")
#element = driver.find_element_by_xpath("//span[@class='ytp-menu-label-secondary']")

try:

    element = WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.ID, "tbPanSrch"))
    )

finally:
    pass

print("after element")
elem = driver.find_element_by_name("ctl00$tbPanSrch")
elem.clear()
elem.send_keys('BNZPM2501F')
elem.send_keys(Keys.RETURN)

sst=True

se=True
while se:
    tx = driver.find_element_by_xpath('//*[@id="lblPanNo"]')
    txv = tx.get_attribute('textContent')
    print(txv)
    if (len(txv) > 7):
        se = False
print("waiting")
print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
driver.implicitly_wait(100) # seconds
print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
print("waiting end")


name_str = driver.find_element_by_xpath('//*[@id="lblName"]').get_attribute('textContent')
status_str = driver.find_element_by_xpath('//*[@id="lblStatus"]').get_attribute('textContent')

print(name_str)
print(status_str)
 
#status_str values
# 1) KYC Not Registered (testing - AWXPR5832J)
# 2) KYC Registered-New KYC (testing - BNZPM2501F)
 
driver.close()
'''










# instantiate a chrome options object so you can set the size and headless preference
#options = Options()
#options.add_argument("--headless")
#chrome_options.add_argument("--window-size=1920x1080")


#driver = webdriver.Firefox(firefox_options=options)
#driver = webdriver.Chrome(chrome_options=options)
driver.get("https://bsestarmfdemo.bseindia.com")
assert "Mutual Fund System - Bombay Stock Exchange Limited" in driver.title
name = driver.find_element_by_name("txtUserId")
name.clear()
name.send_keys("1712301")

member = driver.find_element_by_name("txtMemberId")
member.clear()
member.send_keys("17123")

elem = driver.find_element_by_name("txtPassword")
elem.clear()
elem.send_keys('@654321')

myvalue = True
while myvalue:
    #present = wait.until(EC.text_to_be_present_in_element((By.NAME, "txtCaptcha"), "valueyouwanttomatch"))
    elem = driver.find_element_by_name("txtCaptcha").get_attribute('value')
    print (elem)
    if (len(elem) == 5):
        myvalue = False
print("outside while")

member = driver.find_element_by_name("btnLogin").click()


assert "WELCOME : NATRAYAN" not in driver.page_source


member = driver.find_element_by_xpath('//*[@id="ddtopmenubar"]/ul/li[6]/a')
member.click()

line = 'https://bsestarmfdemo.bseindia.com/RptOrderStatusReportNew.aspx'
driver.get(line)
print (driver.title)
driver.get(line)
print (driver.title)
datee = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, 'txtToDate')))
datee.clear()

date_dict={}
today = date.today()
date_dict['date'] = today
datee.send_keys(date_dict['date'].strftime("%d-%b-%Y"))
sleep(2)    # needed as page refreshes after setting date
# make_ready(driver)    


#transtype =  WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID,"ddlBuySell")))
#transtype = driver.find_element_by_xpath('/html/body/form/div[4]/table/tbody/tr[3]/td[2]/select/option[2]')
#transtype = Select(driver.find_element_by_xpath('//*[@id="ddlBuySell"]'))

#transtype =  WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID,"ddlBuySell")))

element = driver.find_element_by_xpath('//*[@id="ddlBuySell"]')
all_options = element.find_elements_by_tag_name("option")
for option in all_options:
    print("Value is: %s" % option.get_attribute("value"))
    if option.get_attribute("value") == 'P':
        option.click()


#select_fr = ui.Select(driver.find_element_by_xpath("""/html/body/form/div[4]/table/tbody/tr[3]/td[2]/select/option[2]"""))
# select_fr = Select(driver.find_element_by_id("ddlBuySell"))
'''
element = driver.find_element_by_xpath('//*[@id="ddlBuySell"]')
all_options = element.find_elements_by_tag_name("option")
for option in all_options:
    print("Value is: %s" % option.get_attribute("value"))
    if option.get_attribute("value") == 'V':
        option.click()
'''

#transtype.select_by_index(0)
#transtype.click()
# select by visible text
#transtype.select_by_visible_text('PURCHASE')

# select by value 
#transtype.select_by_value('2')

#transtype.click()
sleep(2)

transtype = Select(driver.find_element_by_xpath('//*[@id="ddlOStatus"]'))
transtype.select_by_visible_text('VALID')





submit = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, "btnSubmit")))
submit.click()
sleep(2)
# make_ready(driver)

table = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, "//table[@class='glbTableD']/tbody")))
print ('html loading done')
rows = table.find_elements(By.XPATH, "tr[@class='tblERow'] | tr[@class='tblORow']")

for row in rows:
    fields = row.find_elements(By.XPATH, "td")

    order_id = fields[3].text
    status = fields[18].text
    print("order_id :", order_id, "- Status : ",status)
    if status == "ALLOTMENT DONE":
        status = '6'
    elif status == "SENT TO RTA FOR VALIDATION":
        status = '5'
    elif status == "ORDER CANCELLED BY USER":
        status = '1'
    elif status == "PAYMENT NOT RECEIVED TILL DATE":
        status = '-1'

print(status)

print("completed")
#driver.close()


'''
#ddtopmenubar > ul > li:nth-child(6) > a
export PATH=$PATH:/home/natrayan/project/AwsProject/puppeteer/geko
'''