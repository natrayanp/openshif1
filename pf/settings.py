'''
Author: Natrayan
Description: All settings necessary to transact in mutual funds on BSEStar using its SOAP API
'''

'''
toggle for whether BSEStar's live API is being used or testing
0 = testing; 1 = live
'''
LIVE = 0    


'''
AUTH values for BSEStar
for all following- the 1st element is value for testing environment, the 2nd for live
'''

# assigned by BSEStar
MEMBERID = ['17123', '17123']
USERID = ['1712301', '1712301'] 

# set on your own
PASSWORD = ['@654321', '@123456']
## unique passkey that should ideally be freshly generated random string for every transaction
## but i have used a constant here to simplofy things
PASSKEY = ['0123456789', '9958788281']



'''
SOAP API endpoints for BSEStar
may change from time to time, so confirm with BSEStar
'''
# url for BSEStar order entry webservice which is used to create or cancel transactions
WSDL_ORDER_URL = [
    'http://bsestarmfdemo.bseindia.com/MFOrderEntry/MFOrder.svc?singleWsdl', 
    'http://www.bsestarmf.in/MFOrderEntry/MFOrder.svc?singleWsdl'
]
SVC_ORDER_URL = [
    'http://bsestarmfdemo.bseindia.com/MFOrderEntry/MFOrder.svc', 
    'http://www.bsestarmf.in/MFOrderEntry/MFOrder.svc'
]
METHOD_ORDER_URL = [
    'http://bsestarmfdemo.bseindia.com/MFOrderEntry/',
    'http://bsestarmf.in/MFOrderEntry/'
]
# url for BSEStar upload webservice which is used to do everything besides creating/cancelling 
# transactions like create user (transactions on bse can only be placed after this) etc
WSDL_UPLOAD_URL = [
    'http://bsestarmfdemo.bseindia.com/MFUploadService/MFUploadService.svc?singleWsdl', 
    'http://www.bsestarmf.in/StarMFWebService/StarMFWebService.svc?singleWsdl'
]
SVC_UPLOAD_URL = [
    'http://bsestarmfdemo.bseindia.com/MFUploadService/MFUploadService.svc/Basic', 
    'http://www.bsestarmf.in/StarMFWebService/StarMFWebService.svc/Basic'
]
METHOD_UPLOAD_URL = [
    'http://bsestarmfdemo.bseindia.com/2016/01/IMFUploadService/',
    'http://www.bsestarmf.in/2016/01/IStarMFWebService/'
]
#url for BSEStar AOF upload
WSDL_FILEUP_URL = [
    'http://bsestarmfdemo.bseindia.com/StarMFFileUploadService/StarMFFileUploadService.svc?singleWsdl' 
    #'http://www.bsestarmf.in/StarMFWebService/StarMFWebService.svc?singleWsdl'
]
SVC_FILEUP_URL = [
    'http://bsestarmfdemo.bseindia.com/StarMFFileUploadService/StarMFFileUploadService.svc/Basic'
    #'http://www.bsestarmf.in/StarMFWebService/StarMFWebService.svc/Basic'
]
METHOD_FILEUP_URL = [
    'http://tempuri.org/IStarMFFileUploadService/'
    #'http://www.bsestarmf.in/2016/01/IStarMFWebService/'
]
#url for BSEStar payment link
WSDL_PAYLNK_URL = [
    'http://bsestarmfdemo.bseindia.com/StarMFPaymentGatewayService/StarMFPaymentGatewayService.svc?singleWsdl' 
    #'http://www.bsestarmf.in/StarMFWebService/StarMFWebService.svc?singleWsdl'
]
SVC_PAYLNK_URL = [
    'http://bsestarmfdemo.bseindia.com/StarMFPaymentGatewayService/StarMFPaymentGatewayService.svc/Basic'
    #'http://www.bsestarmf.in/StarMFWebService/StarMFWebService.svc/Basic'
]
METHOD_PAYLNK_URL = [
    'http://tempuri.org/IStarMFPaymentGatewayService/'
    #'http://www.bsestarmf.in/2016/01/IStarMFWebService/'
]


BSESTAR_LOGIN_PG = ["https://bsestarmfdemo.bseindia.com",""]

BSESTAR_ORDER_STATUS_PG = ["https://bsestarmfdemo.bseindia.com/RptOrderStatusReportNew.aspx",""]