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

#Customer creation api which contacts BSE for user creation
BSESTAR_USERCREATION_URL=['https://yr6dazt6aa.execute-api.ap-southeast-1.amazonaws.com/dev/custcreationapi','http://192.168.1.27:8000/custcreationapi']
BSESTAR_AOFUPLOAD_URL=['https://yr6dazt6aa.execute-api.ap-southeast-1.amazonaws.com/dev/fileuploadapi','http://192.168.1.27:8000/custcreationapi']