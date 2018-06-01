from flask import Flask
from flask import request
from flask_cors import CORS, cross_origin

app = Flask(__name__)
CORS(app)

import portfolio
import mforder
import mfsiporder
import fund
import jwtdecodenoverify as jwtnoverify



@app.after_request
def after_request(response):
    userid,entityid=' '*2
    # get the request object somehow
    if request.method!='OPTIONS':
        if jwtnoverify.validatetoken(request):
            userid,entityid=jwtnoverify.validatetoken(request)
        print("inside after requetst end ----------------------------------------------------------------------------------")
        print(request.content_length)
        print(userid)
        print(entityid)
        
        
        print (response.content_length)
        print("inside after requetst end ----------------------------------------------------------------------------------")
    
    return response
