from flask import Flask
from flask_cors import CORS, cross_origin

app = Flask(__name__)
CORS(app)

from userregistration import dbfunc
from userregistration import registrationfatca
from userregistration import jwtdecodenoverify
from userregistration import bankifsc
from userregistration import fileupload