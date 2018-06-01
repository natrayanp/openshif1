from flask import Flask
from flask_cors import CORS, cross_origin

app = Flask(__name__)
CORS(app)

from bsestarmfapi import custcreation
from bsestarmfapi import filuploadapi
#import views
