from flask import Flask
from flask_cors import CORS, cross_origin

app = Flask(__name__)
CORS(app)

from portfolio import dbfunc
from portfolio import jwtdecodenoverify
from portfolio import portfolio
