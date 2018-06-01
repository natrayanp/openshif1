from flask import Flask, request, make_response, jsonify, Response
from flask_cors import CORS, cross_origin
import requests


app = Flask(__name__)
CORS(app)


from loginservices import loginsignup
from loginservices import dbfunc