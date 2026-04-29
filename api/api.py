from os import access
from flask import request, Flask, jsonify
from main import MovieService
from pathlib import Path
from dotenv import load_dotenv
from datetime import datetime, timezone, timedelta
from constant import constansts as cons
import secrets
import bcrypt
import jwt
import os

os.chdir(Path(__file__).parent.parent)
load_dotenv()
secret_key=os.environ.get("SECRET_KEY")
app=Flask(__name__)
app_service=MovieService()
USERS={"admin": b'$2b$12$Gy9z3lihHck5fCP4dAJMB.JzryhwuExZgHJ49GgynNW5t88hEuOLa'} # noqa
REF_TOKENS={}
PUBLIC_PATHS=cons.PUBLIC_PATHS

@app.before_request
def before_request():
    """Authorization check before hitting endpoints of API"""
    if request.path not in PUBLIC_PATHS: #JWT check, request not hitting login or refresh endpoints
        token=request.headers.get("Authorization") #Entire authz token with 'Bearer' in it
        if token is None:
            return jsonify({'status': 'error', 'message': 'Token is missing'}), 401
        else:
            token=token.split(' ')
            if len(token)<2: return jsonify({'status': 'error', 'message': 'Token is invalid'}), 401
            else: token=token[1]
            try: jwt.decode(token, secret_key, algorithms=['HS256'])
            except jwt.ExpiredSignatureError: return jsonify({'status': 'error', 'message': 'Token has expired'}), 401
            except jwt.InvalidTokenError: return jsonify({'status': 'error', 'message': 'Token is invalid'}), 401
    return None

@app.route('/login', methods=["POST"])
def login():
    text=request.get_json(force=True)
    userid=text.get('id')
    pw=text.get("pw")
    if userid not in USERS:
        return jsonify({'status': 'error', 'message': 'User not found'}), 401
    else:
        pw=pw.encode('UTF-8')
        if bcrypt.checkpw(pw, USERS[userid]):
            ref_token=secrets.token_hex(32)
            REF_TOKENS[ref_token]=userid, datetime.now(timezone.utc)+timedelta(days=30)
            access_token=jwt.encode(payload={'id': userid, 'exp': datetime.now(timezone.utc)+timedelta(minutes=15), 'role': 'admin'}, key=secret_key, algorithm='HS256')
            return jsonify({'access_token': access_token, 'refresh_token': ref_token, 'id': userid}), 200
        else:
            return jsonify({'status': 'error', 'message': 'Password is incorrect'}), 401

@app.route("/refresh", methods=['POST'])
def refresh():
    """Acquire new access token endpoint"""
    text=request.get_json(force=True)
    token=text.get('refresh_token')
    if token in REF_TOKENS:
        userid=REF_TOKENS[token][0]
        if REF_TOKENS[token][1]<=datetime.now(timezone.utc):
            return jsonify({'status': 'error', 'message': 'Token has expired'}), 401
        else:
            access_token=jwt.encode(payload={'id': userid, 'exp': datetime.now(timezone.utc)+timedelta(minutes=15), 'role': 'admin'}, key=secret_key, algorithm='HS256')
            return jsonify({'access_token': access_token, 'refresh_token': token})
    else:
        return jsonify({'status': 'error', 'message': 'Token is invalid'}), 401

@app.route("/recommendations", methods=['POST'])
def service():
    text = request.get_json(force=True)
    filter_tools = text['filter_tools']
    response=app_service.recommend(filter_tools)
    response=jsonify(response)
    return response

@app.route("/health", methods=['GET'])
def health():
    return jsonify({'status': 'ok'})

if __name__ == "__main__":
    app.run(debug=False)