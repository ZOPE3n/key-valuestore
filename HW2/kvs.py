import re
import os
import requests
from flask import Flask, request, jsonify, make_response

app = Flask(__name__)
#Set the size limit to 1.5MB
app.config['MAX_CONTENT_LENGTH'] = 1.5 * 1024 * 1024

#Initially, DB is empty
DB = {}

#Global variable which stores if the instance is main instance
isMainInstance = True

"""
put on a key that does not exist (say key=foo, val=bar) creates a resource at /kvs. 
put on a key that exists (say key=foo) replaces the existing val with the new val (say baz),
"""
def put(key, val):
    if not isKeyValid(key):
        j = jsonify(msg='error', error='Key not valid')
        return make_response(j, 404, {'Content-Type': 'application/json'})
    if key in DB:
        DB[key] = val
        j = jsonify(replaced=1, msg='success')
        return make_response(j, 200, {'Content-Type': 'application/json'})
    DB[key] = val
    j = jsonify(replaced=0, msg='success')
    return make_response(j, 201, {'Content-Type': 'application/json'})



"""
get on a key that does not exist returns a None
get on a key that exists returns the last value successfully written (via put/post) to that key
"""
def get(key):
    if not isKeyValid(key):
        j = jsonify(msg='error', error='Key not valid')
        return make_response(j, 404, {'Content-Type': 'application/json'})
    if key in DB:
        j = jsonify(msg='success', value=DB[key])
        return make_response(j, 200, {'Content-Type': 'application/json'})
    j = jsonify(msg='error', error='key does not exist')
    return make_response(j, 404, {'Content-Type' : 'application/json'})


"""
del on a key that does not exist returns a None
del on a key that exists (say foo) deletes the resource at /kvs and returns success. 
"""
def delete(key):
    if not isKeyValid(key):
        j = jsonify(msg='error', error='Key not valid')
        return make_response(j, 404, {'Content-Type': 'application/json'})
    if key in DB:
        del DB[key]
        j = jsonify(msg='success')
        return make_response(j, 200, {'Content-Type' : 'application/json'})
    j = jsonify(msg='error', error='key does not exist')
    return make_response(j, 404, {'Content-Type' : 'application/json'})


"""
Time out function. if the main instance does not respond in three seconds, we suspect the main instance is down.
"""
def timeOut():
    j = jsonify(msg='error', error='service is not available')
    return make_response(j, 404, {'Content-Type' : 'application/json'})


"""
If the client submited a put/post request but didn't give a value, return an error message 
"""
def noValue():
    j = jsonify(msg='error', error='No value provided')
    return make_response(j, 404, {'Content-Type' : 'application/json'})

"""
key should be in charset: [a-zA-Z0-9_] i.e. Alphanumeric including underscore, and case-sensitive
and the size of key should be 1 to 250 characters

"""
def isKeyValid(key):
    if re.match(r'^[A-Za-z0-9_]+$', key):
        return 0 < len(key) < 251
    return False

"""
support GET, POST, PUT, DELETE method 
if current instance is not the main instance, forward the request to main instance
Also set a timer to determine if the main instance crashes
"""
@app.route('/kvs', methods=['GET', 'POST', 'PUT', 'DELETE'])
def kvs():
    values = request.values
    if request.method == 'GET':
        if isMainInstance:
            key = values['key']
            return get(key)
        else:
            try:
                res = requests.get('http://'+MAINIP+'/kvs', params=values, timeout=3) 
                return make_response(res.text, res.status_code, {'Content-Type' : 'application/json'}) 
            except requests.exceptions.Timeout:
                return timeOut()


    if request.method == 'PUT' or request.method == 'POST':
        if 'value' not in values:
            return noValue() 
        if isMainInstance:
            key = values['key'] 
            val = values['value']
            return put(key, val)
        else:
            try:
                res = requests.put('http://'+MAINIP+'/kvs', params=values, timeout=3) 
                return make_response(res.text, res.status_code, {'Content-Type' : 'application/json'}) 
            except requests.exceptions.Timeout:
                return timeOut()

    if request.method == 'DELETE':
        if isMainInstance:
            key = values['key']
            return delete(key)
        else:
            try:
                res = requests.delete('http://'+MAINIP+'/kvs', params=values, timeout=3)  
                return make_response(res.text, res.status_code, {'Content-Type' : 'application/json'})       
            except requests.exceptions.Timeout:
                return timeOut()
           

if __name__ == "__main__":
    app.debug = True
    #Get the Main IP address(if the current instance is the main instance, MAINIP is set to None)
    MAINIP = os.getenv('MAINIP')
    if MAINIP != None:
        isMainInstance = False
    app.run(host="0.0.0.0", port=8080)

