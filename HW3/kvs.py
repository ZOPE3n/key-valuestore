import re
import os
import requests
import random
from flask import Flask, request, jsonify, make_response

app = Flask(__name__)
#Set the size limit to 1.5MB
app.config['MAX_CONTENT_LENGTH'] = 1.5 * 1024 * 1024


#A list to store the view 
view = []

#Stores the IP address:PORT number of current node
IP_PORT = None

#Initially, DB is empty
DB = {}



"""
put on a key that does not exist (say key=foo, val=bar) creates a resource at /kvs. 
put on a key that exists (say key=foo) replaces the existing val with the new val (say baz),
"""
def put(values):
    #Extract key/value
    key = values['key'] 
    val = values['value']
    #Check if key is valid
    if not isKeyValid(key):
        j = jsonify(msg='error', error='Key not valid')
        return make_response(j, 404, {'Content-Type': 'application/json'})

    # #Check if key is already in our DB
    if key in DB:
        DB[key] = val
        j = jsonify(replaced=1, msg='success', owner=IP_PORT)
        return make_response(j, 200, {'Content-Type': 'application/json'})
    else:
        for node in view:
            if node != IP_PORT:
                res = requests.get('http://' + node + '/kvs/get_key', params=values)
                if res.status_code != 404:
                    res = requests.put('http://' + node + '/kvs', params=values)
                    return make_response(res.text, res.status_code, {'Content-Type': 'application/json'})


    #Generate a random number between 0 and length of view - 1
    num = random.randint(0, len(view)-1)
    #if randomly generated node is current node, add key to DB
    #else send a PUT request to the randomly generate node to add the key to it's DB(Call add_key route)
    if view[num] == IP_PORT:
        if key in DB:
            DB[key] = val
            j = jsonify(replaced=1, msg='success', owner=IP_PORT)
            return make_response(j, 200, {'Content-Type': 'application/json'})
        DB[key] = val
        j = jsonify(replaced=0, msg='success', owner=IP_PORT)
        return make_response(j, 201, {'Content-Type': 'application/json'})
    else:
        res = requests.put('http://'+ view[num]+'/kvs/add_key', params=values) 
        return make_response(res.text, res.status_code, {'Content-Type' : 'application/json'}) 

"""
get on a key that does not exist returns a None
get on a key that exists returns the last value successfully written (via put/post) to that key
"""
def get(values):
    #Extract Key
    key = values['key']
    #Check if key is valid 
    if not isKeyValid(key):
        j = jsonify(msg='error', error='Key not valid')
        return make_response(j, 404, {'Content-Type': 'application/json'})
    #if key is in current node's DB, return the value
    if key in DB:
        j = jsonify(msg='success', value=DB[key], owner=IP_PORT)
        return make_response(j, 200, {'Content-Type': 'application/json'})

    #Ask all other nodes if they have the key in their DB
    for node in view:
        if node != IP_PORT:
            res = requests.get('http://'+node+'/kvs/get_key', params=values)
            if res.status_code != 404:
                return make_response(res.text, res.status_code, {'Content-Type' : 'application/json'}) 
    #If none of them have the key, return error
    j = jsonify(msg='error', error='key does not exist')
    return make_response(j, 404, {'Content-Type' : 'application/json'})


"""
del on a key that does not exist returns a None
del on a key that exists (say foo) deletes the resource at /kvs and returns success. 
"""
def delete(values):
    #Extract Key
    key = values['key']
    #Check if key is valid 
    if not isKeyValid(key):
        j = jsonify(msg='error', error='Key not valid')
        return make_response(j, 404, {'Content-Type': 'application/json'})

    #If key is in current node's DB, return the value
    if key in DB:
        del DB[key]
        j = jsonify(msg='success')
        return make_response(j, 200, {'Content-Type' : 'application/json'})

    #Ask all other node to delete the key if they have the key
    for node in view:
        if node != IP_PORT:
            res = requests.delete('http://'+node+'/kvs/remove_key', params=values)
            if res.status_code != 404:
                return make_response(res.text, res.status_code, {'Content-Type' : 'application/json'}) 
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



######################
#   PUBLIC METHOD    #
######################

"""
support GET, POST, PUT, DELETE method 
if current instance is not the main instance, forward the request to main instance
Also set a timer to determine if the main instance crashes
"""
@app.route('/kvs', methods=['GET', 'POST', 'PUT', 'DELETE'])
def kvs():
    values = request.values
    if request.method == 'GET':
        return get(values)
    if request.method == 'PUT' or request.method == 'POST':
        if 'value' not in values:
            return noValue() 
        return put(values)
    if request.method == 'DELETE':
        return delete(values)


"""
Return the number of item in DB of current node
"""
@app.route('/kvs/get_number_of_keys', methods=['GET', 'POST', 'PUT'])
def count():
    j = jsonify(count=len(DB))
    return make_response(j, 200, {'Content-Type': 'application/json'})




"""
View update: Handle client's add/remove nodes requests. 
"""
@app.route('/kvs/view_update', methods=['GET', 'POST', 'PUT'])
def viewUpdate():
    values = request.values
    _type = values['type']
    ip_port = values['ip_port']

    if _type == 'add':
        #Sends an add node request to all other nodes in view
        for node in view:
            if node != IP_PORT:
                requests.put('http://'+ node +'/kvs/add_node', params=values)
                requests.put('http://'+ node +'/kvs/notify_new_node', params=values) 
            else:
                share_key(ip_port)
        #Notify the new node my IP_PORT
        requests.put('http://'+ ip_port +'/kvs/accept_view', data={'ip_port':IP_PORT})   
        if ip_port not in view:
            view.append(ip_port)
        j = jsonify(msg='Success')
        return make_response(j, 200, {'Content-Type': 'application/json'})  

    else:
        if ip_port in view:
            view.remove(ip_port)
        else:
            j = jsonify(msg='error', error='node does not exist')
            return make_response(j, 404, {'Content-Type': 'application/json'})
        #Sends a remove node request to all other nodes in view
        for node in view:
            if node != IP_PORT:
                requests.put('http://'+node+'/kvs/remove_node', params=values)  

        if ip_port == IP_PORT:
            destroy()
        else:
            requests.put('http://'+ip_port+'/kvs/remove_node?ip_port='+str(IP_PORT))
            requests.put('http://'+ip_port+'/kvs/send_data?ip_port='+str(IP_PORT))

        j = jsonify(msg='Success')
        return make_response(j, 200, {'Content-Type': 'application/json'})   
      



######################
#   PRIVATE METHOD   #
######################

"""
add a key in to DB
"""
@app.route('/kvs/add_key', methods=['GET', 'POST', 'PUT', 'DELETE'])
def add_key():
    values = request.values
    key = values['key'] 
    val = values['value']
    if key in DB:
        DB[key] = val
        j = jsonify(replaced=1, msg='success', owner=IP_PORT)
        return make_response(j, 200, {'Content-Type': 'application/json'})
    DB[key] = val
    j = jsonify(replaced=0, msg='success', owner=IP_PORT)
    return make_response(j, 201, {'Content-Type': 'application/json'})


"""
Remove key from database
"""
@app.route('/kvs/remove_key', methods=['GET', 'POST', 'PUT', 'DELETE'])
def remove_key():
    del DB[list(DB.keys())[0]]
    values = request.values
    key = values['key']
    if key in DB:
        del DB[key]
        j = jsonify(msg='success')
        return make_response(j, 200, {'Content-Type' : 'application/json'})
    j = jsonify(msg='error', error='key does not exist')
    return make_response(j, 404, {'Content-Type' : 'application/json'})


"""
Get value from database
"""
@app.route('/kvs/get_key', methods=['GET', 'POST', 'PUT', 'DELETE'])
def get_key():
    values = request.values
    key = values['key'] 
    if key in DB:
        j = jsonify(msg='success', value=DB[key], owner=IP_PORT)
        return make_response(j, 200, {'Content-Type': 'application/json'})
    j = jsonify(msg='error', error='key does not exist')
    return make_response(j, 404, {'Content-Type' : 'application/json'})   


"""
Private route to handle add node requests. 
Add the node into view
"""
@app.route('/kvs/add_node', methods=['GET', 'POST', 'PUT'])
def add_node():
    values = request.values
    ip_port = values['ip_port']
    if ip_port not in view:
        view.append(ip_port)   
    share_key(ip_port)
    j = jsonify(msg='Success')
    return make_response(j, 200, {'Content-Type': 'application/json'})   
      


"""
Method to notify the new node about current view(ask every node send a view_update(ADD) request to the new node)
"""
@app.route('/kvs/notify_new_node', methods=['GET', 'POST', 'PUT'])
def notify_new_node():
    values = request.values
    ip_port = values['ip_port']
    requests.put('http://'+ ip_port +'/kvs/accept_view', data={'ip_port':IP_PORT})
    j = jsonify(msg='Success')
    return make_response(j, 200, {'Content-Type': 'application/json'})   


"""
Function to allow the added node to learn about the existing node without share keys
"""
@app.route('/kvs/accept_view', methods=['GET', 'POST', 'PUT'])
def accept_view():
    values = request.values
    ip_port = values['ip_port']
    if ip_port not in view:
        view.append(ip_port)   
    j = jsonify(msg='Success')
    return make_response(j, 200, {'Content-Type': 'application/json'})   
      


"""
Route to handle remove node requests. 
remove the node from view
"""
@app.route('/kvs/remove_node', methods=['GET', 'POST', 'PUT'])
def remove_node():
    values = request.values
    ip_port = values['ip_port']
    if ip_port in view:
        view.remove(ip_port)    
    j = jsonify(msg='Success')
    return make_response(j, 200, {'Content-Type': 'application/json'})   
      

"""
Send the keys to other nodes before deletion
"""
@app.route('/kvs/send_data', methods=['GET', 'POST', 'PUT'])
def send_data():
    if IP_PORT in view:
        view.remove(IP_PORT)
    values = request.values
    ip_port = values['ip_port']
    requests.put('http://'+view[0]+'/kvs/remove_node?ip_port='+str(ip_port))    
    for key in DB:
        requests.put('http://'+ view[0]+'/kvs?key='+str(key)+'&value='+str(DB[key]))
    requests.put('http://'+ view[0] +'/kvs/accept_view', data={'ip_port':ip_port})   
    j = jsonify(msg='Success')
    return make_response(j, 200, {'Content-Type': 'application/json'})   



"""
Method to send a put request for every key in current database to other nodes before delete current node 
"""
def destroy():
    for key in DB:
        requests.put('http://'+ view[0]+'/kvs?key='+str(key)+'&value='+str(DB[key]))


"""
Method to share keys with the new node 
"""
def share_key(ip_port):   
    #Define estimated average as (# of key in database)/(# of nodes in old view)*(# of node in new view)
    average = int((len(DB)/(len(view)))*(len(view)-1))
    #diff is the number of keys that should be sent to the new node
    diff = len(DB) - average
    keys = list(DB.keys())
    for i in range(diff):
        requests.put('http://'+ip_port+'/kvs/add_key?key='+str(keys[i])+'&value='+str(DB[keys[i]]), timeout=3)  
        del DB[keys[i]]
    return "Success"




####################################
#   PRIVATE METHOD FOR DEBUGGING   #
####################################

"""
Print the view of current node
"""
@app.route('/kvs/print_view', methods=['GET', 'POST', 'PUT'])
def print_view():
    j = jsonify(current_view=view)
    return make_response(j, 200, {'Content-Type': 'application/json'})

"""
Print the databse
"""
@app.route('/kvs/print_DB', methods=['GET', 'POST', 'PUT'])
def print_DB():
    j = jsonify(current_DB=DB)
    return make_response(j, 200, {'Content-Type': 'application/json'})

"""
Print estimated average of keys in one node 
"""
@app.route('/kvs/print_average', methods=['GET', 'POST', 'PUT'])
def print_average():
    j = jsonify(print_average=int(((len(DB)/(len(view)+1))*len(view))))
    return make_response(j, 200, {'Content-Type': 'application/json'})


"""
Print the total number of keys in cluster
"""
@app.route('/kvs/total_number_of_keys', methods=['GET', 'POST', 'PUT'])
def total_number_of_keys():
    total_count = 0
    #Send get_number_of_keys request to all other nodes 
    for node in view:
            if node != IP_PORT:
                res = requests.put('http://'+ node+ '/kvs/get_number_of_keys')
                count = res.json()['count']
                total_count += count
    total_count += len(DB)
    j = jsonify(total=total_count)
    return make_response(j, 200, {'Content-Type': 'application/json'})


#Convert the ENV variable to a list
def convert(viewString):
    if viewString != None:
        view.extend(viewString.split(','))
    else:
        view.append(IP_PORT)    



if __name__ == "__main__":
    app.debug = True
    #Extract the current IP:PORT from ENV variable
    IP_PORT = os.getenv("ip_port") 
    #Extract view from ENV variable 
    VIEW = os.getenv('VIEW')
    convert(VIEW)
    app.run(host="0.0.0.0", port=8080)
