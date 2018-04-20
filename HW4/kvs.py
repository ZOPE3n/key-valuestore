import re
import os
from time import sleep, time
import random
import json
import requests
import threading
from flask import Flask, request, jsonify, make_response

app = Flask(__name__)
#Set the size limit to 1.5MB
app.config['MAX_CONTENT_LENGTH'] = 1.5 * 1024 * 1024

######################
#  GLOBAL VARIABLES  #
######################
"""
A dictionary to store the view and it's partition_id(the nodes in first partition have id 0)
(e.g.{'10.0.0.21:8080': 0, '10.0.0.22:8080': 0, '10.0.0.23:8080': 1, '10.0.0.24:8080': 1})
"""
view = {}


"""
Stores the IP address:PORT number of current node(e.g. 10.0.0.21:8080)
"""
IP_PORT = None


"""
DB is our database. each key/value pair is stored as {key:[value, causal_payload, timestamp]} 
NOTE: 
Causal payload(vector clock) is used establish causality between events. 
e.g. [1,0,0,0] (NOTE: the number of integer in the list is the number of replicas)
Timestamp is the wall clock time on the replica that first processed the write.
"""
DB = {}


"""
An integer to store the maximum number of replicas in a cluster
"""
NUMBER_OF_REPLICAS = None


"""
A list to store the partiton members
"""
PARTITION_MEMBERS = None


######################
#   PUBLIC ROUTE     #
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
            return no_value() 
        return put(values)
    if request.method == 'DELETE':
        return delete(values)


"""
Return the number of item in DB of current node
"""
@app.route('/kvs/get_number_of_keys', methods=['GET', 'POST', 'PUT'])
def count():
    j = jsonify(count=len(DB))
    return make_response(j, 200, {'Content-Type':'application/json'})


"""
View update: Handle client's add/remove nodes requests. 
There are two cases for add a node
1.If after adding this node we have to create another partition, we need to make sure other partition share 
  some keys to this node
2.Otherwise, we just add this node to all existing nodes' view
There are also two case for deletion:
1.If that partition has more than 1 replicas, just remove the node from every other nodes' view
2.If this node is the only node in that partition, we need to make sure this node send out 
  all its node before we respond to the client
"""
@app.route('/kvs/view_update', methods=['GET', 'POST', 'PUT'])
def viewUpdate():
    values = request.values
    _type = values['type']
    ip_port = values['ip_port']

    if _type == 'add':
        flag = (len(view) % NUMBER_OF_REPLICAS == 0)
        #Sends an add node request to all other nodes in view
        for node, partition_id in view.items():
            if node != IP_PORT:
                requests.put('http://'+ node +'/kvs/add_node', params=values, timeout=10)
        if ip_port not in view:
            view[ip_port] = int(len(view)/NUMBER_OF_REPLICAS)

        #send my view to the new node 
        requests.put('http://'+ ip_port +'/kvs/accept_view', data=view, timeout=10)   
        if flag and len(DB) > 10:
            share_key(ip_port)
        number_of_partitions = int(len(view)/NUMBER_OF_REPLICAS)+(len(view)%NUMBER_OF_REPLICAS!=0) 
        sleep(3)
        j = jsonify(msg='success', partition_id=view[ip_port], number_of_partitions=number_of_partitions)
        return make_response(j, 200, {'Content-Type':'application/json'})  
    else:
        #count is the number of replicas in the partition of the node to be deleted
        count = len(get_members(view[ip_port]))
        _id = view[IP_PORT]
        if ip_port in view:
            del view[ip_port]
        else:
            j = jsonify(msg='error', error='node does not exist')
            return make_response(j, 404, {'Content-Type':'application/json'})
        #Sends a remove node request to all other nodes in view
        for node in view:
            if node != IP_PORT:
                requests.put('http://'+node+'/kvs/remove_node', params=values, timeout=10)  
        #If we are deleting the only replica in that partition
        if count == 1:
            res = requests.put('http://'+ip_port+'/kvs/send_data?ip_port='+str(IP_PORT)+'&partition_id='+str(_id), timeout=10)
        number_of_partitions = int(len(view)/NUMBER_OF_REPLICAS)+(len(view)%NUMBER_OF_REPLICAS!=0)
        sleep(2)
        j = jsonify(msg='success', number_of_partitions=number_of_partitions)
        return make_response(j, 200, {'Content-Type':'application/json'})   
      


######################
#   PATITION INFO    #
###################### 
"""
Return the members in a partition give a partition id
"""
@app.route('/kvs/get_partition_members', methods=['GET', 'POST', 'PUT'])
def get_partition_members():
    values = request.values
    partition_id = values['partition_id']
    j = jsonify(msg='Success', partition_members=get_members(int(partition_id)))
    return make_response(j, 200, {'Content-Type':'application/json'})


"""
Returns the partition id where the node belongs to
"""
@app.route('/kvs/get_partition_id', methods=['GET'])
def get_partition_id():
    j = jsonify(msg='Success', partition_id=view[IP_PORT])
    return make_response(j, 200, {'Content-Type':'application/json'})


"""
Return the partition
"""
@app.route('/kvs/get_all_partition_ids', methods=['GET'])
def get_all_partition_ids():
    number_of_partitions = len(view)/NUMBER_OF_REPLICAS
    if len(view)%NUMBER_OF_REPLICAS != 0:
        number_of_partitions += 1
    partition_id_list_ = [x for x in range(0, int(number_of_partitions))]
    j = jsonify(msg='Success', partition_id_list=partition_id_list_)
    return make_response(j, 200, {'Content-Type':'application/json'})


def get_members(partition_id):
    members = [k for k, v in view.items() if v == partition_id]
    members.sort()
    return members



######################
#   PRIVATE METHOD   #
######################

"""
An example of put request: "/kvs?causal_payload=<payload>&key=<keyname>&value=<val>"
put on a key that does not exist (say key=foo, val=bar) creates a resource at /kvs. 
put on a key that exists (say key=foo) replaces the existing val with the new val (say baz)
"""
def put(values):
    #Extract key&value&causal_payload
    key = values['key'] 
    val = values['value']

    #if causal payload was not supplied, set it to all zeros
    causal_payload = handle_empty_causal_payload(values['causal_payload'])

    #Check if key is valid
    if not is_key_valid(key):
        j = jsonify(msg='error', error='Key not valid')
        return make_response(j, 404, {'Content-Type':'application/json'})

    """
    NEED TO HANDLE WHEN CAUSALPAYLOAD IS GIVEN BUT THE KEY ALREAYD EXIST
    """
    #Check if key is already in our DB
    if key in DB:
        value = [val, increment_causal_payload(DB[key][1], PARTITION_MEMBERS.index(IP_PORT)), time()]
        DB[key] = value
        # broadcast_write(key, value)
        j = jsonify(msg='success', partition_id=view[IP_PORT], causal_payload=str(DB[key][1]), timestamp=str(DB[key][2]))
        return make_response(j, 200, {'Content-Type':'application/json'})
    else:
        for node in view:
            if node != IP_PORT:
                try:
                    res = requests.get('http://' + node + '/kvs/get_key', params=values, timeout=0.5)
                    #if the status code is 404, the key doesn't exist in that node's database. Otherwise, send(redirect) the put request
                    if res.status_code != 404:
                        res = requests.put('http://' + node + '/kvs', params=values, timeout=0.5)
                        return make_response(res.text, res.status_code, {'Content-Type':'application/json'})
                except requests.exceptions.Timeout:
                    pass
    num = random.randint(0, int(len(view)/NUMBER_OF_REPLICAS)-(len(view)%NUMBER_OF_REPLICAS==0))
    #if randomly generated node is current node, add key to DB
    #else send a PUT request to the randomly generate node to add the key to it's DB(Call add_key route)
    if view[IP_PORT] == num:
        DB[key] = [val, increment_causal_payload(causal_payload, PARTITION_MEMBERS.index(IP_PORT)), time()]
        j = jsonify(msg='success', partition_id=view[IP_PORT], causal_payload=str(DB[key][1]), timestamp=str(DB[key][2]))
        return make_response(j, 201, {'Content-Type':'application/json'})
    else:
        res = requests.put('http://'+ get_members(num)[0]+'/kvs/add_key', params=values, timeout=0.5) 
        return make_response(res.text, res.status_code, {'Content-Type':'application/json'}) 


"""
An example of get request: "/kvs?key=<keyname>&causal_payload=<payload>"
get on a key that does not exist returns a None
get on a key that exists returns the last value successfully written (via put/post) to that key
"""
def get(values):
    #Extract Key&causal_payload
    key = values['key']
    causal_payload = handle_empty_causal_payload(values['causal_payload'])
    #Check if key is valid 
    if not is_key_valid(key):
        j = jsonify(msg='error', error='Key not valid')
        return make_response(j, 404, {'Content-Type':'application/json'})
    #if key is in current node's DB, return the value
    if key in DB:
        j = jsonify(msg='success', value=str(DB[key][0]), partition_id=view[IP_PORT], causal_payload=str(DB[key][1]), timestamp=str(DB[key][2]))
        return make_response(j, 200, {'Content-Type':'application/json'})

    #Ask all other nodes if they have the key in their DB
    for node in view:
        if node != IP_PORT:
            try:
                res = requests.get('http://'+node+'/kvs/get_key', params=values, timeout=0.5)
                if res.status_code != 404:
                    return make_response(res.text, res.status_code, {'Content-Type':'application/json'}) 
            except requests.exceptions.Timeout:
                pass
            
    #If none of them have the key, return error
    j = jsonify(msg='error', error='key does not exist')
    return make_response(j, 404, {'Content-Type':'application/json'})


# """
# del on a key that does not exist returns a None
# del on a key that exists (say foo) deletes the resource at /kvs and returns success. 
# """
def delete(values):
    return None
#     #Extract Key
#     key = values['key']
#     #Check if key is valid 
#     if not is_key_valid(key):
#         j = jsonify(msg='error', error='Key not valid')
#         return make_response(j, 404, {'Content-Type':'application/json'})

#     #If key is in current node's DB, return the value
#     if key in DB:
#         del DB[key]
#         j = jsonify(msg='success')
#         return make_response(j, 200, {'Content-Type':'application/json'})

#     #Ask all other node to delete the key if they have the key
#     for node in view:
#         if node != IP_PORT:
#             res = requests.delete('http://'+node+'/kvs/remove_key', params=values)
#             if res.status_code != 404:
#                 return make_response(res.text, res.status_code, {'Content-Type':'application/json'}) 
#     j = jsonify(msg='error', error='key does not exist')
#     return make_response(j, 404, {'Content-Type':'application/json'})


"""
If the client submited a put/post request but didn't give a value, return an error message 
"""
def no_value():
    j = jsonify(msg='error', error='No value provided')
    return make_response(j, 404, {'Content-Type':'application/json'})


"""
key should be in charset: [a-zA-Z0-9_] i.e. Alphanumeric including underscore, and case-sensitive
and the size of key should be 1 to 250 characters

"""
def is_key_valid(key):
    if re.match(r'^[A-Za-z0-9_]+$', key):
        return 0 < len(key) < 251
    return False


# """
# When the key value store is not available at this moment, return error message
# """
# def not_available()
#     j = jsonify(msg='error', error='key value store is not available')
#     return make_response(j, 404, {'Content-Type' : 'application/json'})







######################
#   PRIVATE ROUTE    #
######################

"""
add a key in to DB
"""
@app.route('/kvs/add_key', methods=['GET', 'POST', 'PUT', 'DELETE'])
def add_key():
    values = request.values
    key = values['key'] 
    val = values['value']
    causal_payload = handle_empty_causal_payload(values['causal_payload'])    
    value = [val, increment_causal_payload(causal_payload, PARTITION_MEMBERS.index(IP_PORT)), time()]
    if key in DB:
        DB[key] = value
        j = jsonify(msg='success', partition_id=view[IP_PORT], causal_payload=str(DB[key][1]), timestamp=str(DB[key][2]))
        return make_response(j, 200, {'Content-Type':'application/json'})
    DB[key] = value
    j = jsonify(msg='success', partition_id=view[IP_PORT], causal_payload=str(DB[key][1]), timestamp=str(DB[key][2]))
    return make_response(j, 201, {'Content-Type':'application/json'})


"""
Remove key from database
"""
@app.route('/kvs/remove_key', methods=['GET', 'POST', 'PUT', 'DELETE'])
def remove_key():
    values = request.values
    key = values['key']
    if key in DB:
        del DB[key]
        j = jsonify(msg='success')
        return make_response(j, 200, {'Content-Type':'application/json'})
    j = jsonify(msg='error', error='key does not exist')
    return make_response(j, 404, {'Content-Type':'application/json'})

@app.route('/kvs/send', methods=['GET', 'POST', 'PUT', 'DELETE'])
def send():
    res = requests.get('http://localhost:8086/kvs/print_view')
    return make_response(res.text, res.status_code, {'Content-Type':'application/json'})
    # j = jsonify(msg='success', partition_id=view[IP_PORT], causal_payload=str(DB[key][1]), timestamp=str(DB[key][2]))
    # return make_response(j, 201, {'Content-Type':'application/json'})



"""
This route is used by the node which receives client put request to ask other nodes if they have the key in their DB
get_key returns success and the correspond causal_payload to 
"""
@app.route('/kvs/get_key', methods=['GET', 'POST', 'PUT', 'DELETE'])
def get_key():
    values = request.values
    key = values['key'] 
    if key not in DB:
        j = jsonify(msg='error', error='key does not exist')
        return make_response(j, 404, {'Content-Type':'application/json'})   
    causal_payload = values['causal_payload']   

    j = jsonify(msg='success', value=str(DB[key][0]), partition_id=view[IP_PORT], causal_payload=str(DB[key][1]), timestamp=str(DB[key][2]))
    return make_response(j, 200, {'Content-Type':'application/json'})
    # if compare_casual_payloads(causal_payload, DB[key][1]) == 1:
    #     vector_clock = increment_causal_payload(causal_payload, 0)
    # else:
    #     vector_clock = increment_causal_payload(DB[key][1], 0)
    # j = jsonify(msg='success', value=DB[key], owner=IP_PORT, causal_payload=vector_clock)
    # return make_response(j, 200, {'Content-Type':'application/json'})
   

"""
Private route to handle add node requests. 
Add the node into view
"""
@app.route('/kvs/add_node', methods=['GET', 'POST', 'PUT'])
def add_node():
    values = request.values
    ip_port = values['ip_port']
    if ip_port not in view:
        view[ip_port] = int(len(view)/NUMBER_OF_REPLICAS)
    j = jsonify(msg='Success')
    return make_response(j, 200, {'Content-Type':'application/json'})   



"""
Function to allow the added node to learn about the existing node without share keys
"""
@app.route('/kvs/accept_view', methods=['GET', 'POST', 'PUT'])
def accept_view():
    values = request.form.to_dict()
    for key, value in values.items():
        view[key] = int(value)
    global PARTITION_MEMBERS
    PARTITION_MEMBERS = get_members(view[IP_PORT])
    j = jsonify(msg='Success')
    return make_response(j, 200, {'Content-Type':'application/json'})   


"""
Route to handle remove node requests. 
remove the node from view
"""
@app.route('/kvs/remove_node', methods=['GET', 'POST', 'PUT'])
def remove_node():
    values = request.values
    ip_port = values['ip_port']
    if ip_port in view:
        del view[ip_port]   
    j = jsonify(msg='Success')
    return make_response(j, 200, {'Content-Type':'application/json'})   
      

"""
Send the keys to other nodes before deletion
"""
@app.route('/kvs/send_data', methods=['GET', 'POST', 'PUT'])
def send_data():
    values = request.values
    ip_port = values['ip_port']
    partition_id = int(values['partition_id'])
    del view[IP_PORT]
    del view[ip_port]
    for key in DB:
        num = random.randint(smallest_partition(), smallest_partition()+int(len(view)/NUMBER_OF_REPLICAS)-(len(view)%NUMBER_OF_REPLICAS == 0))
        requests.put('http://'+get_members(num)[0]+'/kvs/add_key?key='+str(key)+'&value='+str(DB[key][0])+'&causal_payload=', timeout=3)  
    j = jsonify(msg='Success')
    return make_response(j, 200, {'Content-Type':'application/json'})   


"""
Method to send a put request for every key in current database to other nodes before delete current node 
"""
def destroy():
    for key in DB:
        requests.put('http://'+ view[0]+'/kvs?key='+str(key)+'&value='+str(DB[key]), timeout=3)


def smallest_partition():
    smallest_id = 99999
    for node in view:
        if view[node] < smallest_id:
            smallest_id = view[node]
    return smallest_id


"""
Method to share keys with the new node 
"""
def share_key(ip_port):   
    # number_of_partitions = len(view)/NUMBER_OF_REPLICAS
    # if len(view)%NUMBER_OF_REPLICAS != 0:
    #     number_of_partitions += 1
    # #Define estimated average as (# of key in database)/(# of nodes in old view)*(# of node in new view)
    # average = int((len(DB)/(number_of_partitions))*(number_of_partitions-1))
    # #diff is the number of keys that should be sent to the new node
    # diff = len(DB) - average
    diff = int(len(DB)/2)
    keys = list(DB.keys())
    for i in range(diff+1):
        requests.put('http://'+ip_port+'/kvs/add_key?key='+str(keys[i])+'&value='+str(DB[keys[i]][0])+'&causal_payload=', timeout=3)
        del DB[keys[i]]
        members = get_members(view[IP_PORT])
        members.remove(IP_PORT)
        for node in members:
            requests.put('http://'+node+'/kvs/remove_key?key='+str(keys[i]), timeout=3)
    return "Success"


####################################
#   PRIVATE METHOD FOR DEBUGGING   #
####################################

"""
Print the view of current node
"""
@app.route('/kvs/print_view', methods=['GET'])
def print_view():
    j = jsonify(current_view=view)
    return make_response(j, 200, {'Content-Type':'application/json'})


@app.route('/kvs/total_number_of_keys', methods=['GET', 'POST', 'PUT'])
def total_number_of_keys():
    total_count = []
    #Send get_number_of_keys request to all other nodes 
    for node in view:
            if node != IP_PORT:
                res = requests.put('http://'+ node+ '/kvs/get_number_of_keys', timeout=3)
                count = res.json()['count']
                if count not in total_count:
                    total_count.append(count)
    if len(DB) not in total_count:
        total_count.append(len(DB))
    j = jsonify(total=sum(total_count))
    return make_response(j, 200, {'Content-Type': 'application/json'})


"""
Print the number of keys the current node has
"""
@app.route('/kvs/get_number_of_keys', methods=['GET'])
def get_number_of_keys():
    j = jsonify(count=len(DB))
    return make_response(j, 200, {'Content-Type':'application/json'})


"""
Print the databse
"""
@app.route('/kvs/print_DB', methods=['GET'])
def print_DB():
    j = jsonify(current_DB=DB)
    return make_response(j, 200, {'Content-Type':'application/json'})


"""
Print estimated average of keys in one node 
"""
@app.route('/kvs/print_average', methods=['GET'])
def print_average():
    j = jsonify(print_average=int(((len(DB)/(len(view)+1))*len(view))))
    return make_response(j, 200, {'Content-Type':'application/json'})


"""
Print PARTITION_MEMBERS
"""
@app.route('/kvs/members', methods=['GET'])
def members():
    j = jsonify(members=PARTITION_MEMBERS)
    return make_response(j, 200, {'Content-Type':'application/json'})



"""
Convert the ENV variable to a list
e.g. if 
"""
def construct_initial_view(viewString):
    if viewString == None or len(viewString) < 2:
        return
    view_list = viewString.split(',')
    #partition id start from 0
    partition_id = 0
    for i, node in enumerate(view_list):
        view[node] = partition_id
        if (i+1)%NUMBER_OF_REPLICAS == 0:
            partition_id += 1
    global PARTITION_MEMBERS
    PARTITION_MEMBERS = get_members(view[IP_PORT])


"""
Merger two vector clocks(take the point-wise max)
e.g. [2.2.0.0]+[1.3.0.1] -> [2.3.0.1]
"""
def merge(causal_payload_1, causal_payload_2):
    vector_clock_1 = list(map(int, causal_payload_1.split('.')))
    vector_clock_2 = list(map(int, causal_payload_2.split('.')))
    new_vector_clock = [max(clock) for clock in zip(vector_clock_1, vector_clock_2)]
    string_payload = ''.join(str(clock) + '.' for clock in new_vector_clock)
    return string_payload[:-1]  # remove the extra dot


"""
Handle empty causal payload(set the causal payload to all zeros when the causal payload was not supplied by the client)
"""
def handle_empty_causal_payload(causal_payload):
    if len(causal_payload) < 3:
        vector_clock = [0 for x in range(NUMBER_OF_REPLICAS)]
        string_payload = ''.join(str(clock)+'.' for clock in vector_clock)
        return string_payload[:-1]
    return causal_payload[1:-1]


"""
This function increment the clock of a replica in a vector clock
e.g. incrementCausalPayload('1.0.0.4', 2) --> return 1.0.1.4 
"""
def increment_causal_payload(causal_payload, position):
    vector_clock = causal_payload.split('.')
    vector_clock[position] = str(int(vector_clock[position])+1)
    string_payload = ''.join(str(clock)+'.' for clock in vector_clock)
    return string_payload[:-1]#remove the extra dot


"""
Compare two vector clocks
NOTE: VC1>VC2 iff for all i, VC1[i]>=VC2[i] and there exists a j, VC1[j]>VC2[j](same for VC1<VC2)
Otherwise, they are said to be concurrent
if VC1 wins return 0, and if VC2 wins return 1
if they are concurrent return 2
"""
def compare_casual_payloads(causal_payload_1, causal_payload_2):
    #convert the casual payload into list of integers
    vector_clock_1 = list(map(int, causal_payload_1.split('.')))
    vector_clock_2 = list(map(int, causal_payload_2.split('.')))
    flag1 = False
    flag2 = False
    for i in range(len(vector_clock_1)):
        if vector_clock_1[i] > vector_clock_2[i]:
            flag1 = True
        elif vector_clock_1[i] < vector_clock_2[i]:
            flag2 = True
    if flag1 == flag2:
        return 2
    elif flag1:
        return 0
    elif flag2:
        return 1

################################
#   Backgroud process(Sync)    #
################################

"""
Implement anti-entropy protocol
"""
@app.before_first_request
def activate_job():
    def background_job():
        sleep(3)
        while True:
            members = get_members(view[IP_PORT])
            for node in members:
                if node != IP_PORT:
                    try:
                        requests.put("http://"+node+"/kvs/sync", json=json.dumps(DB), timeout=0.8)
                    except requests.exceptions.Timeout:
                        pass
            sleep(3)
    thread = threading.Thread(target=background_job)
    thread.start()

"""
Route to synchronize database
"""
@app.route('/kvs/sync', methods=['GET', 'PUT', 'POST'])
def sync():
    DB2 = json.loads(request.get_json())
    sync_database(DB2)
    j = jsonify(current_DB=DB)
    return make_response(j, 200, {'Content-Type': 'application/json'})


def sync_database(replica):
    for key in replica.keys():
        if key not in DB:
            DB[key] = replica[key]
        DB[key] = choose_value(DB[key], replica[key])

"""
This function will return the value with either higher vector clock or timestamp
"""
def choose_value(values1, values2):
    causal_payload1 = values1[1]
    causal_payload2 = values2[1]
    timestamp1 = values1[2]
    timestamp2 = values2[2]
    if compare_casual_payloads(causal_payload1, causal_payload2) == 0:
        return values1
    elif compare_casual_payloads(causal_payload1, causal_payload2) == 1:
        return values2

    if timestamp1 > timestamp2:
        return values1
    else:
        return values2


"""
A functiont to handle the empty view problem(i.e. delete the empty key in DB)
"""
def handle_empty_view():
    if '' in view:
        del view[''] 


if __name__ == "__main__":
    app.debug = True
    #Extract the current IP:PORT from ENV variable
    IP_PORT = os.getenv("ip_port") 
    #Extract view from ENV variable 
    VIEW = os.getenv('VIEW')
    #Extract the number of replicas per partition
    NUMBER_OF_REPLICAS = int(os.getenv('K'))
    #Need to handle empty view
    construct_initial_view(VIEW)
    handle_empty_view()
    app.run(host="0.0.0.0", port=8080)
