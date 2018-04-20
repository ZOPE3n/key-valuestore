import requests

for x in range(50):
	requests.put('http://localhost:8093/kvs?causal_payload=&key='+str(x)+'&value=1', timeout=10)

