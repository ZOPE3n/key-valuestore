import requests

for x in range(70):
	requests.put(' http://localhost:8083/kvs?key=CMPS'+str(x)+'&value=1', timeout=100)

