
from flask import Flask, request, redirect, render_template

app = Flask(__name__)

@app.route('/hello')
def hello():
			name = request.args.get('name')
			if name is None:
				return "Hello user!" 
			else:
				return "Hello " + name + "!"

@app.route('/check', methods=['GET', 'POST'])
def check():
			if request.method== 'GET':
				return "This is a GET request"
			if request.method== 'POST':
				return "This is a POST request"
					
if __name__ == "__main__":
	app.debug = True;
	app.run(host = "0.0.0.0", port = 8080)