from flask import Flask
from youtubecrowler import main

app = Flask(__name__)

@app.route('/')
def execute_crowler():
    main()
    return '200'

if __name__ == '__main__':
    app.run(port=5555)
