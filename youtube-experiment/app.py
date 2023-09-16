from flask import Flask
from flask import jsonify
from youtubecrawler import main
from youtubeproxy import main

app = Flask(__name__)

@app.route('/')
def execute_crawler():
    return main()


@app.route('/proxyonly')
def execute_crawler():
    return main()

@app.route('/health')
def health_check():
    return jsonify(status='ok', message='Application is healthy'), 200

if __name__ == '__main__':
    app.run(port=5555)
