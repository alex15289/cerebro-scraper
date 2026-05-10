import os
from flask import Flask, jsonify, request
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

SECRET_KEY = os.environ.get('SECRET_KEY', 'empire2026')
PORT = int(os.environ.get('PORT', '8080'))

@app.route('/')
def index():
    return jsonify({'service': 'CEREBRO AZ Pipeline', 'status': 'online'})

@app.route('/health')
def health():
    return jsonify({'status': 'ok'})

@app.route('/leads')
def leads():
    if request.args.get('key') != SECRET_KEY:
        return jsonify({'error': 'Unauthorized'}), 401
    return jsonify({'leads': [], 'total': 0, 'status': 'online'})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=PORT)
