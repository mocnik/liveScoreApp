from flask import Flask, make_response, request
from flask_socketio import SocketIO, emit

app = Flask(__name__)
socketio = SocketIO(app)

@app.route('/punch', methods=['POST'])
def punch():
    json = request.get_json()
    print(json)
    socketio.emit('new_punch', json)
    return ('', 200)


if __name__ == "__main__":
    socketio.run(app, host='0.0.0.0', port=8000)