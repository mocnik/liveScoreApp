from flask import Flask, g, request, jsonify
from flask_socketio import SocketIO

from db import connect_db, get_categories


app = Flask(__name__)
socketio = SocketIO(app)

app.config.update(
    DB_USERNAME='SYSDBA',
    DB_PASSWORD='masterkey',
    DB_CONNECTION_STRING='localhost:C:\\Users\\ASUS-Rok\\AppData\\Roaming\\OEvent\\Data\\Competition12.gdb'
)


@app.route('/punch', methods=['POST'])
def punch():
    json = request.get_json()
    print(json)
    socketio.emit('new_punch', json)
    return '', 200


@app.route('/categories', methods=['GET'])
def list_categories():
    return jsonify(get_categories(get_db()))


def get_db():
    if not hasattr(g, 'firebird_db'):
        g.firebird_db = connect_db(app.config['DB_CONNECTION_STRING'],
                                   app.config['DB_USERNAME'],
                                   app.config['DB_PASSWORD'])
    return g.firebird_db


@app.teardown_appcontext
def close_db(error):
    if hasattr(g, 'firebird_db'):
        g.firebird_db.close()


if __name__ == "__main__":
    socketio.run(app, host='0.0.0.0', port=8000)