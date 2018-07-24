from flask import Flask, g, request, jsonify
from flask_socketio import SocketIO

from db import connect_db, get_categories, get_category_runners, get_runner_by_start_number, get_competition_data


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


@app.route('/runners/<start_number>', methods=['GET'])
def list_runner(start_number):
    return jsonify(get_runner_by_start_number(get_db(), start_number))


@app.route('/category/<category_id>/runners', methods=['GET'])
def list_category_runners(category_id):
    return jsonify(get_category_runners(get_db(), category_id))


@app.route('/competition', methods=['GET'])
def list_competition_date():
    return jsonify(get_competition_data(get_db()))


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