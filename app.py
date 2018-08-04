from flask import Flask, g, request, jsonify, abort
from flask_socketio import SocketIO
from requests import post
from time import time
from timeit import default_timer as timer

from db import connect_db, get_categories, get_category_runners, get_runner_by_start_number, get_competition_data, \
    get_runner_by_chip_number, get_category_startlist, get_category_official_results
from oevent2xml import to_xml, punch_xml

import os
import click
import sqlite3

app = Flask(__name__)
socketio = SocketIO(app)

app.config.update(
    DB_USERNAME='SYSDBA',
    DB_PASSWORD='masterkey',
    DB_CONNECTION_STRING='127.0.0.1:C:\\Users\\ASUS-Rok\\AppData\\Roaming\\OEvent\\Data\\Competition13.gdb',
    RESULT_FOLDER='C:\\Users\\ASUS-Rok\\liveScoreOut\\',
    SQLITE='punches.db',
    XML_EXPORT=False
)


@app.cli.command('punch', help='Simulate punch')
@click.argument('chip')
@click.argument('station')
@click.option('-t', default=None, help='Punch time')
def test_punch(chip, station, t):
    if t:
        punch_time = int(t)
    else:
        punch_time = int(round(time()))
    data = {'chipNumber': chip, 'time': punch_time, 'stationCode': station}
    post('http://127.0.0.1:8000/punch', json=data)
    click.echo(data)


@app.cli.command('xml', help='Generate IOF v3 xml')
def xml():
    results_file = os.path.join(app.config['RESULT_FOLDER'], "results.xml")
    with open(results_file, "wb") as f:
        f.write(to_xml(get_db()))
    print("Saved to: ", results_file)


@app.cli.command('init_db', help='Initialise database')
def init_db():
    db = get_sqlite()
    with app.open_resource('schema.sql', mode='r') as f:
        db.cursor().executescript(f.read())
    db.commit()


@app.route('/punch', methods=['POST'])
def punch():
    json = request.get_json()
    socketio.emit('new_punch', json)

    json['stationCode'] = int(json['stationCode'])
    if json['stationCode'] < 10:  # below 10 is reserved as finish station
        json['stationCode'] = 0

    sql = '''INSERT OR REPLACE INTO punches(chipNumber, stationCode, time) VALUES (?,?,?)'''
    conn = get_sqlite()
    cur = conn.cursor()
    cur.execute(sql, (json['chipNumber'], json['stationCode'], json['time']))
    conn.commit()
    print(json)

    if app.config['XML_EXPORT']:
        filename = str(json['stationCode']) + "_" + str(json['chipNumber']) + ".xml"
        with open(os.path.join(app.config['RESULT_FOLDER'], filename), "wb") as f:
            f.write(punch_xml(get_db(), json['chipNumber'], json['stationCode'], json['time']))
    return '', 200


def calc_seconds(time_string):
    return sum(x * int(t) for x, t in zip([1, 60, 3600], reversed(time_string.split(":"))))


@app.route('/categories', methods=['GET'])
def list_categories():
    return jsonify(get_categories(get_db()))


@app.route('/runner/<start_number>', methods=['GET'])
def list_runner(start_number):
    runner_data = augment_runners(get_runner_by_start_number(get_db(), start_number))
    if not runner_data:
        abort(404)

    return jsonify(runner_data[0])


@app.route('/category/<category_id>/runners', methods=['GET'])
def list_category_runners(category_id):
    return jsonify(augment_runners(get_category_runners(get_db(), category_id)))


@app.route('/category/<category_id>/results', methods=['GET'])
def list_category_results(category_id):
    if 'station' in request.args:
        s = int(request.args['station'])
    else:
        s = 0

    runners = sorted(
        (runner for runner in augment_runners(get_category_runners(get_db(), category_id)) if s in runner['punches']),
        key=lambda r: r['punches'][s]['time'])

    return jsonify([extract_time(runner, s) for runner in runners])


def extract_time(runner, s):
    runner['competitionTime'] = runner['punches'][s]['time']
    del runner['punches']
    return runner


@app.route('/category/<category_id>/startList', methods=['GET'])
def startlist_category(category_id):
    return jsonify(get_category_startlist(get_db(), category_id))


@app.route('/category/<category_id>/officialResults', methods=['GET'])
def official_results_category(category_id):
    return jsonify(get_category_official_results(get_db(), category_id))


@app.route('/competition', methods=['GET'])
def list_competition_date():
    return jsonify(get_competition_data(get_db()))


def augment_runners(runners):
    for runner in runners:
        runner['punches'] = list_punches(runner['siCardNumber'], runner['startTime'])
    return runners


def list_punches(chip_number, start_time):
    data = query_db('SELECT * FROM punches WHERE chipNumber = ?', (chip_number,))
    return {d[1]: punch_dict(d, start_time) for d in data}


def punch_dict(d, start_time):
    first_start = get_competition_data(get_db())['firstStart']
    return {'chipNumber': d[0], 'time': d[2] - first_start - start_time}


def query_db(query, args=(), one=False):
    cur = get_sqlite().execute(query, args)
    rv = cur.fetchall()
    cur.close()
    return (rv[0] if rv else None) if one else rv


def get_db():
    start = timer()
    db = getattr(g, 'firebird_db', None)
    if db is None:
        db = g.firebird_db = connect_db(app.config['DB_CONNECTION_STRING'],
                                        app.config['DB_USERNAME'],
                                        app.config['DB_PASSWORD'])
    print('DB CONN {:.4f}ms'.format((timer() - start) * 1000))
    return db


def get_sqlite():
    db = getattr(g, 'sqlite_db', None)
    if db is None:
        db = g.sqlite_db = sqlite3.connect(app.config['SQLITE'])
    return db


@app.teardown_appcontext
def close_db(error):
    if hasattr(g, 'firebird_db'):
        g.firebird_db.close()
    if hasattr(g, 'sqlite_db'):
        g.sqlite_db.close()


if __name__ == "__main__":
    socketio.run(app, host='0.0.0.0', port=8000)
