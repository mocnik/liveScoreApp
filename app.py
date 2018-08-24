from flask import Flask, g, request, jsonify, abort
from flask_socketio import SocketIO
from flask_cors import CORS
from requests import post
from timeit import default_timer as timer

from db import connect_db, get_categories, get_category_runners, get_runner_by_start_number, get_competition_data, \
    get_category_startlist, get_category_official_results, query_db, test_conn, STATUS_CODE_SORT
from oevent2xml import to_xml, punch_xml

import os
import time
import click
import sqlite3
import datetime

app = Flask(__name__)
CORS(app)
socketio = SocketIO(app)

app.config.update(
    DB_USERNAME='SYSDBA',
    DB_PASSWORD='masterkey',
    DB_CONNECTION_STRING='192.168.1.143:C:\\Users\\ASUS-Rok\\AppData\\Roaming\\OEvent\\Data\\Competition13.gdb',
    RESULT_FOLDER='C:\\Users\\rokmo\\liveScoreOut\\',
    SQLITE='punches.db',
    XML_EXPORT=True,
    XML_EXPORT_WAIT=20,
    STAGE='1'
)


@app.cli.command('punch', help='Simulate punch')
@click.argument('chip')
@click.argument('station')
@click.option('-t', default=None, help='Punch time')
def test_punch(chip, station, t):
    if t:
        punch_time = int(t)
    else:
        punch_time = int(round(time.time()))
    data = {'chipNumber': chip, 'time': punch_time, 'stationCode': station}
    post('http://127.0.0.1:8000/punch', json=data)
    click.echo(data)


@app.cli.command('xml_one', help='Generate IOF v3 XML')
def xml():
    export_xml()


@app.cli.command('xml_official', help='Generate IOF v3 XML Official Results')
def xml():
    results_file = os.path.join(app.config['RESULT_FOLDER'], "official_results.xml")
    with open(results_file, "wb") as f:
        f.write(to_xml(get_db(), get_sqlite(), app.config['STAGE'], official=True))
    print("Saved to: ", results_file)


def export_xml():
    results_file = os.path.join(app.config['RESULT_FOLDER'], "results.xml")
    with open(results_file, "wb") as f:
        f.write(to_xml(get_db(), get_sqlite(), app.config['STAGE']))
    print("Saved to: ", results_file)


@app.cli.command('test_db', help='Test DB connection')
def test_db():
    print("Firebird DB version %s" % test_conn(get_db()))


@app.cli.command('xml', help='Constantly generate IOF v3 XMLs')
def xml_run():
    while True:
        try:
            export_xml()
        except (SystemExit, KeyboardInterrupt):
            raise
        except Exception as exception:
            print("Failed during update")
        finally:
            time.sleep(app.config['XML_EXPORT_WAIT'])


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

    sql = '''INSERT OR REPLACE INTO punches(chipNumber, stationCode, time, stage) VALUES (?,?,?,?)'''
    conn = get_sqlite()
    cur = conn.cursor()
    cur.execute(sql, (json['chipNumber'], json['stationCode'], json['time'], app.config['STAGE']))
    conn.commit()
    print(json)

    if app.config['XML_EXPORT']:
        filename = str(json['stationCode']) + "_" + str(json['chipNumber']) + ".xml"
        with open(os.path.join(app.config['RESULT_FOLDER'], filename), "wb") as f:
            f.write(punch_xml(get_db(), json['chipNumber'], json['stationCode'], json['time'], app.config['STAGE']))
    return '', 200


def calc_seconds(time_string):
    return sum(x * int(t) for x, t in zip([1, 60, 3600], reversed(time_string.split(":"))))


@app.route('/categories', methods=['GET'])
def list_categories():
    return jsonify(get_categories(get_db()))


@app.route('/runner/<start_number>', methods=['GET'])
def list_runner(start_number):
    runner_data = augment_runners(get_runner_by_start_number(get_db(), start_number, app.config['STAGE']))
    if not runner_data:
        abort(404)

    return jsonify(runner_data[0])


@app.route('/category/<category_id>/runners', methods=['GET'])
def list_category_runners(category_id):
    return jsonify(augment_runners(get_category_runners(get_db(), category_id, app.config['STAGE'])))


@app.route('/category/<category_id>/results', methods=['GET'])
def list_category_results(category_id):
    if 'station' in request.args:
        s = int(request.args['station'])
    else:
        s = 0

    runners = sorted(
        (runner for runner in augment_runners(get_category_runners(get_db(), category_id, app.config['STAGE'])) if s in runner['punches']),
        # key=lambda r: r['punches'][s]['time'])
        key=lambda r: sort_results(r, s))
    return jsonify([extract_time(runner, s) for runner in runners])


def sort_results(r, s):
    return STATUS_CODE_SORT[r['finishType']], r['punches'][s]['time']


def extract_time(runner, s):
    runner['competitionTime'] = runner['punches'][s]['time']
    del runner['punches']
    return runner


@app.route('/category/<category_id>/startList', methods=['GET'])
def startlist_category(category_id):
    return jsonify(get_category_startlist(get_db(), category_id, app.config['STAGE']))


@app.route('/category/<category_id>/officialResults', methods=['GET'])
def official_results_category(category_id):
    return jsonify(get_category_official_results(get_db(), category_id, app.config['STAGE']))


@app.route('/competition', methods=['GET'])
def list_competition_date():
    return jsonify(get_competition_data(get_db(), app.config['STAGE']))


def augment_runners(runners):
    for runner in runners:
        runner['punches'] = list_punches(runner['siCardNumber'], runner['startTime'])
    return runners


def list_punches(chip_number, start_time):
    data = query_db(get_sqlite(), 'SELECT * FROM punches WHERE chipNumber = ? AND stage = ?', (chip_number, app.config['STAGE']))
    return {d[1]: punch_dict(d, start_time) for d in data}


def punch_dict(d, start_time):
    midnight = datetime.datetime.combine(datetime.datetime.today(), datetime.time.min)
    midnight_unix = time.mktime(midnight.timetuple())
    first_start = get_competition_data(get_db(), app.config['STAGE'])['firstStart']
    return {'chipNumber': d[0], 'time': d[3] - first_start - start_time - midnight_unix}


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
