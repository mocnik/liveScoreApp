from timeit import default_timer as timer
from collections import namedtuple

import firebirdsql

STR_ESCAPE = "'"
STATUS_CODES = {
    0: 'Active',
    1: 'OK',
    2: 'DISQ',
    3: 'DNF',
    4: 'DNS',
    5: 'MP'
}


def connect_db(dsn, username, password):
    """Connects to the specific database."""
    return firebirdsql.connect(dsn=dsn, user=username, password=password)


def get_table(conn, table, filters=None, order_by=None):
    """ Return `table` from `conn` and return it as dictionary. """
    start = timer()
    cur = conn.cursor()
    sql = 'SELECT * FROM %s' % table
    if filters:
        sql += ' WHERE ' + ' AND '.join(
            str(k) + ' ' + get_operator(v) + ' ' + STR_ESCAPE + get_value(v) + STR_ESCAPE for k, v in filters.items())
    if order_by:
        if isinstance(order_by, tuple):
            sql += ' ORDER BY ' + ','.join(order_by)
        else:
            sql += ' ORDER BY ' + order_by
    cur.execute(sql)
    data = cur.fetchall()
    desc = [description[0] for description in cur.description]
    cur.close()
    print('{} {:.4f}ms'.format(sql, (timer()-start)*1000))
    return [{d: e for e, d in zip(row, desc)} for row in data]


def get_value(v):
    if isinstance(v, tuple):
        return str(v[1])
    return str(v)


def get_operator(v):
    if isinstance(v, tuple):
        return str(v[0])
    return '='


def get_categories(conn):
    table = get_table(conn, 'OEVCATEGORY')
    return [row['CATEGORYNAME'] for row in table]


def get_category_runners(conn, category_id, stage):
    table = get_table(conn, 'OEVLISTSVIEW', {'CATEGORYNAME': category_id})
    return [to_runner_data(row, stage) for row in table]


def get_category_startlist(conn, category_id, stage):
    table = get_table(conn, 'OEVLISTSVIEW', {'CATEGORYNAME': category_id, 'ISRUNNING' + stage: 1}, 'STARTTIME' + stage)
    return [to_runner_data(row, stage) for row in table]


def get_category_official_results(conn, category_id, stage):
    table = get_table(conn, 'OEVLISTSVIEW',
                      {'CATEGORYNAME': category_id, 'ISRUNNING' + stage: 1, 'FINISHTYPE' + stage: ('>', 0)},
                      ('FINISHTYPE' + stage, 'COMPETITIONTIME' + stage))
    return [to_runner_data(row, stage) for row in table]


def get_runner_by_start_number(conn, start_number,stage):
    table = get_table(conn, 'OEVLISTSVIEW', {'STARTNUMBER': start_number})
    return [to_runner_data(row, stage) for row in table]


def get_runner_by_chip_number(conn, chip_number, stage):
    table = get_table(conn, 'OEVLISTSVIEW', {'CHIPNUMBER' + stage: chip_number})
    return [to_runner_data(row, stage) for row in table]


def get_competitor_by_chip_number(conn, chip_number, stage):
    return get_table(conn, 'OEVLISTSVIEW', {'CHIPNUMBER' + stage: chip_number})


def to_runner_data(table_row, stage):
    d = {'startNumber': table_row['STARTNUMBER'],
         'name': table_row['FIRSTNAME'] + ' ' + table_row['LASTNAME'],
         'siCardNumber': table_row['CHIPNUMBER' + stage],
         'finishType': STATUS_CODES[table_row['FINISHTYPE' + stage]],
         'club': table_row['CLUBLONGNAME'],
         'country': table_row['COUNTRYSHORTNAME']}
    if table_row['STARTTIME' + stage] is not None:
        d['startTime'] = table_row['STARTTIME' + stage] / 100
    if table_row['COMPETITIONTIME' + stage] is not None:
            d['competitionTime'] = table_row['COMPETITIONTIME' + stage] / 100

    return d


def get_competition_data(conn, stage):
    table = get_table(conn, 'OEVCOMPETITION')[0]
    d = {'name': table['COMPETITIONNAME'],
         'place': table['COMPETITIONPLACE'],
         'organizer': table['ORGANIZER'],
         'date': table['DATE' + stage],
         'firstStart': table['FIRSTSTART' + stage]}
    return d


def query_db(conn, query, args=(), one=False):
    cur = conn.execute(query, args)
    rv = cur.fetchall()
    cur.close()
    return (rv[0] if rv else None) if one else rv
