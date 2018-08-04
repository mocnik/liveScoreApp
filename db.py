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


def get_table_simple(conn, table):
    """ Return `table` from `conn` and return it as named tuple. """
    cur = conn.cursor()
    cur.execute("SELECT * FROM %s" % table)
    data = cur.fetchall()
    desc = [description[0] for description in cur.description]
    cur.close()
    table_class = namedtuple(table, desc)
    return [table_class(*row) for row in data]


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


def get_category_runners(conn, category_id):
    table = get_table(conn, 'OEVLISTSVIEW', {'CATEGORYNAME': category_id})
    return [to_runner_data(row) for row in table]


def get_category_startlist(conn, category_id):
    table = get_table(conn, 'OEVLISTSVIEW', {'CATEGORYNAME': category_id, 'ISRUNNING1': 1}, 'STARTTIME1')
    return [to_runner_data(row) for row in table]


def get_category_official_results(conn, category_id):
    table = get_table(conn, 'OEVLISTSVIEW', {'CATEGORYNAME': category_id, 'ISRUNNING1': 1, 'FINISHTYPE1': ('>', 0)},
                      ('FINISHTYPE1', 'COMPETITIONTIME1'))
    return [to_runner_data(row) for row in table]


def get_runner_by_start_number(conn, start_number):
    table = get_table(conn, 'OEVLISTSVIEW', {'STARTNUMBER': start_number})
    return [to_runner_data(row) for row in table]


def get_runner_by_chip_number(conn, chip_number):
    table = get_table(conn, 'OEVLISTSVIEW', {'CHIPNUMBER1': chip_number})
    return [to_runner_data(row) for row in table]


def to_runner_data(table_row):
    d = {'startNumber': table_row['STARTNUMBER'],
         'name': table_row['FIRSTNAME'] + ' ' + table_row['LASTNAME'],
         'siCardNumber': table_row['CHIPNUMBER1'],
         'finishType': STATUS_CODES[table_row['FINISHTYPE1']],
         'club': table_row['CLUBLONGNAME'],
         'country': table_row['COUNTRYSHORTNAME']}
    if table_row['STARTTIME1'] is not None:
        d['startTime'] = table_row['STARTTIME1'] / 100
    if table_row['COMPETITIONTIME1'] is not None:
            d['competitionTime'] = table_row['COMPETITIONTIME1'] / 100

    return d


def get_competition_data(conn):
    table = get_table(conn, 'OEVCOMPETITION')[0]
    d = {'name': table['COMPETITIONNAME'],
         'place': table['COMPETITIONPLACE'],
         'organizer': table['ORGANIZER'],
         'date': table['DATE1'],
         'firstStart': table['FIRSTSTART1']}
    return d
