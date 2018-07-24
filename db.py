import firebirdsql
import numbers

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


def get_table(conn, table, filters=None):
    """ Return `table` from `conn` and return it as dictionary. """
    cur = conn.cursor()
    sql = 'SELECT * FROM %s' % table
    if filters:
        sql += ' WHERE ' + ' AND '.join(str(k) + ' = ' + STR_ESCAPE + str(v) + STR_ESCAPE for k, v in filters.items())
    print(sql)
    cur.execute(sql)
    data = cur.fetchall()
    desc = [description[0] for description in cur.description]
    cur.close()

    return [{d: e for e, d in zip(row, desc)} for row in data]


def get_categories(conn):
    table = get_table(conn, 'OEVCATEGORY')
    return [row['CATEGORYNAME'] for row in table]


def get_category_runners(conn, category_id):
    table = get_table(conn, 'OEVLISTSVIEW', {'CATEGORYNAME': category_id})
    return [to_runner_data(row) for row in table]


def get_runner_by_start_number(conn, start_number):
    table = get_table(conn, 'OEVLISTSVIEW', {'STARTNUMBER': start_number})
    return [to_runner_data(row) for row in table]


def to_runner_data(table_row):
    d = {'startNumber': table_row['STARTNUMBER'],
         'name': table_row['FIRSTNAME'] + ' ' + table_row['LASTNAME'],
         'siCardNumber': table_row['CHIPNUMBER1'],
         'finishType': STATUS_CODES[table_row['FINISHTYPE1']],
         'club': table_row['CLUBLONGNAME'],
         'country': table_row['COUNTRYSHORTNAME']}
    if table_row['STARTTIME1']:
        d['startTime'] = table_row['STARTTIME1'] / 100
    if table_row['COMPETITIONTIME1']:
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
