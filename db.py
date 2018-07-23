import firebirdsql


STR_ESCAPE = "'"


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


def to_runner_data(table_row):
    d = {'startNumber': table_row['STARTNUMBER'],
         'name': table_row['FIRSTNAME'] + ' ' + table_row['LASTNAME'],
         'siCardNumber': table_row['CHIPNUMBER1']}
    if table_row['STARTTIME1']:
        d['startTime'] = table_row['STARTTIME1'] / 100
    if table_row['COMPETITIONTIME1']:
        d['competitionTime'] = table_row['COMPETITIONTIME1'] / 100

    return d
