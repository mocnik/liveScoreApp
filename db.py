import firebirdsql


def connect_db(dsn, username, password):
    """Connects to the specific database."""
    return firebirdsql.connect(dsn=dsn, user=username, password=password)


def get_table(conn, table):
    """ Return `table` from `conn` and return it as dictionary. """
    cur = conn.cursor()
    cur.execute("SELECT * FROM %s" % table)
    data = cur.fetchall()
    desc = [description[0] for description in cur.description]
    cur.close()

    return [{d: e for e, d in zip(row, desc)} for row in data]


def get_categories(conn):
    table = get_table(conn, 'OEVCATEGORY')
    return [row['CATEGORYNAME'] for row in table]
