import scraperwiki

def create_db():
	# main table
	# automatic, without foreign keys set :(

    # people table
    scraperwiki.sqlite.execute('''
        CREATE TABLE IF NOT EXISTS people(
            id INTEGER PRIMARY KEY,
            name TEXT,
            email TEXT)
    ''')

    # categories table
    scraperwiki.sqlite.execute('''
        CREATE TABLE IF NOT EXISTS categories(
            id INTEGER PRIMARY KEY,
            name TEXT,
            parent_category INTEGER,
            FOREIGN KEY (parent_category) REFERENCES categories(id))
    ''')