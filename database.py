import scraperwiki

def create_db():
	# main table
	# automatic, without foreign keys set :(

    # section table
    scraperwiki.sqlite.execute('''
        CREATE TABLE IF NOT EXISTS sections(
            id INTEGER PRIMARY KEY,
            name TEXT,
            parent_section_id INTEGER,
            FOREIGN KEY (parent_section_id) REFERENCES sections(id))
    ''')

    # people table
    scraperwiki.sqlite.execute('''
        CREATE TABLE IF NOT EXISTS people(
            id INTEGER PRIMARY KEY,
            name TEXT,
            email TEXT,
            other TEXT,
            section_id INTEGER,
            FOREIGN KEY (section_id) REFERENCES sections(id))
    ''')

    # categories table
    scraperwiki.sqlite.execute('''
        CREATE TABLE IF NOT EXISTS categories(
            id INTEGER PRIMARY KEY,
            name TEXT,
            parent_category INTEGER,
            FOREIGN KEY (parent_category) REFERENCES categories(id))
    ''')