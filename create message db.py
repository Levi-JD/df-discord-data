import sqlite3

connection = sqlite3.connect("messages.sqlite")
cursor = connection.cursor()

cursor.executescript("""
        PRAGMA journal_mode=WAL;
        PRAGMA synchronous=NORMAL;
        PRAGMA temp_store=MEMORY;
        PRAGMA mmap_size=30000000000; -- ok if ignored on your system
    """)

create_table_query = """
    CREATE TABLE IF NOT EXISTS users (
        user_id INTEGER PRIMARY KEY,

        name TEXT NOT NULL,
        nick TEXT NOT NULL,
        is_bot BOOLEAN 
    );

    CREATE TABLE IF NOT EXISTS messages (
        msg_id INTEGER PRIMARY KEY,

        author_id INTEGER NOT NULL,
        type TEXT NOT NULL,
        message TEXT,
        timestamp INTEGER NOT NULL,

        mentions TEXT,
        FOREIGN KEY (author_id) REFERENCES users(user_id)
    );
    
    CREATE INDEX IF NOT EXISTS idx_user ON messages(author_id);
"""
cursor.executescript(create_table_query)

connection.commit()
connection.close()