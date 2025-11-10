import sqlite3

connection = sqlite3.connect("tokens.sqlite")
cursor = connection.cursor()

cursor.executescript("""
        PRAGMA journal_mode=WAL;
        PRAGMA synchronous=NORMAL;
        PRAGMA temp_store=MEMORY;
        PRAGMA mmap_size=30000000000; -- ok if ignored on your system
    """)

create_table_query = """
    CREATE TABLE IF NOT EXISTS messages (
        msg_id INTEGER PRIMARY KEY,
        message TEXT,
        count INTEGER NOT NULL,
        timestamp INTEGER NOT NULL,
    );
    
    CREATE INDEX IF NOT EXISTS idx_utc_time ON user_tokens_count(timestamp);
    CREATE INDEX IF NOT EXISTS idx_tokens_token ON tokens(token);
    CREATE INDEX IF NOT EXISTS idx_utc_user ON user_tokens_count(user_id);
    CREATE INDEX IF NOT EXISTS idx_utc_token ON user_tokens_count(token_id);
"""
cursor.executescript(create_table_query)

connection.commit()
connection.close()