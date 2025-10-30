import sqlite3

with sqlite3.connect('user_tokens_count.db') as connection:

    cursor = connection.cursor()

    create_table_query = '''
    CREATE TABLE IF NOT EXISTS token_ids (
        token_id INTEGER PRIMARY KEY AUTOINCREMENT,
        token TEXT UNIQUE
    );
    '''

    # Execute the SQL command
    cursor.execute(create_table_query)

    # Commit the changes
    connection.commit()