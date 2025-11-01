import ijson
from itertools import islice
from collections import Counter
import sqlite3
import re
import json
from collections import defaultdict

MAX_MESSAGES = None
BATCH_SIZE = 8000

seen = 0

connection = sqlite3.connect('tokens.sqlite')

cursor = connection.cursor()

tokenids = {}
buffer_counts = defaultdict(int)

with open("tokens.jsonl", "r", encoding="utf-8") as file:
    for line in file:
        obj = json.loads(line)

        tokens = obj["tokens"]

        if tokens == []:
            continue
        seen += 1
        author_id = obj["authid"]
        author_name = obj["authname"]
        #print(author_id)
        #print(seen)
        cursor.execute("INSERT INTO users(id, user_name) VALUES (?, ?) ON CONFLICT (id) DO NOTHING", (author_id,author_name))

        for token in tokens:
            token_id = tokenids.get(token)

            if token_id is None:
                cursor.execute("INSERT INTO tokens(token) VALUES (?) ON CONFLICT(token) DO NOTHING", (token,))
                cursor.execute("SELECT id FROM tokens where token = ?", (token,))
                token_id = cursor.fetchone()[0]
                #print(token_id)
        
            tokenids[token] = token_id

            buffer_counts[(author_id, token_id)] += 1

        if MAX_MESSAGES != None and seen >=MAX_MESSAGES:
            break
        
        if seen % BATCH_SIZE == 0:
            print(seen)
            rows = [(a,b,c) for (a, b), c in buffer_counts.items()]
            buffer_counts.clear()
            #print(rows)
            executemany = """

            INSERT INTO user_tokens_count(user_id, token_id, count)
            VALUES (?, ?, ?)
            ON CONFLICT(user_id, token_id)
            DO UPDATE SET count = count + excluded.count
            """
            cursor.executemany(executemany, rows)
            connection.commit()

connection.commit()