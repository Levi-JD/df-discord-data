import ijson
from itertools import islice
from collections import Counter
import sqlite3
import re
import json
from datetime import datetime, timezone
import spacy
spacy.require_gpu()

connection = sqlite3.connect('messages.sqlite')

cursor = connection.cursor()

limit = 500000

inp = "adrianinoninja"#input("name: ")

sqlquery = """
    SELECT u.user_id, u.nick, u.is_bot
    FROM users AS u
    WHERE u.name = ?
"""

cursor.execute(sqlquery, (inp,))

user_info = cursor.fetchall()[0]
print(user_info)
nick = user_info[1]
is_bot = bool(user_info[2])
id = int(user_info[0])

print(id)
print(nick)
print(is_bot)

sqlquery = """
    SELECT m.type, m.message, m.timestamp, m.mentions
    FROM messages AS m
    WHERE m.author_id = ?
    LIMIT ?;
"""

cursor.execute(sqlquery, (id,limit))

result = cursor.fetchall()


nlp = spacy.load("en_core_web_trf")

msgs = [i[1] for i in result]

for msg_obj, message in zip(result, nlp.pipe(msgs, batch_size=512)):
    
    if len(message) != 0:
        print("message: ", list(message))