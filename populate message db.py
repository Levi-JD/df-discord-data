import ijson
from itertools import islice
from collections import Counter
import sqlite3
import re
import json
from datetime import datetime, timezone
import spacy


connection = sqlite3.connect('messages.sqlite')

cursor = connection.cursor()


MAX_MESSAGES = None
BATCH_SIZE = 5000

seen = 0
users = {}
with open("data.json", "rb") as file:
    items = ijson.items(file, 'messages.item')
    for i in items:

        seen += 1

        author = i.get("author")
        name = author.get("name")
        nick = author.get("nickname")
        isbot = author.get("isBot")
        type = i.get("type")
        msg = i.get("content")
        mentions = [{"username": j["name"], "nickname": j["nickname"], "isbot": j["isBot"]} for j in i.get("mentions")]
        timestamp = int(datetime.fromisoformat(i.get("timestamp")).astimezone(timezone.utc).timestamp())
        
        user = users.get(name)

        if user == None:
            cursor.execute("INSERT INTO users(name, nick, is_bot) VALUES (?, ?, ?)", (name, nick, isbot))
            user_id = cursor.lastrowid
            user = user_id
            users[name] = user

        cursor.execute("INSERT INTO messages(author_id, type, message, timestamp, mentions) VALUES (?, ?, ?, ?, ?)", (user, type, msg, timestamp, json.dumps(mentions)))

        print(seen)

        if seen % BATCH_SIZE == 0:
            connection.commit()

        if MAX_MESSAGES != None and seen >=MAX_MESSAGES:
            break

    connection.commit()