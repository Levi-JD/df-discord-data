import ijson
from itertools import islice
from collections import Counter
import sqlite3
import re
import json
from datetime import datetime, timezone
import spacy

MAX_MESSAGES = 5000
counts = Counter()
names = {}
seen = 0

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

        if MAX_MESSAGES != None and seen >=MAX_MESSAGES:
            break
        
        print(seen)