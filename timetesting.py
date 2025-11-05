from datetime import datetime
import ijson
from itertools import islice
from collections import Counter
import sqlite3
import re
import json
from collections import defaultdict

with open("tokens.jsonl", "r", encoding="utf-8") as file:
    for line in file:
        obj = json.loads(line)

        time = obj["timestamp"]
        timestamp = int(datetime.fromisoformat(time).timestamp())
        print(timestamp)
        