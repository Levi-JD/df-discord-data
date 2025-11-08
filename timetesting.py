from datetime import datetime
import ijson
from itertools import islice
from collections import Counter
import sqlite3
import re
import json
from collections import defaultdict

with open("data.json", "rb") as file:

    items = ijson.items(file, 'messages.item')

    for i in items:
        #timestamp = int(datetime.fromisoformat(i.get("timestamp")))
        timestamp = int(datetime.fromisoformat(i.get("timestamp")).replace(hour=0, minute=0, second=0, microsecond=0).timestamp())
        print(timestamp)