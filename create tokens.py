import ijson
from itertools import islice
from collections import Counter
import sqlite3
import re
import json
from datetime import datetime
MAX_MESSAGES = None
counts = Counter()
names = {}
seen = 0

# One-pass cleanup pattern (codeblocks, inline code, <:emoji:id>, mentions, URLs, :emoji_name:)
CLEANUP = re.compile(
    r"(?:```.*?```)"                      # triple code blocks
    r"|(?:`[^`\n]*`)"                     # inline code
    r"|(?:<a?:\w+:\d+>)"                  # <:name:id> / <a:name:id>
    r"|(?:<@!?(\d+)>|<@&(\d+)>|<#(\d+)>)" # mentions
    r"|(?:@\S+)" # mentions/roles/channels 2
    r"|(?:https?://\S+|www\.\S+)"         # URLs
    r"|(?<!\w):[a-z0-9_]{2,32}:(?!\w)",   # :emoji_name:
    re.DOTALL | re.I
)

# Words (ASCII letters/digits) vs single non-space symbols
TOKEN = re.compile(r"[0-9A-Za-z]+|[^\s0-9A-Za-z]")

def tokenize_msg(text: str):
    # remove unwanted artifacts in ONE pass
    text = CLEANUP.sub(" ", text)
    # find tokens (C-accelerated)
    toks = TOKEN.findall(text)
    # normalize (lower is faster than casefold and good for English-like text)
    for i in range(len(toks)):
        toks[i] = toks[i].lower()
    return toks

with open("tokens.jsonl", "a", encoding="utf-8") as f: 
    with open("data.json", "rb") as file:

        items = ijson.items(file, 'messages.item')

        for i in items:
            seen += 1
            author = i.get("author")
            user_id = str(author.get("id"))
            name = author.get("name")
            msg = i.get("content")
            timestamp = int(datetime.fromisoformat(i.get("timestamp")).replace(hour=0, minute=0, second=0, microsecond=0).timestamp())
            tokens = tokenize_msg(msg)
            #print(tokens)

            json.dump({"tokens": tokens, "authid": user_id, "authname": name, "timestamp": timestamp}, f, ensure_ascii=False)
            f.write("\n")

            if MAX_MESSAGES != None and seen >=MAX_MESSAGES:
                break
            
            print(seen)
