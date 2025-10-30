import ijson
from itertools import islice
from collections import Counter
MAX_MESSAGES = None
counts = Counter()
names = {}
seen = 0
with open("data.json", "rb") as file:

    items = ijson.items(file, 'messages.item')

    for i in items:
        seen += 1
        author = i.get("author")
        user_id = str(author.get("id"))
        name = author.get("name")
        if name != "Deleted User":
                
            if user_id not in names:
                names[user_id] = name
            counts[user_id] += 1
        else:
            print("deleted")
        if MAX_MESSAGES != None and seen >=MAX_MESSAGES:
            break

top100 = counts.most_common(1000)

with open("example.txt", "a", encoding="utf-8") as f:
    for rank, (uid, cnt) in enumerate(top100, 1):
        f.write(f"{rank:3d}. {names.get(uid, 'unknown')} count: {cnt}\n")

"""
print("top 100:")
for rank, (uid, cnt) in enumerate(top100, 1):
    print(f"{rank:3d}. {names.get(uid, 'unknown')} ({uid}): {cnt}")
"""