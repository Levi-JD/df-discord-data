import ijson
from itertools import islice
from collections import Counter

messages = 0
users = set()
user_c = 0
with open("data.json", "rb") as file:

    items = ijson.items(file, 'messages.item')

    for i in items:
        author = i.get("author")
        user_id = str(author.get("id"))
        if user_id != None:
            messages += 1
            if user_id not in users:
                users.add(user_id)
                user_c += 1
        else:
            print("user none")
avrg_msgs = messages/user_c
print(f"Messages: {messages}")
print(f"Users: {user_c}")
print(f"average messages: {avrg_msgs}")