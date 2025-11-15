import sqlite3
import re
from datetime import datetime
from collections import Counter
import spacy
from spacy.matcher import PhraseMatcher
import numpy as np
from spacy.tokens import DocBin
import random
import nltk
from spacy.tokens import Span
nltk.download('wordnet')
from nltk.corpus import wordnet as wn

connection = sqlite3.connect("messages.sqlite")
cursor = connection.cursor()
nlp = spacy.blank("en")


train_db = DocBin(store_user_data=True)
dev_db = DocBin(store_user_data=True)

BATCH_SIZE = 20000

def is_english_word(word):
    return len(wn.synsets(word)) > 0

def filter_overlapping(spans):
    # sort by length DESC (longest first)
    spans = sorted(spans, key=lambda s: (s.end - s.start), reverse=True)
    
    result = []
    seen_tokens = set()

    for span in spans:
        token_range = set(range(span.start, span.end))
        
        # if span overlaps with any accepted span, skip it
        if token_range & seen_tokens:
            continue

        result.append(span)
        seen_tokens |= token_range

    # spaCy prefers spans sorted by start
    return sorted(result, key=lambda s: s.start)

def get_usersn():
    sqlquery = """
        SELECT u.name, u.nick
        FROM users AS u
    """

    cursor.execute(sqlquery)
    
    users_nicks = set()

    for i in cursor.fetchall():
        if len(i[0]) > 3:
            if not is_english_word(i[0]):
                #print(i[0])
                users_nicks.add(i[0])
        if len(i[1]) > 3:
            if not is_english_word(i[1]):
                #print(i[1])
                users_nicks.add(i[1])

    return list(users_nicks)

users = get_usersn()

print(users)

patterns = [nlp.make_doc(name) for name in users]
matcher = PhraseMatcher(nlp.vocab, attr="LOWER")
matcher.add("USERNAME", patterns)

sqlquery = """
    SELECT message FROM messages
"""

cursor.execute(sqlquery)

while True:
    rows = [i[0] for i in cursor.fetchmany(BATCH_SIZE)]
    if not rows:
        break
    print(rows)
    for doc in nlp.pipe(rows, batch_size=1000):
        matches = matcher(doc)

        spans = []

        for match_id, start, end in matches:
            span = Span(doc, start, end, label="USERNAME")
            spans.append(span)
        spans = filter_overlapping(spans)
        

        if not spans:
            continue

        doc.ents = spans
        #print(doc.ents[0].text, doc.ents[0].label_)
        if random.random() < 0.9:
            train_db.add(doc)
        else:
            dev_db.add(doc)


train_db.to_disk("discord_usernames_train.spacy")
dev_db.to_disk("discord_usernames_dev.spacy")