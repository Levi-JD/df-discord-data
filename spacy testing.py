import ijson
from itertools import islice
from collections import Counter
import sqlite3
import re
import json
from datetime import datetime, timezone
import spacy
from collections import Counter
from sklearn.metrics.pairwise import cosine_similarity
import numpy as np
import random

spacy.require_cpu

connection = sqlite3.connect('messages.sqlite')

cursor = connection.cursor()

def jaccard(c1, c2, top_k=50):
    set1 = set([w for w, _ in c1.most_common(top_k)])
    set2 = set([w for w, _ in c2.most_common(top_k)])
    
    if not set1 or not set2:
        return 0.0
    
    return len(set1 & set2) / len(set1 | set2)


def extract_words(msgs, max_msgs=3000, batch_size=256):
    msgs = msgs[:max_msgs]

    noun_counter = Counter()
    verb_counter = Counter()
    adj_counter = Counter()
    non_stop = Counter()
    for doc in nlp.pipe(msgs, batch_size=batch_size):
        for token in doc:
            if not token.is_alpha:
                continue

            # Non-stopwords
            if not token.is_stop:
                non_stop[token.lemma_] += 1

            # POS-specific
            if token.pos_ == "NOUN" or token.pos_ == "PROPN":
                noun_counter[token.lemma_.lower()] += 1
            elif token.pos_ == "VERB":
                verb_counter[token.lemma_.lower()] += 1
            elif token.pos_ == "ADJ":
                adj_counter[token.lemma_.lower()] += 1

    return noun_counter, verb_counter, adj_counter, non_stop

def get_user_msg_data(name,limit=5000):

    sqlquery = """
        SELECT u.user_id, u.nick, u.is_bot
        FROM users AS u
        WHERE u.name = ?
    """
    cursor.execute(sqlquery, (name,))

    user_info = cursor.fetchall()[0]

    nick= user_info[1]
    is_bot= bool(user_info[2])
    id= user_info[0]

    sqlquery= """
        SELECT m.type, m.message, m.timestamp, m.mentions
        FROM messages AS m
        WHERE m.author_id = ?
        ORDER BY timestamp DESC
        LIMIT ?;
    """
    cursor.execute(sqlquery,(id,limit))

    return cursor.fetchall()

def get_lex_sim(msgs1, msgs2):
    nounsA, verbsA, adjsA, nonstopsA = extract_words(msgs1)
    nounsB, verbsB, adjsB, nonstopsB = extract_words(msgs2)

    noun_sim = jaccard(nounsA, nounsB)
    verb_sim = jaccard(verbsA, verbsB)
    adj_sim  = jaccard(adjsA, adjsB)
    nonstop_sim = jaccard(nonstopsA, nonstopsB)

    lex_sim = (
        0.10 * noun_sim +
        0.45 * verb_sim +
        0.25 * adj_sim  +
        0.20 * nonstop_sim
    )

    return lex_sim
nlp = spacy.load("en_core_web_lg")

user_data1 = get_user_msg_data("not_lewi")
user_data2 = get_user_msg_data("bignutty")

msgs1 = [i[1] for i in user_data1]
tmp1 = [i[2] for i in user_data1]
#print(msgs1)
msgs2 = [i[1] for i in user_data2]
tmp2 = [i[2] for i in user_data2]
#print(msgs2)

lex_sim = get_lex_sim(msgs1, msgs2)

#print(noun_sim)
print(lex_sim)
#print(nonstop_sim)

#for i, (msg_obj, message) in enumerate(zip(result, nlp.pipe(msgs, batch_size=256))):