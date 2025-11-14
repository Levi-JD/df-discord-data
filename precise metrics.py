import ijson
from itertools import islice
from collections import Counter
import sqlite3
import re
import json
from datetime import datetime, timezone
import spacy
import numpy as np
import random

# ------------- SETUP -------------

spacy.require_cpu()  # <- actually call this
nlp = spacy.load("en_core_web_sm")

connection = sqlite3.connect('messages.sqlite')
cursor = connection.cursor()


# ------------- BASIC HELPERS -------------

def cosine(vec1, vec2):
    a = np.linalg.norm(vec1)
    b = np.linalg.norm(vec2)
    if a == 0 or b == 0:
        return 0.0
    return float(np.dot(vec1, vec2) / (a * b))


def jaccard(c1, c2, top_k=50):
    set1 = set([w for w, _ in c1.most_common(top_k)])
    set2 = set([w for w, _ in c2.most_common(top_k)])
    if not set1 or not set2:
        return 0.0
    return len(set1 & set2) / len(set1 | set2)


def zoom(sim, base=0.95):
    """Emphasize high similarities near 1.0."""
    return max(0.0, (sim - base) / (1.0 - base))


# ------------- DATA ACCESS -------------

def get_user_msg_data(name, limit=5000):
    sqlquery = """
        SELECT u.user_id, u.nick, u.is_bot
        FROM users AS u
        WHERE u.name = ?
    """
    cursor.execute(sqlquery, (name,))
    user_info = cursor.fetchall()[0]

    user_id = user_info[0]

    sqlquery = """
        SELECT m.type, m.message, m.timestamp, m.mentions
        FROM messages AS m
        WHERE m.author_id = ?
        ORDER BY timestamp DESC
        LIMIT ?;
    """
    cursor.execute(sqlquery, (user_id, limit))
    return cursor.fetchall()  # list of (type, message, timestamp, mentions)


# ------------- LEXICAL SIMILARITY -------------

def extract_words(msgs, max_msgs=5000, batch_size=256):
    msgs = msgs[:max_msgs]

    noun_counter = Counter()
    verb_counter = Counter()
    adj_counter = Counter()
    non_stop = Counter()

    for doc in nlp.pipe(msgs, batch_size=batch_size):
        for token in doc:
            if not token.is_alpha:
                continue

            lemma = token.lemma_.lower()

            if not token.is_stop:
                non_stop[lemma] += 1

            if token.pos_ in ("NOUN", "PROPN"):
                noun_counter[lemma] += 1
            elif token.pos_ == "VERB":
                verb_counter[lemma] += 1
            elif token.pos_ == "ADJ":
                adj_counter[lemma] += 1

    return noun_counter, verb_counter, adj_counter, non_stop


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
        0.25 * adj_sim +
        0.20 * nonstop_sim
    )

    return lex_sim, (noun_sim, verb_sim, adj_sim, nonstop_sim)


# ------------- THREEGRAMS -------------

def get_threegrams(msgs):
    grams = Counter()
    for msg in msgs:
        if not msg:
            continue
        text = msg.lower()
        text = re.sub(r"\s+", " ", text)
        for i in range(len(text) - 2):
            g = text[i:i+3]
            grams[g] += 1
    return grams


def threegram_similarity(grams_a, grams_b, top_k=200):
    top = (grams_a + grams_b).most_common(top_k)
    if not top:
        return 0.0
    vocab = [g for g, _ in top]

    va = np.array([grams_a[g] for g in vocab], dtype=float)
    vb = np.array([grams_b[g] for g in vocab], dtype=float)

    na = np.linalg.norm(va)
    nb = np.linalg.norm(vb)
    if na == 0 or nb == 0:
        return 0.0

    return float((va @ vb) / (na * nb))


# ------------- STYLE STATS (get_avgs) -------------

def get_avgs(msgs, batch_size=256):
    total_msgs = len(msgs)
    if total_msgs == 0:
        return np.zeros(6, dtype=float)

    links = 0
    tokens = 0
    puncts = 0
    digits = 0
    stops = 0
    uppers = 0

    for doc in nlp.pipe(msgs, batch_size=batch_size):
        for token in doc:
            if token.is_digit:
                digits += 1
            if token.is_punct:
                puncts += 1
            if token.like_url:
                links += 1
            if token.is_stop:
                stops += 1
            if token.is_upper:
                uppers += 1
            tokens += 1

    tokens = max(tokens, 1)

    link_ratio = links / total_msgs
    avg_tokens = tokens / total_msgs
    avg_puncts = puncts / total_msgs
    avg_digits = digits / total_msgs
    avg_stops = stops / tokens
    avg_uppers = uppers / total_msgs

    return np.array([
        link_ratio,
        avg_tokens,
        avg_puncts,
        avg_digits,
        avg_stops,
        avg_uppers
    ], dtype=float)


# ------------- TIME-OF-DAY SIMILARITY -------------

def hour_hist(timestamps):
    """Build normalized 24-bin histogram (UTC hours)."""
    if not timestamps:
        return np.zeros(24, dtype=float)

    hours = [datetime.utcfromtimestamp(ts).hour for ts in timestamps]
    hist = np.bincount(hours, minlength=24).astype(float)
    total = hist.sum()
    if total == 0:
        return hist
    return hist / total


def time_similarity(ts1, ts2):
    h1 = hour_hist(ts1)
    h2 = hour_hist(ts2)
    return cosine(h1, h2)


# ------------- FINAL COMBINE + REPORT -------------

def combine_scores(lex_sim, three_sim, style_sim, time_sim):
    three_scaled = zoom(three_sim, base=0.95)

    return (
        0.45 * lex_sim +
        0.30 * three_scaled +
        0.15 * style_sim +
        0.10 * time_sim
    )


def compare_users(name1, name2, limit=5000):
    user_data1 = get_user_msg_data(name1, limit=limit)
    user_data2 = get_user_msg_data(name2, limit=limit)

    msgs1 = [row[1] for row in user_data1 if row[1]]
    ts1   = [row[2] for row in user_data1]

    msgs2 = [row[1] for row in user_data2 if row[1]]
    ts2   = [row[2] for row in user_data2]

    # Lexical
    lex_sim, (noun_sim, verb_sim, adj_sim, nonstop_sim) = get_lex_sim(msgs1, msgs2)

    # Threegrams
    grams1 = get_threegrams(msgs1)
    grams2 = get_threegrams(msgs2)
    three_sim = threegram_similarity(grams1, grams2, top_k=200)

    # Style stats
    avg1 = get_avgs(msgs1)
    avg2 = get_avgs(msgs2)
    style_sim = cosine(avg1, avg2)

    # Time-of-day
    t_sim = time_similarity(ts1, ts2)

    # Final score
    alt_score = combine_scores(lex_sim, three_sim, style_sim, t_sim)

    print(f"Comparing '{name1}' vs '{name2}' (limit={limit} msgs each)")
    print("  Lexical POS+vocab similarity   :", lex_sim)
    print("    noun_sim                      :", noun_sim)
    print("    verb_sim                      :", verb_sim)
    print("    adj_sim                       :", adj_sim)
    print("    non_stop_sim                  :", nonstop_sim)
    print("  Threegram similarity (raw)      :", three_sim)
    print("  Style stat similarity           :", style_sim)
    print("  Time-of-day similarity          :", t_sim)
    print("  ----")
    print("  Combined ALT SCORE              :", alt_score)
    return alt_score


# ------------- EXAMPLE CALL -------------

if __name__ == "__main__":
    compare_users("bignutty", "smallnutty", limit=5000)
