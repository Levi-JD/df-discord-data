import sqlite3
import re
from datetime import datetime
from collections import Counter
from functools import lru_cache

import spacy
import numpy as np

# ------------- CONFIG -------------

MIN_MSGS = 1000   # only compare users with at least this many messages
MAX_MSGS = 3000   # max messages per user

D_LEX   = 2048    # size of lexical hashed vector
D_3GRAM = 2048    # size of 3-gram hashed vector

# weights roughly matching your old combine_scores
W_LEX   = 0.45
W_3GRAM = 0.30
W_STYLE = 0.15
W_TIME  = 0.10


# ------------- SETUP -------------

spacy.require_cpu()
nlp = spacy.load("en_core_web_sm")

connection = sqlite3.connect("messages.sqlite")
cursor = connection.cursor()


# ------------- UTILS -------------

def cosine_matrix(mat):
    """Row-wise cosine similarity matrix: (n, d) -> (n, n)."""
    norms = np.linalg.norm(mat, axis=1, keepdims=True)
    norms[norms == 0] = 1.0
    m_norm = mat / norms
    return m_norm @ m_norm.T


def l2_normalize(vec):
    n = np.linalg.norm(vec)
    if n == 0:
        return vec
    return vec / n


# ------------- DB ACCESS -------------

def get_active_users(min_msgs=MIN_MSGS):
    """
    Return list of (user_id, name, msg_count) for all non-bot users
    with at least min_msgs messages.
    """
    sqlquery = """
        SELECT u.user_id, u.name, COUNT(*) AS msg_count
        FROM users AS u
        JOIN messages AS m ON m.author_id = u.user_id
        WHERE u.is_bot = 0
        GROUP BY u.user_id, u.name
        HAVING msg_count >= ?
        ORDER BY msg_count DESC
    """
    cursor.execute(sqlquery, (min_msgs,))
    return cursor.fetchall()


def get_user_msgs_and_ts(user_id):
    """
    Fetch up to MAX_MSGS most recent messages + timestamps for a user.
    Returns (msgs, timestamps).
    """
    sqlquery = """
        SELECT m.message, m.timestamp
        FROM messages AS m
        WHERE m.author_id = ?
        ORDER BY m.timestamp DESC
        LIMIT ?;
    """
    cursor.execute(sqlquery, (user_id, MAX_MSGS))
    rows = cursor.fetchall()
    msgs = [row[0] for row in rows if row[0]]
    ts   = [row[1] for row in rows]
    return msgs, ts


# ------------- FEATURE EXTRACTORS (per user) -------------

def hashed_lex_and_style(msgs):
    """
    One spaCy pass:
      - build hashed lexical vector (lemmas, non-stop, alpha)
      - build style vector [link_ratio, avg_tokens, avg_puncts,
                           avg_digits, avg_stops_ratio, avg_uppers]
    """
    lex_vec = np.zeros(D_LEX, dtype=float)

    total_msgs = len(msgs)
    if total_msgs == 0:
        style_vec = np.zeros(6, dtype=float)
        return lex_vec, style_vec

    links = 0
    tokens = 0
    puncts = 0
    digits = 0
    stops = 0
    uppers = 0

    for doc in nlp.pipe(msgs, batch_size=256):
        # style + lexical in one pass
        for token in doc:
            # style stats
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

            # lexical vector: lemma of non-stop alphabetic tokens
            if token.is_alpha and not token.is_stop:
                lemma = token.lemma_.lower()
                idx = hash(lemma) % D_LEX
                lex_vec[idx] += 1.0

    tokens = max(tokens, 1)

    link_ratio = links / total_msgs
    avg_tokens = tokens / total_msgs
    avg_puncts = puncts / total_msgs
    avg_digits = digits / total_msgs
    avg_stops = stops / tokens
    avg_uppers = uppers / total_msgs

    style_vec = np.array([
        link_ratio,
        avg_tokens,
        avg_puncts,
        avg_digits,
        avg_stops,
        avg_uppers,
    ], dtype=float)

    return lex_vec, style_vec


def hashed_threegrams(msgs):
    """
    Hashed bag of character 3-grams over all messages.
    """
    vec = np.zeros(D_3GRAM, dtype=float)
    for msg in msgs:
        if not msg:
            continue
        text = msg.lower()
        text = re.sub(r"\s+", " ", text)
        for i in range(len(text) - 2):
            g = text[i:i+3]
            idx = hash(g) % D_3GRAM
            vec[idx] += 1.0
    return vec


def hour_hist(timestamps):
    """
    24-dim normalized histogram of UTC hours.
    """
    hist = np.zeros(24, dtype=float)
    if not timestamps:
        return hist
    hours = [datetime.utcfromtimestamp(ts).hour for ts in timestamps]
    hist = np.bincount(hours, minlength=24).astype(float)
    total = hist.sum()
    if total == 0:
        return hist
    return hist / total


# ------------- USER EMBEDDING -------------

def build_user_embedding(user_id):
    """
    Creates one big weighted vector for a user:
       [ sqrt(W_LEX)*norm(lex),
         sqrt(W_3GRAM)*norm(three),
         sqrt(W_STYLE)*norm(style),
         sqrt(W_TIME)*norm(time) ]
    """
    msgs, ts = get_user_msgs_and_ts(user_id)

    # spaCy-based (lexical + style) in one pass
    lex_vec, style_vec = hashed_lex_and_style(msgs)
    three_vec = hashed_threegrams(msgs)
    time_vec  = hour_hist(ts)

    # L2-normalize each block
    lex_vec   = l2_normalize(lex_vec)
    three_vec = l2_normalize(three_vec)
    style_vec = l2_normalize(style_vec)
    time_vec  = l2_normalize(time_vec)

    # weight blocks so cosine approx matches your old weighting
    sqrt_w_lex   = np.sqrt(W_LEX)
    sqrt_w_3gram = np.sqrt(W_3GRAM)
    sqrt_w_style = np.sqrt(W_STYLE)
    sqrt_w_time  = np.sqrt(W_TIME)

    lex_vec   *= sqrt_w_lex
    three_vec *= sqrt_w_3gram
    style_vec *= sqrt_w_style
    time_vec  *= sqrt_w_time

    # concat into one big vector
    return np.concatenate([lex_vec, three_vec, style_vec, time_vec])


# ------------- MAIN COMPARISON -------------

def compare_all_users_fast():
    users = get_active_users(MIN_MSGS)
    print(f"Found {len(users)} users with at least {MIN_MSGS} messages.")
    print(f"Using up to {MAX_MSGS} most recent messages per user.\n")

    if not users:
        return

    user_ids   = [u[0] for u in users]
    user_names = [u[1] for u in users]
    msg_counts = [u[2] for u in users]

    # build embedding matrix
    embeddings = []
    for uid, name, cnt in users:
        print(f"Building embedding for {name} (id={uid}, msgs={cnt})...")
        emb = build_user_embedding(uid)
        embeddings.append(emb)

    embeddings = np.stack(embeddings, axis=0)  # shape (N, D)

    print("\nComputing pairwise cosine similarity matrix...")
    sim_mat = cosine_matrix(embeddings)  # (N, N)

    # collect upper-triangular pairs
    n = len(users)
    results = []
    for i in range(n):
        for j in range(i + 1, n):
            score = float(sim_mat[i, j])
            results.append(((i, j), score))

    # sort by score descending
    results.sort(key=lambda x: x[1], reverse=True)

    print("\nTop pairs by combined cosine:")
    for (i, j), score in results[:20]:
        print(
            f"{user_names[i]} (msgs={msg_counts[i]})  <->  "
            f"{user_names[j]} (msgs={msg_counts[j]}) : {score:.4f}"
        )


if __name__ == "__main__":
    compare_all_users_fast()
