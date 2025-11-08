# queries.py
import sqlite3
from datetime import datetime
import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize

nltk.download('stopwords')
nltk.download('punkt')

# Sample text
text = "This is a sample sentence showing stopword removal."

# Get English stopwords and tokenize
stop_words = set(stopwords.words('english'))
placeholders = ",".join("?" for _ in stop_words)

def top_tokens_overall(db_path="tokens.sqlite", limit=50):
    con = sqlite3.connect(db_path)
    cur = con.cursor()
    cur.execute("""
        SELECT t.token, SUM(utc.count) AS total
        FROM user_tokens_count AS utc
        JOIN tokens AS t ON t.id = utc.token_id
        GROUP BY utc.token_id
        ORDER BY total DESC
        LIMIT ?
    """, (limit,))
    rows = cur.fetchall()
    con.close()
    return rows

def top_tokens_for_user(user_id, db_path="tokens.sqlite", limit=50):
    con = sqlite3.connect(db_path)
    cur = con.cursor()
    cur.execute("""
        SELECT t.token, utc.count
        FROM user_tokens_count AS utc
        JOIN tokens AS t ON t.id = utc.token_id
        WHERE utc.user_id = ?
        ORDER BY utc.count DESC
        LIMIT ?
    """, (user_id, limit))
    rows = cur.fetchall()
    con.close()
    return rows

def top_tokens_for_users(user_ids, db_path="tokens.sqlite", limit=50):
    con = sqlite3.connect(db_path)
    cur = con.cursor()
    placeholders = ",".join("?" for _ in user_ids)
    sql = f"""
        SELECT t.token, SUM(utc.count) AS total
        FROM user_tokens_count AS utc
        JOIN tokens AS t ON t.id = utc.token_id
        WHERE utc.user_id IN ({placeholders})M
        GROUP BY utc.token_id
        ORDER BY total DESC
        LIMIT ?
    """
    cur.execute(sql, (*user_ids, limit))
    rows = cur.fetchall()
    con.close()
    return rows

def amount_said(db_path="tokens.sqlite", word="bla"):
    con = sqlite3.connect(db_path)
    cur = con.cursor()
    cur.execute("""
        SELECT t.token, SUM(utc.count) AS total
        FROM user_tokens_count AS utc
        JOIN tokens AS t ON t.id = utc.token_id
        WHERE t.token IN (?)
        GROUP BY utc.token_id
        ORDER BY total DESC
    """, (word,))
    rows = cur.fetchall()
    con.close()
    return rows

def top_tokens_time_overall(db_path="tokens.sqlite", limit=50, start="2016-01-01T00:00:00", end="2026-01-01T00:00:00"):
    con = sqlite3.connect(db_path)
    cur = con.cursor()
    cur.execute(f"""
        SELECT t.token, SUM(utc.count) AS total
        FROM user_tokens_count AS utc
        JOIN tokens AS t ON t.id = utc.token_id
        WHERE utc.timestamp BETWEEN ? AND ? AND t.token NOT IN ({placeholders})
        GROUP BY utc.token_id
        ORDER BY total DESC
        LIMIT ? 
    """, (start, end, *stop_words, limit,))
    rows = cur.fetchall()
    con.close()
    return rows

if __name__ == "__main__":

    start = input("Start date: ")
    end = input("End: ")

    if start != "" and end != "":
        start_date = int(datetime.fromisoformat(start+"T00:00:00").timestamp())
        end_date = int(datetime.fromisoformat(end+"T00:00:00").timestamp())
        print(top_tokens_time_overall(start=start_date, end=end_date))

    else:
        print(top_tokens_time_overall())


    #word = input("Word: ").lower()
    #print("used", amount_said(word=word), " times")
    #print("Top overall:", top_tokens_overall()[:100])
    #print("Top for user 123:", top_tokens_for_user("123")[:10])
    #print("Top for users [123,456]:", top_tokens_for_users(["123", "456"])[:10])
