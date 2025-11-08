# queries.py
import sqlite3
from datetime import datetime
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

def top_tokens_time_overall(db_path="tokens.sqlite", limit=10, start="2016-01-01T00:00:00", end="2026-01-01T00:00:00"):
    con = sqlite3.connect(db_path)
    cur = con.cursor()
    cur.execute("""
        SELECT t.token, SUM(utc.count) AS total
        FROM user_tokens_count AS utc
        JOIN tokens AS t ON t.id = utc.token_id
        WHERE utc.timestamp BETWEEN ? AND ?
        GROUP BY utc.token_id
        ORDER BY total DESC
        LIMIT ? 
    """, (start, end, limit,))
    rows = cur.fetchall()
    con.close()
    return rows

if __name__ == "__main__":

    start = input("Start date: ")
    end = input("End: ")

    start_date = int(datetime.fromisoformat(start+"T00:00:00").timestamp())
    end_date = int(datetime.fromisoformat(end+"T00:00:00").timestamp())

    print(start_date)
    print(end_date)

    print(top_tokens_time_overall(start=start_date, end=end_date))

    #word = input("Word: ").lower()
    #print("used", amount_said(word=word), " times")
    #print("Top overall:", top_tokens_overall()[:100])
    #print("Top for user 123:", top_tokens_for_user("123")[:10])
    #print("Top for users [123,456]:", top_tokens_for_users(["123", "456"])[:10])
