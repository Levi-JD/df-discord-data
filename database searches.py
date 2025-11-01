# queries.py
import sqlite3

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
        WHERE utc.user_id IN ({placeholders})
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

if __name__ == "__main__":
    pass
    #word = input("Word: ")
    #print("used", amount_said(word=word), " times")
    #print("Top overall:", top_tokens_overall()[:100])
    #print("Top for user 123:", top_tokens_for_user("123")[:10])
    #print("Top for users [123,456]:", top_tokens_for_users(["123", "456"])[:10])
