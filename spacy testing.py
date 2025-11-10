import ijson
from itertools import islice
from collections import Counter
import sqlite3
import re
import json
from datetime import datetime, timezone
import spacy


connection = sqlite3.connect('messages.sqlite')

cursor = connection.cursor()

sqlquery = """
    SELECT
"""
cursor.execute()