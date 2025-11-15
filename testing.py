import spacy
import sqlite3
spacy.require_gpu(0)
nlp = spacy.load("output_trf/model-best")
connection = sqlite3.connect("messages.sqlite")
cursor = connection.cursor()


print("ENTS:", [(ent.text, ent.label_) for ent in doc.ents])
