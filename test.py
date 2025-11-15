import spacy
from spacy.tokens import DocBin

# Which file to inspect:
FILENAME = "discord_usernames_dev.spacy"

# Load DocBin
db = DocBin().from_disk(FILENAME)

# Need a blank vocab to reconstruct docs
nlp = spacy.blank("en")

# Convert to docs
docs = list(db.get_docs(nlp.vocab))

print(f"Loaded {len(docs)} docs")

# Show first N examples
N = 20

for i, doc in enumerate(docs[:N]):
    print("-" * 60)
    print(f"Doc {i}:")
    print("TEXT:", doc.text)
    
    print("TOKENS:", [t.text for t in doc])
    
    print("ENTS:", [(ent.text, ent.start_char, ent.end_char, ent.label_) for ent in doc.ents])