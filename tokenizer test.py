import re

# One-pass cleanup pattern (codeblocks, inline code, <:emoji:id>, mentions, URLs, :emoji_name:)
CLEANUP = re.compile(
    r"(?:```.*?```)"                      # triple code blocks
    r"|(?:`[^`\n]*`)"                     # inline code
    r"|(?:<a?:\w+:\d+>)"                  # <:name:id> / <a:name:id>
    r"|(?:<@!?(\d+)>|<@&(\d+)>|<#(\d+)>)" # mentions/roles/channels
    r"|(?:https?://\S+|www\.\S+)"         # URLs
    r"|(?<!\w):[a-z0-9_]{2,32}:(?!\w)",   # :emoji_name:
    re.DOTALL | re.I
)

# Words (ASCII letters/digits) vs single non-space symbols
TOKEN = re.compile(r"[0-9A-Za-z]+|[^\s0-9A-Za-z]")

def tokenize_msg(text: str):
    # remove unwanted artifacts in ONE pass
    text = CLEANUP.sub(" ", text)
    # find tokens (C-accelerated)
    toks = TOKEN.findall(text)
    # normalize (lower is faster than casefold and good for English-like text)
    for i in range(len(toks)):
        toks[i] = toks[i].lower()
    return toks

while True:
    print(tokenize_msg(input("msg: ")))