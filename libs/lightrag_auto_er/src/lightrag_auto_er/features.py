import re
import unicodedata
from typing import Any

def generate_hybrid_acronym(text: Any) -> str:
    """
    Generates an acronym based on Capitals OR Start of Words.
    Example: "Abu Dhabi" -> "AD"
    Example: "Ministry of Finance" -> "MOF"
    """
    if not isinstance(text, str) or not text:
        return ""
    
    # Clean special chars, replace with space to preserve word boundaries
    clean_text = re.sub(r'[^\w\s]', ' ', text)
    acronym = []
    is_start_of_word = True
    
    for char in clean_text:
        if char.isspace():
            is_start_of_word = True
            continue
        # Rule: Include if Capital OR Start of Word
        if char.isupper() or is_start_of_word:
            acronym.append(char.upper())
        is_start_of_word = False
        
    return "".join(acronym).lower() 

def normalise_text_preserved(value: Any) -> str:
    if not isinstance(value, str):
        return ""
    
    v = value.lower()
    v = unicodedata.normalize("NFKD", v)
    v = "".join(c for c in v if not unicodedata.combining(c))
    
    # --- CHANGED LINE ---
    # We added \. \@ \+ \# \- \_ \/ \: to the brackets.
    # This tells Python: "Do NOT replace these specific symbols with a space."
    v = re.sub(r"[^a-z0-9\s\.\@\+\#\-\_\/\:]", " ", v)
    # --------------------

    v = re.sub(r"\s+", " ", v).strip()
    return v
