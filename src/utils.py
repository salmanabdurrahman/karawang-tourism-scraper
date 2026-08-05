"""
Shared pure utility functions (no browser, filesystem, or network access).

Small text/domain helpers extracted from the pipeline scripts so they can be
imported and tested independently. Importing this module has no side effects:
Sastrawi factories and the stopword set are built lazily on first use.

Text helpers preserve existing data contracts while exposing the shared NLP
pipeline used by the content-based recommender.

Author: Salman Abdurrahman
Date: 2025
"""

import hashlib
import re
from datetime import datetime, timedelta
from typing import List

# NOTE: nltk / Sastrawi are imported lazily inside the functions that need
# them, so importing this module stays side-effect free and does not pull in
# heavy NLP dependencies for scripts that only use the text helpers.

# Custom stopwords (slang & common words in tourism reviews)
CUSTOM_STOPWORDS = [
    "yg",
    "ga",
    "gak",
    "kalo",
    "klo",
    "sy",
    "aku",
    "saya",
    "kamu",
    "dia",
    "ini",
    "itu",
    "di",
    "ke",
    "dari",
    "dan",
    "atau",
    "tapi",
    "jadi",
    "jdi",
    "bgt",
    "banget",
    "aja",
    "saja",
    "ada",
    "buat",
    "cuma",
    "dgn",
    "dg",
    "sdh",
    "sudah",
    "blm",
    "belum",
    "dlu",
    "dulu",
    "deh",
    "dong",
    "kok",
    "sih",
    "nih",
    "tuh",
    "nya",
    "dr",
    "utk",
    "untuk",
    "sm",
    "sama",
    "banyak",
    "tempat",
    "wisata",
    "nya",
    "lagi",
    "karena",
    "sangat",
    "agak",
    "lumayan",
    "ok",
    "oke",
    "bagus",
    "keren",
    "mantap",
    "recommended",
    "rekomen",
]

# The paper's normalization examples include these forms. The mapping also
# covers common Indonesian review abbreviations so documents and user queries
# pass through exactly the same normalization step.
WORD_NORMALIZATION = {
    "tau": "tahu",
    "buat": "untuk",
    "yg": "yang",
    "ga": "tidak",
    "gak": "tidak",
    "kalo": "kalau",
    "klo": "kalau",
    "sy": "saya",
    "jdi": "jadi",
    "bgt": "banget",
    "dgn": "dengan",
    "dg": "dengan",
    "sdh": "sudah",
    "blm": "belum",
    "dlu": "dulu",
    "dr": "dari",
    "utk": "untuk",
    "sm": "sama",
}

# Sastrawi's default list contains several useful descriptive terms. Keep
# those terms so TF-IDF can use them as content signals, matching the paper's
# examples where words such as "bagus", "banyak", "tempat", and "untuk"
# remain after stopword removal.
CONTENT_WORDS_TO_KEEP = {
    "bagus",
    "banyak",
    "tempat",
    "untuk",
    "keren",
    "mantap",
    "recommended",
    "rekomen",
}

_stopwords_set = None
_stemmer = None
_stem_cache = {}


def _get_stopwords_set() -> set:
    """
    Builds the Indonesian stopword set once (Sastrawi base + custom words).

    Returns:
        set: Stopword set used by remove_stopwords.
    """
    global _stopwords_set
    if _stopwords_set is None:
        from Sastrawi.StopWordRemover.StopWordRemoverFactory import StopWordRemoverFactory

        stopwords_indo = set(StopWordRemoverFactory().get_stop_words())
        stopwords_indo.update(CUSTOM_STOPWORDS)
        stopwords_indo.difference_update(CONTENT_WORDS_TO_KEEP)
        _stopwords_set = stopwords_indo
    return _stopwords_set


def _get_stemmer():
    """
    Builds the Sastrawi stemmer once.

    Returns:
        Stemmer: Sastrawi stemmer instance used by stemming.
    """
    global _stemmer
    if _stemmer is None:
        from Sastrawi.Stemmer.StemmerFactory import StemmerFactory

        _stemmer = StemmerFactory().create_stemmer()
    return _stemmer


# ---------------------------------------------------------------- text cleaning
def clean_text(text: str) -> str:
    """
    Cleans text from special characters and Google Maps artifacts.

    Removes common encoding issues and normalizes whitespace.

    Args:
        text (str): Raw text to clean.

    Returns:
        str: Cleaned text with normalized whitespace, or "" for non-str input.
    """
    if not isinstance(text, str):
        return ""

    # Remove Google Maps specific artifacts
    artifacts = ["Óóä", "¬†", "", "", ""]
    for artifact in artifacts:
        text = text.replace(artifact, "")

    # Normalize whitespace
    text = re.sub(r"\s+", " ", text).strip()

    return text


def clean_attributes(text: str) -> str:
    """
    Cleans and formats place attributes text.

    Removes leading special characters and formats as comma-separated list.

    Args:
        text (str): Raw attributes text with pipe separators.

    Returns:
        str: Comma-separated cleaned attributes, or "" for non-str input.
    """
    if not isinstance(text, str):
        return ""

    text = clean_text(text)
    items = text.split("|")

    clean_items = []
    for item in items:
        # Remove leading non-alphanumeric characters
        cleaned = re.sub(r"^[^a-zA-Z0-9]+", "", item).strip()
        if cleaned:
            clean_items.append(cleaned)

    return ", ".join(clean_items)


# ---------------------------------------------------------------- user anonymization
def anonymize_user(user_name: str) -> str:
    """
    Anonymizes user name using MD5 hashing.

    Args:
        user_name (str): Original user name.

    Returns:
        str: First 10 characters of MD5 hash, or "anonymous" if empty.
    """
    if not isinstance(user_name, str) or not user_name:
        return "anonymous"

    user_name = user_name.strip().lower()
    hash_object = hashlib.md5(user_name.encode("utf-8"))

    return hash_object.hexdigest()[:10]


# ---------------------------------------------------------------- timestamp conversion
def convert_relative_time(text: str) -> str:
    """
    Converts relative time text to ISO date format.

    Handles various Indonesian time expressions like:
    - "2 jam yang lalu" -> date 2 hours ago
    - "3 hari yang lalu" -> date 3 days ago
    - "1 minggu yang lalu" -> date 1 week ago
    - "2 bulan yang lalu" -> date 2 months ago
    - "1 tahun yang lalu" -> date 1 year ago

    Args:
        text (str): Relative time text in Indonesian.

    Returns:
        str: ISO date string (YYYY-MM-DD), empty string if parsing fails.
    """
    if not isinstance(text, str) or not text:
        return ""

    text = text.lower().replace("diedit", "").strip()
    current_time = datetime.now()
    delta = timedelta(0)

    try:
        # Recent times (minutes, seconds, just now)
        if any(word in text for word in ["menit", "detik", "baru saja"]):
            delta = timedelta(days=0)

        # Hours
        elif "jam" in text:
            match = re.search(r"(\d+)", text)
            hours = int(match.group(1)) if match else 1
            delta = timedelta(hours=hours)

        # Days
        elif "hari" in text:
            match = re.search(r"(\d+)", text)
            days = int(match.group(1)) if match else 1
            delta = timedelta(days=days)

        # Weeks
        elif "minggu" in text:
            match = re.search(r"(\d+)", text)
            weeks = int(match.group(1)) if match else 1
            delta = timedelta(weeks=weeks)

        # Months (approximate: 30 days per month)
        elif "bulan" in text:
            match = re.search(r"(\d+)", text)
            months = int(match.group(1)) if match else 1
            delta = timedelta(days=months * 30)

        # Years (approximate: 365 days per year)
        elif "tahun" in text:
            match = re.search(r"(\d+)", text)
            years = int(match.group(1)) if match else 1
            delta = timedelta(days=years * 365)

        past_date = current_time - delta
        return past_date.strftime("%Y-%m-%d")

    except Exception:
        return ""


def parse_int_from_text(text: str) -> int:
    """
    Extracts integer from text by removing all non-digit characters.

    Args:
        text (str): Text containing numbers.

    Returns:
        int: Extracted integer, 0 if no digits found or non-str input.
    """
    if not isinstance(text, str):
        return 0

    nums = re.sub(r"\D", "", text)
    return int(nums) if nums else 0


# ---------------------------------------------------------------- filename/name normalization
def sanitize_filename(filename: str) -> str:
    """
    Sanitizes a string to be used as a safe filename.

    Keeps alphanumeric characters plus space, hyphen, and underscore.

    Args:
        filename (str): Original filename.

    Returns:
        str: Sanitized filename (empty if only invalid characters).
    """
    safe_chars = [c for c in filename if c.isalnum() or c in (" ", "-", "_")]
    return "".join(safe_chars).strip()


def clean_name_key(text: str) -> str:
    """
    Cleans a name to be used as a matching key (Join Key).

    Args:
        text (str): Raw place name.

    Returns:
        str: Normalized key (lowercase, alphanumeric only), "" for non-str input.
    """
    if not isinstance(text, str):
        return ""
    text = text.lower()
    # Remove leftover artifacts from old scraping (if any)
    text = text.replace("óóä", "").replace("¬†", "")
    # Remove non-alphanumeric characters to make matching easier
    text = re.sub(r"[^a-z0-9]", "", text)
    return text


def clean_display_name(text: str) -> str:
    """
    Cleans a place name so it displays neatly:
    1. Remove the words 'Wisata' and 'Karawang'
    2. Remove non-letter characters at start/end
    3. Convert to Title Case

    Args:
        text (str): Raw place name.

    Returns:
        str: Cleaned display name, "" for non-str input.
    """
    if not isinstance(text, str):
        return ""

    # Remove leftover artifacts first
    text = text.replace("Óóä", "").replace("¬†", "")

    # Remove the words 'wisata' and 'karawang' (case insensitive)
    text = re.sub(r"\b(wisata|karawang)\b", "", text, flags=re.IGNORECASE)

    # Remove stray punctuation/digits at start or end of the string
    text = re.sub(r"^[\W_]+|[\W_]+$", "", text)

    # Normalize whitespace
    text = re.sub(r"\s+", " ", text).strip()

    # Convert to Title Case
    return text.title()


# ---------------------------------------------------------------- NLP pipeline steps
def case_folding(text: str) -> str:
    """
    Normalizes text for NLP: lowercase, digits and symbols removed.

    Args:
        text (str): Raw text.

    Returns:
        str: Folded text (letters and spaces only), "" for non-str input.
    """
    if not isinstance(text, str):
        return ""
    text = text.lower()
    # Remove digits
    text = re.sub(r"\d+", "", text)
    # Remove symbols, punctuation, emoji (keep letters & spaces only)
    text = re.sub(r"[^a-z\s]", "", text)
    # Remove extra whitespace
    text = re.sub(r"\s+", " ", text).strip()
    return text


def tokenizing(text: str) -> List[str]:
    """
    Tokenizes text into words using NLTK.

    Args:
        text (str): Cleaned text.

    Returns:
        list of str: Tokens, or [] for empty/falsy input.
    """
    if not text:
        return []
    from nltk.tokenize import word_tokenize

    return word_tokenize(text)


def normalize_tokens(tokens: List[str]) -> List[str]:
    """
    Normalizes Indonesian spelling variants and common review abbreviations.

    Args:
        tokens (list of str): Case-folded tokens.

    Returns:
        list of str: Tokens translated through ``WORD_NORMALIZATION`` when a
        mapping exists.
    """
    return [WORD_NORMALIZATION.get(token, token) for token in tokens]


def remove_stopwords(tokens: List[str]) -> List[str]:
    """
    Removes Indonesian stopwords while retaining descriptive content terms.

    Args:
        tokens (list of str): Normalized tokens.

    Returns:
        list of str: Tokens without stopwords.
    """
    stopwords = _get_stopwords_set()
    return [word for word in tokens if word not in stopwords]


def stemming(tokens: List[str]) -> List[str]:
    """
    Stems tokens using Sastrawi with a process-local token cache.

    Stemming each token preserves the output of Sastrawi for this word-level
    pipeline while avoiding repeated work across review documents. The cache
    is built lazily, so importing this module remains side-effect free.

    Args:
        tokens (list of str): Tokens to stem.

    Returns:
        list of str: Stemmed tokens.
    """
    global _stem_cache
    stemmer = _get_stemmer()
    stemmed_tokens = []

    for token in tokens:
        if token not in _stem_cache:
            _stem_cache[token] = stemmer.stem(token)
        stemmed_tokens.append(_stem_cache[token])

    return stemmed_tokens


def preprocess_text(text: str) -> str:
    """
    Runs the shared Indonesian preprocessing pipeline for documents and queries.

    The order follows the reference paper: case folding, tokenizing, word
    normalization, stopword removal, and stemming.

    Args:
        text (str): Raw document or user query.

    Returns:
        str: Space-separated stemmed tokens.
    """
    folded = case_folding(text)
    tokens = tokenizing(folded)
    tokens = normalize_tokens(tokens)
    tokens = remove_stopwords(tokens)
    return " ".join(stemming(tokens))
