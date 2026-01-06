"""Vendor name normalization utilities."""

import re
from typing import Optional


# Common business suffixes to standardize
BUSINESS_SUFFIXES = {
    r"\bllc\b": "LLC",
    r"\bl\.l\.c\.\b": "LLC",
    r"\binc\b": "INC",
    r"\binc\.\b": "INC",
    r"\bincorporated\b": "INC",
    r"\bcorp\b": "CORP",
    r"\bcorp\.\b": "CORP",
    r"\bcorporation\b": "CORP",
    r"\bco\b": "CO",
    r"\bco\.\b": "CO",
    r"\bcompany\b": "CO",
    r"\bltd\b": "LTD",
    r"\bltd\.\b": "LTD",
    r"\blimited\b": "LTD",
    r"\blp\b": "LP",
    r"\bl\.p\.\b": "LP",
    r"\bllp\b": "LLP",
    r"\bl\.l\.p\.\b": "LLP",
    r"\bpllc\b": "PLLC",
    r"\bp\.l\.l\.c\.\b": "PLLC",
    r"\bpc\b": "PC",
    r"\bp\.c\.\b": "PC",
    r"\bdba\b": "DBA",
    r"\bd/b/a\b": "DBA",
    r"\bd\.b\.a\.\b": "DBA",
}

# Words to remove (common filler words that don't help matching)
REMOVE_WORDS = {
    "the", "of", "and", "&", "for", "a", "an",
}

# Common abbreviation expansions
ABBREVIATIONS = {
    "intl": "INTERNATIONAL",
    "int'l": "INTERNATIONAL",
    "natl": "NATIONAL",
    "nat'l": "NATIONAL",
    "svcs": "SERVICES",
    "svc": "SERVICE",
    "mgmt": "MANAGEMENT",
    "mgt": "MANAGEMENT",
    "assoc": "ASSOCIATES",
    "assn": "ASSOCIATION",
    "grp": "GROUP",
    "sys": "SYSTEMS",
    "tech": "TECHNOLOGY",
    "techs": "TECHNOLOGIES",
    "govt": "GOVERNMENT",
    "gov": "GOVERNMENT",
    "univ": "UNIVERSITY",
    "hosp": "HOSPITAL",
    "med": "MEDICAL",
    "ctr": "CENTER",
    "cntr": "CENTER",
}


def normalize_vendor_name(name: Optional[str]) -> Optional[str]:
    """
    Normalize a vendor name for matching purposes.

    Transformations:
    - Convert to uppercase
    - Remove extra whitespace
    - Standardize business suffixes
    - Expand common abbreviations
    - Remove punctuation except essential chars
    - Remove common filler words

    Args:
        name: Original vendor name

    Returns:
        Normalized name, or None if input is None/empty
    """
    if not name:
        return None

    # Start with uppercase
    normalized = name.upper().strip()

    # Remove extra whitespace
    normalized = re.sub(r"\s+", " ", normalized)

    # Standardize business suffixes
    for pattern, replacement in BUSINESS_SUFFIXES.items():
        normalized = re.sub(pattern, replacement, normalized, flags=re.IGNORECASE)

    # Expand abbreviations
    words = normalized.split()
    words = [ABBREVIATIONS.get(w.lower(), w) for w in words]
    normalized = " ".join(words)

    # Remove most punctuation (keep apostrophes in names, hyphens)
    normalized = re.sub(r"[.,;:!?\"()[\]{}]", "", normalized)

    # Standardize ampersands
    normalized = re.sub(r"\s*&\s*", " AND ", normalized)

    # Remove filler words (but be careful not to remove if it's the whole name)
    words = normalized.split()
    if len(words) > 1:
        words = [w for w in words if w.lower() not in REMOVE_WORDS]
    normalized = " ".join(words)

    # Final whitespace cleanup
    normalized = re.sub(r"\s+", " ", normalized).strip()

    return normalized if normalized else None


def extract_name_components(name: str) -> dict:
    """
    Extract structured components from a vendor name.

    Returns:
        Dict with keys: base_name, suffix, dba_name
    """
    result = {
        "base_name": name,
        "suffix": None,
        "dba_name": None,
    }

    if not name:
        return result

    upper = name.upper()

    # Check for DBA
    dba_match = re.search(r"\b(?:DBA|D/B/A|D\.B\.A\.)\s+(.+)$", upper, re.IGNORECASE)
    if dba_match:
        result["dba_name"] = dba_match.group(1).strip()
        upper = upper[:dba_match.start()].strip()

    # Extract suffix
    suffix_pattern = r"\b(LLC|INC|CORP|CO|LTD|LP|LLP|PLLC|PC)\.?\s*$"
    suffix_match = re.search(suffix_pattern, upper, re.IGNORECASE)
    if suffix_match:
        result["suffix"] = suffix_match.group(1).upper()
        upper = upper[:suffix_match.start()].strip()

    # Clean up trailing commas/punctuation
    upper = re.sub(r"[,.\s]+$", "", upper)

    result["base_name"] = upper

    return result


def generate_name_variants(name: str) -> list[str]:
    """
    Generate potential name variants for fuzzy matching.

    Useful for finding vendors that might be the same entity
    under slightly different names.
    """
    variants = set()
    normalized = normalize_vendor_name(name)

    if not normalized:
        return []

    variants.add(normalized)

    # Without suffix
    components = extract_name_components(normalized)
    variants.add(components["base_name"])

    # Common typo patterns: missing spaces, extra spaces
    variants.add(normalized.replace(" ", ""))

    # With/without "THE" prefix
    if normalized.startswith("THE "):
        variants.add(normalized[4:])
    else:
        variants.add("THE " + normalized)

    return list(variants)
