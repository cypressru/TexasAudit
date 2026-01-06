"""Address normalization utilities."""

import re
from typing import Optional, NamedTuple


class ParsedAddress(NamedTuple):
    """Parsed address components."""
    street: Optional[str]
    city: Optional[str]
    state: Optional[str]
    zip_code: Optional[str]
    normalized: str


# Street type abbreviations
STREET_TYPES = {
    "avenue": "AVE",
    "ave": "AVE",
    "boulevard": "BLVD",
    "blvd": "BLVD",
    "circle": "CIR",
    "cir": "CIR",
    "court": "CT",
    "ct": "CT",
    "drive": "DR",
    "dr": "DR",
    "expressway": "EXPY",
    "expy": "EXPY",
    "freeway": "FWY",
    "fwy": "FWY",
    "highway": "HWY",
    "hwy": "HWY",
    "lane": "LN",
    "ln": "LN",
    "parkway": "PKWY",
    "pkwy": "PKWY",
    "place": "PL",
    "pl": "PL",
    "road": "RD",
    "rd": "RD",
    "street": "ST",
    "st": "ST",
    "terrace": "TER",
    "ter": "TER",
    "trail": "TRL",
    "trl": "TRL",
    "way": "WAY",
}

# Directional abbreviations
DIRECTIONS = {
    "north": "N",
    "south": "S",
    "east": "E",
    "west": "W",
    "northeast": "NE",
    "northwest": "NW",
    "southeast": "SE",
    "southwest": "SW",
    "n": "N",
    "s": "S",
    "e": "E",
    "w": "W",
    "ne": "NE",
    "nw": "NW",
    "se": "SE",
    "sw": "SW",
}

# Unit type abbreviations
UNIT_TYPES = {
    "apartment": "APT",
    "apt": "APT",
    "building": "BLDG",
    "bldg": "BLDG",
    "floor": "FL",
    "fl": "FL",
    "suite": "STE",
    "ste": "STE",
    "unit": "UNIT",
    "room": "RM",
    "rm": "RM",
    "#": "UNIT",
}

# State abbreviations (for Texas and neighboring states)
STATE_ABBREVS = {
    "texas": "TX",
    "oklahoma": "OK",
    "new mexico": "NM",
    "arkansas": "AR",
    "louisiana": "LA",
}


def normalize_address(
    address: Optional[str],
    city: Optional[str] = None,
    state: Optional[str] = None,
    zip_code: Optional[str] = None,
) -> ParsedAddress:
    """
    Normalize an address for matching purposes.

    Can accept either a full address string or components separately.
    """
    if not address and not any([city, state, zip_code]):
        return ParsedAddress(None, None, None, None, "")

    # If components provided separately
    if city or state or zip_code:
        norm_street = _normalize_street(address) if address else None
        norm_city = city.upper().strip() if city else None
        norm_state = _normalize_state(state) if state else None
        norm_zip = _normalize_zip(zip_code) if zip_code else None

        parts = [p for p in [norm_street, norm_city, norm_state, norm_zip] if p]
        normalized = " ".join(parts)

        return ParsedAddress(norm_street, norm_city, norm_state, norm_zip, normalized)

    # Parse full address string
    return _parse_full_address(address)


def _normalize_street(street: str) -> str:
    """Normalize a street address."""
    normalized = street.upper().strip()

    # Remove extra whitespace
    normalized = re.sub(r"\s+", " ", normalized)

    # Standardize street types
    words = normalized.split()
    words = [STREET_TYPES.get(w.lower(), w) for w in words]

    # Standardize directions
    words = [DIRECTIONS.get(w.lower(), w) for w in words]

    # Standardize unit types
    words = [UNIT_TYPES.get(w.lower(), w) for w in words]

    # Remove periods
    normalized = " ".join(words).replace(".", "")

    # Standardize PO Box
    normalized = re.sub(r"\bP\.?O\.?\s*BOX\b", "PO BOX", normalized, flags=re.IGNORECASE)

    return normalized


def _normalize_state(state: str) -> str:
    """Normalize state to 2-letter abbreviation."""
    state = state.strip().lower()

    if len(state) == 2:
        return state.upper()

    return STATE_ABBREVS.get(state, state.upper()[:2])


def _normalize_zip(zip_code: str) -> str:
    """Normalize ZIP code to 5 digits."""
    # Remove any non-digits
    digits = re.sub(r"[^\d]", "", zip_code)

    # Take first 5 digits
    return digits[:5] if len(digits) >= 5 else digits


def _parse_full_address(address: str) -> ParsedAddress:
    """Parse a full address string into components."""
    if not address:
        return ParsedAddress(None, None, None, None, "")

    normalized = address.upper().strip()
    normalized = re.sub(r"\s+", " ", normalized)

    street = None
    city = None
    state = None
    zip_code = None

    # Try to extract ZIP code (5 digits, optionally with -4 extension)
    zip_match = re.search(r"\b(\d{5})(?:-\d{4})?\s*$", normalized)
    if zip_match:
        zip_code = zip_match.group(1)
        normalized = normalized[:zip_match.start()].strip()

    # Try to extract state (2 letters before ZIP or at end)
    state_match = re.search(r"\b([A-Z]{2})\s*$", normalized)
    if state_match:
        state = state_match.group(1)
        normalized = normalized[:state_match.start()].strip()

    # Remove trailing comma
    normalized = normalized.rstrip(",").strip()

    # Try to extract city (text after last comma)
    if "," in normalized:
        parts = normalized.rsplit(",", 1)
        street = parts[0].strip()
        city = parts[1].strip()
    else:
        # No comma - assume it's all street address
        street = normalized

    # Normalize street
    if street:
        street = _normalize_street(street)

    # Build normalized string
    parts = [p for p in [street, city, state, zip_code] if p]
    full_normalized = " ".join(parts)

    return ParsedAddress(street, city, state, zip_code, full_normalized)


def addresses_match(addr1: str, addr2: str, threshold: float = 0.85) -> bool:
    """
    Check if two addresses likely refer to the same location.

    Uses normalized comparison with optional fuzzy matching.
    """
    if not addr1 or not addr2:
        return False

    parsed1 = normalize_address(addr1)
    parsed2 = normalize_address(addr2)

    # Exact match on normalized form
    if parsed1.normalized == parsed2.normalized:
        return True

    # Check ZIP codes - if both have ZIPs and they differ, probably not same
    if parsed1.zip_code and parsed2.zip_code:
        if parsed1.zip_code != parsed2.zip_code:
            return False

    # Fuzzy matching on street
    if parsed1.street and parsed2.street:
        from rapidfuzz import fuzz
        similarity = fuzz.ratio(parsed1.street, parsed2.street) / 100.0
        return similarity >= threshold

    return False
