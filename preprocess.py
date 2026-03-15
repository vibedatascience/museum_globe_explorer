#!/usr/bin/env python3
"""
Preprocess MetObjects.csv into met_data.js for the globe explorer.

Pipeline:
  1. Parse CSV (485K rows)
  2. Resolve country (Country field -> Culture fallback)
  3. Classify into 9 simplified departments
  4. Aggregate by country
  5. Filter countries with < 20 artifacts
  6. Sample 30-50 artifacts per country
  7. Geocode with static lat/lon table
  8. Generate color palettes from department distribution
  9. Write met_data.js
"""

import csv
import json
import random
import re
import ssl
import sys
import time
import urllib.request
import urllib.error
from collections import Counter, defaultdict

random.seed(42)

# ---------------------------------------------------------------------------
# 1. COUNTRY NORMALIZATION
#    Handles historical names, prefixes, regional variants, pipe-delimited
# ---------------------------------------------------------------------------
COUNTRY_NORMALIZE = {
    # Historical / alternate names
    "Byzantine Egypt": "Egypt",
    "Roman Egypt": "Egypt",
    "Coptic Egypt": "Egypt",
    "Upper Egypt": "Egypt",
    "Lower Egypt": "Egypt",
    "Nubia": "Sudan",
    "Persia": "Iran",
    "Mesopotamia": "Iraq",
    "Babylonia": "Iraq",
    "Assyria": "Iraq",
    "Sumer": "Iraq",
    "Ottoman Empire": "Turkey",
    "Byzantine Empire": "Turkey",
    "Anatolia": "Turkey",
    "Asia Minor": "Turkey",
    "Ceylon": "Sri Lanka",
    "Siam": "Thailand",
    "Burma": "Myanmar",
    "Formosa": "Taiwan",
    "Zaire": "Democratic Republic of the Congo",
    "Belgian Congo": "Democratic Republic of the Congo",
    "Rhodesia": "Zimbabwe",
    "Southern Rhodesia": "Zimbabwe",
    "Northern Rhodesia": "Zambia",
    "Abyssinia": "Ethiopia",
    "Dahomey": "Benin",
    "Gold Coast": "Ghana",
    "French West Africa": "Mali",
    "French Indochina": "Vietnam",
    "Indochina": "Vietnam",
    "Dutch East Indies": "Indonesia",
    "East Indies": "Indonesia",
    "Cathay": "China",
    "Tibet": "China",
    "Manchuria": "China",
    "Soviet Union": "Russia",
    "USSR": "Russia",
    "Prussia": "Germany",
    "Bavaria": "Germany",
    "Saxony": "Germany",
    "Bohemia": "Czech Republic",
    "Yugoslavia": "Serbia",
    "Czechoslovakia": "Czech Republic",
    "New Spain": "Mexico",
    "Viceroyalty of Peru": "Peru",
    "Gran Colombia": "Colombia",
    "Mughal India": "India",
    "British India": "India",
    "Hindustan": "India",
    "New Guinea": "Papua New Guinea",
    "Borneo": "Indonesia",
    "Sumatra": "Indonesia",
    "Java": "Indonesia",
    "Bali": "Indonesia",
    "Sulawesi": "Indonesia",
    "Timor": "Indonesia",
    "Celebes": "Indonesia",

    # Common variants in the data
    "England": "United Kingdom",
    "Scotland": "United Kingdom",
    "Wales": "United Kingdom",
    "Ireland": "Ireland",
    "Northern Ireland": "United Kingdom",
    "Great Britain": "United Kingdom",
    "America": "United States",
    "USA": "United States",
    "U.S.A.": "United States",
    "Holland": "Netherlands",
    "The Netherlands": "Netherlands",
    "Flanders": "Belgium",
    "Etruria": "Italy",
    "Kingdom of Naples": "Italy",
    "Sardinia": "Italy",
    "Sicily": "Italy",
    "Tuscany": "Italy",
    "Venice": "Italy",
    "Catalonia": "Spain",
    "Castile": "Spain",
    "Andalusia": "Spain",
    "Navarre": "Spain",
    "Galicia": "Spain",
    "Korea": "South Korea",
    "Republic of Korea": "South Korea",

    # Regional prefixes (after prefix stripping these may remain)
    "Northern Italy": "Italy",
    "Southern Italy": "Italy",
    "Central Italy": "Italy",
    "Northern France": "France",
    "Southern France": "France",
    "Northern Germany": "Germany",
    "Southern Germany": "Germany",
    "Central Germany": "Germany",
    "Northern India": "India",
    "Southern India": "India",
    "Central India": "India",
    "Western India": "India",
    "Eastern India": "India",
    "Northern China": "China",
    "Southern China": "China",
    "Eastern China": "China",
    "Western China": "China",
    "Northern Peru": "Peru",
    "Southern Peru": "Peru",
    "Central Peru": "Peru",
    "Northern Iran": "Iran",
    "Southern Iran": "Iran",
    "Western Iran": "Iran",
    "Eastern Iran": "Iran",
    "Northwestern Iran": "Iran",
    "Northeastern Iran": "Iran",
    "Central Asia": "Uzbekistan",
    "Southeast Asia": "Thailand",
    "East Africa": "Kenya",
    "West Africa": "Nigeria",
    "North Africa": "Morocco",
    "Sub-Saharan Africa": "Nigeria",
    "Central Africa": "Democratic Republic of the Congo",
    "Southern Africa": "South Africa",
    "Near East": "Iraq",
    "Middle East": "Iraq",
    "Far East": "China",
    "Levant": "Lebanon",
    "Palestine": "Palestine",
    "Holy Land": "Israel",
    "Canaan": "Israel",
}

# ---------------------------------------------------------------------------
# 2. CULTURE -> COUNTRY MAPPING
# ---------------------------------------------------------------------------
CULTURE_TO_COUNTRY = {
    "American": "United States",
    "French": "France",
    "French, Paris": "France",
    "French, Sèvres": "France",
    "probably French": "France",
    "French or South Netherlandish": "France",
    "Greek, Attic": "Greece",
    "Greek": "Greece",
    "Greek, South Italian, Apulian": "Italy",
    "Greek, South Italian, Lucanian": "Italy",
    "Greek, South Italian": "Italy",
    "Greek, Corinthian": "Greece",
    "Greek, Laconian": "Greece",
    "Greek, Boeotian": "Greece",
    "Japan": "Japan",
    "Japanese": "Japan",
    "China": "China",
    "Chinese": "China",
    "Italian": "Italy",
    "Italian, Venice": "Italy",
    "Italian, Florence": "Italy",
    "Italian, Naples": "Italy",
    "Italian, Rome": "Italy",
    "British": "United Kingdom",
    "British, London": "United Kingdom",
    "British, Staffordshire": "United Kingdom",
    "English": "United Kingdom",
    "Roman": "Italy",
    "German": "Germany",
    "German, Meissen": "Germany",
    "German, Augsburg": "Germany",
    "German, Nuremberg": "Germany",
    "Cypriot": "Cyprus",
    "European": None,  # too vague
    "American or European": None,
    "Coptic": "Egypt",
    "Egyptian": "Egypt",
    "Spanish": "Spain",
    "Dutch": "Netherlands",
    "Netherlandish": "Netherlands",
    "South Netherlandish": "Belgium",
    "Flemish": "Belgium",
    "Iranian": "Iran",
    "Iran": "Iran",
    "Persian": "Iran",
    "Sasanian": "Iran",
    "Mexican": "Mexico",
    "Aztec": "Mexico",
    "Maya": "Mexico",
    "Olmec": "Mexico",
    "Mixtec": "Mexico",
    "Zapotec": "Mexico",
    "Indian": "India",
    "Mughal": "India",
    "Rajput": "India",
    "Korean": "South Korea",
    "Korea": "South Korea",
    "Indonesian": "Indonesia",
    "Indonesia (Java)": "Indonesia",
    "Javanese": "Indonesia",
    "Balinese": "Indonesia",
    "Turkish": "Turkey",
    "Ottoman": "Turkey",
    "Seljuk": "Turkey",
    "Russian": "Russia",
    "Austrian": "Austria",
    "Swiss": "Switzerland",
    "Swedish": "Sweden",
    "Danish": "Denmark",
    "Norwegian": "Norway",
    "Finnish": "Finland",
    "Portuguese": "Portugal",
    "Brazilian": "Brazil",
    "Colombian": "Colombia",
    "Peruvian": "Peru",
    "Moche": "Peru",
    "Chimú": "Peru",
    "Inca": "Peru",
    "Nazca": "Peru",
    "Paracas": "Peru",
    "Wari": "Peru",
    "Chavin": "Peru",
    "Thai": "Thailand",
    "Khmer": "Cambodia",
    "Vietnamese": "Vietnam",
    "Tibetan": "China",
    "Nepalese": "Nepal",
    "Pakistani": "Pakistan",
    "Afghan": "Afghanistan",
    "Iraqi": "Iraq",
    "Syrian": "Syria",
    "Lebanese": "Lebanon",
    "Minoan": "Greece",
    "Mycenaean": "Greece",
    "Etruscan": "Italy",
    "Frankish": "France",
    "Carolingian": "France",
    "Merovingian": "France",
    "Byzantine": "Turkey",
    "Nigerian": "Nigeria",
    "Benin": "Nigeria",
    "Yoruba": "Nigeria",
    "Igbo": "Nigeria",
    "Akan": "Ghana",
    "Asante": "Ghana",
    "Ashanti": "Ghana",
    "Dogon": "Mali",
    "Bambara": "Mali",
    "Bamana": "Mali",
    "Kongo": "Democratic Republic of the Congo",
    "Luba": "Democratic Republic of the Congo",
    "Kuba": "Democratic Republic of the Congo",
    "Fang": "Gabon",
    "Baule": "Côte d'Ivoire",
    "Senufo": "Côte d'Ivoire",
    "Dan": "Côte d'Ivoire",
    "Asmat people": "Indonesia",
    "Asmat": "Indonesia",
    "Maori": "New Zealand",
    "Polynesian": "New Zealand",
    "Hawaiian": "United States",
    "Melanesian": "Papua New Guinea",
    "Aboriginal Australian": "Australia",
    "Armenian": "Armenia",
    "Georgian": "Georgia",
    "Polish": "Poland",
    "Hungarian": "Hungary",
    "Romanian": "Romania",
    "Czech": "Czech Republic",
    "Irish": "Ireland",
    "Scottish": "United Kingdom",
    "Welsh": "United Kingdom",
    "Tibetan Buddhist": "China",
    "Central Asian": "Uzbekistan",
    "Sogdian": "Uzbekistan",
    "Gandharan": "Pakistan",
    "Gandhara": "Pakistan",
    "Philippine": "Philippines",
    "Filipino": "Philippines",
    "Burmese": "Myanmar",
    "Cambodian": "Cambodia",
    "Congolese": "Democratic Republic of the Congo",
    "Cameroon": "Cameroon",
    "Cameroonian": "Cameroon",
    "Malian": "Mali",
    "Senegalese": "Senegal",
    "Ghanaian": "Ghana",
    "Ethiopian": "Ethiopia",
    "Sudanese": "Sudan",
    "Moroccan": "Morocco",
    "Tunisian": "Tunisia",
    "Algerian": "Algeria",
    "Libyan": "Libya",
    "Ivorian": "Côte d'Ivoire",
    "Costa Rican": "Costa Rica",
    "Panamanian": "Panama",
    "Ecuadorian": "Ecuador",
    "Venezuelan": "Venezuela",
    "Argentine": "Argentina",
    "Chilean": "Chile",
    "Bolivian": "Bolivia",
    "Guatemalan": "Guatemala",
    "Cuban": "Cuba",
}

# ---------------------------------------------------------------------------
# 3. CLASSIFICATION / DEPARTMENT -> SIMPLIFIED CATEGORY
# ---------------------------------------------------------------------------
# Substring match on Classification field (case-insensitive)
CLASSIFICATION_TO_DEPT = [
    ("Painting", "Paintings"),
    ("Miniature", "Paintings"),
    ("Screen", "Paintings"),
    ("Scroll", "Paintings"),
    ("Fresco", "Paintings"),
    ("Sculpture", "Sculpture"),
    ("Statue", "Sculpture"),
    ("Stele", "Sculpture"),
    ("Relief", "Sculpture"),
    ("Bronze", "Sculpture"),
    ("Terracotta", "Sculpture"),
    ("Figure", "Sculpture"),
    ("Mask", "Sculpture"),
    ("Stone", "Sculpture"),
    ("Jade", "Sculpture"),
    ("Ivory", "Sculpture"),
    ("Ceramic", "Ceramics"),
    ("Pottery", "Ceramics"),
    ("Porcelain", "Ceramics"),
    ("Vase", "Ceramics"),
    ("Glass", "Ceramics"),
    ("Stucco", "Ceramics"),
    ("Faience", "Ceramics"),
    ("Vessel", "Ceramics"),
    ("Arms", "Arms & Armor"),
    ("Armor", "Arms & Armor"),
    ("Sword", "Arms & Armor"),
    ("Dagger", "Arms & Armor"),
    ("Helmet", "Arms & Armor"),
    ("Shield", "Arms & Armor"),
    ("Firearm", "Arms & Armor"),
    ("Textile", "Textiles"),
    ("Lace", "Textiles"),
    ("Velvet", "Textiles"),
    ("Embroid", "Textiles"),
    ("Tapestry", "Textiles"),
    ("Woven", "Textiles"),
    ("Silk", "Textiles"),
    ("Carpet", "Textiles"),
    ("Costume", "Textiles"),
    ("Jewelry", "Jewelry"),
    ("Gold and Silver", "Jewelry"),
    ("Gem", "Jewelry"),
    ("Metal-Ornament", "Jewelry"),
    ("Medal", "Jewelry"),
    ("Metalwork", "Jewelry"),
    ("Silver", "Jewelry"),
    ("Enamel", "Jewelry"),
    ("Coin", "Jewelry"),
    ("Seal", "Jewelry"),
    ("Drawing", "Drawings"),
    ("Print", "Drawings"),
    ("Photograph", "Drawings"),
    ("Negative", "Drawings"),
    ("Book", "Drawings"),
    ("Codex", "Drawings"),
    ("Codice", "Drawings"),
    ("Manuscript", "Drawings"),
    ("Calligraph", "Drawings"),
    ("Ephemera", "Drawings"),
    ("Album", "Drawings"),
    ("Reliquar", "Religious"),
    ("Liturgical", "Religious"),
    ("Icon", "Religious"),
    ("Crozier", "Religious"),
    ("Cross", "Religious"),
    ("Altar", "Religious"),
    ("Musical", "Musical"),
    ("Instrument", "Musical"),
    ("Idiophone", "Musical"),
    ("Chordophone", "Musical"),
    ("Aerophone", "Musical"),
    ("Membranophone", "Musical"),
]

# Fallback: map Department to simplified category
DEPARTMENT_TO_DEPT = {
    "Egyptian Art": "Sculpture",
    "Greek and Roman Art": "Sculpture",
    "Asian Art": "Paintings",
    "European Paintings": "Paintings",
    "Arms and Armor": "Arms & Armor",
    "Costume Institute": "Textiles",
    "Islamic Art": "Ceramics",
    "The American Wing": "Paintings",
    "Drawings and Prints": "Drawings",
    "Photographs": "Drawings",
    "European Sculpture and Decorative Arts": "Sculpture",
    "Medieval Art": "Religious",
    "The Cloisters": "Religious",
    "Modern and Contemporary Art": "Paintings",
    "Musical Instruments": "Musical",
    "Arts of Africa, Oceania, and the Americas": "Sculpture",
    "Ancient Near Eastern Art": "Sculpture",
    "Robert Lehman Collection": "Paintings",
    "The Libraries": "Drawings",
}

# ---------------------------------------------------------------------------
# 4. EMOJI MAPPING (Classification/Object Name -> emoji)
# ---------------------------------------------------------------------------
EMOJI_MAP = [
    ("Painting", "🎨"),
    ("Vase", "🏺"),
    ("Ceramic", "🏺"),
    ("Pottery", "🏺"),
    ("Porcelain", "🏺"),
    ("Sculpture", "🗿"),
    ("Statue", "🗿"),
    ("Figure", "🗿"),
    ("Head", "🗿"),
    ("Relief", "🗿"),
    ("Mask", "🎭"),
    ("Textile", "🧵"),
    ("Silk", "🧵"),
    ("Tapestry", "🧶"),
    ("Carpet", "🧶"),
    ("Costume", "👘"),
    ("Robe", "👘"),
    ("Kimono", "👘"),
    ("Sword", "⚔️"),
    ("Dagger", "🗡️"),
    ("Armor", "🛡️"),
    ("Helmet", "⚔️"),
    ("Firearm", "🔫"),
    ("Shield", "🛡️"),
    ("Ring", "💍"),
    ("Earring", "✨"),
    ("Necklace", "📿"),
    ("Bracelet", "📿"),
    ("Amulet", "🪲"),
    ("Pendant", "📿"),
    ("Coin", "🪙"),
    ("Medal", "🏅"),
    ("Crown", "👑"),
    ("Cross", "✝️"),
    ("Reliquary", "✝️"),
    ("Altar", "✝️"),
    ("Icon", "🖼️"),
    ("Drawing", "📋"),
    ("Print", "📋"),
    ("Photograph", "📸"),
    ("Book", "📚"),
    ("Manuscript", "📜"),
    ("Scroll", "📜"),
    ("Instrument", "🎵"),
    ("Drum", "🥁"),
    ("Flute", "🎵"),
    ("Lute", "🎵"),
    ("Bowl", "🥣"),
    ("Plate", "🍽️"),
    ("Cup", "🍷"),
    ("Bottle", "🍾"),
    ("Jar", "⚱️"),
    ("Box", "📦"),
    ("Mirror", "🪞"),
    ("Lamp", "🪔"),
    ("Ewer", "🫖"),
    ("Scarab", "🪲"),
    ("Coffin", "⚱️"),
    ("Mummy", "⚱️"),
    ("Sarcophagus", "⚱️"),
    ("Stele", "📋"),
    ("Tablet", "📋"),
    ("Seal", "📿"),
    ("Gold", "✨"),
    ("Silver", "✨"),
    ("Bronze", "🗿"),
    ("Glass", "🍷"),
    ("Jade", "💎"),
    ("Ivory", "🗿"),
    ("Furniture", "🪑"),
    ("Chair", "🪑"),
    ("Table", "🪑"),
    ("Tile", "🔷"),
]

# ---------------------------------------------------------------------------
# 5. GEOCODE TABLE
# ---------------------------------------------------------------------------
GEOCODE = {
    "Egypt": (26.0, 30.0),
    "United States": (39.0, -98.0),
    "Iran": (32.0, 53.0),
    "Peru": (-10.0, -76.0),
    "France": (46.5, 2.5),
    "Mexico": (23.0, -102.0),
    "India": (22.0, 78.0),
    "Indonesia": (-2.0, 118.0),
    "China": (35.0, 105.0),
    "Turkey": (39.0, 35.0),
    "Papua New Guinea": (-6.0, 147.0),
    "Germany": (51.0, 10.0),
    "Nigeria": (9.0, 8.0),
    "Italy": (42.0, 12.5),
    "Democratic Republic of the Congo": (-2.5, 23.5),
    "Syria": (35.0, 38.0),
    "Spain": (40.0, -4.0),
    "Iraq": (33.0, 44.0),
    "Canada": (56.0, -96.0),
    "Mali": (17.0, -4.0),
    "Colombia": (4.0, -72.0),
    "Côte d'Ivoire": (7.5, -5.5),
    "Japan": (36.0, 138.0),
    "Cameroon": (6.0, 12.0),
    "United Kingdom": (54.0, -2.0),
    "Costa Rica": (10.0, -84.0),
    "Guatemala": (15.5, -90.0),
    "Greece": (39.0, 22.0),
    "Pakistan": (30.0, 70.0),
    "Thailand": (15.0, 101.0),
    "Bolivia": (17.0, -65.0),
    "Nepal": (28.0, 84.0),
    "Ecuador": (0.0, -78.0),
    "South Korea": (36.0, 128.0),
    "Philippines": (12.0, 122.0),
    "Sudan": (15.0, 32.0),
    "Ghana": (8.0, -1.5),
    "Cyprus": (35.0, 33.0),
    "Russia": (60.0, 100.0),
    "Austria": (47.5, 14.5),
    "Netherlands": (52.0, 5.5),
    "Belgium": (50.5, 4.5),
    "Switzerland": (47.0, 8.0),
    "Cambodia": (12.5, 105.0),
    "Myanmar": (21.0, 96.0),
    "Vietnam": (16.0, 108.0),
    "Sri Lanka": (7.0, 81.0),
    "Afghanistan": (34.0, 66.0),
    "Morocco": (32.0, -5.0),
    "Ethiopia": (9.0, 39.0),
    "Tanzania": (-6.0, 35.0),
    "Kenya": (0.0, 37.5),
    "South Africa": (-29.0, 25.0),
    "Tunisia": (34.0, 9.0),
    "Algeria": (28.0, 3.0),
    "Lebanon": (34.0, 36.0),
    "Israel": (31.5, 35.0),
    "Palestine": (32.0, 35.3),
    "Jordan": (31.0, 36.5),
    "Yemen": (15.5, 48.0),
    "Uzbekistan": (41.0, 64.0),
    "Taiwan": (23.5, 121.0),
    "Argentina": (-34.0, -64.0),
    "Brazil": (-10.0, -55.0),
    "Cuba": (22.0, -79.5),
    "Panama": (9.0, -80.0),
    "Venezuela": (8.0, -66.0),
    "Chile": (-33.0, -71.0),
    "Honduras": (14.5, -87.0),
    "El Salvador": (13.7, -89.0),
    "Nicaragua": (13.0, -85.0),
    "Dominican Republic": (19.0, -70.5),
    "Haiti": (19.0, -72.0),
    "Jamaica": (18.1, -77.3),
    "Puerto Rico": (18.2, -66.5),
    "Portugal": (39.5, -8.0),
    "Sweden": (62.0, 15.0),
    "Denmark": (56.0, 10.0),
    "Norway": (62.0, 10.0),
    "Finland": (64.0, 26.0),
    "Poland": (52.0, 20.0),
    "Hungary": (47.0, 20.0),
    "Romania": (46.0, 25.0),
    "Czech Republic": (50.0, 15.5),
    "Serbia": (44.0, 21.0),
    "Ireland": (53.0, -8.0),
    "Australia": (-25.0, 134.0),
    "New Zealand": (-41.0, 174.0),
    "Gabon": (0.0, 11.5),
    "Senegal": (14.5, -14.5),
    "Zimbabwe": (-20.0, 30.0),
    "Zambia": (-15.0, 28.0),
    "Benin": (9.3, 2.3),
    "Burkina Faso": (12.0, -1.5),
    "Libya": (27.0, 17.0),
    "Armenia": (40.0, 45.0),
    "Georgia": (42.0, 43.5),
    "Mongolia": (47.0, 105.0),
    "Malaysia": (4.0, 109.5),
    "Singapore": (1.3, 103.8),
}

# Department colors (for palette generation)
DEPT_COLORS = {
    "Paintings":    "#E3120B",
    "Sculpture":    "#006BA2",
    "Ceramics":     "#EBB434",
    "Arms & Armor": "#3EBCD2",
    "Textiles":     "#379A8B",
    "Jewelry":      "#D1B07C",
    "Drawings":     "#9A607F",
    "Religious":    "#758D99",
    "Musical":      "#674E1F",
}


# ===========================================================================
# HELPER FUNCTIONS
# ===========================================================================

def normalize_country(raw):
    """Normalize a raw country string. Returns None if unresolvable."""
    if not raw or not raw.strip():
        return None

    # Take first if pipe-delimited
    val = raw.split("|")[0].strip()

    # Strip common prefixes
    for prefix in [
        "present-day ", "probably ", "possibly ",
        "formerly ", "modern-day ", "now ", "ancient ",
    ]:
        if val.lower().startswith(prefix):
            val = val[len(prefix):].strip()

    # Direct lookup
    if val in COUNTRY_NORMALIZE:
        return COUNTRY_NORMALIZE[val]

    # Check if already a valid geocoded country
    if val in GEOCODE:
        return val

    # Try stripping "Northern/Southern/Eastern/Western/Central" prefix
    for d in ["Northern ", "Southern ", "Eastern ", "Western ",
              "Central ", "Northwestern ", "Northeastern ",
              "Southwestern ", "Southeastern "]:
        if val.startswith(d):
            rest = val[len(d):]
            if rest in GEOCODE:
                return rest
            if rest in COUNTRY_NORMALIZE:
                return COUNTRY_NORMALIZE[rest]

    return None


def resolve_culture(culture):
    """Try to map a culture string to a country."""
    if not culture or not culture.strip():
        return None

    val = culture.strip()

    # Direct lookup
    if val in CULTURE_TO_COUNTRY:
        return CULTURE_TO_COUNTRY[val]

    # Try first part before comma (e.g. "Greek, Attic" -> "Greek")
    if "," in val:
        first = val.split(",")[0].strip()
        if first in CULTURE_TO_COUNTRY:
            return CULTURE_TO_COUNTRY[first]

    # Try with common suffixes stripped
    for suffix in [" people", " People", " culture", " Culture"]:
        if val.endswith(suffix):
            base = val[:-len(suffix)].strip()
            if base in CULTURE_TO_COUNTRY:
                return CULTURE_TO_COUNTRY[base]

    return None


def classify_dept(classification, department, obj_name):
    """Classify a row into one of the 9 simplified departments."""
    # Try Classification field first (substring match)
    if classification:
        cl = classification.lower()
        for substr, dept in CLASSIFICATION_TO_DEPT:
            if substr.lower() in cl:
                return dept

    # Try Object Name
    if obj_name:
        on = obj_name.lower()
        for substr, dept in CLASSIFICATION_TO_DEPT:
            if substr.lower() in on:
                return dept

    # Fallback to Department
    if department and department in DEPARTMENT_TO_DEPT:
        return DEPARTMENT_TO_DEPT[department]

    return "Sculpture"  # last resort default


def get_emoji(classification, obj_name, title):
    """Pick an emoji based on Classification, Object Name, or Title."""
    for text in [classification, obj_name, title]:
        if text:
            t = text.lower()
            for substr, emoji in EMOJI_MAP:
                if substr.lower() in t:
                    return emoji
    return "🏛️"


def get_color_for_dept(dept):
    """Return a hex color for a department."""
    return DEPT_COLORS.get(dept, "#758D99")


def make_palette(dept_dist):
    """Generate 5 colors from the top departments for a country."""
    # Sort departments by count descending
    top = sorted(dept_dist.items(), key=lambda x: -x[1])
    colors = []
    for dept, _ in top[:5]:
        colors.append(DEPT_COLORS.get(dept, "#758D99"))
    # Pad if needed
    while len(colors) < 5:
        colors.append("#B4A48A")
    return colors[:5]


def parse_date(obj_begin, obj_end):
    """Parse date fields into a single representative integer year."""
    try:
        begin = int(float(obj_begin)) if obj_begin and obj_begin.strip() else None
    except (ValueError, OverflowError):
        begin = None
    try:
        end = int(float(obj_end)) if obj_end and obj_end.strip() else None
    except (ValueError, OverflowError):
        end = None

    if begin is not None and end is not None:
        return (begin + end) // 2
    if begin is not None:
        return begin
    if end is not None:
        return end
    return None


def fetch_image_urls(D):
    """Fetch primaryImageSmall from Met API for all sampled artifacts."""
    API_BASE = "https://collectionapi.metmuseum.org/public/collection/v1/objects/"

    # SSL context (some systems have cert issues with this API)
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE

    all_ids = []
    for country in D:
        for art in country["a"]:
            if art["id"]:
                all_ids.append(art["id"])

    total = len(all_ids)
    print(f"\nFetching image URLs from Met API for {total} artifacts...")
    print(f"  (Rate limited to ~10 req/sec — this will take ~5 min)")

    # Build id -> image URL map
    img_map = {}
    fetched = 0
    errors = 0
    has_img = 0
    consecutive_errors = 0

    for i, obj_id in enumerate(all_ids):
        # Back off if we're getting consecutive errors (rate limited)
        if consecutive_errors >= 5:
            print(f"  Rate limited — backing off 30s at {i}/{total}...")
            time.sleep(30)
            consecutive_errors = 0

        try:
            url = API_BASE + str(obj_id)
            req = urllib.request.Request(url, headers={"User-Agent": "MetGlobeExplorer/1.0"})
            with urllib.request.urlopen(req, timeout=10, context=ctx) as resp:
                data = json.loads(resp.read().decode("utf-8"))
                img_url = data.get("primaryImageSmall", "")
                if img_url:
                    img_map[obj_id] = img_url
                    has_img += 1
            fetched += 1
            consecutive_errors = 0
        except urllib.error.HTTPError as e:
            errors += 1
            if e.code == 403:
                consecutive_errors += 1
            elif e.code == 404:
                fetched += 1  # valid response, object just doesn't exist
                consecutive_errors = 0
        except Exception:
            errors += 1
            consecutive_errors += 1

        # Rate limit: ~10 req/sec to stay safe
        time.sleep(0.1)
        # Progress
        if (i + 1) % 200 == 0 or (i + 1) == total:
            print(f"  {i+1}/{total} fetched, {has_img} with images, {errors} errors")

    # Apply to D
    for country in D:
        for art in country["a"]:
            if art["id"] in img_map:
                art["img"] = img_map[art["id"]]

    print(f"  Done: {has_img}/{fetched} artifacts have images ({has_img/max(fetched,1)*100:.0f}%)")
    return D


# ===========================================================================
# MAIN PIPELINE
# ===========================================================================

def main():
    csv_path = "MetObjects.csv"
    out_path = "met_data.js"

    print(f"Reading {csv_path}...")

    # Accumulate per-country data
    # country -> { rows: [...], dept_dist: Counter }
    country_data = defaultdict(lambda: {"rows": [], "dept_dist": Counter()})
    skipped = 0
    total = 0

    with open(csv_path, encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            total += 1

            # Resolve country
            country = normalize_country(row.get("Country", ""))
            if not country:
                country = resolve_culture(row.get("Culture", ""))
            if not country:
                skipped += 1
                continue

            # Must have geocode
            if country not in GEOCODE:
                skipped += 1
                continue

            # Parse date
            date = parse_date(row.get("Object Begin Date", ""),
                              row.get("Object End Date", ""))

            # Classify
            classification = row.get("Classification", "").strip()
            department = row.get("Department", "").strip()
            obj_name = row.get("Object Name", "").strip()
            title = row.get("Title", "").strip()
            dept = classify_dept(classification, department, obj_name)

            # Extract fields
            obj_id = row.get("Object ID", "").strip()
            is_highlight = row.get("Is Highlight", "").strip().lower() == "true"
            is_public = row.get("Is Public Domain", "").strip().lower() == "true"

            country_data[country]["rows"].append({
                "id": obj_id,
                "title": title or obj_name or "Untitled",
                "date": date,
                "dept": dept,
                "classification": classification,
                "obj_name": obj_name,
                "is_highlight": is_highlight,
                "is_public": is_public,
            })
            country_data[country]["dept_dist"][dept] += 1

    print(f"Total rows: {total}")
    print(f"Skipped (no country/geocode): {skipped}")
    print(f"Resolved countries: {len(country_data)}")

    # Filter: minimum 20 artifacts
    MIN_COUNT = 20
    filtered = {k: v for k, v in country_data.items() if len(v["rows"]) >= MIN_COUNT}
    print(f"Countries with >= {MIN_COUNT} artifacts: {len(filtered)}")

    # Build output
    D = []
    for country, data in sorted(filtered.items(), key=lambda x: -len(x[1]["rows"])):
        rows = data["rows"]
        dept_dist = data["dept_dist"]
        la, lo = GEOCODE[country]
        ct = len(rows)

        # Date range
        dates = [r["date"] for r in rows if r["date"] is not None]
        if dates:
            dr_min = min(dates)
            dr_max = max(dates)
        else:
            dr_min = 0
            dr_max = 2000

        # Sample artifacts: prioritize highlights, then public domain, then random
        highlights = [r for r in rows if r["is_highlight"]]
        public = [r for r in rows if r["is_public"] and not r["is_highlight"]]
        rest = [r for r in rows if not r["is_public"] and not r["is_highlight"]]

        random.shuffle(highlights)
        random.shuffle(public)
        random.shuffle(rest)

        # Pick up to 40 per country
        MAX_SAMPLE = 40
        sample = []
        for pool in [highlights, public, rest]:
            for r in pool:
                if len(sample) >= MAX_SAMPLE:
                    break
                sample.append(r)
            if len(sample) >= MAX_SAMPLE:
                break

        # Build artifact list
        artifacts = []
        for r in sample:
            emoji = get_emoji(r["classification"], r["obj_name"], r["title"])
            color = get_color_for_dept(r["dept"])
            title = r["title"]
            # Truncate long titles
            if len(title) > 50:
                title = title[:47] + "..."
            artifacts.append({
                "t": title,
                "d": r["date"] if r["date"] is not None else 0,
                "e": emoji,
                "c": color,
                "p": r["dept"],
                "id": int(r["id"]) if r["id"].isdigit() else 0,
            })

        palette = make_palette(dept_dist)

        D.append({
            "n": country,
            "la": la,
            "lo": lo,
            "ct": ct,
            "co": palette,
            "dr": [dr_min, dr_max],
            "a": artifacts,
        })

    # Fetch image URLs from Met API
    D = fetch_image_urls(D)

    # Write met_data.js
    print(f"\nWriting {out_path} with {len(D)} countries...")

    depts_js = json.dumps({
        k: {"color": v, "bg": v + "18"}
        for k, v in DEPT_COLORS.items()
    }, indent=2)

    d_js = json.dumps(D, ensure_ascii=False, indent=1)

    with open(out_path, "w", encoding="utf-8") as f:
        f.write("// Auto-generated by preprocess.py — do not edit manually\n")
        f.write(f"const DEPTS = {depts_js};\n\n")
        f.write(f"const D = {d_js};\n")

    # Stats
    total_artifacts = sum(c["ct"] for c in D)
    total_sampled = sum(len(c["a"]) for c in D)
    print(f"\nDone!")
    print(f"  Countries: {len(D)}")
    print(f"  Total artifact count: {total_artifacts:,}")
    print(f"  Sampled artifacts: {total_sampled}")
    print(f"  File size: {len(open(out_path).read()):,} bytes")

    # Print top 10
    print(f"\nTop 10 countries:")
    for c in D[:10]:
        print(f"  {c['ct']:>7,}  {c['n']}")


if __name__ == "__main__":
    main()
