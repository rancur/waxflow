"""Automatic smart playlist creation in Lexicon DJ.

Creates and maintains organized smartlist trees for:
- Genre hierarchy (Bass Music, House, Techno, Trance, etc.)
- Energy / Danceability / Popularity / Happiness tiers
- Star ratings
- BPM ranges (DJ-focused brackets)
- Key (Camelot wheel)
"""

import asyncio
import json
import logging
import time

import httpx

from tasks.helpers import (
    LEXICON_API_URL,
    get_config,
    get_db,
    log_activity,
    set_config,
)

log = logging.getLogger("worker.playlists")

# ============================================================
# Genre tree: folder > smartlists
# ============================================================

# Each leaf has "rules" = list of dicts with field/operator/values.
# "_matchAll": False means any rule can match (OR).  Default is True (AND).
# "_type": "folder" marks intermediate folders.

GENRE_TREE = {
    "Bass Music": {
        "_type": "folder",
        "Drum & Bass": {
            "rules": [
                {"field": "genre", "operator": "StringContains", "values": ["Drum & Bass"]},
                {"field": "genre", "operator": "StringContains", "values": ["Jungle"]},
                {"field": "genre", "operator": "StringContains", "values": ["Liquid"]},
            ],
            "_matchAll": False,
        },
        "Dubstep": {
            "rules": [
                {"field": "genre", "operator": "StringContains", "values": ["Dubstep"]},
                {"field": "genre", "operator": "StringContains", "values": ["Riddim"]},
                {"field": "genre", "operator": "StringContains", "values": ["Brostep"]},
                {"field": "genre", "operator": "StringContains", "values": ["Chillstep"]},
            ],
            "_matchAll": False,
        },
        "140 / Deep Dubstep": {
            "rules": [
                {"field": "genre", "operator": "StringContains", "values": ["140"]},
                {"field": "genre", "operator": "StringContains", "values": ["Grime"]},
                {"field": "genre", "operator": "StringContains", "values": ["Deep Dubstep"]},
            ],
            "_matchAll": False,
        },
        "Trap / Hybrid Trap": {
            "rules": [
                {"field": "genre", "operator": "StringContains", "values": ["Trap"]},
                {"field": "genre", "operator": "StringContains", "values": ["Hybrid Trap"]},
            ],
            "_matchAll": False,
        },
        "Halftime / Experimental": {
            "rules": [
                {"field": "genre", "operator": "StringContains", "values": ["Halftime"]},
                {"field": "genre", "operator": "StringContains", "values": ["Experimental Bass"]},
                {"field": "genre", "operator": "StringContains", "values": ["Glitch"]},
                {"field": "genre", "operator": "StringContains", "values": ["IDM"]},
            ],
            "_matchAll": False,
        },
        "Bass House / G-House": {
            "rules": [
                {"field": "genre", "operator": "StringContains", "values": ["Bass House"]},
                {"field": "genre", "operator": "StringContains", "values": ["G-House"]},
            ],
            "_matchAll": False,
        },
    },
    "House": {
        "_type": "folder",
        "Deep House": {
            "rules": [{"field": "genre", "operator": "StringContains", "values": ["Deep House"]}],
        },
        "Tech House": {
            "rules": [{"field": "genre", "operator": "StringContains", "values": ["Tech House"]}],
        },
        "Progressive House": {
            "rules": [
                {"field": "genre", "operator": "StringContains", "values": ["Progressive House"]},
                {"field": "genre", "operator": "StringContains", "values": ["Melodic House"]},
            ],
            "_matchAll": False,
        },
        "Electro House": {
            "rules": [
                {"field": "genre", "operator": "StringContains", "values": ["Electro House"]},
                {"field": "genre", "operator": "StringContains", "values": ["Big Room"]},
                {"field": "genre", "operator": "StringContains", "values": ["Complextro"]},
            ],
            "_matchAll": False,
        },
        "Minimal / Micro House": {
            "rules": [
                {"field": "genre", "operator": "StringContains", "values": ["Minimal"]},
                {"field": "genre", "operator": "StringContains", "values": ["Micro House"]},
            ],
            "_matchAll": False,
        },
        "Afro House": {
            "rules": [{"field": "genre", "operator": "StringContains", "values": ["Afro House"]}],
        },
        "Disco / Nu-Disco": {
            "rules": [
                {"field": "genre", "operator": "StringContains", "values": ["Disco"]},
                {"field": "genre", "operator": "StringContains", "values": ["Nu-Disco"]},
                {"field": "genre", "operator": "StringContains", "values": ["Nu Disco"]},
            ],
            "_matchAll": False,
        },
        "House (General)": {
            "rules": [{"field": "genre", "operator": "StringContains", "values": ["House"]}],
        },
    },
    "Techno": {
        "_type": "folder",
        "Techno (General)": {
            "rules": [{"field": "genre", "operator": "StringContains", "values": ["Techno"]}],
        },
        "Hard Techno / Industrial": {
            "rules": [
                {"field": "genre", "operator": "StringContains", "values": ["Hard Techno"]},
                {"field": "genre", "operator": "StringContains", "values": ["Industrial"]},
                {"field": "genre", "operator": "StringContains", "values": ["Schranz"]},
            ],
            "_matchAll": False,
        },
        "Melodic Techno": {
            "rules": [
                {"field": "genre", "operator": "StringContains", "values": ["Melodic Techno"]},
            ],
        },
        "Acid Techno": {
            "rules": [
                {"field": "genre", "operator": "StringContains", "values": ["Acid"]},
            ],
        },
        "Detroit Techno": {
            "rules": [
                {"field": "genre", "operator": "StringContains", "values": ["Detroit"]},
            ],
        },
    },
    "Trance": {
        "_type": "folder",
        "Trance (General)": {
            "rules": [{"field": "genre", "operator": "StringContains", "values": ["Trance"]}],
        },
        "Psytrance": {
            "rules": [
                {"field": "genre", "operator": "StringContains", "values": ["Psytrance"]},
                {"field": "genre", "operator": "StringContains", "values": ["Goa"]},
            ],
            "_matchAll": False,
        },
        "Uplifting Trance": {
            "rules": [{"field": "genre", "operator": "StringContains", "values": ["Uplifting"]}],
        },
        "Progressive Trance": {
            "rules": [{"field": "genre", "operator": "StringContains", "values": ["Progressive Trance"]}],
        },
    },
    "Breaks & Breakbeat": {
        "_type": "folder",
        "Breakbeat": {
            "rules": [
                {"field": "genre", "operator": "StringContains", "values": ["Breakbeat"]},
                {"field": "genre", "operator": "StringContains", "values": ["Breaks"]},
            ],
            "_matchAll": False,
        },
        "UK Garage / 2-Step": {
            "rules": [
                {"field": "genre", "operator": "StringContains", "values": ["UK Garage"]},
                {"field": "genre", "operator": "StringContains", "values": ["2-Step"]},
                {"field": "genre", "operator": "StringContains", "values": ["Garage"]},
            ],
            "_matchAll": False,
        },
    },
    "Downtempo & Ambient": {
        "_type": "folder",
        "Ambient": {
            "rules": [
                {"field": "genre", "operator": "StringContains", "values": ["Ambient"]},
                {"field": "genre", "operator": "StringContains", "values": ["Chill"]},
            ],
            "_matchAll": False,
        },
        "Downtempo": {
            "rules": [
                {"field": "genre", "operator": "StringContains", "values": ["Downtempo"]},
                {"field": "genre", "operator": "StringContains", "values": ["Trip-Hop"]},
                {"field": "genre", "operator": "StringContains", "values": ["Trip Hop"]},
                {"field": "genre", "operator": "StringContains", "values": ["Lo-Fi"]},
            ],
            "_matchAll": False,
        },
    },
    "Hip-Hop & R&B": {
        "_type": "folder",
        "Hip-Hop": {
            "rules": [
                {"field": "genre", "operator": "StringContains", "values": ["Hip-Hop"]},
                {"field": "genre", "operator": "StringContains", "values": ["Hip Hop"]},
                {"field": "genre", "operator": "StringContains", "values": ["Rap"]},
            ],
            "_matchAll": False,
        },
        "R&B / Soul": {
            "rules": [
                {"field": "genre", "operator": "StringContains", "values": ["R&B"]},
                {"field": "genre", "operator": "StringContains", "values": ["Soul"]},
                {"field": "genre", "operator": "StringContains", "values": ["RnB"]},
            ],
            "_matchAll": False,
        },
    },
    "Pop & Indie": {
        "_type": "folder",
        "Pop": {
            "rules": [
                {"field": "genre", "operator": "StringContains", "values": ["Pop"]},
                {"field": "genre", "operator": "StringContains", "values": ["Synthpop"]},
            ],
            "_matchAll": False,
        },
        "Indie / Alternative": {
            "rules": [
                {"field": "genre", "operator": "StringContains", "values": ["Indie"]},
                {"field": "genre", "operator": "StringContains", "values": ["Alternative"]},
            ],
            "_matchAll": False,
        },
    },
    "Rock & Metal": {
        "_type": "folder",
        "Rock": {
            "rules": [
                {"field": "genre", "operator": "StringContains", "values": ["Rock"]},
                {"field": "genre", "operator": "StringContains", "values": ["Punk"]},
            ],
            "_matchAll": False,
        },
        "Metal": {
            "rules": [
                {"field": "genre", "operator": "StringContains", "values": ["Metal"]},
                {"field": "genre", "operator": "StringContains", "values": ["Metalcore"]},
            ],
            "_matchAll": False,
        },
    },
    "Electronic (Other)": {
        "_type": "folder",
        "Synthwave / Retrowave": {
            "rules": [
                {"field": "genre", "operator": "StringContains", "values": ["Synthwave"]},
                {"field": "genre", "operator": "StringContains", "values": ["Retrowave"]},
                {"field": "genre", "operator": "StringContains", "values": ["Outrun"]},
            ],
            "_matchAll": False,
        },
        "Future Bass / Wave": {
            "rules": [
                {"field": "genre", "operator": "StringContains", "values": ["Future Bass"]},
                {"field": "genre", "operator": "StringContains", "values": ["Wave"]},
            ],
            "_matchAll": False,
        },
        "Hardstyle / Hardcore": {
            "rules": [
                {"field": "genre", "operator": "StringContains", "values": ["Hardstyle"]},
                {"field": "genre", "operator": "StringContains", "values": ["Hardcore"]},
                {"field": "genre", "operator": "StringContains", "values": ["Gabber"]},
                {"field": "genre", "operator": "StringContains", "values": ["Frenchcore"]},
            ],
            "_matchAll": False,
        },
        "Midtempo / Electro": {
            "rules": [
                {"field": "genre", "operator": "StringContains", "values": ["Midtempo"]},
                {"field": "genre", "operator": "StringContains", "values": ["Electro"]},
            ],
            "_matchAll": False,
        },
    },
}

# ============================================================
# Attribute playlists (energy, danceability, popularity, happiness)
# ============================================================

ATTRIBUTE_PLAYLISTS = {
    "Energy": {
        "field": "energy",
        "tiers": [
            ("Banging Energy", 9, 10),
            ("High Energy", 7, 8),
            ("Medium Energy", 5, 6),
            ("Low Energy", 3, 4),
            ("Chill Energy", 1, 2),
            ("Unknown Energy", 0, 0),
        ],
    },
    "Danceability": {
        "field": "danceability",
        "tiers": [
            ("Extreme Danceability", 9, 10),
            ("High Danceability", 7, 8),
            ("Medium Danceability", 5, 6),
            ("Low Danceability", 3, 4),
            ("Undanceable", 1, 2),
            ("Unknown Danceability", 0, 0),
        ],
    },
    "Popularity": {
        "field": "popularity",
        "tiers": [
            ("Extreme Popularity", 9, 10),
            ("High Popularity", 7, 8),
            ("Medium Popularity", 5, 6),
            ("Low Popularity", 3, 4),
            ("Unpopular", 1, 2),
            ("Unknown Popularity", 0, 0),
        ],
    },
    "Happiness": {
        "field": "happiness",
        "tiers": [
            ("Extreme Happiness", 9, 10),
            ("High Happiness", 7, 8),
            ("Medium Happiness", 5, 6),
            ("Low Happiness", 3, 4),
            ("Sadness", 1, 2),
            ("Unknown Happiness", 0, 0),
        ],
    },
}

# ============================================================
# Rating playlists
# ============================================================

RATING_PLAYLISTS = [
    ("5 Stars", 5, 5),
    ("4 Stars", 4, 4),
    ("3 Stars", 3, 3),
    ("2 Stars", 2, 2),
    ("1 Star", 1, 1),
    ("Not Rated", 0, 0),
]

# ============================================================
# BPM ranges
# ============================================================

BPM_RANGES = [
    ("60-80 BPM (Downtempo)", 60, 80),
    ("80-100 BPM (Hip-Hop / Halftime)", 80, 100),
    ("100-120 BPM (House / Deep)", 100, 120),
    ("120-130 BPM (Tech House / Progressive)", 120, 130),
    ("130-140 BPM (Techno / Trance)", 130, 140),
    ("140-150 BPM (Dubstep / Trap)", 140, 150),
    ("150-170 BPM (Drum & Bass)", 150, 170),
    ("170+ BPM (Jungle / Hardcore)", 170, 300),
]

# ============================================================
# Key playlists (Camelot wheel)
# ============================================================

CAMELOT_KEYS = [
    "1A", "1B", "2A", "2B", "3A", "3B", "4A", "4B",
    "5A", "5B", "6A", "6B", "7A", "7B", "8A", "8B",
    "9A", "9B", "10A", "10B", "11A", "11B", "12A", "12B",
]

# ============================================================
# Lexicon API helpers
# ============================================================


def _get_lexicon_url(db_path: str) -> str:
    return get_config(db_path, "lexicon_api_url") or LEXICON_API_URL


def _fetch_existing_playlists(client: httpx.Client) -> dict:
    """Fetch all playlists from Lexicon and index by name+parentId for dedup."""
    resp = client.get("/v1/playlists")
    if resp.status_code != 200:
        log.warning("Failed to fetch playlists: HTTP %d", resp.status_code)
        return {}

    data = resp.json()
    root_playlists = data.get("data", {}).get("playlists", [])

    index = {}  # (name, parentId) -> playlist dict

    def _walk(items):
        for item in items:
            key = (item["name"], item.get("parentId"))
            index[key] = item
            if "playlists" in item:
                _walk(item["playlists"])

    _walk(root_playlists)
    return index


def _create_folder(client: httpx.Client, name: str, parent_id: int, existing: dict) -> int | None:
    """Create a folder in Lexicon. Returns the folder ID. Idempotent."""
    key = (name, parent_id)
    if key in existing:
        return existing[key]["id"]

    resp = client.post("/v1/playlist", json={
        "name": name,
        "type": "1",
        "parentId": parent_id,
    })
    if resp.status_code in (200, 201):
        new_id = resp.json().get("data", {}).get("id")
        if new_id:
            existing[key] = {"id": new_id, "name": name, "parentId": parent_id}
            log.info("Created folder: %s (id=%d, parent=%d)", name, new_id, parent_id)
            return new_id
    else:
        log.warning("Failed to create folder '%s': HTTP %d %s", name, resp.status_code, resp.text[:200])
    return None


def _create_smartlist(
    client: httpx.Client,
    name: str,
    parent_id: int,
    rules: list[dict],
    match_all: bool,
    existing: dict,
) -> bool:
    """Create a smartlist in Lexicon. Returns True if newly created, False if already exists."""
    key = (name, parent_id)
    if key in existing:
        return False

    payload = {
        "name": name,
        "type": "3",
        "parentId": parent_id,
        "smartlist": {
            "matchAll": match_all,
            "rules": rules,
        },
    }
    resp = client.post("/v1/playlist", json=payload)
    if resp.status_code in (200, 201):
        new_id = resp.json().get("data", {}).get("id")
        if new_id:
            existing[key] = {"id": new_id, "name": name, "parentId": parent_id}
            log.info("Created smartlist: %s (id=%d, parent=%d)", name, new_id, parent_id)
            return True
    else:
        log.warning("Failed to create smartlist '%s': HTTP %d %s", name, resp.status_code, resp.text[:200])
    return False


# ============================================================
# Category builders
# ============================================================


def _build_genre_tree(client: httpx.Client, root_id: int, existing: dict) -> int:
    """Build the full genre folder/smartlist tree. Returns count of playlists created."""
    waxflow_folder = _create_folder(client, "Genres", root_id, existing)
    if not waxflow_folder:
        return 0

    count = 0
    for top_name, children in GENRE_TREE.items():
        top_folder = _create_folder(client, top_name, waxflow_folder, existing)
        if not top_folder:
            continue

        for child_name, child_data in children.items():
            if child_name.startswith("_"):
                continue
            if child_data.get("_type") == "folder":
                # Nested sub-folder
                sub_folder = _create_folder(client, child_name, top_folder, existing)
                if not sub_folder:
                    continue
                for sub_name, sub_data in child_data.items():
                    if sub_name.startswith("_"):
                        continue
                    rules = sub_data.get("rules", [])
                    match_all = sub_data.get("_matchAll", True)
                    if _create_smartlist(client, sub_name, sub_folder, rules, match_all, existing):
                        count += 1
            else:
                rules = child_data.get("rules", [])
                match_all = child_data.get("_matchAll", True)
                if _create_smartlist(client, child_name, top_folder, rules, match_all, existing):
                    count += 1

    return count


def _build_attribute_playlists(client: httpx.Client, root_id: int, existing: dict, category: str) -> int:
    """Build tiered smartlists for an attribute (energy, danceability, etc.)."""
    config = ATTRIBUTE_PLAYLISTS.get(category)
    if not config:
        return 0

    folder = _create_folder(client, f"{category}", root_id, existing)
    if not folder:
        return 0

    count = 0
    field = config["field"]
    for name, lo, hi in config["tiers"]:
        if lo == 0 and hi == 0:
            # "Unknown" = value equals 0
            rules = [{"field": field, "operator": "NumberEquals", "values": ["0"]}]
        elif lo == hi:
            rules = [{"field": field, "operator": "NumberEquals", "values": [str(lo)]}]
        else:
            rules = [{"field": field, "operator": "NumberBetween", "values": [str(lo), str(hi)]}]
        if _create_smartlist(client, name, folder, rules, True, existing):
            count += 1

    return count


def _build_rating_playlists(client: httpx.Client, root_id: int, existing: dict) -> int:
    """Build star-rating smartlists."""
    folder = _create_folder(client, "Ratings", root_id, existing)
    if not folder:
        return 0

    count = 0
    for name, lo, hi in RATING_PLAYLISTS:
        if lo == 0 and hi == 0:
            rules = [{"field": "rating", "operator": "NumberEquals", "values": ["0"]}]
        else:
            rules = [{"field": "rating", "operator": "NumberEquals", "values": [str(lo)]}]
        if _create_smartlist(client, name, folder, rules, True, existing):
            count += 1

    return count


def _build_bpm_playlists(client: httpx.Client, root_id: int, existing: dict) -> int:
    """Build BPM-range smartlists."""
    folder = _create_folder(client, "BPM", root_id, existing)
    if not folder:
        return 0

    count = 0
    for name, lo, hi in BPM_RANGES:
        rules = [{"field": "bpm", "operator": "NumberBetween", "values": [str(lo), str(hi)]}]
        if _create_smartlist(client, name, folder, rules, True, existing):
            count += 1

    return count


def _build_key_playlists(client: httpx.Client, root_id: int, existing: dict) -> int:
    """Build Camelot key smartlists."""
    folder = _create_folder(client, "Keys", root_id, existing)
    if not folder:
        return 0

    count = 0
    for camelot_key in CAMELOT_KEYS:
        rules = [{"field": "key", "operator": "StringEquals", "values": [camelot_key]}]
        if _create_smartlist(client, camelot_key, folder, rules, True, existing):
            count += 1

    return count


# ============================================================
# Main task
# ============================================================


def _run_create_playlists(db_path: str):
    """Synchronous main logic for building all auto-playlists.

    Called every 30s by the worker loop.  Actual work only happens when:
    - The configured interval has elapsed since last run, OR
    - The rebuild flag is set (manual trigger from UI).
    """
    # Check master toggle
    enabled = get_config(db_path, "auto_playlists_enabled")
    if enabled == "0":
        return

    # Check for rebuild flag (manual trigger — always run immediately)
    rebuild = get_config(db_path, "auto_playlists_rebuild")
    force = rebuild == "1"

    if not force:
        # Check if enough time has passed since last run
        last_run_str = get_config(db_path, "auto_playlists_last_run") or "0"
        try:
            last_run = int(last_run_str)
        except (ValueError, TypeError):
            last_run = 0

        interval_str = get_config(db_path, "auto_playlists_interval_seconds") or "86400"
        try:
            interval = int(interval_str)
        except (ValueError, TypeError):
            interval = 86400

        if last_run > 0 and (time.time() - last_run) < interval:
            return  # Not time yet

    if force:
        set_config(db_path, "auto_playlists_rebuild", "0")
        log.info("Rebuild flag detected — forcing full playlist creation")

    lexicon_url = _get_lexicon_url(db_path)

    try:
        with httpx.Client(base_url=lexicon_url, timeout=60) as client:
            # Fetch existing playlists for idempotency
            existing = _fetch_existing_playlists(client)
            root_id = 1  # Lexicon ROOT playlist

            total_created = 0

            # Genre playlists
            if get_config(db_path, "auto_playlists_genres") != "0":
                total_created += _build_genre_tree(client, root_id, existing)

            # Attribute playlists
            for attr_key, config_key in [
                ("Energy", "auto_playlists_energy"),
                ("Danceability", "auto_playlists_danceability"),
                ("Popularity", "auto_playlists_popularity"),
                ("Happiness", "auto_playlists_happiness"),
            ]:
                if get_config(db_path, config_key) != "0":
                    total_created += _build_attribute_playlists(client, root_id, existing, attr_key)

            # Rating playlists
            if get_config(db_path, "auto_playlists_rating") != "0":
                total_created += _build_rating_playlists(client, root_id, existing)

            # BPM playlists
            if get_config(db_path, "auto_playlists_bpm") != "0":
                total_created += _build_bpm_playlists(client, root_id, existing)

            # Key playlists
            if get_config(db_path, "auto_playlists_key") != "0":
                total_created += _build_key_playlists(client, root_id, existing)

            # Track state
            set_config(db_path, "auto_playlists_last_run", str(int(time.time())))

            # Count total managed playlists (all managed folders + their children)
            managed_count = sum(
                1 for (name, _pid) in existing
                if name in ("Genres","Energy","Danceability","Popularity","Happiness","Ratings","BPM","Keys") or _pid is not None
            )

            # Store created IDs for tracking
            created_ids = {}
            for (name, pid), info in existing.items():
                if isinstance(info, dict) and "id" in info:
                    if name in ("Genres","Energy","Danceability","Popularity","Happiness","Ratings","BPM","Keys"):
                        created_ids[name] = info["id"]
            set_config(db_path, "auto_playlists_created_ids", json.dumps(created_ids))

            if total_created > 0:
                log.info("Auto-playlists: created %d new playlists", total_created)
                log_activity(
                    db_path, "auto_playlists", None,
                    f"Created {total_created} new auto-playlists in Lexicon",
                    {"created": total_created},
                )
            else:
                log.info("Auto-playlists: all playlists already exist, nothing to create")

    except httpx.ConnectError:
        log.warning("Auto-playlists: Lexicon API unreachable at %s", lexicon_url)
    except Exception as e:
        log.error("Auto-playlists failed: %s", e, exc_info=True)


async def create_playlists(db_path: str):
    """Async entry point for the auto-playlist creation task."""
    await asyncio.to_thread(_run_create_playlists, db_path)
