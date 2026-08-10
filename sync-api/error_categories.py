"""Error-bucket classification for the Errors page.

WHY THIS IS ITS OWN MODULE
    The Errors page groups failures into buckets, and "Retry All" acts on a
    bucket. If the endpoint that RENDERS a bucket and the endpoint that RETRIES
    it classify tracks even slightly differently, the button retries a different
    set than the number beside it claims. Both now call this one function, so
    they cannot drift.

The rules are ordered: the first match wins. `verify_codec` is checked before the
error text because lossy tracks used to fall through to `download_failed`.
"""

from __future__ import annotations

# Display order on the Errors page.
ERROR_CATEGORIES: tuple[str, ...] = (
    "not_lossless",
    "no_tidal_match",
    "download_failed",
    "lexicon_sync_failed",
    "wrong_version",
    "other",
)

# Renamed in 2.13.0. The old key said "fingerprint_mismatch", but no fingerprint
# is ever compared -- the check is duration-based, and what it actually catches is
# a DIFFERENT EDIT of the right song (radio edit vs extended mix, and so on).
# Kept so an older frontend still resolves the bucket.
CATEGORY_ALIASES: dict[str, str] = {"fingerprint_mismatch": "wrong_version"}


def canonical_category(name: str) -> str:
    """Map a possibly-legacy category key to its current name."""
    return CATEGORY_ALIASES.get(name, name)


def categorize_error(track: dict) -> str:
    """Bucket one errored track. Returns a key from ERROR_CATEGORIES."""
    err = (track.get("pipeline_error") or "").lower()
    verify_status = track.get("verify_status")
    verify_codec = (track.get("verify_codec") or "").lower()

    if verify_codec in ("aac", "mp3"):
        return "not_lossless"
    if verify_status == "fail" and "not lossless" in err:
        return "not_lossless"
    if "not lossless" in err or "aac" in err or "mp3" in err:
        return "not_lossless"
    if (
        "no tidal match" in err
        or "no match" in err
        or "not found on tidal" in err
        or "permanently unavailable" in err
    ):
        return "no_tidal_match"
    if "geo-restricted" in err or "unavailable on tidal" in err:
        return "download_failed"
    if "download failed" in err or "download error" in err:
        return "download_failed"
    if "lexicon" in err:
        return "lexicon_sync_failed"
    if "fingerprint" in err or "mismatched" in err or "wrong version" in err:
        return "wrong_version"
    return "other"
