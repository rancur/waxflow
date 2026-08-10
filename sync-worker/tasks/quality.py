"""Audio quality scoring — the shared vocabulary for "is this file good enough?".

WHY A LADDER RATHER THAN A BOOLEAN
    WaxFlow used to answer one question: lossless, yes or no. Anything else was an
    error, so a 320 kbps MP3 of a track that exists nowhere else was refused
    entirely and the track simply stayed missing. That is the wrong trade for a DJ
    library: a 320k file you can play beats a perfect file you do not have.

    Scoring instead of gating lets the pipeline say "import this now, and keep
    looking for something better", which is what the upgrade watcher acts on.

THE LADDER
    hi-res lossless   50   lossless and sample_rate > 48 kHz
    24-bit lossless   40   lossless and bit_depth >= 24
    16-bit lossless   30   lossless, 44.1-48 kHz          <- TARGET
    320k lossy        20   lossy and effective bitrate >= 320k (less tolerance)
    below floor        0   rejected

    `score` orders strictly WITHIN a tier as well, so `is_upgrade` is a plain `>`
    and two candidates in the same tier still compare sensibly.

MIRRORED FILE
    An identical copy lives at sync-api/quality.py. sync-api cannot import
    sync-worker code (separate containers, separate build contexts), and this is
    the same convention v3_schema.py / init_db.py already use. A test asserts the
    two copies are byte-identical, so edit BOTH or the test fails.

    Everything here is therefore a pure function over plain dicts. `score_file`
    imports ffprobe lazily, inside the function body, so the module stays importable
    in an environment with no worker dependencies at all.
"""

from __future__ import annotations

from dataclasses import dataclass, field

# Codecs that are actually lossless. `alac` and `wav`/`pcm_*` included; anything
# not listed is treated as lossy, which is the safe default for an unknown codec.
LOSSLESS_CODECS = frozenset({
    "flac", "alac", "wav", "aiff", "ape", "wavpack", "tta", "tak",
    "pcm_s16le", "pcm_s16be", "pcm_s24le", "pcm_s24be", "pcm_s32le", "pcm_s32be",
    "pcm_f32le", "pcm_f64le",
})

TIER_HIRES = 50
TIER_24BIT = 40
TIER_LOSSLESS = 30
TIER_LOSSY_HIGH = 20
TIER_BELOW = 0

TIER_NAMES = {
    TIER_HIRES: "hi-res",
    TIER_24BIT: "24-bit",
    TIER_LOSSLESS: "lossless",
    TIER_LOSSY_HIGH: "320k",
    TIER_BELOW: "below-floor",
}

FLOOR_TIER = TIER_LOSSY_HIGH      # the lowest we will import at all
TARGET_TIER = TIER_LOSSLESS       # what we keep hunting for

# A "320k" file rarely measures exactly 320000. VBR and container overhead move it
# either way, so allow a small shortfall rather than rejecting real 320k rips.
LOSSY_FLOOR_BPS = 320_000
LOSSY_FLOOR_TOLERANCE = 0.05

# Codecs whose bit_depth ffprobe reports as 0 even though the depth is in the name.
_PCM_DEPTHS = {"pcm_s16le": 16, "pcm_s16be": 16, "pcm_s24le": 24, "pcm_s24be": 24,
               "pcm_s32le": 32, "pcm_s32be": 32}


@dataclass(frozen=True)
class QualityScore:
    tier: int
    tier_name: str
    score: int
    codec: str
    sample_rate: int
    bit_depth: int
    bit_rate: int
    lossless: bool
    reasons: tuple = field(default_factory=tuple)

    @property
    def meets_floor(self) -> bool:
        return self.tier >= FLOOR_TIER

    @property
    def meets_target(self) -> bool:
        return self.tier >= TARGET_TIER

    def as_dict(self) -> dict:
        return {
            "tier": self.tier, "tier_name": self.tier_name, "score": self.score,
            "codec": self.codec, "sample_rate": self.sample_rate,
            "bit_depth": self.bit_depth, "bit_rate": self.bit_rate,
            "lossless": self.lossless, "reasons": list(self.reasons),
        }


def _int(value) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def effective_bit_depth(codec: str, reported: int) -> int:
    """Bit depth, recovering it from the codec name when ffprobe reports 0."""
    if reported:
        return reported
    return _PCM_DEPTHS.get((codec or "").lower(), 0)


def score_probe(probe: dict) -> QualityScore:
    """Score an ffprobe-shaped dict. Pure; this is the function to test against."""
    codec = str(probe.get("codec") or "unknown").lower()
    sample_rate = _int(probe.get("sample_rate"))
    bit_rate = _int(probe.get("bit_rate"))
    bit_depth = effective_bit_depth(codec, _int(probe.get("bit_depth")))
    lossless = codec in LOSSLESS_CODECS
    reasons: list[str] = []

    if lossless:
        if sample_rate > 48_000:
            tier = TIER_HIRES
        elif bit_depth >= 24:
            tier = TIER_24BIT
        else:
            tier = TIER_LOSSLESS
            if sample_rate and sample_rate < 44_100:
                # Lossless at a reduced sample rate is not target quality.
                tier = TIER_LOSSY_HIGH
                reasons.append(f"lossless but only {sample_rate} Hz")
    else:
        floor = LOSSY_FLOOR_BPS * (1 - LOSSY_FLOOR_TOLERANCE)
        if bit_rate >= floor:
            tier = TIER_LOSSY_HIGH
            reasons.append(f"lossy {codec} at {bit_rate // 1000}k")
        else:
            tier = TIER_BELOW
            reasons.append(
                f"lossy {codec} at {bit_rate // 1000}k, below the "
                f"{LOSSY_FLOOR_BPS // 1000}k floor" if bit_rate
                else f"lossy {codec} with an unreadable bitrate")

    # Strict ordering inside a tier, so is_upgrade() can be a plain comparison.
    score = (tier * 1_000_000
             + min(sample_rate, 192_000) * 4
             + bit_depth * 1_000
             + min(bit_rate // 1000, 9_999))

    return QualityScore(
        tier=tier, tier_name=TIER_NAMES[tier], score=score, codec=codec,
        sample_rate=sample_rate, bit_depth=bit_depth, bit_rate=bit_rate,
        lossless=lossless, reasons=tuple(reasons),
    )


def score_row(row: dict) -> QualityScore:
    """Score a `tracks` row using the verify_* columns already stored."""
    return score_probe({
        "codec": row.get("verify_codec"),
        "sample_rate": row.get("verify_sample_rate"),
        "bit_depth": row.get("verify_bit_depth"),
        "bit_rate": row.get("quality_bit_rate"),
    })


def score_file(path: str) -> QualityScore:
    """Probe a file and score it.

    ffprobe is imported HERE rather than at module scope so this module stays
    importable (and its copy verifiable) without the worker's dependencies.
    """
    from tasks.lossless_verify import ffprobe_audio
    return score_probe(ffprobe_audio(path))


def meets_floor(score: QualityScore) -> bool:
    return score.tier >= FLOOR_TIER


def meets_target(score: QualityScore) -> bool:
    return score.tier >= TARGET_TIER


def is_upgrade(current: QualityScore | None, candidate: QualityScore,
               min_delta: int = 1) -> bool:
    """Is `candidate` meaningfully better than what we already have?

    Requires a strict improvement: equal quality is not worth a re-download, and
    treating it as one is how an upgrade loop starts churning the same track.
    """
    if not candidate.meets_floor:
        return False
    if current is None:
        return True
    return candidate.score - current.score >= min_delta
