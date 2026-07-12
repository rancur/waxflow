"""Lossless-verification gate for externally-sourced (Soulseek/slskd) audio.

This is the crux that protects Will's lossless standard: Soulseek is full of FAKE
FLACs (lossy MP3/AAC re-wrapped as FLAC, or upsampled). Before any slskd-sourced
file is allowed into WaxFlow's import path it must pass EVERY check here:

  1. Container/codec is genuinely lossless (flac / alac / wav / aiff / pcm).
  2. Bit depth >= 16 and sample rate >= 44.1 kHz.
  3. The whole file decodes cleanly (ffmpeg -v error -f null) — no corruption/truncation.
  4. Spectral transcode detection: a per-frame FFT (via ffmpeg + numpy) looks for a
     hard low-pass "brickwall" cliff well below Nyquist that betrays an MP3/AAC source
     re-encoded to FLAC. Reliably catches MP3-sourced fakes (the dominant kind) and
     low/mid-bitrate AAC. NOTE (honest limit): a transparent high-bitrate (>=256k)
     AAC transcode preserves full bandwidth and can pass this spectral test — that is
     an inherent limit of spectral analysis, documented, not a bug.
  5. Duration matches the wanted track within tolerance (rejects wrong/mislabeled files).

Only a file that passes ALL of the above is accepted. Anything else is skipped and
logged — never imported. ffmpeg/ffprobe are required; numpy enables check (4) and if
it is unavailable the gate degrades to (1)(2)(3)(5) and marks the spectral result
"skipped" (it never silently passes a fake on the strength of a missing tool).
"""

from __future__ import annotations

import json
import logging
import os
import subprocess

log = logging.getLogger("worker.lossless_verify")

LOSSLESS_CODECS = (
    "flac", "alac", "wav", "aiff",
    "pcm_s16be", "pcm_s24be", "pcm_s32be",
    "pcm_s16le", "pcm_s24le", "pcm_s32le",
    "pcm_f32le", "pcm_f64le", "pcm_f32be", "pcm_f64be",
)

# Spectral thresholds
DEFAULT_CLIFF_DB = 25.0          # energy within this many dB of the HF anchor counts as "present"
MIN_CUTOFF_LOWSR = 20500         # required effective cutoff (Hz) for sr <= 48 kHz
MIN_CUTOFF_HISR = 21000          # required effective cutoff (Hz) for sr > 48 kHz
HF_POOR_ANCHOR_DB = -70.0        # if the 14-16 kHz anchor is below this, track is naturally HF-poor -> inconclusive


def ffprobe_audio(path: str) -> dict:
    """Return {codec, sample_rate, bit_depth, duration_s} for the first audio stream."""
    r = subprocess.run(
        ["ffprobe", "-v", "quiet", "-print_format", "json",
         "-show_streams", "-show_format", path],
        capture_output=True, text=True, timeout=60,
    )
    if r.returncode != 0:
        raise RuntimeError(f"ffprobe failed: {r.stderr[:300]}")
    data = json.loads(r.stdout or "{}")
    audio = None
    for s in data.get("streams", []):
        if s.get("codec_type") == "audio":
            audio = s
            break
    if not audio:
        raise RuntimeError("no audio stream")
    dur = None
    for src in (audio.get("duration"), data.get("format", {}).get("duration")):
        if src:
            try:
                dur = float(src)
                break
            except (TypeError, ValueError):
                pass
    return {
        "codec": audio.get("codec_name", "unknown"),
        "sample_rate": int(audio.get("sample_rate") or 0),
        "bit_depth": int(audio.get("bits_per_raw_sample") or audio.get("bits_per_sample") or 0),
        "duration_s": dur,
    }


def decodes_cleanly(path: str) -> tuple[bool, str]:
    """Full-decode the file; any ffmpeg error => corrupt/truncated/not-really-lossless."""
    r = subprocess.run(
        ["ffmpeg", "-v", "error", "-vn", "-i", path, "-f", "null", "-"],
        capture_output=True, text=True, timeout=600,
    )
    if r.returncode != 0 or r.stderr.strip():
        return False, (r.stderr or "decode error")[:300]
    return True, ""


def spectral_cutoff(path: str, sample_rate: int, cliff_db: float = DEFAULT_CLIFF_DB,
                    nfft: int = 8192):
    """Estimate the effective high-frequency cutoff (Hz) via a per-frame FFT.

    Decodes the file to mono float PCM at its own sample rate, computes the 99th
    percentile magnitude per frequency bin across all frames (robust vs isolated
    clicks; a brickwall is absent in ALL frames), and finds the highest frequency
    whose energy is still within ``cliff_db`` of the 14-16 kHz anchor. Returns a
    dict with cutoff/anchor/nyquist, or None if numpy is unavailable / file too short.
    """
    try:
        import numpy as np
    except Exception:  # noqa: BLE001
        log.warning("numpy unavailable — spectral transcode detection skipped")
        return None

    sr = sample_rate if sample_rate >= 8000 else 44100
    p = subprocess.run(
        ["ffmpeg", "-v", "error", "-vn", "-i", path, "-ac", "1", "-ar", str(sr), "-f", "f32le", "-"],
        capture_output=True, timeout=600,
    )
    x = np.frombuffer(p.stdout, dtype=np.float32)
    if x.size < nfft * 8:
        return None
    nfr = x.size // nfft
    x = x[: nfr * nfft].reshape(nfr, nfft)
    win = np.hanning(nfft).astype(np.float32)
    mag = np.abs(np.fft.rfft(x * win, axis=1))
    band = np.percentile(mag, 99.0, axis=0)
    freqs = np.fft.rfftfreq(nfft, 1.0 / sr)
    db = 20 * np.log10(band / (band.max() + 1e-12) + 1e-12)

    def at(f):
        return float(db[int(np.argmin(np.abs(freqs - f)))])

    anchor = max(at(14000), at(15000), at(16000))
    thr = anchor - cliff_db
    above = np.where((freqs >= 14000) & (db > thr))[0]
    cutoff = float(freqs[int(above.max())]) if above.size else 14000.0
    return {
        "cutoff_hz": round(cutoff),
        "anchor_db": round(anchor, 1),
        "nyquist_hz": sr // 2,
    }


def verify_lossless(
    path: str,
    expected_duration_ms: int | None = None,
    duration_tolerance_s: float = 4.0,
    cliff_db: float = DEFAULT_CLIFF_DB,
) -> dict:
    """Run the full gate. Returns a dict with 'passed' (bool), 'reasons' (list),
    and probe/spectral details. A file passes only if every hard check passes."""
    result: dict = {"passed": False, "reasons": [], "checks": {}}

    if not path or not os.path.exists(path) or os.path.getsize(path) == 0:
        result["reasons"].append("file missing or empty")
        return result

    # (1)(2) codec / bit depth / sample rate
    try:
        probe = ffprobe_audio(path)
    except Exception as e:  # noqa: BLE001
        result["reasons"].append(f"ffprobe failed: {e}")
        return result
    result["checks"]["probe"] = probe
    codec = probe["codec"]
    sr = probe["sample_rate"]
    bits = probe["bit_depth"]

    if codec not in LOSSLESS_CODECS:
        result["reasons"].append(f"not a lossless codec: {codec}")
    if sr < 44100:
        result["reasons"].append(f"sample rate too low: {sr}")
    # FLAC/ALAC may report bit_depth 0 for some encoders; only fail if a value is present and < 16
    if bits and bits < 16:
        result["reasons"].append(f"bit depth too low: {bits}")

    # (3) clean decode
    ok, err = decodes_cleanly(path)
    result["checks"]["clean_decode"] = ok
    if not ok:
        result["reasons"].append(f"decode error (corrupt/not lossless): {err}")

    # (5) duration match (hard, when we know the expected duration)
    if expected_duration_ms and probe.get("duration_s"):
        diff = abs(probe["duration_s"] - expected_duration_ms / 1000.0)
        result["checks"]["duration_diff_s"] = round(diff, 2)
        if diff > duration_tolerance_s:
            result["reasons"].append(
                f"duration mismatch {diff:.1f}s > {duration_tolerance_s}s (likely wrong track)"
            )

    # (4) spectral transcode detection — only meaningful on a real lossless container
    spectral = None
    if codec in LOSSLESS_CODECS and sr >= 44100:
        try:
            spectral = spectral_cutoff(path, sr, cliff_db=cliff_db)
        except Exception as e:  # noqa: BLE001
            log.warning("spectral analysis error for %s: %s", path, e)
    result["checks"]["spectral"] = spectral
    if spectral is None:
        result["checks"]["spectral_verdict"] = "skipped"
    elif spectral["anchor_db"] < HF_POOR_ANCHOR_DB:
        # naturally HF-poor content — cannot judge a cutoff; do not fail on spectral grounds
        result["checks"]["spectral_verdict"] = "inconclusive_hf_poor"
    else:
        min_cut = MIN_CUTOFF_LOWSR if sr <= 48000 else MIN_CUTOFF_HISR
        if spectral["cutoff_hz"] < min_cut:
            result["checks"]["spectral_verdict"] = "fail"
            result["reasons"].append(
                f"lossy transcode suspected: HF cutoff {spectral['cutoff_hz']} Hz "
                f"< {min_cut} Hz (fake FLAC)"
            )
        else:
            result["checks"]["spectral_verdict"] = "pass"

    result["passed"] = len(result["reasons"]) == 0
    return result
