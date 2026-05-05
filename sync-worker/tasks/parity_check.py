"""Parity check: compare Spotify total vs Lexicon synced count."""

import asyncio
import logging
import time

import httpx

from tasks.helpers import get_config, get_db, get_spotify_client, log_activity, set_config

log = logging.getLogger("worker.parity")


def _check(db_path: str):
    """Synchronous parity check (runs in thread)."""
    with get_db(db_path) as conn:
        total_row = conn.execute("SELECT COUNT(*) as cnt FROM tracks").fetchone()
        synced_row = conn.execute(
            "SELECT COUNT(*) as cnt FROM tracks WHERE lexicon_status = 'synced'"
        ).fetchone()
        error_row = conn.execute(
            "SELECT COUNT(*) as cnt FROM tracks WHERE pipeline_stage = 'error'"
        ).fetchone()
        in_progress_row = conn.execute(
            "SELECT COUNT(*) as cnt FROM tracks WHERE pipeline_stage NOT IN ('complete', 'error')"
        ).fetchone()

    total = total_row["cnt"] if total_row else 0
    synced = synced_row["cnt"] if synced_row else 0
    errors = error_row["cnt"] if error_row else 0
    in_progress = in_progress_row["cnt"] if in_progress_row else 0

    parity_pct = (synced / total * 100) if total > 0 else 0.0

    # Also check Spotify liked count if auth is available
    spotify_total = None
    sp = get_spotify_client(db_path)
    if sp:
        try:
            results = sp.current_user_saved_tracks(limit=1)
            spotify_total = results.get("total", 0)
        except Exception as e:
            log.warning("Could not fetch Spotify total: %s", e)

    # Load previous stats before updating (for delta and drop detection)
    prev_pct_str = get_config(db_path, "parity_pct")
    prev_synced_str = get_config(db_path, "parity_synced")
    prev_total_str = get_config(db_path, "parity_total_tracks")
    prev_errors_str = get_config(db_path, "parity_errors")
    prev_spotify_str = get_config(db_path, "parity_spotify_total")

    prev_pct = float(prev_pct_str) if prev_pct_str else None
    prev_synced = int(prev_synced_str) if prev_synced_str else 0
    prev_total = int(prev_total_str) if prev_total_str else 0
    prev_errors = int(prev_errors_str) if prev_errors_str else 0
    prev_spotify = int(prev_spotify_str) if prev_spotify_str else None

    # Store parity stats in config for the dashboard API to read
    set_config(db_path, "parity_total_tracks", str(total))
    set_config(db_path, "parity_synced", str(synced))
    set_config(db_path, "parity_errors", str(errors))
    set_config(db_path, "parity_in_progress", str(in_progress))
    set_config(db_path, "parity_pct", f"{parity_pct:.1f}")
    if spotify_total is not None:
        set_config(db_path, "parity_spotify_total", str(spotify_total))
        missing = spotify_total - total
        set_config(db_path, "parity_missing_from_db", str(max(0, missing)))

    log.info(
        "Parity: %d total, %d synced (%.1f%%), %d errors, %d in-progress%s",
        total, synced, parity_pct, errors, in_progress,
        f", spotify={spotify_total}" if spotify_total is not None else "",
    )

    # Log activity only if there are notable issues
    if errors > 0 or (spotify_total is not None and spotify_total > total):
        log_activity(
            db_path, "parity_check", None,
            f"Parity: {synced}/{total} synced ({parity_pct:.1f}%), {errors} errors",
            {
                "total": total,
                "synced": synced,
                "errors": errors,
                "in_progress": in_progress,
                "parity_pct": parity_pct,
                "spotify_total": spotify_total,
            },
        )

    # Check for parity drop below threshold
    if prev_pct is not None:
        _check_parity_drop_alert(
            db_path=db_path,
            parity_pct=parity_pct,
            prev_pct=prev_pct,
            synced=synced,
            total=total,
            errors=errors,
            in_progress=in_progress,
            prev_synced=prev_synced,
            prev_total=prev_total,
            prev_errors=prev_errors,
            spotify_total=spotify_total,
            prev_spotify=prev_spotify,
        )

    # Milestone detection
    prev_milestone_str = get_config(db_path, "parity_last_milestone_pct")
    prev_milestone_pct = float(prev_milestone_str) if prev_milestone_str else 0.0
    milestones = [90, 95, 99, 100]
    for milestone in milestones:
        if parity_pct >= milestone and prev_milestone_pct < milestone:
            msg = f"Parity milestone reached: {milestone}% ({synced}/{total} synced)"
            log.info(msg)
            log_activity(
                db_path, "parity_milestone", None, msg,
                {"milestone": milestone, "synced": synced, "total": total, "parity_pct": parity_pct},
            )
            set_config(db_path, "parity_last_milestone_pct", f"{parity_pct:.1f}")
            _notify_parity_milestone(db_path, milestone, synced, total, parity_pct)
            break  # only fire highest new milestone per check


def _check_parity_drop_alert(
    db_path: str,
    parity_pct: float,
    prev_pct: float,
    synced: int,
    total: int,
    errors: int,
    in_progress: int,
    prev_synced: int,
    prev_total: int,
    prev_errors: int,
    spotify_total: int | None,
    prev_spotify: int | None,
):
    """Send alert webhook if parity dropped below the configured threshold."""
    threshold_str = get_config(db_path, "parity_alert_threshold")
    threshold = float(threshold_str) if threshold_str else 95.0

    # Only alert when parity is below threshold
    if parity_pct >= threshold:
        return

    # Only alert when parity actually dropped (not just staying low)
    if parity_pct >= prev_pct:
        return

    # Cooldown: suppress repeated alerts within the configured window
    cooldown_str = get_config(db_path, "parity_alert_cooldown_seconds")
    cooldown = int(cooldown_str) if cooldown_str else 1800
    last_sent_str = get_config(db_path, "parity_alert_last_sent_ts")
    if last_sent_str:
        try:
            elapsed = time.time() - float(last_sent_str)
            if elapsed < cooldown:
                log.debug(
                    "Parity drop alert suppressed: cooldown active (%ds remaining)",
                    int(cooldown - elapsed),
                )
                return
        except (ValueError, TypeError):
            pass

    # Build delta: what changed since the previous check
    delta: dict = {
        "new_tracks_added": max(0, total - prev_total),
        "newly_synced": max(0, synced - prev_synced),
        "new_errors": max(0, errors - prev_errors),
    }
    if spotify_total is not None and prev_spotify is not None:
        delta["new_liked_songs"] = max(0, spotify_total - prev_spotify)

    # Infer likely cause(s) from the delta
    causes: list[str] = []
    if delta.get("new_liked_songs", 0) > 0:
        causes.append(f"{delta['new_liked_songs']} new liked song(s) not yet synced")
    if delta["new_errors"] > 0:
        causes.append(f"{delta['new_errors']} track(s) moved to error state")
    if delta["new_tracks_added"] > 0 and delta["newly_synced"] == 0:
        causes.append(f"{delta['new_tracks_added']} new track(s) added but none synced yet")

    log.warning(
        "Parity dropped below %.1f%% threshold: %.1f%% (was %.1f%%)%s",
        threshold, parity_pct, prev_pct,
        " — " + "; ".join(causes) if causes else "",
    )
    log_activity(
        db_path, "parity_drop_alert", None,
        f"Parity dropped to {parity_pct:.1f}% (below {threshold:.0f}% threshold, was {prev_pct:.1f}%)",
        {
            "parity_pct": parity_pct,
            "prev_pct": prev_pct,
            "threshold": threshold,
            "synced": synced,
            "total": total,
            "errors": errors,
            "delta": delta,
            "causes": causes,
        },
    )

    _notify_parity_drop(
        db_path=db_path,
        parity_pct=parity_pct,
        prev_pct=prev_pct,
        threshold=threshold,
        synced=synced,
        total=total,
        errors=errors,
        delta=delta,
        causes=causes,
    )
    set_config(db_path, "parity_alert_last_sent_ts", str(time.time()))


def _notify_parity_drop(
    db_path: str,
    parity_pct: float,
    prev_pct: float,
    threshold: float,
    synced: int,
    total: int,
    errors: int,
    delta: dict,
    causes: list[str],
):
    """POST a drop-below-threshold alert to the configured webhook URL."""
    webhook_url = get_config(db_path, "webhook_url")
    if not webhook_url:
        return

    try:
        payload = {
            "event": "parity_drop_alert",
            "parity_pct": round(parity_pct, 1),
            "prev_pct": round(prev_pct, 1),
            "threshold": threshold,
            "synced": synced,
            "total": total,
            "errors": errors,
            "delta": delta,
            "causes": causes,
        }
        with httpx.Client(timeout=5) as client:
            client.post(webhook_url, json=payload)
        log.info("Parity drop alert sent to webhook (parity=%.1f%%)", parity_pct)
    except Exception as e:
        log.warning("Failed to send parity drop webhook: %s", e)


def _notify_parity_milestone(db_path: str, milestone: int, synced: int, total: int, parity_pct: float):
    """Send webhook notification when a parity milestone is crossed."""
    webhook_url = get_config(db_path, "webhook_url")
    if not webhook_url:
        return

    try:
        payload = {
            "event": "parity_milestone",
            "milestone": milestone,
            "synced": synced,
            "total": total,
            "parity_pct": round(parity_pct, 1),
        }
        with httpx.Client(timeout=5) as client:
            client.post(webhook_url, json=payload)
    except Exception:
        pass  # Don't fail parity check on notification error


async def parity_check(db_path: str):
    """Run parity check (async wrapper)."""
    await asyncio.to_thread(_check, db_path)
