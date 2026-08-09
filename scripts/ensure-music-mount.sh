#!/bin/bash
# WaxFlow: ensure the NAS "music" SMB share is mounted at /Volumes/music so Lexicon
# can read freshly-downloaded tracks the worker writes directly to /volume1/music.
# (Synology Drive two-way sync CANNOT deliver container/bind-mount writes — proven
#  2026-07-11 — so SMB is the authoritative delivery path.)
#
# Deployed on the Lexicon host Mac at ~/.waxflow/ensure-music-mount.sh, run every
# ~2 min by LaunchAgent com.waxflow.mount-music. THIS repo copy is canonical —
# deploy changes with:
#   scp scripts/ensure-music-mount.sh <you>@<lexicon-mac>:.waxflow/ensure-music-mount.sh
#
# v2 (2026-07-20) — WRONG-MOUNTPOINT HEAL. Root cause of the Jul-18 sleep incident:
# when the Mac sleeps, the SMB session drops; on wake macOS auto-remounts the share
# but, if the old /Volumes/music directory is still lingering, the new mount lands at
# /Volumes/music-1 (or -2, ...). AppleScript `mount volume` then treats the share as
# "already mounted" and NO-OPS, so v1 of this script logged MOUNT FAILED every 2 min
# forever while every Lexicon path (/Volumes/music/...) was dead. v2 detects the
# same share mounted at a wrong mountpoint, unmounts it, clears any stale dir, and
# remounts at the canonical path.
MP="/Volumes/music"
# v5 (2026-08-09) — ADDRESS THE NAS BY IP, NOT BY BONJOUR SERVICE NAME.
# This was "CCPD-Database._smb._tcp.local", the Bonjour SERVICE-INSTANCE name.
# That is not a hostname: it resolves only through service discovery, and when
# the advertisement went stale (after the NAS's Container Manager restart) every
# connection attempt HUNG instead of failing — Finder sat on "Connecting to
# smb://CCPD-...local/music" indefinitely, and so did `mount volume`.
#
# Measured at the time: `CCPD-Database._smb._tcp.local` did not resolve at all,
# while `CCPD-Database.local` and 192.168.1.221 both resolved instantly and
# `smbutil status 192.168.1.221` negotiated fine. Mounting by IP succeeded in 1s.
#
# Configure per host. Put your NAS address in ~/.waxflow/waxflow.conf:
#     WAXFLOW_SHARE_HOST=192.168.1.50      # IP is the most reliable
#     WAXFLOW_SHARE_NAME=music
# An IP or a plain hostname (nas.local) both work. A Bonjour SERVICE name does
# not — that is the bug described above.
[ -r "$HOME/.waxflow/waxflow.conf" ] && . "$HOME/.waxflow/waxflow.conf"
SHARE_HOST="${WAXFLOW_SHARE_HOST:-}"
SHARE_NAME="${WAXFLOW_SHARE_NAME:-music}"
if [ -z "$SHARE_HOST" ]; then
  echo "$(date '+%Y-%m-%dT%H:%M:%S') CONFIG MISSING: set WAXFLOW_SHARE_HOST in ~/.waxflow/waxflow.conf (e.g. your NAS IP)" >>"$HOME/.waxflow/mount-music.log"
  exit 64
fi
URL="smb://${SHARE_HOST}/${SHARE_NAME}"
LOG="$HOME/.waxflow/mount-music.log"
ts() { date "+%Y-%m-%dT%H:%M:%S"; }

# 1) Healthy already? (mounted at the canonical path AND readable)
#
# v3 (2026-08-09) — DO NOT UNMOUNT ON A SINGLE SLOW ls.
# The v2 check treated one failed `ls` as proof of a stale handle and unmounted.
# Under NAS load (observed at load ~10 after a Container Manager restart) a
# healthy share intermittently fails a single `ls`, so this script would unmount
# a WORKING mount and then fail to restore it: `osascript mount volume` cannot
# authenticate from a launchd agent (no GUI/keychain access), which is why the log
# filled with "MOUNT FAILED" every 2 minutes and the sync agent aborted on 32 of
# its last 40 passes while manual runs from a terminal always succeeded.
#
# Require several consecutive failures, spaced out, before touching the mount. A
# genuinely stale handle stays broken; a briefly-slow server recovers on retry.
if mount | grep -q " on ${MP} (smbfs"; then
  readable=0; lserr=""
  for attempt in 1 2 3; do
    lserr="$(ls "${MP}" 2>&1 >/dev/null)"
    if [ -z "$lserr" ]; then readable=1; break; fi
    [ "$attempt" -lt 3 ] && sleep 3
  done
  if [ "$readable" -eq 1 ]; then exit 0; fi

  # EPERM = macOS TCC denying THIS PROCESS access to a network volume. The mount
  # is healthy. Fix is one-time and GUI-only: System Settings -> Privacy &
  # Security -> Full Disk Access -> add /bin/bash.
  case "$lserr" in
    *"Operation not permitted"*)
      echo "$(ts) EPERM reading ${MP} — TCC is denying this process, mount is FINE. Not unmounting. Grant Full Disk Access to /bin/bash." >>"$LOG"
      exit 2
      ;;
  esac

  # NEVER unmount by default.
  #
  # v4 (2026-08-09). v3 still unmounted on any non-EPERM failure, and that is how
  # the share was lost: a transient read failure -> unmount -> remount hangs. From
  # a launchd agent `mount volume` cannot reach the keychain, and `mount_smbfs`
  # gets "Authentication error", so the ONLY way back is a human in Finder.
  # Trading a possibly-stale mount for a definitely-unmountable one is a bad deal:
  # macOS usually re-establishes a dropped SMB session by itself, and a stale
  # handle costs one sync cycle whereas a failed remount costs every cycle until
  # someone notices.
  #
  # Set WAXFLOW_ALLOW_REMOUNT=1 to opt into the old destructive behaviour when
  # running interactively (where remounting actually works).
  if [ "${WAXFLOW_ALLOW_REMOUNT:-0}" != "1" ]; then
    echo "$(ts) ${MP} unreadable (${lserr}) — leaving the mount ALONE (remount is unreliable from launchd). Re-run interactively with WAXFLOW_ALLOW_REMOUNT=1, or reconnect in Finder." >>"$LOG"
    exit 3
  fi
  echo "$(ts) stale mount at ${MP} (${lserr}) — remounting (WAXFLOW_ALLOW_REMOUNT=1)" >>"$LOG"
  umount "${MP}" 2>/dev/null || diskutil unmount "${MP}" >/dev/null 2>&1 \
    || diskutil unmount force "${MP}" >/dev/null 2>&1
fi

# 2) WRONG-MOUNTPOINT: the same music share mounted anywhere other than ${MP}
#    (typically /Volumes/music-1 after a sleep/wake remount race). `mount volume`
#    no-ops while such a mount exists, so it MUST be unmounted first.
mount | grep -E "/music on /Volumes/[^ ]+ \(smbfs" | grep -v " on ${MP} (" \
  | sed -E 's|.* on (/Volumes/[^ ]+) \(smbfs.*|\1|' | while IFS= read -r wrong; do
    echo "$(ts) share mounted at WRONG mountpoint ${wrong} — unmounting" >>"$LOG"
    umount "${wrong}" 2>/dev/null || diskutil unmount "${wrong}" >/dev/null 2>&1 \
      || diskutil unmount force "${wrong}" >/dev/null 2>&1
    if mount | grep -q " on ${wrong} (smbfs"; then
      echo "$(ts) FAILED to unmount ${wrong} (in use?) — cannot heal this cycle" >>"$LOG"
    fi
done

# 3) A stale (non-mount) /Volumes/music directory forces the next mount to music-1.
#    Remove it only if it is empty and not a mountpoint.
if [ -d "${MP}" ] && ! mount | grep -q " on ${MP} (smbfs"; then
  rmdir "${MP}" 2>/dev/null && echo "$(ts) removed stale empty dir ${MP}" >>"$LOG"
fi

# 4) Mount (Finder/keychain credentials) and verify it landed at the canonical path.
#
# HARD TIMEOUT, because `osascript mount volume` can block FOREVER: when the SMB
# session needs re-authentication it waits on NetAuthAgent for a credential
# dialog, which never appears in a launchd context. Observed 2026-08-09 — two
# osascript processes wedged for minutes, blocking BOTH agents and holding the
# sync lock, so the sync stopped running entirely. An agent that hangs is worse
# than one that fails: the failure retries next cycle, the hang never does.
# (macOS has no coreutils `timeout`, hence the watchdog-subshell.)
MOUNT_TIMEOUT="${WAXFLOW_MOUNT_TIMEOUT:-45}"
/usr/bin/osascript -e "try" -e "mount volume \"${URL}\"" -e "end try" >>"$LOG" 2>&1 &
OSPID=$!
( sleep "$MOUNT_TIMEOUT"; kill -9 "$OSPID" 2>/dev/null ) >/dev/null 2>&1 &
WATCHDOG=$!
wait "$OSPID" 2>/dev/null
kill "$WATCHDOG" 2>/dev/null

sleep 3
if mount | grep -q " on ${MP} (smbfs" && ls "${MP}" >/dev/null 2>&1; then
  echo "$(ts) mounted OK at ${MP}" >>"$LOG"; exit 0
fi
echo "$(ts) MOUNT FAILED (not at ${MP} after ${MOUNT_TIMEOUT}s). If this repeats, the SMB session needs re-auth: reconnect once in Finder (Go > Connect to Server > ${URL})." >>"$LOG"
exit 1
