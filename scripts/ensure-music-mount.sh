#!/bin/bash
# WaxFlow: ensure the NAS "music" SMB share is mounted at /Volumes/music so Lexicon
# can read freshly-downloaded tracks the worker writes directly to /volume1/music.
# (Synology Drive two-way sync CANNOT deliver container/bind-mount writes — proven
#  2026-07-11 — so SMB is the authoritative delivery path.)
#
# Deployed on the Lexicon host Mac at ~/.waxflow/ensure-music-mount.sh, run every
# ~2 min by LaunchAgent com.waxflow.mount-music. THIS repo copy is canonical —
# deploy changes with:
#   scp scripts/ensure-music-mount.sh willcurran@192.168.1.116:.waxflow/ensure-music-mount.sh
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
SHARE_HOST="CCPD-Database._smb._tcp.local"
URL="smb://${SHARE_HOST}/music"
LOG="$HOME/.waxflow/mount-music.log"
ts() { date "+%Y-%m-%dT%H:%M:%S"; }

# 1) Healthy already? (mounted at the canonical path AND readable)
if mount | grep -q " on ${MP} (smbfs"; then
  if ls "${MP}" >/dev/null 2>&1; then exit 0; fi
  echo "$(ts) stale mount detected at ${MP}, remounting" >>"$LOG"
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
/usr/bin/osascript -e "try" -e "mount volume \"${URL}\"" -e "end try" >>"$LOG" 2>&1
sleep 3
if mount | grep -q " on ${MP} (smbfs" && ls "${MP}" >/dev/null 2>&1; then
  echo "$(ts) mounted OK at ${MP}" >>"$LOG"; exit 0
else
  echo "$(ts) MOUNT FAILED (share not at ${MP} after mount attempt)" >>"$LOG"; exit 1
fi
