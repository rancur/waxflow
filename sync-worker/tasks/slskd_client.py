"""slskd (Soulseek) REST client for the WaxFlow lossless-verified fallback.

Context / security
------------------
slskd runs on pi-dl inside the SAME network namespace as sabnzbd's VPN container
(gluetun-sab, Mullvad WireGuard) — ``network_mode: service:gluetun-sab`` — so ALL
of slskd's Soulseek/P2P traffic egresses only through the VPN and never leaks the
home IP. This module only talks to slskd's LAN REST API (:5030) and fetches the
resulting completed files from a companion LAN-only read-only file server (:5031,
nginx, basic-auth). Those two LAN hops carry no P2P traffic and are not the VPN.

The Soulseek account + API key + file-server password live in 1Password
(vault "Barry", item "Soulseek (slskd)") and are injected into the worker via env.

Endpoints used:
  GET  /api/v0/server                         -> server/login state
  POST /api/v0/searches                       -> start a search
  GET  /api/v0/searches/{id}                  -> search state
  GET  /api/v0/searches/{id}/responses        -> per-peer results
  POST /api/v0/transfers/downloads/{user}     -> enqueue a download
  GET  /api/v0/transfers/downloads/{user}     -> transfer state
File bytes are fetched from SLSKD_FILES_URL (nginx) by relative on-disk path.
"""

from __future__ import annotations

import logging
import os
import time
import uuid
import urllib.parse

import httpx

log = logging.getLogger("worker.slskd")


def _env(name: str, default: str = "") -> str:
    return (os.environ.get(name) or default).strip()


SLSKD_URL = _env("SLSKD_URL", "http://192.168.20.31:5030").rstrip("/")
SLSKD_API_KEY = _env("SLSKD_API_KEY", "")
SLSKD_FILES_URL = _env("SLSKD_FILES_URL", "http://192.168.20.31:5031").rstrip("/")
SLSKD_FILES_USER = _env("SLSKD_FILES_USER", "waxflow")
SLSKD_FILES_PASSWORD = _env("SLSKD_FILES_PASSWORD", SLSKD_API_KEY)

_TERMINAL_FAIL = ("Errored", "Cancelled", "Rejected", "TimedOut")


class SlskdError(Exception):
    pass


class SlskdClient:
    """Thin, dependency-light slskd REST client (httpx only)."""

    def __init__(
        self,
        base: str | None = None,
        api_key: str | None = None,
        files_url: str | None = None,
        files_user: str | None = None,
        files_password: str | None = None,
        timeout: float = 30.0,
    ):
        self.base = (base or SLSKD_URL).rstrip("/")
        self.api_key = api_key if api_key is not None else SLSKD_API_KEY
        self.files_url = (files_url or SLSKD_FILES_URL).rstrip("/")
        self.files_user = files_user or SLSKD_FILES_USER
        self.files_password = files_password if files_password is not None else SLSKD_FILES_PASSWORD
        self.timeout = timeout

    # -- config / health -----------------------------------------------------
    @property
    def configured(self) -> bool:
        return bool(self.base and self.api_key)

    def _headers(self) -> dict:
        return {"X-API-Key": self.api_key, "Content-Type": "application/json"}

    def _api(self, path: str) -> str:
        return f"{self.base}/api/v0{path}"

    def server_state(self) -> dict:
        with httpx.Client(timeout=self.timeout) as c:
            r = c.get(self._api("/server"), headers=self._headers())
            r.raise_for_status()
            return r.json()

    def is_logged_in(self) -> bool:
        try:
            return bool(self.server_state().get("isLoggedIn"))
        except Exception as e:  # noqa: BLE001
            log.warning("slskd server_state failed: %s", e)
            return False

    # -- search --------------------------------------------------------------
    def search(self, text: str, wait: float = 25.0, poll: float = 3.0) -> list[dict]:
        """Run a Soulseek search and return the per-peer response list."""
        sid = str(uuid.uuid4())
        with httpx.Client(timeout=self.timeout) as c:
            c.post(
                self._api("/searches"),
                headers=self._headers(),
                json={"id": sid, "searchText": text},
            ).raise_for_status()
            deadline = time.time() + wait
            while time.time() < deadline:
                time.sleep(poll)
                s = c.get(self._api(f"/searches/{sid}"), headers=self._headers()).json()
                if str(s.get("state", "")).startswith("Completed"):
                    break
            r = c.get(self._api(f"/searches/{sid}/responses"), headers=self._headers())
            r.raise_for_status()
            return r.json() or []

    # -- transfers -----------------------------------------------------------
    def enqueue(self, username: str, filename: str, size: int) -> None:
        enc = urllib.parse.quote(username, safe="")
        with httpx.Client(timeout=self.timeout) as c:
            r = c.post(
                self._api(f"/transfers/downloads/{enc}"),
                headers=self._headers(),
                json=[{"filename": filename, "size": size}],
            )
            r.raise_for_status()

    def transfer_state(self, username: str, filename: str):
        """Return (state, percentComplete, id) for a specific transfer, or (None,..)."""
        enc = urllib.parse.quote(username, safe="")
        with httpx.Client(timeout=self.timeout) as c:
            r = c.get(self._api(f"/transfers/downloads/{enc}"), headers=self._headers())
            if r.status_code == 404:
                return None, None, None
            r.raise_for_status()
            t = r.json()
        users = [t] if isinstance(t, dict) else (t or [])
        for u in users:
            if u.get("username") != username:
                continue
            for d in u.get("directories", []):
                for f in d.get("files", []):
                    if f.get("filename") == filename:
                        return f.get("state"), f.get("percentComplete"), f.get("id")
        return None, None, None

    def download_and_wait(
        self, username: str, filename: str, size: int, timeout_s: float = 150.0, poll: float = 5.0
    ) -> bool:
        """Enqueue a download and block until it succeeds or terminally fails.

        Returns True only on 'Completed, Succeeded'. Because the VPN has no
        forwarded port, transfers from firewalled peers can never connect and
        end 'Errored'/'TimedOut'; callers should try the next candidate peer.
        """
        self.enqueue(username, filename, size)
        t0 = time.time()
        last = None
        while time.time() - t0 < timeout_s:
            time.sleep(poll)
            state, pct, _ = self.transfer_state(username, filename)
            if state != last:
                log.info("slskd transfer %s: state=%s pct=%s", username[:16], state, pct)
                last = state
            if state and "Completed, Succeeded" in state:
                return True
            if state and any(x in state for x in _TERMINAL_FAIL):
                return False
        log.info("slskd transfer %s timed out after %ss", username[:16], timeout_s)
        return False

    # -- file retrieval (from the LAN read-only file server) -----------------
    @staticmethod
    def ondisk_relpath(remote_filename: str) -> str:
        """Map a Soulseek remote path to slskd's on-disk relative path.

        slskd stores a completed download as ``<last remote directory>/<basename>``
        under its downloads dir (path separators are normalised). E.g.
        ``media\\Artist\\Album\\03 - Song.flac`` -> ``Album/03 - Song.flac``.
        """
        norm = remote_filename.replace("\\", "/")
        parts = [p for p in norm.split("/") if p]
        if len(parts) >= 2:
            return f"{parts[-2]}/{parts[-1]}"
        return parts[-1] if parts else remote_filename

    def _files_auth(self):
        if self.files_user:
            return (self.files_user, self.files_password)
        return None

    def head_file(self, relpath: str):
        """Return (ok, size) for a completed file on the file server."""
        url = f"{self.files_url}/" + urllib.parse.quote(relpath)
        with httpx.Client(timeout=self.timeout) as c:
            r = c.head(url, auth=self._files_auth())
            if r.status_code == 200:
                return True, int(r.headers.get("Content-Length") or 0)
            return False, 0

    def fetch_file(self, relpath: str, dest_path: str) -> int:
        """Stream a completed file from the file server to dest_path. Returns bytes."""
        url = f"{self.files_url}/" + urllib.parse.quote(relpath)
        written = 0
        with httpx.Client(timeout=None) as c:
            with c.stream("GET", url, auth=self._files_auth()) as r:
                r.raise_for_status()
                with open(dest_path, "wb") as fh:
                    for chunk in r.iter_bytes(chunk_size=1 << 20):
                        fh.write(chunk)
                        written += len(chunk)
        return written


def get_client() -> SlskdClient:
    return SlskdClient()
