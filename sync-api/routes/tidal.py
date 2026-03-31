import json
import os
import time

import httpx
from fastapi import APIRouter

from db import get_db

router = APIRouter(prefix="/api", tags=["tidal"])

_TIDAL_CLIENT_ID = "zU4XHVVkc2tDPo4t"  # tiddl's client ID

_AUTH_PATHS = [
    "/tiddl-auth/auth.json",       # mounted from Tidarr
    "/app/data/tiddl-auth.json",   # our own auth file from web UI flow
]


@router.get("/tidal/status")
async def tidal_status():
    """Check if Tidal is connected and token is valid."""
    for path in _AUTH_PATHS:
        try:
            with open(path) as f:
                auth = json.load(f)
            expires = auth.get("expires_at", 0)
            return {
                "connected": True,
                "user_id": auth.get("user_id"),
                "country": auth.get("country_code"),
                "expires_at": expires,
                "expired": expires < time.time(),
                "hours_left": round((expires - time.time()) / 3600, 1),
                "source": path,
            }
        except Exception:
            continue
    return {"connected": False}


@router.post("/tidal/auth/start")
async def tidal_auth_start():
    """Start Tidal device code auth flow."""
    async with httpx.AsyncClient() as client:
        resp = await client.post(
            "https://auth.tidal.com/v1/oauth2/device_authorization",
            data={
                "client_id": _TIDAL_CLIENT_ID,
                "scope": "r_usr w_usr w_sub",
            },
        )
        if resp.status_code == 200:
            data = resp.json()
            # Store the device code temporarily in app_config
            with get_db() as conn:
                conn.execute(
                    "INSERT OR REPLACE INTO app_config (key, value) VALUES ('_tidal_device_code', ?)",
                    (json.dumps(data),),
                )
            return {
                "verification_uri": data.get("verificationUriComplete", data.get("verificationUri")),
                "user_code": data.get("userCode"),
                "device_code": data.get("deviceCode"),
                "expires_in": data.get("expiresIn", 300),
                "interval": data.get("interval", 5),
            }
        return {"error": f"Tidal auth failed: {resp.status_code}"}


@router.post("/tidal/auth/poll")
async def tidal_auth_poll():
    """Poll Tidal to check if user has authorized."""
    with get_db() as conn:
        row = conn.execute(
            "SELECT value FROM app_config WHERE key = '_tidal_device_code'"
        ).fetchone()
        if not row:
            return {"status": "no_pending_auth"}
        device_data = json.loads(row[0])

    async with httpx.AsyncClient() as client:
        resp = await client.post(
            "https://auth.tidal.com/v1/oauth2/token",
            data={
                "client_id": _TIDAL_CLIENT_ID,
                "device_code": device_data["deviceCode"],
                "grant_type": "urn:ietf:params:oauth:grant-type:device_code",
                "scope": "r_usr w_usr w_sub",
            },
        )
        data = resp.json()

        if resp.status_code == 200 and "access_token" in data:
            # Success — save the token
            auth = {
                "token": data["access_token"],
                "refresh_token": data.get("refresh_token", ""),
                "expires_at": int(time.time()) + data.get("expires_in", 86400),
                "user_id": str(data.get("user", {}).get("userId", "")),
                "country_code": data.get("user", {}).get("countryCode", "US"),
            }
            auth_path = "/app/data/tiddl-auth.json"
            os.makedirs(os.path.dirname(auth_path), exist_ok=True)
            with open(auth_path, "w") as f:
                json.dump(auth, f, indent=2)

            # Clean up device code and log
            with get_db() as conn:
                conn.execute(
                    "DELETE FROM app_config WHERE key = '_tidal_device_code'"
                )
                conn.execute(
                    "INSERT INTO activity_log (event_type, message) VALUES (?, ?)",
                    ("tidal_auth", f"Tidal connected: user_id={auth['user_id']}"),
                )

            return {
                "status": "authorized",
                "user_id": auth["user_id"],
                "country": auth["country_code"],
            }

        if data.get("sub_status") == 1002 or "pending" in str(
            data.get("error", "")
        ):
            return {"status": "pending"}

        return {
            "status": "error",
            "error": data.get("error_description", str(data)),
        }
