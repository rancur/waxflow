import os

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from routes.dashboard import router as dashboard_router
from routes.tracks import router as tracks_router
from routes.matching import router as matching_router
from routes.downloads import router as downloads_router
from routes.uploads import router as uploads_router
from routes.playlists import router as playlists_router
from routes.lexicon import router as lexicon_router
from routes.admin import router as admin_router
from routes.spotify import router as spotify_router
from routes.tidal import router as tidal_router

app = FastAPI(
    title="WaxFlow API",
    description="All your music, flowing home. Spotify Liked Songs to Lexicon DJ.",
    version="2.0.0",
)

_cors_origins_env = os.environ.get("CORS_ORIGINS", "")
_cors_origins = [o.strip() for o in _cors_origins_env.split(",") if o.strip()] if _cors_origins_env else []

app.add_middleware(
    CORSMiddleware,
    allow_origins=_cors_origins if _cors_origins else ["*"],
    allow_credentials=bool(_cors_origins),  # credentials only valid with explicit origins
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(dashboard_router)
app.include_router(tracks_router)
app.include_router(matching_router)
app.include_router(downloads_router)
app.include_router(uploads_router)
app.include_router(playlists_router)
app.include_router(lexicon_router)
app.include_router(admin_router)
app.include_router(spotify_router)
app.include_router(tidal_router)


@app.get("/")
async def root():
    return {"service": "waxflow", "version": "2.0.0"}
