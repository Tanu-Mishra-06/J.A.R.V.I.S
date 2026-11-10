"""Application configuration for J.A.R.V.I.S."""

import os
from dataclasses import dataclass, field
from typing import List


def _find_demo_dir() -> str:
    env_dir = os.getenv("JARVIS_DEMO_DIR")
    if env_dir and os.path.isdir(env_dir):
        return env_dir
    curr = os.path.dirname(os.path.abspath(__file__))
    for _ in range(5):
        cand = os.path.join(curr, "datasets", "demo")
        if os.path.isdir(cand) and os.path.exists(os.path.join(cand, "openrca_ground_truth.json")):
            return cand
        curr = os.path.dirname(curr)
    return os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "datasets", "demo")


@dataclass
class Settings:
    DATABASE_URL: str = os.getenv("JARVIS_DATABASE_URL", "sqlite:///./jarvis.db")
    DB_PATH: str = os.getenv("JARVIS_DB_PATH", os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "jarvis.db"))
    UPLOAD_DIR: str = os.getenv("JARVIS_UPLOAD_DIR", os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "datasets", "uploads"))
    DEMO_DIR: str = _find_demo_dir()
    MAX_UPLOAD_SIZE_MB: int = int(os.getenv("JARVIS_MAX_UPLOAD_SIZE_MB", "100"))
    CORS_ORIGINS: List[str] = field(default_factory=lambda: ["http://localhost:5173", "http://localhost:3000", "*"])
    APP_NAME: str = "J.A.R.V.I.S."
    VERSION: str = "1.0.0"
    DEBUG: bool = os.getenv("JARVIS_DEBUG", "True").lower() in ("true", "1", "yes")
    PORT: int = int(os.getenv("PORT", "5000"))


settings = Settings()
os.makedirs(settings.UPLOAD_DIR, exist_ok=True)
os.makedirs(settings.DEMO_DIR, exist_ok=True)
