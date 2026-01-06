"""
Configuration management for Fraudit.

Loads configuration from config.yaml, .env file, and environment variables.
Priority: Environment variables > .env file > config.yaml
"""

import os
from pathlib import Path
from typing import Any

import yaml

# Load .env file if it exists (before reading os.environ)
def _load_dotenv():
    """Load .env file from project root."""
    current = Path.cwd()
    for path in [current] + list(current.parents):
        env_path = path / ".env"
        if env_path.exists():
            with open(env_path) as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith("#") and "=" in line:
                        key, _, value = line.partition("=")
                        key = key.strip()
                        value = value.strip().strip('"').strip("'")
                        # Only set if not already in environment
                        if key not in os.environ:
                            os.environ[key] = value
            break

_load_dotenv()


class Config:
    """Application configuration singleton."""

    _instance = None
    _config: dict = {}

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._load_config()
        return cls._instance

    def _find_config_file(self) -> Path | None:
        """Find config.yaml in current directory or parent directories."""
        current = Path.cwd()
        for path in [current] + list(current.parents):
            config_path = path / "config.yaml"
            if config_path.exists():
                return config_path
        return None

    def _load_config(self) -> None:
        """Load configuration from file and environment."""
        config_path = self._find_config_file()

        if config_path:
            with open(config_path) as f:
                self._config = yaml.safe_load(f) or {}
        else:
            self._config = {}

        # Apply environment variable overrides
        self._apply_env_overrides()

    def _apply_env_overrides(self) -> None:
        """Override config values with environment variables."""
        env_mappings = {
            "FRAUDIT_DB_HOST": ("database", "host"),
            "FRAUDIT_DB_PORT": ("database", "port"),
            "FRAUDIT_DB_NAME": ("database", "name"),
            "FRAUDIT_DB_USER": ("database", "user"),
            "FRAUDIT_DB_PASSWORD": ("database", "password"),
            "FRAUDIT_SOCRATA_TOKEN": ("api_keys", "socrata"),
            "FRAUDIT_WEB_HOST": ("web", "host"),
            "FRAUDIT_WEB_PORT": ("web", "port"),
            "FRAUDIT_DATA_DIR": ("data", "data_dir"),
        }

        for env_var, path in env_mappings.items():
            value = os.environ.get(env_var)
            if value is not None:
                self._set_nested(path, value)

    def _set_nested(self, path: tuple, value: Any) -> None:
        """Set a nested config value."""
        current = self._config
        for key in path[:-1]:
            if key not in current:
                current[key] = {}
            current = current[key]

        # Type conversion for known integer fields
        if path[-1] == "port":
            value = int(value)

        current[path[-1]] = value

    def _get_nested(self, path: tuple, default: Any = None) -> Any:
        """Get a nested config value."""
        current = self._config
        for key in path:
            if not isinstance(current, dict) or key not in current:
                return default
            current = current[key]
        return current

    @property
    def database_url(self) -> str:
        """Get SQLAlchemy database URL."""
        db = self._config.get("database", {})
        host = db.get("host", "localhost")
        port = db.get("port", 5432)
        name = db.get("name", "fraudit")
        user = db.get("user", "postgres")
        password = db.get("password", "")

        # Handle Unix socket paths (start with /)
        if host.startswith("/"):
            # Socket connection: postgresql://user@/dbname?host=/path/to/socket
            if password:
                return f"postgresql://{user}:{password}@/{name}?host={host}"
            return f"postgresql://{user}@/{name}?host={host}"

        # TCP connection
        if password:
            return f"postgresql://{user}:{password}@{host}:{port}/{name}"
        return f"postgresql://{user}@{host}:{port}/{name}"

    @property
    def socrata_token(self) -> str | None:
        """Get Socrata API token."""
        return self._get_nested(("api_keys", "socrata"))

    @property
    def sync_interval_hours(self) -> int:
        """Get sync interval in hours."""
        return self._get_nested(("sync", "interval_hours"), 6)

    @property
    def sync_sources(self) -> list[str]:
        """Get list of sync sources."""
        return self._get_nested(("sync", "sources"), [])

    @property
    def detection_thresholds(self) -> dict:
        """Get fraud detection thresholds."""
        return self._get_nested(("detection", "thresholds"), {})

    @property
    def web_host(self) -> str:
        """Get web server host."""
        return self._get_nested(("web", "host"), "127.0.0.1")

    @property
    def web_port(self) -> int:
        """Get web server port."""
        return self._get_nested(("web", "port"), 5000)

    @property
    def web_debug(self) -> bool:
        """Get web server debug mode."""
        return self._get_nested(("web", "debug"), False)

    @property
    def data_dir(self) -> Path:
        """Get data directory path."""
        dir_path = self._get_nested(("data", "data_dir"), "./data")
        path = Path(dir_path)
        path.mkdir(parents=True, exist_ok=True)
        return path

    @property
    def start_fiscal_year(self) -> int | None:
        """Get starting fiscal year for data sync."""
        return self._get_nested(("data", "start_fiscal_year"))

    def reload(self) -> None:
        """Reload configuration from file."""
        self._load_config()

    def get(self, *path: str, default: Any = None) -> Any:
        """Get a config value by path."""
        return self._get_nested(path, default)


# Global config instance
config = Config()
