import os
from pathlib import Path


def _load_env_file() -> None:
    env_path = Path(__file__).resolve().parent.parent / ".env"
    if not env_path.exists():
        return

    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        os.environ.setdefault(key, value)


_load_env_file()


def _to_bool(value: str, default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


class Settings:
    DATABASE_URL = os.getenv(
        "DATABASE_URL",
        "mysql+pymysql://root:himanshu_123@localhost:3306/excel_db",
    )

    UPLOAD_DIR = os.getenv("UPLOAD_DIR", "data/uploads")
    EXCEL_CHUNK_SIZE = int(os.getenv("EXCEL_CHUNK_SIZE", "500"))
    CLEANUP_EXPIRED_UPLOADS = _to_bool(
        os.getenv("CLEANUP_EXPIRED_UPLOADS", "true"), default=True
    )
    UPLOAD_FILE_RETENTION_HOURS = int(
        os.getenv("UPLOAD_FILE_RETENTION_HOURS", "72")
    )
    REQUIRED_COLUMNS = tuple(
        column.strip()
        for column in os.getenv("REQUIRED_COLUMNS", "name,email,age").split(",")
        if column.strip()
    )


settings = Settings()