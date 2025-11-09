import os

def _str_to_bool(value: str | None, default: bool = False) -> bool:
    if value is None:
        return default
    normalized = value.strip().lower()
    return normalized in {"1", "true", "yes", "on"}


INIT_DB = _str_to_bool(os.getenv("INIT_DB"))
