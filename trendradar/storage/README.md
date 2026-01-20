# Storage backends and SQLite helpers.
# Inputs: local/remote storage configs, DB files, data models.
# Outputs: persisted news/RSS data and query helpers. Update on file changes.

- `__init__.py`: Storage exports and factory helpers.
- `base.py`: Storage abstractions and data models.
- `local.py`: Local SQLite backend implementation.
- `manager.py`: Storage manager and backend routing.
- `remote.py`: Remote S3-compatible backend implementation.
- `rss_schema.sql`: SQLite schema for RSS storage.
- `schema.sql`: SQLite schema for news storage.
- `sqlite_mixin.py`: Shared SQLite read/write logic and queries.
