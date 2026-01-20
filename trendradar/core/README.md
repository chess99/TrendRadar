# Core logic for config, data IO, analysis helpers.
# Inputs: config files, storage managers, frequency words.
# Outputs: normalized data, stats, and detection results. Update on file changes.

- `__init__.py`: Core exports and public API surface.
- `analyzer.py`: Frequency stats and ranking aggregation helpers.
- `config.py`: Multi-account config parsing and validation helpers.
- `data.py`: Load/save titles and detect new titles from storage.
- `frequency.py`: Keyword matching and frequency word loading.
- `loader.py`: Load config from YAML + env overrides.
