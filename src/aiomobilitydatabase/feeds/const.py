"""Constants for the feeds subpackage."""

DEFAULT_TIMEOUT_SECONDS = 30.0
STATIC_DB_FILENAME = "static.db"
GBFS_LANGUAGE_PREFERENCE = "en"
# The production catalog's public export, the only one kept current (QA's
# hasn't been regenerated since April 2025; DEV's answers 403).
PROD_PUBLIC_CATALOG_URL = "https://files.mobilitydatabase.org/feeds_v2.csv"
