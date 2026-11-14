"""The language the library prefers text in, shared by GTFS-RT and GBFS."""

from __future__ import annotations

from .const import GBFS_LANGUAGE_PREFERENCE


def is_preferred_language(tag: object) -> bool:
    """Return True if a language tag names the preferred language.

    BCP 47 tags ignore case, and a tag names its language by its primary
    subtag ("en-US" is English); "en_US", common though not BCP 47, reads
    the same way.
    """
    return (
        isinstance(tag, str)
        and tag.replace("_", "-").split("-")[0].casefold() == GBFS_LANGUAGE_PREFERENCE
    )
