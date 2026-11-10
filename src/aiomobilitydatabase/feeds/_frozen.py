"""A read-only, hashable ``dict`` for mapping fields on frozen models."""

from __future__ import annotations

from typing import Any, NoReturn, Self


class FrozenDict[K, V](dict[K, V]):
    """A ``dict`` that refuses mutation, and hashes and pickles like a value.

    Mapping fields on frozen models (``GbfsStation.rental_uris``,
    ``GbfsStation.vehicle_types_available``, ``GbfsVehicle.rental_uris``)
    hold one of these. It subclasses ``dict`` rather than wrapping one
    (``types.MappingProxyType``) because consumers serialize models:
    ``dataclasses.asdict`` rebuilds ``dict`` instances itself but sends any
    other mapping through ``copy.deepcopy``, which can't pickle a
    ``mappingproxy``; and JSON encoders (``json``, ``orjson``) encode
    ``dict`` subclasses natively. Equality is plain ``dict`` equality, so a
    ``FrozenDict`` still equals the ``dict`` literal with the same items.
    """

    def __hash__(self) -> int:  # type: ignore[override]
        """Hash the items, so models holding one stay hashable."""
        return hash(frozenset(self.items()))

    def __reduce__(self) -> tuple[type[Self], tuple[dict[K, V]]]:
        """Rebuild through the constructor.

        ``dict``'s own pickling restores items with ``__setitem__``, which
        this class refuses; copying, deep-copying and pickling all route
        through here instead.
        """
        return (type(self), (dict(self),))

    def _readonly(self, *args: Any, **kwargs: Any) -> NoReturn:
        raise TypeError(f"{type(self).__name__} is read-only")

    __setitem__ = _readonly
    __delitem__ = _readonly
    __ior__ = _readonly
    clear = _readonly
    pop = _readonly
    popitem = _readonly
    setdefault = _readonly
    update = _readonly
