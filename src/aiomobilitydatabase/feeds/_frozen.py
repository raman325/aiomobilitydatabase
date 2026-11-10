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

    Like ``object.__setattr__`` on a frozen dataclass, calling ``dict``'s
    own methods directly (``dict.__setitem__(fd, ...)``, ``fd.__init__``)
    still mutates one. That is a deliberate bypass, not a supported path.
    """

    __slots__ = ()

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

    def _refuse(self) -> NoReturn:
        raise TypeError(f"{type(self).__name__} is read-only")

    # Each mutator is spelled out rather than aliased to one function: the
    # lowest supported mypy rejects assigning a plain callable over dict's
    # overloaded methods, while a ``*args: Any, **kwargs: Any`` override is
    # compatible with every overload.
    def __setitem__(self, *args: Any, **kwargs: Any) -> NoReturn:
        """Refuse item assignment."""
        self._refuse()

    def __delitem__(self, *args: Any, **kwargs: Any) -> NoReturn:
        """Refuse item deletion."""
        self._refuse()

    def __ior__(self, *args: Any, **kwargs: Any) -> NoReturn:  # type: ignore[misc]
        """Refuse in-place merge (``|=``)."""
        self._refuse()

    def clear(self, *args: Any, **kwargs: Any) -> NoReturn:
        """Refuse clearing."""
        self._refuse()

    def pop(self, *args: Any, **kwargs: Any) -> NoReturn:
        """Refuse popping a key."""
        self._refuse()

    def popitem(self, *args: Any, **kwargs: Any) -> NoReturn:
        """Refuse popping an item."""
        self._refuse()

    def setdefault(self, *args: Any, **kwargs: Any) -> NoReturn:
        """Refuse inserting a default."""
        self._refuse()

    def update(self, *args: Any, **kwargs: Any) -> NoReturn:
        """Refuse merging in place."""
        self._refuse()
