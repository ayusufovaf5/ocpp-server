from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class PendingRemoteStart:
    id_tag: str
    transaction_id: int


class PendingRemoteStarts:
    def __init__(self) -> None:
        self._items: dict[tuple[str, int], PendingRemoteStart] = {}

    def put(
        self,
        charge_point_id: str,
        connector_id: int,
        *,
        id_tag: str,
        transaction_id: int,
    ) -> None:
        self._items[(charge_point_id, connector_id)] = PendingRemoteStart(
            id_tag=id_tag,
            transaction_id=transaction_id,
        )

    def peek(self, charge_point_id: str, connector_id: int) -> PendingRemoteStart | None:
        return self._items.get((charge_point_id, connector_id))

    def take(self, charge_point_id: str, connector_id: int) -> PendingRemoteStart | None:
        return self._items.pop((charge_point_id, connector_id), None)

    def clear(self) -> None:
        self._items.clear()


_pending = PendingRemoteStarts()


def get_pending_remote_starts() -> PendingRemoteStarts:
    return _pending


def set_pending_remote_starts(registry: PendingRemoteStarts | None) -> None:
    global _pending
    _pending = registry if registry is not None else PendingRemoteStarts()
