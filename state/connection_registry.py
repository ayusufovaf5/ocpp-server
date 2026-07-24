from __future__ import annotations

from fastapi import WebSocket


class ConnectionRegistry:
    def __init__(self) -> None:
        self._connections: dict[str, WebSocket] = {}

    def register(self, charge_point_id: str, websocket: WebSocket) -> None:
        self._connections[charge_point_id] = websocket

    def unregister(self, charge_point_id: str, websocket: WebSocket | None = None) -> None:
        current = self._connections.get(charge_point_id)
        if current is None:
            return
        if websocket is not None and current is not websocket:
            return
        del self._connections[charge_point_id]

    def get(self, charge_point_id: str) -> WebSocket | None:
        return self._connections.get(charge_point_id)

    def is_connected(self, charge_point_id: str) -> bool:
        return charge_point_id in self._connections


_registry: ConnectionRegistry | None = None


def get_connection_registry() -> ConnectionRegistry:
    global _registry
    if _registry is None:
        _registry = ConnectionRegistry()
    return _registry


def set_connection_registry(registry: ConnectionRegistry | None) -> None:
    global _registry
    _registry = registry
