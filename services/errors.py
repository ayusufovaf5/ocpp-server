class UnknownChargerError(Exception):
    def __init__(self, charge_point_id: str) -> None:
        self.charge_point_id = charge_point_id
        super().__init__(f"Unknown charge point: {charge_point_id}")


class UnsupportedActionError(Exception):
    def __init__(self, action: str) -> None:
        self.action = action
        super().__init__(f"Unsupported action: {action}")


class MissingOcppTransactionIdError(Exception):
    def __init__(self, session_id: int) -> None:
        self.session_id = session_id
        super().__init__(f"Session {session_id} has no ocpp_transaction_id")


class ChargerOfflineError(Exception):
    def __init__(self, charge_point_id: str) -> None:
        self.charge_point_id = charge_point_id
        super().__init__(f"Charge point is offline: {charge_point_id}")


class ChargerTimeoutError(Exception):
    def __init__(self, charge_point_id: str, action: str, timeout_seconds: float) -> None:
        self.charge_point_id = charge_point_id
        self.action = action
        self.timeout_seconds = timeout_seconds
        super().__init__(
            f"Outbound OCPP call timed out after {timeout_seconds}s: {charge_point_id} {action}"
        )


class ChargerCallError(Exception):
    def __init__(
        self,
        *,
        error_code: str,
        error_description: str = "",
        error_details: dict | None = None,
    ) -> None:
        self.error_code = error_code
        self.error_description = error_description
        self.error_details = error_details or {}
        super().__init__(f"{error_code}: {error_description}")
