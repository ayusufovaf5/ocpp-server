class UnknownChargerError(Exception):
    def __init__(self, charge_point_id: str) -> None:
        self.charge_point_id = charge_point_id
        super().__init__(f"Unknown charge point: {charge_point_id}")


class UnsupportedActionError(Exception):
    def __init__(self, action: str) -> None:
        self.action = action
        super().__init__(f"Unsupported action: {action}")
