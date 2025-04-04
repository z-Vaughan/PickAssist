from ._constants import MIDWAY_COOKIE_FILENAME


class MidwayUnauthenticatedError(Exception):
    """
    Exception thrown when Midway authentication fails.
    """

    def __init__(self):
        message_1: str = (
            f"Midway cookies from {MIDWAY_COOKIE_FILENAME} either not found, invalid, or expired."
        )
        message_2: str = "Try running `mwinit -o` to get new cookies."
        super().__init__(f"{message_1} {message_2}")
